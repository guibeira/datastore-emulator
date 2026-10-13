use crate::google::datastore::import_export::datastore_v3::{
    EntityProto, Property, Reference, property_value::ReferenceValue,
};
use crate::google::datastore::v1::key::PathElement;
use tracing;

use crate::google::datastore::import_export::dsbackups::ExportMetadata;
use crate::google::datastore::import_export::dsbackups::OverallExportMetadata;
use crate::google::datastore::v1::filter::FilterType;
use crate::google::datastore::v1::key::path_element::IdType;
use crate::google::datastore::v1::query_result_batch::MoreResultsType;
use crate::google::datastore::v1::value::ValueType;
use crate::google::datastore::v1::{
    ArrayValue, Filter, LatLng, PartitionId, Projection, PropertyOrder, PropertyReference, Value,
    property_order,
};
use chrono::DateTime;
use prost::Message;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::{Arc, atomic::AtomicI64};

use crate::google::datastore::v1::{Entity, EntityResult, Key};
use crate::leveldb::LogReader; // Added to resolve error E0433
use bincode;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error, ser::SerializeStruct};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write}; // For file I/O
use std::time::SystemTime; // Importing LogReader to read log files

const GRPC_MAX_MESSAGE_SIZE_BYTES: usize = 4 * 1024 * 1024; // 4 MiB
const MAX_ENTITIES_PAYLOAD_BYTES: usize = (GRPC_MAX_MESSAGE_SIZE_BYTES as f64 * 0.8) as usize; // 80% of gRPC max message size
const METADATA_KINDS: [&str; 3] = ["__kind__", "__namespace__", "__property__"];

fn get_representation_for_value(value: &Value) -> Vec<&'static str> {
    let mut representations = Vec::new();
    if let Some(value_type) = &value.value_type {
        match value_type {
            ValueType::NullValue(_) => representations.push("NULL"),
            ValueType::BooleanValue(_) => representations.push("BOOLEAN"),
            ValueType::IntegerValue(_) => representations.push("INT64"),
            ValueType::DoubleValue(_) => representations.push("DOUBLE"),
            ValueType::TimestampValue(_) => representations.push("INT64"),
            ValueType::KeyValue(_) => representations.push("REFERENCE"),
            ValueType::StringValue(_) => representations.push("STRING"),
            ValueType::BlobValue(_) => representations.push("STRING"),
            ValueType::GeoPointValue(_) => representations.push("POINT"),
            ValueType::EntityValue(_) => representations.push("STRING"),
            ValueType::ArrayValue(array_value) => {
                for v in &array_value.values {
                    representations.extend(get_representation_for_value(v));
                }
            }
        }
    }
    representations
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    // Datastore canonical cross-type ordering:
    // null < integer/timestamp (compared numerically) < boolean < string <
    // blob < double < geopoint < key < entity < array.
    fn type_rank(v: &Value) -> u8 {
        match &v.value_type {
            None => 0,
            Some(ValueType::NullValue(_)) => 1,
            Some(ValueType::IntegerValue(_)) | Some(ValueType::TimestampValue(_)) => 2,
            Some(ValueType::BooleanValue(_)) => 3,
            Some(ValueType::StringValue(_)) => 4,
            Some(ValueType::BlobValue(_)) => 5,
            Some(ValueType::DoubleValue(_)) => 6,
            Some(ValueType::GeoPointValue(_)) => 7,
            Some(ValueType::KeyValue(_)) => 8,
            Some(ValueType::EntityValue(_)) => 9,
            Some(ValueType::ArrayValue(_)) => 10,
        }
    }

    fn timestamp_micros(ts: &pbjson_types::Timestamp) -> i128 {
        ts.seconds as i128 * 1_000_000 + ts.nanos as i128 / 1_000
    }

    let rank_ord = type_rank(a).cmp(&type_rank(b));
    if rank_ord != Ordering::Equal {
        return rank_ord;
    }

    match (&a.value_type, &b.value_type) {
        (Some(ValueType::NullValue(_)), Some(ValueType::NullValue(_))) => Ordering::Equal,
        // Integers and timestamps share rank 2 and compare numerically (microseconds).
        (Some(ValueType::IntegerValue(av)), Some(ValueType::IntegerValue(bv))) => av.cmp(bv),
        (Some(ValueType::IntegerValue(av)), Some(ValueType::TimestampValue(bv))) => {
            (*av as i128).cmp(&timestamp_micros(bv))
        }
        (Some(ValueType::TimestampValue(av)), Some(ValueType::IntegerValue(bv))) => {
            timestamp_micros(av).cmp(&(*bv as i128))
        }
        (Some(ValueType::TimestampValue(av)), Some(ValueType::TimestampValue(bv))) => {
            timestamp_micros(av).cmp(&timestamp_micros(bv))
        }
        (Some(ValueType::DoubleValue(av)), Some(ValueType::DoubleValue(bv))) => {
            av.partial_cmp(bv).unwrap_or(Ordering::Equal)
        }
        (Some(ValueType::BooleanValue(av)), Some(ValueType::BooleanValue(bv))) => av.cmp(bv),
        (Some(ValueType::StringValue(av)), Some(ValueType::StringValue(bv))) => av.cmp(bv),
        (Some(ValueType::BlobValue(av)), Some(ValueType::BlobValue(bv))) => av.cmp(bv),
        (Some(ValueType::GeoPointValue(av)), Some(ValueType::GeoPointValue(bv))) => av
            .latitude
            .partial_cmp(&bv.latitude)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                av.longitude
                    .partial_cmp(&bv.longitude)
                    .unwrap_or(Ordering::Equal)
            }),
        (Some(ValueType::KeyValue(av)), Some(ValueType::KeyValue(bv))) => compare_keys(av, bv),
        // Same-rank but unsupported deeper compare (e.g. entity/array): treat as equal.
        _ => Ordering::Equal,
    }
}

fn sort_value(value: &Value, descending: bool) -> Option<&Value> {
    if value.exclude_from_indexes {
        return None;
    }

    match &value.value_type {
        Some(ValueType::ArrayValue(array)) => {
            let indexed_values = array.values.iter().filter(|v| !v.exclude_from_indexes);
            if descending {
                indexed_values.max_by(|a, b| compare_values(a, b))
            } else {
                indexed_values.min_by(|a, b| compare_values(a, b))
            }
        }
        _ => Some(value),
    }
}

/// Resolves a nested property path (e.g., "organizations.key.consistentId") against an entity's properties.
/// Returns all values found at the path (handles arrays at any level).
fn resolve_nested_property<'a>(
    properties: &'a HashMap<String, Value>,
    property_path: &str,
) -> Vec<&'a Value> {
    let parts: Vec<&str> = property_path.split('.').collect();
    if parts.is_empty() {
        return Vec::new();
    }

    resolve_property_path_recursive(properties, &parts)
}

/// Recursively resolve a property path through nested entities and arrays.
fn resolve_property_path_recursive<'a>(
    properties: &'a HashMap<String, Value>,
    path_parts: &[&str],
) -> Vec<&'a Value> {
    if path_parts.is_empty() {
        return Vec::new();
    }

    let current_part = path_parts[0];
    let remaining_parts = &path_parts[1..];

    // Get the value for the current property name
    let Some(value) = properties.get(current_part) else {
        return Vec::new();
    };

    // If this is the last part of the path, return the value(s)
    if remaining_parts.is_empty() {
        return match &value.value_type {
            Some(ValueType::ArrayValue(array)) => array.values.iter().collect(),
            _ => vec![value],
        };
    }

    // Otherwise, we need to traverse deeper
    match &value.value_type {
        Some(ValueType::EntityValue(entity)) => {
            // Single embedded entity - recurse into its properties
            resolve_property_path_recursive(&entity.properties, remaining_parts)
        }
        Some(ValueType::ArrayValue(array)) => {
            // Array of values - for each element that is an entity, recurse and collect results
            let mut results = Vec::with_capacity(array.values.len());
            for item in &array.values {
                if let Some(ValueType::EntityValue(entity)) = &item.value_type {
                    results.extend(resolve_property_path_recursive(
                        &entity.properties,
                        remaining_parts,
                    ));
                }
            }
            results
        }
        _ => {
            // Value is not an entity/array, can't traverse further
            Vec::new()
        }
    }
}

/// Property-index key for a scalar value. The type tag keeps values of
/// different types apart (int `1`, double `1.0` and string `"1"` must not
/// collide). Arrays and nulls are not indexed.
/// Property index keys are `kind SEP path SEP tagged value` bytes. The
/// separator is never part of UTF-8 text, so the parts cannot collide.
const INDEX_KEY_SEPARATOR: u8 = 0xFF;

pub type IndexKey = Vec<u8>;
pub type PropertyIndex = HashMap<IndexKey, BTreeSet<Arc<KeyStruct>>>;

fn push_index_key_prefix(buf: &mut Vec<u8>, kind: &str, path: &str) {
    buf.extend_from_slice(kind.as_bytes());
    buf.push(INDEX_KEY_SEPARATOR);
    buf.extend_from_slice(path.as_bytes());
    buf.push(INDEX_KEY_SEPARATOR);
}

fn push_len_prefixed(buf: &mut Vec<u8>, text: &str) {
    write!(buf, "{}:", text.len()).expect("write to a Vec");
    buf.extend_from_slice(text.as_bytes());
}

/// Appends the tagged index form of `value_type`. Returns false, with `buf`
/// untouched, for the types that are not indexed.
fn push_indexable_value(buf: &mut Vec<u8>, value_type: &ValueType) -> bool {
    match value_type {
        ValueType::StringValue(s) => {
            buf.extend_from_slice(b"s:");
            buf.extend_from_slice(s.as_bytes());
        }
        ValueType::IntegerValue(i) => write!(buf, "i:{i}").expect("write to a Vec"),
        ValueType::DoubleValue(d) => write!(buf, "d:{d}").expect("write to a Vec"),
        ValueType::BooleanValue(b) => write!(buf, "b:{b}").expect("write to a Vec"),
        ValueType::TimestampValue(t) => {
            if DateTime::from_timestamp(t.seconds, t.nanos as u32).is_none() {
                return false;
            }
            write!(buf, "t:{}:{}", t.seconds, t.nanos).expect("write to a Vec");
        }
        ValueType::KeyValue(key) => {
            buf.extend_from_slice(b"k:");
            let (project_id, database_id, namespace) =
                key.partition_id.as_ref().map_or(("", "", ""), |p| {
                    (
                        p.project_id.as_str(),
                        p.database_id.as_str(),
                        p.namespace_id.as_str(),
                    )
                });
            push_len_prefixed(buf, project_id);
            push_len_prefixed(buf, database_id);
            push_len_prefixed(buf, namespace);
            for element in &key.path {
                match &element.id_type {
                    Some(IdType::Id(id)) => {
                        push_len_prefixed(buf, &element.kind);
                        write!(buf, "#{id}").expect("write to a Vec");
                    }
                    Some(IdType::Name(name)) => {
                        push_len_prefixed(buf, &element.kind);
                        buf.push(b'=');
                        push_len_prefixed(buf, name);
                    }
                    None => {}
                }
            }
        }
        _ => return false,
    }
    true
}

/// Calls `visitor` with the index key of each indexed value in `value`,
/// array elements one by one. `buf` holds the key prefix and comes back
/// unchanged.
fn for_each_index_key_of_value(buf: &mut Vec<u8>, value: &Value, visitor: &mut impl FnMut(&[u8])) {
    let prefix_len = buf.len();
    let mut visit = |item: &Value| {
        if item.exclude_from_indexes {
            return;
        }
        if let Some(value_type) = item.value_type.as_ref()
            && push_indexable_value(buf, value_type)
        {
            visitor(&buf[..]);
            buf.truncate(prefix_len);
        }
    };
    match &value.value_type {
        Some(ValueType::ArrayValue(array)) => array.values.iter().for_each(visit),
        Some(_) => visit(value),
        None => {}
    }
}

/// Index keys of every indexed property in `properties`, nested entities
/// included. `buf` holds `kind SEP path-so-far`, the path starting at
/// `path_start`.
fn for_each_index_key_of_properties(
    buf: &mut Vec<u8>,
    path_start: usize,
    properties: &HashMap<String, Value>,
    visitor: &mut impl FnMut(&[u8]),
) {
    for (name, value) in properties {
        let path_len = buf.len();
        if path_len > path_start {
            buf.push(b'.');
        }
        buf.extend_from_slice(name.as_bytes());
        for_each_index_key_of_property(buf, path_start, value, visitor);
        buf.truncate(path_len);
    }
}

/// Index keys of one property value and of the entities nested in it.
/// `buf` holds `kind SEP path` of the property.
fn for_each_index_key_of_property(
    buf: &mut Vec<u8>,
    path_start: usize,
    value: &Value,
    visitor: &mut impl FnMut(&[u8]),
) {
    if value.exclude_from_indexes {
        return;
    }
    buf.push(INDEX_KEY_SEPARATOR);
    for_each_index_key_of_value(buf, value, visitor);
    buf.pop();

    match &value.value_type {
        Some(ValueType::EntityValue(entity)) => {
            for_each_index_key_of_properties(buf, path_start, &entity.properties, visitor);
        }
        Some(ValueType::ArrayValue(array)) => {
            for item in &array.values {
                if item.exclude_from_indexes {
                    continue;
                }
                if let Some(ValueType::EntityValue(entity)) = &item.value_type {
                    for_each_index_key_of_properties(buf, path_start, &entity.properties, visitor);
                }
            }
        }
        _ => {}
    }
}

/// Calls `visitor` with every index key of `entity`, an entity of `kind`.
fn for_each_index_key_of_entity(kind: &str, entity: &Entity, visitor: &mut impl FnMut(&[u8])) {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(kind.as_bytes());
    buf.push(INDEX_KEY_SEPARATOR);
    let path_start = buf.len();
    for_each_index_key_of_properties(&mut buf, path_start, &entity.properties, visitor);
}

/// Calls `visitor` with every index key of the property `name` of an
/// entity of `kind`.
fn for_each_index_key_of_named_property(
    kind: &str,
    name: &str,
    value: &Value,
    visitor: &mut impl FnMut(&[u8]),
) {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(kind.as_bytes());
    buf.push(INDEX_KEY_SEPARATOR);
    let path_start = buf.len();
    buf.extend_from_slice(name.as_bytes());
    for_each_index_key_of_property(&mut buf, path_start, value, visitor);
}

/// The index keys an equality filter on `value` looks up for the property
/// `path` of `kind`: one per indexed value, array elements one by one.
pub fn index_keys_for_value(kind: &str, path: &str, value: &Value) -> Vec<IndexKey> {
    let mut buf = Vec::with_capacity(64);
    push_index_key_prefix(&mut buf, kind, path);
    let mut keys = Vec::new();
    for_each_index_key_of_value(&mut buf, value, &mut |key| keys.push(key.to_vec()));
    keys
}

fn insert_index_entry(indexes: &mut PropertyIndex, index_key: &[u8], key: &Arc<KeyStruct>) {
    match indexes.get_mut(index_key) {
        Some(keys) => {
            keys.insert(Arc::clone(key));
        }
        None => {
            indexes.insert(index_key.to_vec(), BTreeSet::from([Arc::clone(key)]));
        }
    }
}

fn remove_index_entry(indexes: &mut PropertyIndex, index_key: &[u8], key: &KeyStruct) {
    if let Some(keys) = indexes.get_mut(index_key) {
        keys.remove(key);
        if keys.is_empty() {
            indexes.remove(index_key);
        }
    }
}

/// A `__key__` filter with its target keys, matched with `compare_keys` so no
/// `KeyStruct` is built per candidate.
enum PreparedKeyFilter {
    Compare(i32, Key),
    In(Vec<Key>),
    NotIn(Vec<Key>),
}

fn try_prepare_key_filter(filter: &FilterType) -> Option<PreparedKeyFilter> {
    let FilterType::PropertyFilter(pf) = filter else {
        return None;
    };
    if pf.op == 11 {
        return None;
    }
    let property = pf.property.as_ref()?;
    if property.name != "__key__" {
        return None;
    }
    let value = pf.value.as_ref()?;

    let key_values = |arr: &ArrayValue| -> Vec<Key> {
        arr.values
            .iter()
            .filter_map(|v| match &v.value_type {
                Some(ValueType::KeyValue(k)) => Some(k.clone()),
                _ => None,
            })
            .collect()
    };
    match (pf.op, &value.value_type) {
        (op @ (1 | 2 | 3 | 4 | 5 | 9), Some(ValueType::KeyValue(k))) => {
            Some(PreparedKeyFilter::Compare(op, k.clone()))
        }
        (6, Some(ValueType::ArrayValue(arr))) => Some(PreparedKeyFilter::In(key_values(arr))),
        (13, Some(ValueType::ArrayValue(arr))) => Some(PreparedKeyFilter::NotIn(key_values(arr))),
        _ => None,
    }
}

fn index_lookup_covers_filter(filter: &FilterType) -> bool {
    match filter {
        FilterType::PropertyFilter(property_filter) => {
            let Some(property) = property_filter.property.as_ref() else {
                return false;
            };
            let Some(filter_value) = property_filter.value.as_ref() else {
                return false;
            };

            if property.name == "__key__" {
                return matches!(property_filter.op, 5 | 6 | 11);
            }

            match property_filter.op {
                5 => !matches!(filter_value.value_type, Some(ValueType::ArrayValue(_))),
                6 => matches!(filter_value.value_type, Some(ValueType::ArrayValue(_))),
                _ => false,
            }
        }
        FilterType::CompositeFilter(composite_filter) => {
            if !matches!(composite_filter.op, 1 | 2) {
                return false;
            }
            composite_filter.filters.iter().all(|filter| {
                filter
                    .filter_type
                    .as_ref()
                    .is_some_and(index_lookup_covers_filter)
            })
        }
    }
}

fn match_prepared_key_filter(entity_key: &Key, kind: &PreparedKeyFilter) -> bool {
    match kind {
        PreparedKeyFilter::Compare(op, target) => {
            let ord = compare_keys(entity_key, target);
            match op {
                1 => ord.is_lt(),
                2 => ord.is_le(),
                3 => ord.is_gt(),
                4 => ord.is_ge(),
                5 => ord.is_eq(),
                9 => !ord.is_eq(),
                _ => false,
            }
        }
        PreparedKeyFilter::In(keys) => keys.iter().any(|k| compare_keys(k, entity_key).is_eq()),
        PreparedKeyFilter::NotIn(keys) => !keys.iter().any(|k| compare_keys(k, entity_key).is_eq()),
    }
}

fn match_key_against_filter(entity_key: &Key, filter_value: &Value, op: i32) -> bool {
    let compare_with = |other: &Key| compare_keys(entity_key, other);

    match op {
        0 => true,
        1 | 2 | 3 | 4 | 5 | 9 => {
            let Some(ValueType::KeyValue(filter_key)) = &filter_value.value_type else {
                return false;
            };
            let ord = compare_with(filter_key);
            match op {
                1 => ord.is_lt(),
                2 => ord.is_le(),
                3 => ord.is_gt(),
                4 => ord.is_ge(),
                5 => ord.is_eq(),
                9 => !ord.is_eq(),
                _ => false,
            }
        }
        6 => {
            let Some(ValueType::ArrayValue(arr)) = &filter_value.value_type else {
                return false;
            };
            arr.values.iter().any(|v| match &v.value_type {
                Some(ValueType::KeyValue(k)) => compare_with(k).is_eq(),
                _ => false,
            })
        }
        13 => {
            let Some(ValueType::ArrayValue(arr)) = &filter_value.value_type else {
                return true;
            };
            !arr.values.iter().any(|v| match &v.value_type {
                Some(ValueType::KeyValue(k)) => compare_with(k).is_eq(),
                _ => false,
            })
        }
        _ => false,
    }
}

fn values_match_filter<'a>(
    entity_values: impl IntoIterator<Item = &'a Value>,
    filter_value: &Value,
    op: i32,
) -> bool {
    let mut entity_values = entity_values.into_iter();

    match op {
        0 => entity_values.next().is_some(), // OPERATOR_UNSPECIFIED
        1 => entity_values.any(|v| compare_values(v, filter_value).is_lt()),
        2 => entity_values.any(|v| compare_values(v, filter_value).is_le()),
        3 => entity_values.any(|v| compare_values(v, filter_value).is_gt()),
        4 => entity_values.any(|v| compare_values(v, filter_value).is_ge()),
        5 => entity_values.any(|v| v.value_type == filter_value.value_type),
        6 => {
            if let Some(ValueType::ArrayValue(array_value)) = &filter_value.value_type {
                entity_values.any(|v| {
                    array_value
                        .values
                        .iter()
                        .any(|candidate| candidate.value_type == v.value_type)
                })
            } else {
                false
            }
        }
        9 => entity_values.any(|v| v.value_type != filter_value.value_type),
        13 => {
            if let Some(ValueType::ArrayValue(array_value)) = &filter_value.value_type {
                entity_values.any(|v| {
                    !array_value
                        .values
                        .iter()
                        .any(|candidate| candidate.value_type == v.value_type)
                })
            } else {
                entity_values.next().is_some()
            }
        }
        _ => false,
    }
}

fn convert_legacy_bytes_value(meaning: Option<i32>, bytes: &[u8]) -> ValueType {
    match meaning {
        // Legacy BLOB and BYTESTRING values are stored in stringValue in
        // Datastore export protos, but the bytes are not guaranteed to be UTF-8.
        Some(14 | 16) => ValueType::BlobValue(bytes.to_vec()),
        _ => match String::from_utf8(bytes.to_vec()) {
            Ok(value) => ValueType::StringValue(value),
            Err(_) => ValueType::BlobValue(bytes.to_vec()),
        },
    }
}

fn convert_property_value(prop: &Property) -> Option<ValueType> {
    let prop_val = &prop.value;
    if let Some(v) = prop_val.int64_value {
        return Some(ValueType::IntegerValue(v));
    }
    if let Some(v) = prop_val.boolean_value {
        return Some(ValueType::BooleanValue(v));
    }
    if let Some(v) = &prop_val.string_value {
        return Some(convert_legacy_bytes_value(prop.meaning, v));
    }
    if let Some(v) = prop_val.double_value {
        return Some(ValueType::DoubleValue(v));
    }
    if let Some(v) = &prop_val.pointvalue {
        return Some(ValueType::GeoPointValue(LatLng {
            latitude: v.x,
            longitude: v.y,
        }));
    }
    if let Some(v) = &prop_val.referencevalue {
        let key = convert_reference_value_to_key(v);
        return Some(ValueType::KeyValue(key));
    }
    // Note: list_value and user_value are not directly handled here as they are
    // part of the Property structure itself or deprecated.
    None
}

fn convert_reference_value_to_key(reference_value: &ReferenceValue) -> Key {
    let path = reference_value
        .pathelement
        .iter()
        .map(|el| {
            let id_type = if let Some(id) = el.id {
                Some(IdType::Id(id))
            } else {
                el.name.as_ref().map(|n| IdType::Name(n.clone()))
            };
            crate::google::datastore::v1::key::PathElement {
                kind: el.r#type.clone(),
                id_type,
            }
        })
        .collect();

    Key {
        partition_id: Some(PartitionId {
            project_id: normalize_app_id(&reference_value.app),
            namespace_id: reference_value.name_space.clone().unwrap_or_default(),
            database_id: "".to_string(),
        }),
        path,
    }
}

/// Legacy GAE app IDs are prefixed with a partition specifier (`s~`, `e~`,
/// `dev~`). Datastore v1 callers use the bare project_id, so strip the
/// prefix when present.
fn normalize_app_id(app: &str) -> String {
    if let Some(idx) = app.find('~') {
        app[idx + 1..].to_string()
    } else {
        app.to_string()
    }
}

fn convert_reference_to_key(reference: &Reference) -> Key {
    let path = reference
        .path
        .element
        .iter()
        .map(|el| {
            let id_type = if let Some(id) = el.id {
                Some(IdType::Id(id))
            } else {
                el.name.as_ref().map(|n| IdType::Name(n.clone()))
            };
            crate::google::datastore::v1::key::PathElement {
                kind: el.r#type.clone(),
                id_type,
            }
        })
        .collect();

    Key {
        partition_id: Some(PartitionId {
            project_id: normalize_app_id(&reference.app),
            namespace_id: reference.name_space.clone().unwrap_or_default(),
            database_id: "".to_string(),
        }),
        path,
    }
}

pub fn converter_dump(dump_entities: Vec<EntityProto>) -> Vec<EntityWithMetadata> {
    dump_entities
        .into_par_iter()
        .map(|entity_proto| {
            let v1_key = Some(convert_reference_to_key(&entity_proto.key));

            // Group properties by name to handle multi-valued properties
            let mut grouped_props: HashMap<String, (Vec<Value>, bool)> = HashMap::new();

            for prop in &entity_proto.property {
                if let Some(v1_value_type) = convert_property_value(prop) {
                    let entry = grouped_props
                        .entry(prop.name.clone())
                        .or_insert((vec![], false));
                    entry.0.push(Value {
                        value_type: Some(v1_value_type),
                        ..Default::default()
                    });
                }
            }

            for prop in &entity_proto.raw_property {
                if let Some(v1_value_type) = convert_property_value(prop) {
                    let entry = grouped_props
                        .entry(prop.name.clone())
                        .or_insert((vec![], true));
                    entry.0.push(Value {
                        value_type: Some(v1_value_type),
                        ..Default::default()
                    });
                    entry.1 = true; // Mark as exclude_from_indexes
                }
            }

            let mut properties: HashMap<String, Value> = HashMap::new();
            for (name, (mut values, exclude)) in grouped_props {
                if values.len() > 1 {
                    // It's an array
                    properties.insert(
                        name,
                        Value {
                            value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
                            exclude_from_indexes: exclude,
                            ..Default::default()
                        },
                    );
                } else if let Some(mut single_value) = values.pop() {
                    // It's a single value
                    single_value.exclude_from_indexes = exclude;
                    properties.insert(name, single_value);
                }
            }

            let v1_entity = Entity {
                key: v1_key,
                properties,
            };

            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            let timestamp_now = pbjson_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: (now.as_nanos() % 1_000_000_000) as i32,
            };

            EntityWithMetadata {
                entity: v1_entity,
                version: 1,
                create_time: timestamp_now.clone(),
                update_time: timestamp_now,
            }
        })
        .collect()
}

fn rewrite_key_project_id(key: &mut Key, project_id: &str) {
    if let Some(partition) = key.partition_id.as_mut() {
        partition.project_id = project_id.to_string();
    }
}

fn rewrite_value_project_id(value: &mut Value, project_id: &str) {
    match value.value_type.as_mut() {
        Some(ValueType::KeyValue(key)) => rewrite_key_project_id(key, project_id),
        Some(ValueType::ArrayValue(array)) => {
            for value in array.values.iter_mut() {
                rewrite_value_project_id(value, project_id);
            }
        }
        Some(ValueType::EntityValue(entity)) => rewrite_entity_project_id(entity, project_id),
        _ => {}
    }
}

fn rewrite_entity_project_id(entity: &mut Entity, project_id: &str) {
    if let Some(key) = entity.key.as_mut() {
        rewrite_key_project_id(key, project_id);
    }
    for value in entity.properties.values_mut() {
        rewrite_value_project_id(value, project_id);
    }
}

type ImportReadResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn validate_export_relative_path(
    raw_path: &str,
    description: &str,
) -> ImportReadResult<std::path::PathBuf> {
    if raw_path.is_empty() {
        return Err(format!("{} path is empty", description).into());
    }
    if raw_path.starts_with('/') || raw_path.starts_with('\\') || raw_path.contains('\\') {
        return Err(format!(
            "{} path must be a relative slash-separated path: {}",
            description, raw_path
        )
        .into());
    }

    let path = std::path::Path::new(raw_path);
    if path.is_absolute()
        || !path
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return Err(format!(
            "{} path must not contain root, current, or parent components: {}",
            description, raw_path
        )
        .into());
    }

    Ok(path.to_path_buf())
}

pub fn read_overall_metadata(export_dir: &str) -> ImportReadResult<OverallExportMetadata> {
    let mut metadata_file = None;
    let path_to_search = std::path::Path::new(export_dir).join("exports");
    for entry in std::fs::read_dir(&path_to_search)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".overall_export_metadata")
        {
            metadata_file = Some(entry.path());
            break;
        }
    }

    let metadata_file = metadata_file.ok_or_else(|| {
        format!(
            "Could not find .overall_export_metadata file in {}",
            path_to_search.display()
        )
    })?;
    tracing::debug!("Found metadata file: {}", metadata_file.display());

    let reader = LogReader::new(&metadata_file)?;
    let mut records_read = 0usize;
    let mut empty_metadata = None;
    let mut last_decode_error = None;
    for record_result in reader {
        records_read += 1;
        let record = record_result?;
        match OverallExportMetadata::decode(&record[..]) {
            Ok(metadata) if !metadata.exports.is_empty() => return Ok(metadata),
            Ok(metadata) => {
                tracing::debug!(
                    "Skipping empty OverallExportMetadata record {} in {}",
                    records_read,
                    metadata_file.display()
                );
                empty_metadata.get_or_insert(metadata);
            }
            Err(e) => {
                tracing::debug!(
                    "Skipping non-metadata record {} in {}: {}",
                    records_read,
                    metadata_file.display(),
                    e
                );
                last_decode_error = Some(e);
            }
        }
    }

    if let Some(metadata) = empty_metadata {
        return Ok(metadata);
    }

    if records_read > 0 {
        let detail = last_decode_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "no decodable metadata records".to_string());
        return Err(format!(
            "Failed to decode OverallExportMetadata from {} after reading {} records: {}",
            metadata_file.display(),
            records_read,
            detail
        )
        .into());
    }

    Err(format!(
        "No records found in overall metadata file {}",
        metadata_file.display()
    )
    .into())
}

/// Read and convert the export at `path` without touching a storage, so the
/// caller holds the storage lock only while `insert_imported` runs.
pub fn prepare_dump_for_project(
    path: &str,
    target_project_id: Option<&str>,
) -> Result<Vec<EntityWithMetadata>, tonic::Status> {
    let dump_entities = read_dump(path).map_err(|e| {
        tonic::Status::invalid_argument(format!("Failed to read datastore export: {}", e))
    })?;
    let mut entities_with_metadata = converter_dump(dump_entities);
    if let Some(project_id) = target_project_id {
        for ewm in entities_with_metadata.iter_mut() {
            rewrite_entity_project_id(&mut ewm.entity, project_id);
        }
    }
    tracing::info!(
        "Importing {} entities from dump at {}",
        entities_with_metadata.len(),
        path
    );
    Ok(entities_with_metadata)
}

pub fn read_dump(export_dir: &str) -> ImportReadResult<Vec<EntityProto>> {
    tracing::info!("Reading datastore export from: {}", export_dir);
    let metadata = read_overall_metadata(export_dir)?;
    let mut entity_protos = Vec::new();
    let mut processed_metadata_files = 0usize;
    let mut skipped_metadata_files = 0usize;

    for export_entry in metadata.exports {
        let kind_name = export_entry
            .kind
            .as_ref()
            .map_or_else(String::new, |k| k.kind.clone());
        tracing::info!("Processing kind: {}", kind_name);
        let metadata_relative_path =
            validate_export_relative_path(&export_entry.path, "Export metadata")?;
        let metadata_path = std::path::PathBuf::from(export_dir)
            .join("exports")
            .join(metadata_relative_path);

        if !metadata_path.exists() {
            skipped_metadata_files += 1;
            tracing::warn!(
                "Skipping kind {} because its metadata file was not found at: {}",
                kind_name,
                metadata_path.display()
            );
            continue;
        }
        processed_metadata_files += 1;

        let data = std::fs::read(&metadata_path).map_err(|e| {
            format!(
                "Failed to read metadata file {}: {}",
                metadata_path.display(),
                e
            )
        })?;
        let export_metadata = ExportMetadata::decode(&data[..])
            .map_err(|e| format!("Failed to decode ExportMetadata for {}: {}", kind_name, e))?;

        let output_file_names = export_metadata
            .items
            .ok_or_else(|| format!("ExportMetadata for kind {} is missing items", kind_name))?
            .outputs;
        let parent_dir_for_output_files = metadata_path
            .parent()
            .ok_or_else(|| format!("Metadata path has no parent: {}", metadata_path.display()))?
            .to_path_buf();

        for output_file_name_str in output_file_names {
            let output_relative_path =
                validate_export_relative_path(&output_file_name_str, "Export output")?;
            let output_file_path = parent_dir_for_output_files.join(output_relative_path);
            if !output_file_path.exists() {
                return Err(
                    format!("Output file {} does not exist.", output_file_path.display()).into(),
                );
            }

            let reader = LogReader::new(&output_file_path)
                .map_err(|e| format!("Error opening file {}: {}", output_file_path.display(), e))?;
            for (i, record_result) in reader.enumerate() {
                let record = record_result.map_err(|e| {
                    format!(
                        "Error reading record {} from file {}: {}",
                        i + 1,
                        output_file_path.display(),
                        e
                    )
                })?;
                let entity_proto = EntityProto::decode(&record[..]).map_err(|e| {
                    format!(
                        "Failed to decode EntityProto from file {}: {}",
                        output_file_path.display(),
                        e
                    )
                })?;
                entity_protos.push(entity_proto);
            }
        }
    }

    if processed_metadata_files == 0 && skipped_metadata_files > 0 {
        return Err(format!(
            "No importable kind metadata files found in {}; skipped {} missing metadata files",
            export_dir, skipped_metadata_files
        )
        .into());
    }

    tracing::info!("Finished reading datastore export.");
    Ok(entity_protos)
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug,
)]
pub enum KeyId {
    IntId(i64),
    StringId(String),
}

// Custom key structure for efficient indexing
#[derive(
    serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug,
)]
pub struct KeyStruct {
    pub project_id: String,
    pub database_id: String,
    pub namespace: String,
    pub path_elements: Vec<(String, KeyId)>, // (kind, id/name)
}

impl KeyStruct {
    pub fn from_datastore_key(key: &Key) -> Self {
        let mut path_elements = Vec::new();
        for path_element in &key.path {
            let kind = path_element.kind.clone();
            let id_type = match &path_element.id_type {
                Some(IdType::Id(id)) => KeyId::IntId(*id),
                Some(IdType::Name(name)) => KeyId::StringId(name.clone()),
                None => continue,
            };
            path_elements.push((kind, id_type));
        }
        Self {
            project_id: key
                .partition_id
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.project_id.clone()),
            database_id: key
                .partition_id
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.database_id.clone()),
            namespace: key
                .partition_id
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.namespace_id.clone()),
            path_elements,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct EntityWithMetadata {
    pub entity: Entity,
    pub version: u64,
    pub create_time: pbjson_types::Timestamp,
    pub update_time: pbjson_types::Timestamp,
}

#[derive(Deserialize)]
struct SerializableEntityWithMetadata {
    entity_bytes: Vec<u8>,
    version: u64,
    create_time_bytes: Vec<u8>,
    update_time_bytes: Vec<u8>,
}

impl Serialize for EntityWithMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("EntityWithMetadata", 4)?;
        state.serialize_field("entity_bytes", &self.entity.encode_to_vec())?;
        state.serialize_field("version", &self.version)?;
        state.serialize_field("create_time_bytes", &self.create_time.encode_to_vec())?;
        state.serialize_field("update_time_bytes", &self.update_time.encode_to_vec())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for EntityWithMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = SerializableEntityWithMetadata::deserialize(deserializer)?;
        let entity = Entity::decode(&s.entity_bytes[..]).map_err(Error::custom)?;
        let create_time =
            pbjson_types::Timestamp::decode(&s.create_time_bytes[..]).map_err(Error::custom)?;
        let update_time =
            pbjson_types::Timestamp::decode(&s.update_time_bytes[..]).map_err(Error::custom)?;

        Ok(EntityWithMetadata {
            entity,
            version: s.version,
            create_time,
            update_time,
        })
    }
}

// Transaction state
#[derive(Debug)]
pub struct TransactionState;

/// A query result candidate. Keys are compared through the entity key with
/// `compare_keys`, so no per-candidate `KeyStruct` is built.
#[derive(Clone)]
pub(crate) struct QueryCandidate {
    pub(crate) entity_metadata: Arc<EntityWithMetadata>,
}

impl QueryCandidate {
    fn entity_key(&self) -> Option<&Key> {
        self.entity_metadata.entity.key.as_ref()
    }
}

/// Same order as `KeyStruct`'s `Ord`, without building a `KeyStruct`.
fn compare_keys(a: &Key, b: &Key) -> Ordering {
    fn partition(key: &Key) -> (&str, &str, &str) {
        key.partition_id.as_ref().map_or(("", "", ""), |p| {
            (
                p.project_id.as_str(),
                p.database_id.as_str(),
                p.namespace_id.as_str(),
            )
        })
    }
    // `Ok` (integer id) sorts before `Err` (name), like `KeyId`.
    fn path(key: &Key) -> impl Iterator<Item = (&str, Result<i64, &str>)> {
        key.path.iter().filter_map(|element| {
            let id = match element.id_type.as_ref()? {
                IdType::Id(id) => Ok(*id),
                IdType::Name(name) => Err(name.as_str()),
            };
            Some((element.kind.as_str(), id))
        })
    }

    partition(a)
        .cmp(&partition(b))
        .then_with(|| path(a).cmp(path(b)))
}

fn compare_candidate_keys(a: &QueryCandidate, b: &QueryCandidate) -> Ordering {
    match (a.entity_key(), b.entity_key()) {
        (Some(a), Some(b)) => compare_keys(a, b),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn compare_query_candidates(
    a: &QueryCandidate,
    b: &QueryCandidate,
    order: &[PropertyOrder],
) -> Ordering {
    let mut final_ordering = Ordering::Equal;
    for order_by in order {
        if final_ordering != Ordering::Equal {
            break;
        }

        let Some(property) = order_by.property.as_ref() else {
            continue;
        };
        let prop_name = &property.name;
        let descending = order_by.direction == property_order::Direction::Descending as i32;

        if prop_name == "__key__" {
            let ordering = compare_candidate_keys(a, b);

            final_ordering = if descending {
                ordering.reverse()
            } else {
                ordering
            };
            continue;
        }

        let a_val = a
            .entity_metadata
            .entity
            .properties
            .get(prop_name)
            .and_then(|v| sort_value(v, descending));
        let b_val = b
            .entity_metadata
            .entity
            .properties
            .get(prop_name)
            .and_then(|v| sort_value(v, descending));

        let ordering = match (a_val, b_val) {
            (Some(a), Some(b)) => compare_values(a, b),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };

        final_ordering = if descending {
            ordering.reverse()
        } else {
            ordering
        };
    }
    final_ordering
}

enum SortComponent {
    Key { descending: bool },
    Value { index: usize, descending: bool },
}

/// The sort values of every candidate, looked up once, so a comparison does
/// no property lookups. Orders by `order`, then by key, like
/// `compare_query_position`.
struct SortView<'a> {
    components: Vec<SortComponent>,
    value_count: usize,
    values: Vec<Option<&'a Value>>,
    candidates: &'a [QueryCandidate],
}

impl<'a> SortView<'a> {
    fn new(candidates: &'a [QueryCandidate], order: &[PropertyOrder]) -> Self {
        let mut components = Vec::with_capacity(order.len());
        let mut names: Vec<(&str, bool)> = Vec::new();
        for order_by in order {
            let Some(property) = order_by.property.as_ref() else {
                continue;
            };
            let descending = order_by.direction == property_order::Direction::Descending as i32;
            if property.name == "__key__" {
                components.push(SortComponent::Key { descending });
            } else {
                components.push(SortComponent::Value {
                    index: names.len(),
                    descending,
                });
                names.push((property.name.as_str(), descending));
            }
        }

        let mut values = Vec::with_capacity(candidates.len() * names.len());
        for candidate in candidates {
            let properties = &candidate.entity_metadata.entity.properties;
            values.extend(names.iter().map(|(name, descending)| {
                properties
                    .get(*name)
                    .and_then(|value| sort_value(value, *descending))
            }));
        }

        Self {
            components,
            value_count: names.len(),
            values,
            candidates,
        }
    }

    fn compare(&self, a: usize, b: usize) -> Ordering {
        for component in &self.components {
            let (ordering, descending) = match *component {
                SortComponent::Key { descending } => (
                    compare_candidate_keys(&self.candidates[a], &self.candidates[b]),
                    descending,
                ),
                SortComponent::Value { index, descending } => {
                    let a_value = self.values[a * self.value_count + index];
                    let b_value = self.values[b * self.value_count + index];
                    let ordering = match (a_value, b_value) {
                        (Some(a), Some(b)) => compare_values(a, b),
                        (Some(_), None) => Ordering::Greater,
                        (None, Some(_)) => Ordering::Less,
                        (None, None) => Ordering::Equal,
                    };
                    (ordering, descending)
                }
            };
            let ordering = if descending {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        compare_candidate_keys(&self.candidates[a], &self.candidates[b])
    }
}

/// The candidates at `indices`, in that order, moved out without cloning.
fn take_in_order(candidates: Vec<QueryCandidate>, indices: &[usize]) -> Vec<QueryCandidate> {
    let mut slots: Vec<Option<QueryCandidate>> = candidates.into_iter().map(Some).collect();
    indices
        .iter()
        .map(|&index| slots[index].take().expect("each index at most once"))
        .collect()
}

/// Sorts by `order`, then by key. With `keep`, only the first `keep`
/// candidates come back, found with a partial sort.
fn sort_candidates(
    candidates: Vec<QueryCandidate>,
    order: &[PropertyOrder],
    keep: Option<usize>,
) -> Vec<QueryCandidate> {
    let view = SortView::new(&candidates, order);
    let mut indices: Vec<usize> = (0..candidates.len()).collect();
    if let Some(keep) = keep
        && keep < indices.len()
    {
        if keep == 0 {
            indices.clear();
        } else {
            indices.select_nth_unstable_by(keep - 1, |&a, &b| view.compare(a, b));
            indices.truncate(keep);
        }
    }
    indices.sort_unstable_by(|&a, &b| view.compare(a, b));
    drop(view);
    take_in_order(candidates, &indices)
}

/// Orders candidates by `order`, then by key, like Datastore. The key
/// tie-break makes the order total, so a cursor names one exact position.
fn compare_query_position(
    a: &QueryCandidate,
    b: &QueryCandidate,
    order: &[PropertyOrder],
) -> Ordering {
    compare_query_candidates(a, b, order).then_with(|| compare_candidate_keys(a, b))
}

/// Query cursors are `CURSOR_MAGIC` followed by an `Entity` proto that holds
/// the key and the sort values of the last consumed result. A query resumes
/// after that position, so inserts and deletes between pages do not shift it.
/// An entity without a key points to the start of the results.
const CURSOR_MAGIC: [u8; 2] = [0xDC, 0x01];

fn encode_cursor(position: Option<&QueryCandidate>, order: &[PropertyOrder]) -> Vec<u8> {
    use prost::encoding::{self, WireType};

    let Some(candidate) = position else {
        return CURSOR_MAGIC.to_vec();
    };
    let entity = &candidate.entity_metadata.entity;
    let sort_values = || {
        order.iter().filter_map(|order_by| {
            let property = order_by.property.as_ref()?;
            if property.name == "__key__" {
                return None;
            }
            let descending = order_by.direction == property_order::Direction::Descending as i32;
            let value = sort_value(entity.properties.get(&property.name)?, descending)?;
            Some((&property.name, value))
        })
    };
    let entry_len = |name: &String, value: &Value| {
        encoding::string::encoded_len(1, name) + encoding::message::encoded_len(2, value)
    };

    // Write the `Entity` fields directly, so the key and values are not cloned.
    let key_len = entity
        .key
        .as_ref()
        .map_or(0, |key| encoding::message::encoded_len(1, key));
    let values_len: usize = sort_values()
        .map(|(name, value)| {
            let len = entry_len(name, value);
            encoding::key_len(3) + encoding::encoded_len_varint(len as u64) + len
        })
        .sum();
    let mut buf = Vec::with_capacity(CURSOR_MAGIC.len() + key_len + values_len);
    buf.extend_from_slice(&CURSOR_MAGIC);
    if let Some(key) = entity.key.as_ref() {
        encoding::message::encode(1, key, &mut buf);
    }
    for (name, value) in sort_values() {
        encoding::encode_key(3, WireType::LengthDelimited, &mut buf);
        encoding::encode_varint(entry_len(name, value) as u64, &mut buf);
        encoding::string::encode(1, name, &mut buf);
        encoding::message::encode(2, value, &mut buf);
    }
    buf
}

/// Returns the position stored in a cursor. `None` means the start of the
/// results (empty cursor, or a cursor from an empty batch).
fn decode_cursor(cursor: &[u8]) -> Result<Option<QueryCandidate>, tonic::Status> {
    if cursor.is_empty() {
        return Ok(None);
    }
    let invalid = || tonic::Status::invalid_argument("Invalid query cursor");
    let payload = cursor.strip_prefix(&CURSOR_MAGIC[..]).ok_or_else(invalid)?;
    let entity = Entity::decode(payload).map_err(|_| invalid())?;
    if entity.key.is_none() {
        return Ok(None);
    }
    Ok(Some(QueryCandidate {
        entity_metadata: Arc::new(EntityWithMetadata {
            entity,
            ..Default::default()
        }),
    }))
}

/// Keeps the candidates after `start` and up to `end`, both cursor positions.
/// Returns true when `end` removed candidates.
fn retain_between_cursors(
    candidates: &mut Vec<QueryCandidate>,
    start: Option<&QueryCandidate>,
    end: Option<&QueryCandidate>,
    order: &[PropertyOrder],
) -> bool {
    if start.is_none() && end.is_none() {
        return false;
    }
    let mut cut_by_end = false;
    candidates.retain(|candidate| {
        if start.is_some_and(|start| compare_query_position(candidate, start, order).is_le()) {
            return false;
        }
        if end.is_some_and(|end| compare_query_position(candidate, end, order).is_gt()) {
            cut_by_end = true;
            return false;
        }
        true
    });
    cut_by_end
}

fn hash_distinct_value<H: Hasher>(hasher: &mut H, value: &Value, scratch: &mut Vec<u8>) {
    match &value.value_type {
        Some(ValueType::StringValue(s)) => {
            b's'.hash(hasher);
            s.hash(hasher);
        }
        Some(ValueType::IntegerValue(i)) => {
            b'i'.hash(hasher);
            i.hash(hasher);
        }
        Some(ValueType::BooleanValue(b)) => {
            b'b'.hash(hasher);
            b.hash(hasher);
        }
        _ => {
            b'?'.hash(hasher);
            scratch.clear();
            value
                .encode_length_delimited(scratch)
                .expect("encode distinct value signature");
            scratch.hash(hasher);
        }
    }
}

#[derive(Clone, PartialEq)]
enum DistinctComponent {
    Key(Option<Key>),
    Value(Value),
    Missing,
}

fn hash_distinct_signature(
    candidate: &QueryCandidate,
    distinct_on: &[PropertyReference],
    scratch: &mut Vec<u8>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for prop in distinct_on {
        if prop.name == "__key__" {
            1u8.hash(&mut hasher);
            if let Some(key) = candidate.entity_key() {
                scratch.clear();
                key.encode_length_delimited(scratch)
                    .expect("encode distinct key signature");
                scratch.hash(&mut hasher);
            }
        } else if let Some(value) = candidate.entity_metadata.entity.properties.get(&prop.name) {
            2u8.hash(&mut hasher);
            hash_distinct_value(&mut hasher, value, scratch);
        } else {
            3u8.hash(&mut hasher);
        }
        0xFFu8.hash(&mut hasher);
    }
    hasher.finish()
}

fn build_distinct_signature(
    candidate: &QueryCandidate,
    distinct_on: &[PropertyReference],
) -> Vec<DistinctComponent> {
    distinct_on
        .iter()
        .map(|prop| {
            if prop.name == "__key__" {
                DistinctComponent::Key(candidate.entity_key().cloned())
            } else if let Some(value) = candidate.entity_metadata.entity.properties.get(&prop.name)
            {
                DistinctComponent::Value(value.clone())
            } else {
                DistinctComponent::Missing
            }
        })
        .collect()
}

fn distinct_signature_matches(
    stored: &[DistinctComponent],
    candidate: &QueryCandidate,
    distinct_on: &[PropertyReference],
) -> bool {
    stored.len() == distinct_on.len()
        && stored
            .iter()
            .zip(distinct_on)
            .all(|(stored, prop)| match stored {
                DistinctComponent::Key(stored) if prop.name == "__key__" => {
                    match (stored.as_ref(), candidate.entity_key()) {
                        (Some(stored), Some(key)) => compare_keys(stored, key).is_eq(),
                        (None, None) => true,
                        _ => false,
                    }
                }
                DistinctComponent::Value(stored) => {
                    candidate.entity_metadata.entity.properties.get(&prop.name) == Some(stored)
                }
                DistinctComponent::Missing => candidate
                    .entity_metadata
                    .entity
                    .properties
                    .get(&prop.name)
                    .is_none(),
                _ => false,
            })
}

fn candidate_matches_filter(
    candidate: &QueryCandidate,
    filter_type: Option<&FilterType>,
    prepared_key_filter: Option<&PreparedKeyFilter>,
    candidates_prefiltered: bool,
) -> bool {
    if candidates_prefiltered {
        return true;
    }
    if let Some(pkf) = prepared_key_filter {
        return candidate
            .entity_key()
            .is_some_and(|key| match_prepared_key_filter(key, pkf));
    }
    match filter_type {
        Some(filter_type) => {
            DatastoreStorage::apply_filter(candidate.entity_metadata.as_ref(), filter_type)
        }
        None => true,
    }
}

/// `Some(descending)` when the order is empty or only `__key__` in one
/// direction. Candidates arrive in ascending key order, so such queries need
/// no sort, or just a reverse.
fn key_only_order(order: &[PropertyOrder]) -> Option<bool> {
    let mut descending = None;
    for order_by in order {
        if order_by
            .property
            .as_ref()
            .is_none_or(|p| p.name != "__key__")
        {
            return None;
        }
        let this = order_by.direction == property_order::Direction::Descending as i32;
        match descending {
            Some(previous) if previous != this => return None,
            _ => descending = Some(this),
        }
    }
    Some(descending.unwrap_or(false))
}

/// Index scans drop entities without an indexed value for any ordered
/// property (`__key__` is always indexed).
fn has_indexed_order_values(candidate: &QueryCandidate, order: &[PropertyOrder]) -> bool {
    order.iter().all(|order_by| {
        let Some(name) = order_by.property.as_ref().map(|p| p.name.as_str()) else {
            return true;
        };
        if name == "__key__" {
            return true;
        }
        let descending = order_by.direction == property_order::Direction::Descending as i32;
        candidate
            .entity_metadata
            .entity
            .properties
            .get(name)
            .and_then(|v| sort_value(v, descending))
            .is_some()
    })
}

/// Projected property names, without duplicates and without `__key__`.
/// Keys-only projections return one row per entity and are not expanded.
fn projected_property_names(projection: &[Projection], is_keys_only: bool) -> Vec<&str> {
    let mut names: Vec<&str> = Vec::new();
    if is_keys_only {
        return names;
    }
    for name in projection
        .iter()
        .filter_map(|p| p.property.as_ref().map(|pr| pr.name.as_str()))
    {
        if name != "__key__" && !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Projection queries also sort by the projected properties, ascending, so
/// each expanded row has a distinct position. The names go in alphabetical
/// order, like the composite index Datastore recommends for the query.
fn projection_order(mut order: Vec<PropertyOrder>, projected_names: &[&str]) -> Vec<PropertyOrder> {
    let mut names = projected_names.to_vec();
    names.sort_unstable();
    for name in names {
        let present = order
            .iter()
            .any(|order_by| order_by.property.as_ref().is_some_and(|p| p.name == name));
        if !present {
            order.push(PropertyOrder {
                property: Some(PropertyReference {
                    name: name.to_string(),
                }),
                direction: property_order::Direction::Ascending as i32,
            });
        }
    }
    order
}

/// Indexed values of a projected property. Arrays yield their indexed
/// elements and dotted names resolve into embedded entities.
fn projected_index_values<'a>(
    properties: &'a HashMap<String, Value>,
    name: &str,
) -> Vec<&'a Value> {
    let values = match properties.get(name) {
        Some(value) if value.exclude_from_indexes => return Vec::new(),
        Some(Value {
            value_type: Some(ValueType::ArrayValue(array)),
            ..
        }) => array.values.iter().collect(),
        Some(value) => vec![value],
        None if name.contains('.') => resolve_nested_property(properties, name),
        None => return Vec::new(),
    };
    values
        .into_iter()
        .filter(|value| !value.exclude_from_indexes)
        .collect()
}

/// Expands a candidate into one row per combination of the indexed values of
/// the projected properties, like an index scan. A row holds one value per
/// projected property. An entity without an indexed value for some projected
/// property yields no rows.
fn expand_projection_rows(candidate: QueryCandidate, names: &[&str]) -> Vec<QueryCandidate> {
    let source = candidate.entity_metadata.as_ref();
    let mut rows: Vec<HashMap<String, Value>> = vec![HashMap::new()];
    for &name in names {
        let values: Vec<Value> = projected_index_values(&source.entity.properties, name)
            .into_iter()
            .map(|value| {
                let mut value = value.clone();
                value.exclude_from_indexes = false;
                value
            })
            .collect();
        if values.is_empty() {
            return Vec::new();
        }
        let mut next = Vec::with_capacity(rows.len() * values.len());
        for row in &rows {
            for value in &values {
                let mut row = row.clone();
                row.insert(name.to_string(), value.clone());
                next.push(row);
            }
        }
        rows = next;
    }

    rows.into_iter()
        .map(|properties| QueryCandidate {
            entity_metadata: Arc::new(EntityWithMetadata {
                entity: Entity {
                    key: source.entity.key.clone(),
                    properties,
                },
                version: source.version,
                create_time: source.create_time.clone(),
                update_time: source.update_time.clone(),
            }),
        })
        .collect()
}

/// Keeps the first candidate of each distinct-on signature, preserving order.
/// One candidate per `distinct_on` signature: the first in query order, as
/// if the candidates had been sorted. Comes back in input order, so the
/// caller sorts only the representatives.
fn dedup_distinct_candidates(
    candidates: Vec<QueryCandidate>,
    distinct_on: &[PropertyReference],
    order: &[PropertyOrder],
) -> Vec<QueryCandidate> {
    let view = SortView::new(&candidates, order);
    // Signature hash -> (signature, index of its first candidate so far).
    let mut groups: HashMap<u64, Vec<(Vec<DistinctComponent>, usize)>> = HashMap::new();
    let mut scratch: Vec<u8> = Vec::new();

    for (index, candidate) in candidates.iter().enumerate() {
        let signature_hash = hash_distinct_signature(candidate, distinct_on, &mut scratch);
        let same_hash = groups.entry(signature_hash).or_default();
        match same_hash
            .iter_mut()
            .find(|(signature, _)| distinct_signature_matches(signature, candidate, distinct_on))
        {
            Some((_, first)) => {
                if view.compare(index, *first).is_lt() {
                    *first = index;
                }
            }
            None => same_hash.push((build_distinct_signature(candidate, distinct_on), index)),
        }
    }

    let mut kept: Vec<usize> = groups
        .into_values()
        .flatten()
        .map(|(_, first)| first)
        .collect();
    kept.sort_unstable();
    drop(view);
    take_in_order(candidates, &kept)
}

#[derive(Default, Debug)]
pub struct DatastoreStorage {
    // Stores using BTreeMap for ordered storage, mapping a full KeyStruct to its EntityWithMetadata
    // The key allocation is shared with every index entry of the entity.
    pub entities: BTreeMap<Arc<KeyStruct>, Arc<EntityWithMetadata>>,
    // Secondary view used to avoid full-store scans when a query targets one project/kind.
    pub scoped_entities:
        HashMap<(String, String), BTreeMap<Arc<KeyStruct>, Arc<EntityWithMetadata>>>,
    // Secondary view for ancestor queries, keyed by (project, descendant kind, ancestor key).
    pub ancestor_index: HashMap<(String, String, KeyStruct), BTreeSet<Arc<KeyStruct>>>,
    // Equality index: (kind, property path, value) -> entity keys.
    pub indexes: PropertyIndex,
    // Active transactions
    pub transactions: HashMap<String, TransactionState>,
    // Per-project counters for auto ID allocation. One sequence for all kinds
    // and namespaces, like the Google emulator, so IDs do not repeat across kinds.
    pub entity_id_counters: HashMap<String, AtomicI64>,
}

impl DatastoreStorage {
    fn candidates_from_index_keys<'a, K: Borrow<KeyStruct> + 'a>(
        &'a self,
        project_id_filter: &'a str,
        kind_name: &'a str,
        database_id: &'a str,
        namespace: &'a str,
        keys: impl IntoIterator<Item = K> + 'a,
    ) -> impl Iterator<Item = QueryCandidate> + 'a {
        let scope_key = (project_id_filter.to_string(), kind_name.to_string());
        let scoped_entities = self.scoped_entities.get(&scope_key);

        keys.into_iter()
            .filter(move |key| {
                let key_struct = key.borrow();
                key_struct.project_id == project_id_filter
                    && key_struct.database_id == database_id
                    && key_struct.namespace == namespace
                    && key_struct
                        .path_elements
                        .last()
                        .is_some_and(|(k, _)| k == kind_name)
            })
            .filter_map(move |key| {
                let key_struct = key.borrow();
                scoped_entities
                    .and_then(|entities| entities.get(key_struct))
                    .or_else(|| self.entities.get(key_struct))
                    .map(|entity_metadata| QueryCandidate {
                        entity_metadata: Arc::clone(entity_metadata),
                    })
            })
    }

    fn query_simple_index_candidates<'a>(
        &'a self,
        project_id_filter: &'a str,
        kind_name: &'a str,
        database_id: &'a str,
        namespace: &'a str,
        filter_type: &'a FilterType,
    ) -> Option<Box<dyn Iterator<Item = QueryCandidate> + 'a>> {
        let FilterType::PropertyFilter(property_filter) = filter_type else {
            return None;
        };
        let property = property_filter.property.as_ref()?;
        let filter_value = property_filter.value.as_ref()?;

        if property.name == "__key__" {
            if property_filter.op != 11 {
                return None;
            }

            let Some(ValueType::KeyValue(key)) = &filter_value.value_type else {
                return None;
            };
            let ancestor_key_struct = KeyStruct::from_datastore_key(key);
            if ancestor_key_struct.project_id != project_id_filter
                || ancestor_key_struct.database_id != database_id
                || ancestor_key_struct.path_elements.is_empty()
            {
                return Some(Box::new(std::iter::empty()));
            }
            if self.ancestor_index.is_empty()
                && self.scoped_entities.is_empty()
                && !self.entities.is_empty()
            {
                return None;
            }

            let index_key = (
                project_id_filter.to_string(),
                kind_name.to_string(),
                ancestor_key_struct.clone(),
            );
            let descendants = self.candidates_from_index_keys(
                project_id_filter,
                kind_name,
                database_id,
                namespace,
                self.ancestor_index
                    .get(&index_key)
                    .into_iter()
                    .flatten()
                    .map(Arc::as_ref),
            );
            // The ancestor sorts before its descendants.
            let ancestor_itself = self.candidates_from_index_keys(
                project_id_filter,
                kind_name,
                database_id,
                namespace,
                std::iter::once(ancestor_key_struct),
            );
            return Some(Box::new(ancestor_itself.chain(descendants)));
        }

        let index_keys = index_keys_for_value(kind_name, &property.name, filter_value);
        if index_keys.is_empty() {
            return None;
        }

        match property_filter.op {
            5 if !matches!(filter_value.value_type, Some(ValueType::ArrayValue(_))) => {
                let Some(keys) = self.indexes.get(&index_keys[0]) else {
                    return Some(Box::new(std::iter::empty()));
                };
                Some(Box::new(self.candidates_from_index_keys(
                    project_id_filter,
                    kind_name,
                    database_id,
                    namespace,
                    keys.iter().map(Arc::as_ref),
                )))
            }
            6 if matches!(filter_value.value_type, Some(ValueType::ArrayValue(_))) => {
                let mut unique_keys = BTreeSet::new();
                for index_key in &index_keys {
                    if let Some(keys) = self.indexes.get(index_key) {
                        unique_keys.extend(keys.iter().cloned());
                    }
                }
                Some(Box::new(self.candidates_from_index_keys(
                    project_id_filter,
                    kind_name,
                    database_id,
                    namespace,
                    unique_keys,
                )))
            }
            _ => None,
        }
    }

    fn index_lookup_for_filter(
        &self,
        project_id: &str,
        kind_name: &str,
        filter_type: &FilterType,
    ) -> Option<BTreeSet<Arc<KeyStruct>>> {
        match filter_type {
            FilterType::PropertyFilter(property_filter) => {
                let property = property_filter.property.as_ref()?;
                let filter_value = property_filter.value.as_ref()?;

                if property.name == "__key__" {
                    match property_filter.op {
                        5 => {
                            if let Some(ValueType::KeyValue(key)) = &filter_value.value_type {
                                let key_struct = KeyStruct::from_datastore_key(key);
                                if key_struct.project_id == project_id
                                    && key_struct
                                        .path_elements
                                        .last()
                                        .is_some_and(|(k, _)| k == kind_name)
                                {
                                    return Some(BTreeSet::from([Arc::new(key_struct)]));
                                }
                                return Some(BTreeSet::new());
                            }
                            None
                        }
                        6 => {
                            if let Some(ValueType::ArrayValue(array)) = &filter_value.value_type {
                                let mut result = BTreeSet::new();
                                for value in &array.values {
                                    if let Some(ValueType::KeyValue(key)) = &value.value_type {
                                        let key_struct = KeyStruct::from_datastore_key(key);
                                        if key_struct.project_id == project_id
                                            && key_struct
                                                .path_elements
                                                .last()
                                                .is_some_and(|(k, _)| k == kind_name)
                                        {
                                            result.insert(Arc::new(key_struct));
                                        }
                                    }
                                }
                                return Some(result);
                            }
                            None
                        }
                        11 => {
                            if let Some(ValueType::KeyValue(key)) = &filter_value.value_type {
                                let ancestor_key_struct = KeyStruct::from_datastore_key(key);
                                if ancestor_key_struct.project_id != project_id
                                    || ancestor_key_struct.path_elements.is_empty()
                                {
                                    return Some(BTreeSet::new());
                                }
                                if self.ancestor_index.is_empty()
                                    && self.scoped_entities.is_empty()
                                    && !self.entities.is_empty()
                                {
                                    return None;
                                }

                                let mut keys = self
                                    .ancestor_index
                                    .get(&(
                                        project_id.to_string(),
                                        kind_name.to_string(),
                                        ancestor_key_struct.clone(),
                                    ))
                                    .cloned()
                                    .unwrap_or_default();
                                if ancestor_key_struct
                                    .path_elements
                                    .last()
                                    .is_some_and(|(k, _)| k == kind_name)
                                {
                                    keys.insert(Arc::new(ancestor_key_struct));
                                }
                                return Some(keys);
                            }
                            None
                        }
                        _ => None,
                    }
                } else {
                    let index_keys = index_keys_for_value(kind_name, &property.name, filter_value);
                    if index_keys.is_empty() {
                        return None;
                    }

                    match property_filter.op {
                        5 | 6 => {
                            let mut acc = BTreeSet::new();
                            for index_key in &index_keys {
                                if let Some(keys) = self.indexes.get(index_key) {
                                    acc.extend(keys.iter().cloned());
                                }
                            }
                            Some(acc)
                        }
                        _ => None,
                    }
                }
            }
            FilterType::CompositeFilter(composite) => {
                let mut child_sets = Vec::new();
                for filter in &composite.filters {
                    if let Some(filter_type) = filter.filter_type.as_ref() {
                        if let Some(child) =
                            self.index_lookup_for_filter(project_id, kind_name, filter_type)
                        {
                            child_sets.push(child);
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }

                if child_sets.is_empty() {
                    return Some(BTreeSet::new());
                }

                match composite.op {
                    1 => {
                        // AND
                        child_sets.sort_by_key(|set| set.len());
                        let mut iter = child_sets.into_iter();
                        let mut acc = iter.next().unwrap();
                        for set in iter {
                            acc = acc.intersection(&set).cloned().collect();
                            if acc.is_empty() {
                                break;
                            }
                        }
                        Some(acc)
                    }
                    2 => {
                        // OR
                        let mut acc = BTreeSet::new();
                        for set in child_sets {
                            acc.extend(set);
                        }
                        Some(acc)
                    }
                    _ => None,
                }
            }
        }
    }

    fn counter_scope_from_key(key: &Key) -> Option<String> {
        key.path.last()?;
        Some(
            key.partition_id
                .as_ref()
                .map(|p| p.project_id.clone())
                .unwrap_or_default(),
        )
    }

    pub(crate) fn observe_key_id(&mut self, key: &Key) {
        if let Some(project) = Self::counter_scope_from_key(key)
            && let Some(path_element) = key.path.last()
            && let Some(IdType::Id(id)) = path_element.id_type
        {
            self.entity_id_counters
                .entry(project)
                .and_modify(|counter| {
                    let current = counter.load(std::sync::atomic::Ordering::SeqCst);
                    if current < id {
                        counter.store(id, std::sync::atomic::Ordering::SeqCst);
                    }
                })
                .or_insert_with(|| AtomicI64::new(id));
        }
    }

    pub fn next_auto_id(&mut self, key: &Key) -> Result<i64, tonic::Status> {
        let scope = Self::counter_scope_from_key(key).ok_or_else(|| {
            tonic::Status::invalid_argument(
                "The entity key has no path elements to determine the 'kind' for ID generation.",
            )
        })?;
        let counter = self
            .entity_id_counters
            .entry(scope)
            .or_insert_with(|| AtomicI64::new(0));
        let next = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        Ok(next)
    }

    fn rebuild_entity_id_counters(&mut self) {
        self.entity_id_counters.clear();
        let keys: Vec<Key> = self
            .entities
            .values()
            .filter_map(|metadata| metadata.entity.key.clone())
            .collect();
        for key in keys {
            self.observe_key_id(&key);
        }
    }

    // Add this new method to load data from disk
    pub fn load_from_disk(&mut self, path: &str) -> std::io::Result<()> {
        // Try to open the file. If it doesn't exist, just return Ok without doing anything.
        let start_time = SystemTime::now();
        let mut file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(
                    "Data file '{}' not found. Starting with an empty store.",
                    path
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            tracing::info!(
                "Data file '{}' is empty. Starting with an empty store.",
                path
            );
            return Ok(());
        }

        // Deserialize the buffer directly into the BTreeMap
        let (entities, _): (BTreeMap<KeyStruct, EntityWithMetadata>, _) =
            bincode::serde::decode_from_slice(&buffer, bincode::config::standard())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        tracing::info!("Loading {} entities from '{}'...", entities.len(), path);
        self.entities = entities
            .into_iter()
            .map(|(key, metadata)| (Arc::new(key), Arc::new(metadata)))
            .collect();

        // Bulk rebuild indexes
        tracing::info!("Rebuilding indexes for {} entities...", self.entities.len());
        let mut new_indexes = PropertyIndex::new();
        let mut new_scoped_entities: HashMap<
            (String, String),
            BTreeMap<Arc<KeyStruct>, Arc<EntityWithMetadata>>,
        > = HashMap::new();
        let mut new_ancestor_index = HashMap::new();
        for (key_struct, metadata) in &self.entities {
            if let Some((kind, _)) = key_struct.path_elements.last() {
                for_each_index_key_of_entity(kind, &metadata.entity, &mut |index_key| {
                    insert_index_entry(&mut new_indexes, index_key, key_struct)
                });
                new_scoped_entities
                    .entry((key_struct.project_id.clone(), kind.clone()))
                    .or_default()
                    .insert(Arc::clone(key_struct), Arc::clone(metadata));
            }
            Self::add_ancestor_index_entries(&mut new_ancestor_index, key_struct);
        }
        self.indexes = new_indexes;
        self.scoped_entities = new_scoped_entities;
        self.ancestor_index = new_ancestor_index;
        self.rebuild_entity_id_counters();

        let end_time = SystemTime::now();
        let diff = end_time.duration_since(start_time).unwrap_or_default();
        tracing::info!("Load complete in {} seconds.", diff.as_secs_f64());
        Ok(())
    }

    // Add this new method to save data to disk
    pub fn save_to_disk(&self, path: &str) -> std::io::Result<()> {
        tracing::info!("Saving {} entities to '{}'...", self.entities.len(), path);

        // Serialize the whole BTreeMap
        let entities: BTreeMap<KeyStruct, EntityWithMetadata> = self
            .entities
            .iter()
            .map(|(key, metadata)| (KeyStruct::clone(key), metadata.as_ref().clone()))
            .collect();

        let buffer = bincode::serde::encode_to_vec(&entities, bincode::config::standard())
            .map_err(std::io::Error::other)?;

        // Write the serialized buffer to the file
        let mut file = std::fs::File::create(path)?;
        file.write_all(&buffer)?;
        tracing::info!("Data saved successfully.");
        Ok(())
    }

    pub fn clean_transaction(&mut self, transaction_id: &str) {
        // clean up the transaction state
        self.transactions.remove(transaction_id);
    }

    pub fn import_dump(&mut self, path: &str) -> Result<(), tonic::Status> {
        self.import_dump_for_project(path, None)
    }

    /// Import a Cloud Datastore export from `path`. If `target_project_id` is
    /// `Some`, imported entity keys and key-valued properties are rewritten to
    /// that value. This lets exports from prod be ingested into the local
    /// emulator under a different project_id (the one the caller queries
    /// with).
    pub fn import_dump_for_project(
        &mut self,
        path: &str,
        target_project_id: Option<&str>,
    ) -> Result<(), tonic::Status> {
        self.insert_imported(prepare_dump_for_project(path, target_project_id)?)
    }

    /// Store entities prepared by `prepare_dump_for_project`.
    pub fn insert_imported(
        &mut self,
        entities: Vec<EntityWithMetadata>,
    ) -> Result<(), tonic::Status> {
        for entity_metadata in entities {
            if entity_metadata.entity.key.is_some() {
                self.insert_entity(&entity_metadata.entity)?;
            } else {
                return Err(tonic::Status::invalid_argument(
                    "Entity missing key during import",
                ));
            }
        }
        Ok(())
    }

    pub fn insert_entity(
        &mut self,
        entity: &Entity,
    ) -> Result<(Key, Arc<EntityWithMetadata>), tonic::Status> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let timestamp_now = pbjson_types::Timestamp {
            seconds: now.as_secs() as i64,
            nanos: (now.as_nanos() % 1_000_000_000) as i32,
        };

        self.insert_entity_at(entity, timestamp_now)
    }

    pub(crate) fn insert_entity_at(
        &mut self,
        entity: &Entity,
        timestamp_now: pbjson_types::Timestamp,
    ) -> Result<(Key, Arc<EntityWithMetadata>), tonic::Status> {
        self.insert_owned_entity_at(entity.clone(), timestamp_now)
    }

    pub(crate) fn insert_owned_entity_at(
        &mut self,
        mut entity: Entity,
        timestamp_now: pbjson_types::Timestamp,
    ) -> Result<(Key, Arc<EntityWithMetadata>), tonic::Status> {
        let Some(original_key) = entity.key.as_ref() else {
            return Err(tonic::Status::invalid_argument("Entity missing key"));
        };

        let needs_auto_id = original_key
            .path
            .iter()
            .any(|path_element| path_element.id_type.is_none());
        let new_id_value = if needs_auto_id {
            Some(self.next_auto_id(original_key)?)
        } else {
            None
        };

        let mut key_with_new_id = original_key.clone();
        if let Some(id_value) = new_id_value {
            for path_element in key_with_new_id.path.iter_mut() {
                if path_element.id_type.is_none() {
                    path_element.id_type = Some(IdType::Id(id_value));
                }
            }
        }

        entity.key = Some(key_with_new_id.clone());

        let entity_metadata = Arc::new(EntityWithMetadata {
            entity,
            version: 1, // Initial version
            create_time: timestamp_now.clone(),
            update_time: timestamp_now,
        });
        self.put_entity(Arc::clone(&entity_metadata));

        Ok((key_with_new_id, entity_metadata))
    }

    /// Stores `entity_metadata` under its key and brings every index up to
    /// date, replacing a previous entity with that key. Returns the previous
    /// entity.
    pub fn put_entity(
        &mut self,
        entity_metadata: Arc<EntityWithMetadata>,
    ) -> Option<Arc<EntityWithMetadata>> {
        let key = entity_metadata
            .entity
            .key
            .as_ref()
            .expect("stored entities have a key");
        let key_struct = KeyStruct::from_datastore_key(key);
        let (key_struct, previous) = match self.entities.get_key_value(&key_struct) {
            Some((shared, previous)) => (Arc::clone(shared), Some(Arc::clone(previous))),
            None => (Arc::new(key_struct), None),
        };

        self.entities
            .insert(Arc::clone(&key_struct), Arc::clone(&entity_metadata));
        self.upsert_scoped_entity(&key_struct, Arc::clone(&entity_metadata));
        match &previous {
            Some(previous) => {
                self.replace_indexes(&key_struct, &previous.entity, &entity_metadata.entity)
            }
            None => {
                self.observe_key_id(key);
                self.add_ancestor_indexes(&key_struct);
                self.update_indexes(&key_struct, &entity_metadata.entity);
            }
        }
        previous
    }

    pub fn apply_filter(entity_metadata: &EntityWithMetadata, filter: &FilterType) -> bool {
        match filter {
            FilterType::PropertyFilter(property_filter) => {
                if let Some(ref property) = property_filter.property {
                    if let Some(ref filter_value) = property_filter.value {
                        // Renamed `value` to `filter_value`
                        // Special handling for HAS_ANCESTOR as it operates on keys, not properties
                        if property.name == "__key__" && property_filter.op == 11 {
                            // HAS_ANCESTOR = 11
                            if let Some(ValueType::KeyValue(ancestor_key_value)) =
                                &filter_value.value_type
                                && let Some(entity_key) = &entity_metadata.entity.key
                            {
                                let entity_partition_id_obj = entity_key.partition_id.as_ref();
                                let ancestor_partition_id_obj =
                                    ancestor_key_value.partition_id.as_ref();

                                let partitions_match =
                                    match (entity_partition_id_obj, ancestor_partition_id_obj) {
                                        (Some(ep), Some(ap)) => {
                                            ep.project_id == ap.project_id
                                                && ep.namespace_id == ap.namespace_id
                                        }
                                        (None, None) => true,
                                        _ => false,
                                    };
                                if !partitions_match {
                                    return false;
                                }

                                if entity_key.path.len() >= ancestor_key_value.path.len() {
                                    for (i, ancestor_path_element) in
                                        ancestor_key_value.path.iter().enumerate()
                                    {
                                        let entity_path_element = &entity_key.path[i];
                                        if entity_path_element.kind != ancestor_path_element.kind
                                            || entity_path_element.id_type
                                                != ancestor_path_element.id_type
                                        {
                                            return false;
                                        }
                                    }
                                    return true;
                                }
                            }
                            false
                        }
                        // Regular property filters
                        else {
                            // Check if this is a nested property path (contains dots)
                            if property.name == "__key__" {
                                if let Some(key) = entity_metadata.entity.key.as_ref() {
                                    match_key_against_filter(key, filter_value, property_filter.op)
                                } else {
                                    false
                                }
                            } else if let Some(value) =
                                entity_metadata.entity.properties.get(&property.name)
                            {
                                // Simple property lookup. Also covers dotted names stored
                                // as one property, like a legacy StructuredProperty.
                                if value.exclude_from_indexes {
                                    false
                                } else {
                                    match &value.value_type {
                                        Some(ValueType::ArrayValue(array)) => values_match_filter(
                                            array
                                                .values
                                                .iter()
                                                .filter(|inner| !inner.exclude_from_indexes),
                                            filter_value,
                                            property_filter.op,
                                        ),
                                        _ => values_match_filter(
                                            std::iter::once(value),
                                            filter_value,
                                            property_filter.op,
                                        ),
                                    }
                                }
                            } else if property.name.contains('.') {
                                // Nested property path - use recursive resolution
                                let entity_values = resolve_nested_property(
                                    &entity_metadata.entity.properties,
                                    &property.name,
                                );
                                values_match_filter(
                                    entity_values
                                        .into_iter()
                                        .filter(|v| !v.exclude_from_indexes),
                                    filter_value,
                                    property_filter.op,
                                )
                            } else {
                                false
                            }
                        }
                    } else {
                        // Filter value is missing
                        false
                    }
                } else {
                    // Property reference is missing
                    false
                }
            }
            FilterType::CompositeFilter(composity_filter) => {
                // Combine results based on the composite filter operator
                // AND = 1, OR = 2
                match composity_filter.op {
                    1 => {
                        // AND we can translate to ALL are true
                        composity_filter
                            .filters
                            .iter()
                            .filter_map(|filter| filter.filter_type.as_ref())
                            .all(|filter_type| {
                                DatastoreStorage::apply_filter(entity_metadata, filter_type)
                            })
                    }
                    2 => {
                        // Or we can translate to ANY is true
                        composity_filter
                            .filters
                            .iter()
                            .filter_map(|filter| filter.filter_type.as_ref())
                            .any(|filter_type| {
                                DatastoreStorage::apply_filter(entity_metadata, filter_type)
                            })
                    }
                    _ => {
                        // OPERATOR_UNSPECIFIED
                        true
                    }
                }
            }
        }
        // The code below was unreachable because all match arms returned explicitly.
        // If none of the match arms are hit (which shouldn't happen with a valid FilterType),
        // the default behavior would be not to filter, i.e., return true.
        // However, the current match logic covers all FilterType cases (PropertyFilter, CompositeFilter).
        // If FilterType is None, the filter is not applied in the previous call.
        // Therefore, removing the final `true` is safe, as the match should be exhaustive for valid FilterType.
    }

    pub fn get_entity(&self, key: &Key) -> Option<Arc<EntityWithMetadata>> {
        let key_struct = KeyStruct::from_datastore_key(key);
        self.entities.get(&key_struct).cloned()
    }
    fn get_metadata(
        &self,
        metadata_key: &str,
        project_id: &str,
        database_id: &str,
        namespace: &str,
    ) -> Vec<EntityWithMetadata> {
        let mut results = Vec::new();

        match metadata_key {
            "__kind__" => {
                let mut unique_kinds = HashSet::new();
                for key_struct in self.entities.keys() {
                    if key_struct.project_id == project_id
                        && key_struct.database_id == database_id
                        && key_struct.namespace == namespace
                        && let Some((kind, _)) = key_struct.path_elements.last()
                    {
                        unique_kinds.insert((
                            kind.clone(),
                            key_struct.project_id.clone(),
                            key_struct.namespace.clone(),
                        ));
                    }
                }

                results = unique_kinds
                    .into_iter()
                    .map(|(kind, project_id, namespace)| EntityWithMetadata {
                        version: 1,
                        create_time: pbjson_types::Timestamp::default(),
                        update_time: pbjson_types::Timestamp::default(),
                        entity: Entity {
                            properties: HashMap::new(),
                            key: Some(Key {
                                partition_id: Some(PartitionId {
                                    project_id,
                                    namespace_id: namespace,
                                    database_id: database_id.to_string(),
                                }),
                                path: vec![PathElement {
                                    kind: "__kind__".to_string(),
                                    id_type: Some(IdType::Name(kind)),
                                }],
                            }),
                        },
                    })
                    .collect();
            }
            "__namespace__" => {
                let mut unique_namespaces = HashSet::new();
                for key_struct in self.entities.keys() {
                    if key_struct.project_id == project_id && key_struct.database_id == database_id
                    {
                        unique_namespaces.insert(key_struct.namespace.clone());
                    }
                }

                results = unique_namespaces
                    .into_iter()
                    .map(|namespace| {
                        let id_type = if namespace.is_empty() {
                            IdType::Id(1)
                        } else {
                            IdType::Name(namespace)
                        };
                        EntityWithMetadata {
                            version: 1,
                            create_time: pbjson_types::Timestamp::default(),
                            update_time: pbjson_types::Timestamp::default(),
                            entity: Entity {
                                properties: HashMap::new(),
                                key: Some(Key {
                                    partition_id: Some(PartitionId {
                                        project_id: project_id.to_string(),
                                        namespace_id: "".to_string(),
                                        database_id: database_id.to_string(),
                                    }),
                                    path: vec![PathElement {
                                        kind: "__namespace__".to_string(),
                                        id_type: Some(id_type),
                                    }],
                                }),
                            },
                        }
                    })
                    .collect();
            }
            "__property__" => {
                // map from (kind, property_name, namespace) to set of representations
                let mut props_by_kind: HashMap<(String, String, String), HashSet<&'static str>> =
                    HashMap::new();

                for (key_struct, entity_meta) in &self.entities {
                    if key_struct.project_id != project_id
                        || key_struct.database_id != database_id
                        || key_struct.namespace != namespace
                    {
                        continue;
                    }
                    if let Some((kind, _)) = key_struct.path_elements.last() {
                        for (prop_name, prop_value) in &entity_meta.entity.properties {
                            if !prop_value.exclude_from_indexes {
                                let representations = get_representation_for_value(prop_value);
                                let entry = props_by_kind
                                    .entry((
                                        kind.clone(),
                                        prop_name.clone(),
                                        key_struct.namespace.clone(),
                                    ))
                                    .or_default();
                                entry.extend(representations);
                            }
                        }
                    }
                }

                results = props_by_kind
                    .into_iter()
                    .map(|((kind, prop_name, namespace), representations)| {
                        let property_key = Key {
                            partition_id: Some(PartitionId {
                                project_id: project_id.to_string(),
                                namespace_id: namespace,
                                database_id: database_id.to_string(),
                            }),
                            path: vec![
                                PathElement {
                                    kind: "__kind__".to_string(),
                                    id_type: Some(IdType::Name(kind)),
                                },
                                PathElement {
                                    kind: "__property__".to_string(),
                                    id_type: Some(IdType::Name(prop_name)),
                                },
                            ],
                        };

                        let mut properties = HashMap::new();
                        let array_values: Vec<Value> = representations
                            .into_iter()
                            .map(|rep| Value {
                                value_type: Some(ValueType::StringValue(rep.to_string())),
                                ..Default::default()
                            })
                            .collect();
                        properties.insert(
                            "property_representation".to_string(),
                            Value {
                                value_type: Some(ValueType::ArrayValue(ArrayValue {
                                    values: array_values,
                                })),
                                ..Default::default()
                            },
                        );

                        EntityWithMetadata {
                            version: 1,
                            create_time: pbjson_types::Timestamp::default(),
                            update_time: pbjson_types::Timestamp::default(),
                            entity: Entity {
                                key: Some(property_key),
                                properties,
                            },
                        }
                    })
                    .collect();
            }
            _ => {
                // For other metadata kinds, return an empty result set
                tracing::warn!("Unsupported metadata kind: {}", metadata_key);
            }
        }
        results
    }
    /// Every entity in the project and namespace, in key order (kindless queries).
    pub(crate) fn kindless_candidates<'a>(
        &'a self,
        project_id_filter: &'a str,
        database_id: &'a str,
        namespace: &'a str,
    ) -> impl Iterator<Item = QueryCandidate> + 'a {
        self.entities
            .iter()
            .filter(move |(key_struct, _)| {
                key_struct.project_id == project_id_filter
                    && key_struct.database_id == database_id
                    && key_struct.namespace == namespace
            })
            .map(|(_, entity_metadata)| QueryCandidate {
                entity_metadata: Arc::clone(entity_metadata),
            })
    }

    /// Candidates in ascending key order; the flag says the filter was
    /// already applied. The iterator borrows the storage, so callers hold
    /// the lock while they consume it, and a limit stops the scan early.
    pub(crate) fn query_candidates<'a>(
        &'a self,
        project_id_filter: &'a str,
        kind_name: &'a str,
        database_id: &'a str,
        namespace: &'a str,
        filter_type: Option<&'a FilterType>,
    ) -> (Box<dyn Iterator<Item = QueryCandidate> + 'a>, bool) {
        if METADATA_KINDS.contains(&kind_name) {
            let mut candidates: Vec<QueryCandidate> = self
                .get_metadata(kind_name, project_id_filter, database_id, namespace)
                .into_iter()
                .map(|metadata| QueryCandidate {
                    entity_metadata: Arc::new(metadata),
                })
                .collect();
            // Other candidate sources are in key order; cursors rely on it.
            candidates.sort_by(compare_candidate_keys);
            return (Box::new(candidates.into_iter()), false);
        }

        if let Some(filter_type) = filter_type
            && let Some(candidates) = self.query_simple_index_candidates(
                project_id_filter,
                kind_name,
                database_id,
                namespace,
                filter_type,
            )
        {
            return (candidates, true);
        }

        if let Some(filter_type) = filter_type
            && let Some(candidate_keys) =
                self.index_lookup_for_filter(project_id_filter, kind_name, filter_type)
        {
            let candidates_prefiltered = index_lookup_covers_filter(filter_type);
            return (
                Box::new(self.candidates_from_index_keys(
                    project_id_filter,
                    kind_name,
                    database_id,
                    namespace,
                    candidate_keys,
                )),
                candidates_prefiltered,
            );
        }

        let prepared_key_filter = filter_type.and_then(try_prepare_key_filter);

        let matches = move |entity_metadata: &Arc<EntityWithMetadata>| {
            if let Some(ref pkf) = prepared_key_filter {
                entity_metadata
                    .entity
                    .key
                    .as_ref()
                    .is_some_and(|key| match_prepared_key_filter(key, pkf))
            } else {
                filter_type.is_none_or(|filter_type| {
                    DatastoreStorage::apply_filter(entity_metadata.as_ref(), filter_type)
                })
            }
        };
        let to_candidate = |entity_metadata: &Arc<EntityWithMetadata>| QueryCandidate {
            entity_metadata: Arc::clone(entity_metadata),
        };

        let scope_key = (project_id_filter.to_string(), kind_name.to_string());
        let candidates: Box<dyn Iterator<Item = QueryCandidate> + 'a> =
            if let Some(scoped_entities) = self.scoped_entities.get(&scope_key) {
                Box::new(
                    scoped_entities
                        .iter()
                        .filter(move |(key_struct, _)| {
                            key_struct.database_id == database_id
                                && key_struct.namespace == namespace
                        })
                        .filter(move |(_, entity_metadata)| matches(entity_metadata))
                        .map(move |(_, entity_metadata)| to_candidate(entity_metadata)),
                )
            } else if !self.scoped_entities.is_empty() {
                Box::new(std::iter::empty())
            } else {
                Box::new(
                    self.entities
                        .iter()
                        .filter(move |(key_struct, _)| {
                            key_struct.project_id == project_id_filter
                                && key_struct.database_id == database_id
                                && key_struct.namespace == namespace
                                && key_struct
                                    .path_elements
                                    .last()
                                    .is_some_and(|(k, _)| k == kind_name)
                        })
                        .filter(move |(_, entity_metadata)| matches(entity_metadata))
                        .map(move |(_, entity_metadata)| to_candidate(entity_metadata)),
                )
            };

        (candidates, filter_type.is_some())
    }

    pub(crate) fn get_entities_from_candidates(
        candidates: impl Iterator<Item = QueryCandidate>,
        filter_type: Option<&FilterType>,
        candidates_prefiltered: bool,
        limit: Option<i32>,
        offset: i32,
        start_cursor: Vec<u8>,
        end_cursor: Vec<u8>,
        projection: Vec<Projection>,
        distinct_on: Vec<PropertyReference>,
        order: Vec<PropertyOrder>,
    ) -> Result<crate::google::datastore::v1::QueryResultBatch, tonic::Status> {
        let start_position = decode_cursor(&start_cursor)?;
        let end_position = decode_cursor(&end_cursor)?;
        let prepared_key_filter = filter_type.and_then(try_prepare_key_filter);

        let limit = limit.filter(|value| *value >= 0);

        let is_keys_only = !projection.is_empty()
            && projection
                .iter()
                .all(|p| p.property.as_ref().is_some_and(|pr| pr.name == "__key__"));
        let projected_names = projected_property_names(&projection, is_keys_only);
        let order = projection_order(order, &projected_names);

        let matching = candidates.filter(|candidate| {
            candidate_matches_filter(
                candidate,
                filter_type,
                prepared_key_filter.as_ref(),
                candidates_prefiltered,
            )
        });

        let mut cut_by_end_cursor = false;
        let mut filtered_entities: Vec<QueryCandidate>;
        if distinct_on.is_empty() && key_only_order(&order) == Some(false) {
            // Candidates already arrive in the result order, so the cursors
            // cut a prefix and a suffix, and the scan stops after
            // offset + limit (plus one, to learn whether more results exist).
            debug_assert!(projected_names.is_empty());
            let after_start = matching.skip_while(|candidate| {
                start_position
                    .as_ref()
                    .is_some_and(|start| compare_query_position(candidate, start, &order).is_le())
            });
            let before_end = after_start.take_while(|candidate| {
                let past_end = end_position
                    .as_ref()
                    .is_some_and(|end| compare_query_position(candidate, end, &order).is_gt());
                cut_by_end_cursor |= past_end;
                !past_end
            });
            filtered_entities = match limit {
                Some(limit_value) => before_end
                    .take(
                        (offset.max(0) as usize)
                            .saturating_add(limit_value as usize)
                            .saturating_add(1),
                    )
                    .collect(),
                None => before_end.collect(),
            };
        } else {
            filtered_entities = matching.collect();
            if !projected_names.is_empty() {
                filtered_entities = filtered_entities
                    .into_iter()
                    .flat_map(|candidate| expand_projection_rows(candidate, &projected_names))
                    .collect();
            }
            filtered_entities.retain(|candidate| has_indexed_order_values(candidate, &order));

            if !distinct_on.is_empty() {
                filtered_entities =
                    dedup_distinct_candidates(filtered_entities, &distinct_on, &order);
            }
            // Drop the candidates outside the cursors before the sort, so the
            // bounded sort below also works for later pages. After the dedup,
            // a group that a previous page returned does not come back.
            cut_by_end_cursor = retain_between_cursors(
                &mut filtered_entities,
                start_position.as_ref(),
                end_position.as_ref(),
                &order,
            );
        }

        // 2. Sort entities. Candidates arrive in ascending key order, so a
        // `__key__`-only order needs no sort (or just a reverse).
        if let Some(descending) = key_only_order(&order) {
            if descending {
                filtered_entities.reverse();
            }
        } else {
            let keep = limit.map(|limit_value| {
                (offset.max(0) as usize)
                    .saturating_add(limit_value as usize)
                    .saturating_add(1)
            });
            filtered_entities = sort_candidates(filtered_entities, &order, keep);
        }

        // 3. Paginate and apply projections
        let skipped = (offset.max(0) as usize).min(filtered_entities.len());
        let skipped_cursor = skipped
            .checked_sub(1)
            .map(|last| encode_cursor(Some(&filtered_entities[last]), &order))
            .unwrap_or_default();

        let mut results = Vec::new();
        let mut current_entities_payload_size: usize = 0;
        let mut new_more_results_state = MoreResultsType::NoMoreResults;

        let entity_result_type = if is_keys_only {
            3 // KEY_ONLY
        } else if !projection.is_empty() {
            2 // PROJECTION
        } else {
            1 // FULL
        };

        for candidate in filtered_entities.iter().skip(skipped) {
            let entity_metadata = candidate.entity_metadata.as_ref();
            // Apply limit and payload size checks
            if let Some(limit_value) = limit
                && results.len() >= limit_value as usize
            {
                new_more_results_state = MoreResultsType::MoreResultsAfterLimit;
                break;
            }

            let entity_size_bytes = if is_keys_only {
                entity_metadata
                    .entity
                    .key
                    .as_ref()
                    .map_or(0, Message::encoded_len)
            } else {
                entity_metadata.entity.encoded_len()
            };
            let cursor = encode_cursor(Some(candidate), &order);
            let result_size_bytes = entity_size_bytes + cursor.len();
            if !results.is_empty()
                && (current_entities_payload_size + result_size_bytes > MAX_ENTITIES_PAYLOAD_BYTES)
            {
                new_more_results_state = MoreResultsType::NotFinished;
                break;
            }

            // Apply projection
            let result_entity = if is_keys_only {
                Entity {
                    key: entity_metadata.entity.key.clone(),
                    properties: HashMap::new(),
                }
            } else if !projection.is_empty() {
                let mut projected_properties = HashMap::new();
                for p in &projection {
                    if let Some(prop_ref) = &p.property
                        && let Some(value) = entity_metadata.entity.properties.get(&prop_ref.name)
                    {
                        projected_properties.insert(prop_ref.name.clone(), value.clone());
                    }
                }
                Entity {
                    key: entity_metadata.entity.key.clone(),
                    properties: projected_properties,
                }
            } else {
                entity_metadata.entity.clone()
            };

            let (create_time, update_time, version) = if entity_result_type == 1 {
                (
                    Some(entity_metadata.create_time.clone()),
                    Some(entity_metadata.update_time.clone()),
                    entity_metadata.version as i64,
                )
            } else {
                (None, None, 0)
            };

            results.push(EntityResult {
                entity: Some(result_entity),
                create_time,
                update_time,
                cursor,
                version,
            });
            current_entities_payload_size += result_size_bytes;
        }

        if new_more_results_state == MoreResultsType::NoMoreResults && cut_by_end_cursor {
            new_more_results_state = MoreResultsType::MoreResultsAfterCursor;
        }

        // Points after the last consumed result. An empty batch keeps the
        // position where it started.
        let end_cursor = match results.last() {
            Some(last) => last.cursor.clone(),
            None if skipped > 0 => skipped_cursor.clone(),
            None if !start_cursor.is_empty() => start_cursor,
            None => encode_cursor(None, &order),
        };

        tracing::debug!(
            "QueryResultBatch: skipped={} returned={} more_results={:?} total={} end_cursor_len={}",
            skipped,
            results.len(),
            new_more_results_state,
            filtered_entities.len(),
            end_cursor.len()
        );

        Ok(crate::google::datastore::v1::QueryResultBatch {
            entity_result_type,
            skipped_results: skipped as i32,
            read_time: None,
            skipped_cursor,
            snapshot_version: 0,
            entity_results: results,
            more_results: new_more_results_state as i32,
            end_cursor,
        })
    }

    /// Filter, order, distinct and paginate candidates for an aggregation.
    /// Unlike `get_entities_from_candidates` this neither clones entities nor
    /// applies cursors or projections.
    pub(crate) fn aggregation_candidates(
        candidates: impl Iterator<Item = QueryCandidate>,
        filter_type: Option<&FilterType>,
        candidates_prefiltered: bool,
        limit: Option<i32>,
        offset: i32,
        distinct_on: Vec<PropertyReference>,
        order: Vec<PropertyOrder>,
    ) -> Vec<QueryCandidate> {
        let prepared_key_filter = filter_type.and_then(try_prepare_key_filter);
        let limit = limit.filter(|value| *value >= 0);
        let matching = candidates
            .filter(|candidate| {
                candidate_matches_filter(
                    candidate,
                    filter_type,
                    prepared_key_filter.as_ref(),
                    candidates_prefiltered,
                )
            })
            .filter(|candidate| has_indexed_order_values(candidate, &order));
        // Candidates in result order with a limit: stop the scan early.
        let mut matching: Vec<QueryCandidate> = match limit {
            Some(limit_value)
                if distinct_on.is_empty() && key_only_order(&order) == Some(false) =>
            {
                matching
                    .take((offset.max(0) as usize).saturating_add(limit_value as usize))
                    .collect()
            }
            _ => matching.collect(),
        };

        if !distinct_on.is_empty() {
            matching = dedup_distinct_candidates(matching, &distinct_on, &order);
        }
        match key_only_order(&order) {
            Some(false) => {}
            Some(true) => matching.reverse(),
            None => {
                let keep = limit.map(|limit_value| {
                    (offset.max(0) as usize).saturating_add(limit_value as usize)
                });
                matching = sort_candidates(matching, &order, keep);
            }
        }

        let skipped = (offset.max(0) as usize).min(matching.len());
        matching.drain(..skipped);
        if let Some(limit) = limit {
            matching.truncate(limit as usize);
        }
        matching
    }

    pub fn get_entities(
        &self,
        project_id_filter: String,
        kind_name: String,
        database_id: String,
        namespace: String,
        filter: Option<Filter>,
        limit: Option<i32>,
        offset: i32,
        start_cursor: Vec<u8>,
        end_cursor: Vec<u8>,
        projection: Vec<Projection>,
        distinct_on: Vec<PropertyReference>,
        order: Vec<PropertyOrder>,
    ) -> Result<crate::google::datastore::v1::QueryResultBatch, tonic::Status> {
        let filter_type = filter.as_ref().and_then(|f| f.filter_type.clone());
        let (candidates, candidates_prefiltered) = self.query_candidates(
            &project_id_filter,
            &kind_name,
            &database_id,
            &namespace,
            filter_type.as_ref(),
        );
        Self::get_entities_from_candidates(
            candidates,
            filter_type.as_ref(),
            candidates_prefiltered,
            limit,
            offset,
            start_cursor,
            end_cursor,
            projection,
            distinct_on,
            order,
        )
    }

    pub fn update_indexes(&mut self, key_struct: &Arc<KeyStruct>, entity: &Entity) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };
        let indexes = &mut self.indexes;
        for_each_index_key_of_entity(kind, entity, &mut |index_key| {
            insert_index_entry(indexes, index_key, key_struct)
        });
    }

    pub fn remove_from_indexes(&mut self, key_struct: &KeyStruct, entity: &Entity) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };
        let indexes = &mut self.indexes;
        for_each_index_key_of_entity(kind, entity, &mut |index_key| {
            remove_index_entry(indexes, index_key, key_struct)
        });
    }

    fn add_property_indexes(
        &mut self,
        key_struct: &Arc<KeyStruct>,
        kind: &str,
        prop_name: &str,
        value: &Value,
    ) {
        let indexes = &mut self.indexes;
        for_each_index_key_of_named_property(kind, prop_name, value, &mut |index_key| {
            insert_index_entry(indexes, index_key, key_struct)
        });
    }

    fn remove_property_indexes(
        &mut self,
        key_struct: &KeyStruct,
        kind: &str,
        prop_name: &str,
        value: &Value,
    ) {
        let indexes = &mut self.indexes;
        for_each_index_key_of_named_property(kind, prop_name, value, &mut |index_key| {
            remove_index_entry(indexes, index_key, key_struct)
        });
    }

    pub fn replace_indexes(
        &mut self,
        key_struct: &Arc<KeyStruct>,
        previous_entity: &Entity,
        next_entity: &Entity,
    ) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };

        for (prop_name, previous_value) in &previous_entity.properties {
            match next_entity.properties.get(prop_name) {
                Some(next_value) if next_value == previous_value => {}
                Some(next_value) => {
                    self.remove_property_indexes(key_struct, kind, prop_name, previous_value);
                    self.add_property_indexes(key_struct, kind, prop_name, next_value);
                }
                None => {
                    self.remove_property_indexes(key_struct, kind, prop_name, previous_value);
                }
            }
        }

        for (prop_name, next_value) in &next_entity.properties {
            if !previous_entity.properties.contains_key(prop_name) {
                self.add_property_indexes(key_struct, kind, prop_name, next_value);
            }
        }
    }

    pub(crate) fn upsert_scoped_entity(
        &mut self,
        key_struct: &Arc<KeyStruct>,
        entity_metadata: Arc<EntityWithMetadata>,
    ) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };

        self.scoped_entities
            .entry((key_struct.project_id.clone(), kind.clone()))
            .or_default()
            .insert(Arc::clone(key_struct), entity_metadata);
    }

    fn add_ancestor_index_entries(
        ancestor_index: &mut HashMap<(String, String, KeyStruct), BTreeSet<Arc<KeyStruct>>>,
        key_struct: &Arc<KeyStruct>,
    ) {
        let Some((descendant_kind, _)) = key_struct.path_elements.last() else {
            return;
        };
        if key_struct.path_elements.len() <= 1 {
            return;
        }

        for ancestor_len in 1..key_struct.path_elements.len() {
            let ancestor_key = KeyStruct {
                project_id: key_struct.project_id.clone(),
                database_id: key_struct.database_id.clone(),
                namespace: key_struct.namespace.clone(),
                path_elements: key_struct.path_elements[..ancestor_len].to_vec(),
            };
            ancestor_index
                .entry((
                    key_struct.project_id.clone(),
                    descendant_kind.clone(),
                    ancestor_key,
                ))
                .or_default()
                .insert(Arc::clone(key_struct));
        }
    }

    pub(crate) fn add_ancestor_indexes(&mut self, key_struct: &Arc<KeyStruct>) {
        Self::add_ancestor_index_entries(&mut self.ancestor_index, key_struct);
    }

    fn remove_ancestor_indexes(&mut self, key_struct: &KeyStruct) {
        let Some((descendant_kind, _)) = key_struct.path_elements.last() else {
            return;
        };
        if key_struct.path_elements.len() <= 1 {
            return;
        }

        let mut empty_entries = Vec::new();
        for ancestor_len in 1..key_struct.path_elements.len() {
            let ancestor_key = KeyStruct {
                project_id: key_struct.project_id.clone(),
                database_id: key_struct.database_id.clone(),
                namespace: key_struct.namespace.clone(),
                path_elements: key_struct.path_elements[..ancestor_len].to_vec(),
            };
            let index_key = (
                key_struct.project_id.clone(),
                descendant_kind.clone(),
                ancestor_key,
            );
            if let Some(indexed_keys) = self.ancestor_index.get_mut(&index_key) {
                indexed_keys.remove(key_struct);
                if indexed_keys.is_empty() {
                    empty_entries.push(index_key);
                }
            }
        }

        for index_key in empty_entries {
            self.ancestor_index.remove(&index_key);
        }
    }

    fn remove_scoped_entity(&mut self, key_struct: &KeyStruct) {
        let Some((kind, _)) = key_struct.path_elements.last() else {
            return;
        };

        let scope_key = (key_struct.project_id.clone(), kind.clone());
        let remove_empty_scope =
            if let Some(scoped_entities) = self.scoped_entities.get_mut(&scope_key) {
                scoped_entities.remove(key_struct);
                scoped_entities.is_empty()
            } else {
                false
            };
        if remove_empty_scope {
            self.scoped_entities.remove(&scope_key);
        }
    }

    pub fn delete_entity(&mut self, key_to_delete: &Key) -> Option<Arc<EntityWithMetadata>> {
        let key_struct_to_delete = KeyStruct::from_datastore_key(key_to_delete);

        let (key_struct, removed_entity_metadata) =
            self.entities.remove_entry(&key_struct_to_delete)?;
        self.remove_from_indexes(&key_struct, &removed_entity_metadata.entity);
        self.remove_ancestor_indexes(&key_struct);
        self.remove_scoped_entity(&key_struct);
        Some(removed_entity_metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_only_candidate(id: i64) -> QueryCandidate {
        QueryCandidate {
            entity_metadata: Arc::new(EntityWithMetadata {
                entity: Entity {
                    key: Some(Key {
                        partition_id: Some(PartitionId {
                            project_id: "p".to_string(),
                            ..Default::default()
                        }),
                        path: vec![PathElement {
                            kind: "Kind".to_string(),
                            id_type: Some(IdType::Id(id)),
                        }],
                    }),
                    properties: HashMap::new(),
                },
                ..Default::default()
            }),
        }
    }

    fn int_value(value: i64) -> Value {
        Value {
            value_type: Some(ValueType::IntegerValue(value)),
            ..Default::default()
        }
    }

    fn str_value(value: &str) -> Value {
        Value {
            value_type: Some(ValueType::StringValue(value.to_string())),
            ..Default::default()
        }
    }

    fn array_value(values: Vec<Value>) -> Value {
        Value {
            value_type: Some(ValueType::ArrayValue(ArrayValue { values })),
            ..Default::default()
        }
    }

    fn candidate_with(id: i64, properties: Vec<(&str, Value)>) -> QueryCandidate {
        let candidate = key_only_candidate(id);
        let mut entity = candidate.entity_metadata.entity.clone();
        entity.properties = properties
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect();
        QueryCandidate {
            entity_metadata: Arc::new(EntityWithMetadata {
                entity,
                ..Default::default()
            }),
        }
    }

    fn order_by(name: &str, descending: bool) -> PropertyOrder {
        PropertyOrder {
            property: Some(PropertyReference {
                name: name.to_string(),
            }),
            direction: if descending {
                property_order::Direction::Descending as i32
            } else {
                property_order::Direction::Ascending as i32
            },
        }
    }

    fn ids(candidates: &[QueryCandidate]) -> Vec<i64> {
        candidates
            .iter()
            .map(
                |candidate| match candidate.entity_key().unwrap().path[0].id_type {
                    Some(IdType::Id(id)) => id,
                    _ => panic!("numeric id"),
                },
            )
            .collect()
    }

    #[test]
    fn sort_candidates_orders_by_property_then_key() {
        let candidates = vec![
            candidate_with(1, vec![("score", int_value(5))]),
            candidate_with(2, vec![("score", int_value(9))]),
            candidate_with(3, vec![("score", int_value(5))]),
            candidate_with(4, vec![("score", int_value(7))]),
        ];

        let sorted = sort_candidates(candidates.clone(), &[order_by("score", true)], None);
        assert_eq!(ids(&sorted), vec![2, 4, 1, 3]);

        let sorted = sort_candidates(
            candidates.clone(),
            &[order_by("score", false), order_by("__key__", true)],
            None,
        );
        assert_eq!(ids(&sorted), vec![3, 1, 4, 2]);

        let first_two = sort_candidates(candidates, &[order_by("score", true)], Some(2));
        assert_eq!(ids(&first_two), vec![2, 4]);
    }

    #[test]
    fn sort_candidates_uses_min_or_max_array_element_by_direction() {
        let candidates = vec![
            candidate_with(
                1,
                vec![("tags", array_value(vec![int_value(1), int_value(9)]))],
            ),
            candidate_with(
                2,
                vec![("tags", array_value(vec![int_value(4), int_value(5)]))],
            ),
        ];

        let ascending = sort_candidates(candidates.clone(), &[order_by("tags", false)], None);
        assert_eq!(ids(&ascending), vec![1, 2]);

        let descending = sort_candidates(candidates, &[order_by("tags", true)], None);
        assert_eq!(ids(&descending), vec![1, 2]);
    }

    #[test]
    fn dedup_distinct_keeps_the_first_candidate_in_query_order_per_group() {
        let candidates = vec![
            candidate_with(1, vec![("group", str_value("a")), ("score", int_value(5))]),
            candidate_with(2, vec![("group", str_value("b")), ("score", int_value(9))]),
            candidate_with(3, vec![("group", str_value("a")), ("score", int_value(7))]),
            candidate_with(4, vec![("group", str_value("b")), ("score", int_value(1))]),
        ];
        let distinct_on = vec![PropertyReference {
            name: "group".to_string(),
        }];

        let highest =
            dedup_distinct_candidates(candidates.clone(), &distinct_on, &[order_by("score", true)]);
        assert_eq!(ids(&highest), vec![2, 3]);

        let lowest =
            dedup_distinct_candidates(candidates, &distinct_on, &[order_by("score", false)]);
        assert_eq!(ids(&lowest), vec![1, 4]);
    }

    fn equals_filter(name: &str, value: Value) -> FilterType {
        FilterType::PropertyFilter(crate::google::datastore::v1::PropertyFilter {
            property: Some(PropertyReference {
                name: name.to_string(),
            }),
            op: 5,
            value: Some(value),
        })
    }

    fn entity_value(properties: Vec<(&str, Value)>) -> Value {
        Value {
            value_type: Some(ValueType::EntityValue(Entity {
                key: None,
                properties: properties
                    .into_iter()
                    .map(|(name, value)| (name.to_string(), value))
                    .collect(),
            })),
            ..Default::default()
        }
    }

    #[test]
    fn indexes_rebuilt_on_load_cover_nested_properties() {
        let mut storage = DatastoreStorage::default();
        let candidate = candidate_with(
            1,
            vec![("nested", entity_value(vec![("field", str_value("x"))]))],
        );
        storage
            .insert_entity(&candidate.entity_metadata.entity)
            .unwrap();
        let path = std::env::temp_dir().join(format!(
            "datastore-emulator-index-rebuild-{}.bin",
            uuid::Uuid::new_v4()
        ));
        storage.save_to_disk(path.to_str().unwrap()).unwrap();

        let mut loaded = DatastoreStorage::default();
        loaded.load_from_disk(path.to_str().unwrap()).unwrap();
        std::fs::remove_file(&path).ok();

        let filter = equals_filter("nested.field", str_value("x"));
        let (candidates, _) = loaded.query_candidates("p", "Kind", "", "", Some(&filter));
        assert_eq!(candidates.count(), 1);
    }

    fn key_in_database(database_id: &str, kind: &str, name: &str) -> Key {
        Key {
            partition_id: Some(PartitionId {
                project_id: "p".to_string(),
                database_id: database_id.to_string(),
                namespace_id: String::new(),
            }),
            path: vec![PathElement {
                kind: kind.to_string(),
                id_type: Some(IdType::Name(name.to_string())),
            }],
        }
    }

    fn entity_in_database(
        database_id: &str,
        kind: &str,
        name: &str,
        props: Vec<(&str, Value)>,
    ) -> Entity {
        Entity {
            key: Some(key_in_database(database_id, kind, name)),
            properties: props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    fn key_value(key: Key) -> Value {
        Value {
            value_type: Some(ValueType::KeyValue(key)),
            ..Default::default()
        }
    }

    #[test]
    fn entities_with_the_same_path_in_different_databases_are_distinct() {
        let mut storage = DatastoreStorage::default();
        storage
            .insert_entity(&entity_in_database(
                "",
                "Kind",
                "one",
                vec![("db", str_value("default"))],
            ))
            .unwrap();
        storage
            .insert_entity(&entity_in_database(
                "db1",
                "Kind",
                "one",
                vec![("db", str_value("db1"))],
            ))
            .unwrap();

        assert_eq!(storage.entities.len(), 2);
        let found = storage
            .get_entity(&key_in_database("db1", "Kind", "one"))
            .unwrap();
        assert_eq!(found.entity.properties["db"], str_value("db1"));
        assert!(
            storage
                .get_entity(&key_in_database("db2", "Kind", "one"))
                .is_none()
        );
    }

    #[test]
    fn compare_keys_distinguishes_databases() {
        let default = key_in_database("", "Kind", "one");
        let named = key_in_database("db1", "Kind", "one");
        assert_ne!(compare_keys(&default, &named), Ordering::Equal);
    }

    #[test]
    fn key_values_in_different_databases_index_differently() {
        let default = index_keys_for_value(
            "Kind",
            "ref",
            &key_value(key_in_database("", "Kind", "one")),
        );
        let named = index_keys_for_value(
            "Kind",
            "ref",
            &key_value(key_in_database("db1", "Kind", "one")),
        );
        assert_ne!(default, named);
    }

    fn names(candidates: impl Iterator<Item = QueryCandidate>) -> Vec<String> {
        candidates
            .filter_map(|c| {
                let key = c.entity_metadata.entity.key.clone()?;
                match key.path.last()?.id_type.clone()? {
                    IdType::Name(name) => Some(name),
                    IdType::Id(id) => Some(id.to_string()),
                }
            })
            .collect()
    }

    #[test]
    fn query_candidates_only_see_the_requested_database() {
        let mut storage = DatastoreStorage::default();
        storage
            .insert_entity(&entity_in_database(
                "",
                "Kind",
                "one",
                vec![("tag", str_value("x"))],
            ))
            .unwrap();
        storage
            .insert_entity(&entity_in_database(
                "db1",
                "Kind",
                "two",
                vec![("tag", str_value("x"))],
            ))
            .unwrap();

        let filter = equals_filter("tag", str_value("x"));
        let (indexed, _) = storage.query_candidates("p", "Kind", "db1", "", Some(&filter));
        assert_eq!(names(indexed), vec!["two"]);
        let (scanned, _) = storage.query_candidates("p", "Kind", "db1", "", None);
        assert_eq!(names(scanned), vec!["two"]);
        assert_eq!(
            names(storage.kindless_candidates("p", "db1", "")),
            vec!["two"]
        );
    }

    #[test]
    fn metadata_kinds_are_scoped_to_the_database_and_carry_it() {
        let mut storage = DatastoreStorage::default();
        storage
            .insert_entity(&entity_in_database("db1", "Kind", "one", vec![]))
            .unwrap();
        storage
            .insert_entity(&entity_in_database("", "Other", "one", vec![]))
            .unwrap();

        let (kinds, _) = storage.query_candidates("p", "__kind__", "db1", "", None);
        let kinds: Vec<QueryCandidate> = kinds.collect();
        let keys: Vec<&Key> = kinds
            .iter()
            .filter_map(|c| c.entity_metadata.entity.key.as_ref())
            .collect();
        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0].path[0].id_type,
            Some(IdType::Name("Kind".to_string()))
        );
        assert_eq!(keys[0].partition_id.as_ref().unwrap().database_id, "db1");
    }

    #[test]
    fn array_elements_excluded_from_indexes_are_not_indexed() {
        let mut storage = DatastoreStorage::default();
        let mut excluded = str_value("a");
        excluded.exclude_from_indexes = true;
        let candidate = candidate_with(
            1,
            vec![("tags", array_value(vec![excluded, str_value("b")]))],
        );
        storage
            .insert_entity(&candidate.entity_metadata.entity)
            .unwrap();

        let hidden = equals_filter("tags", str_value("a"));
        let (hidden, _) = storage.query_candidates("p", "Kind", "", "", Some(&hidden));
        assert_eq!(hidden.count(), 0);

        let visible = equals_filter("tags", str_value("b"));
        let (visible, _) = storage.query_candidates("p", "Kind", "", "", Some(&visible));
        assert_eq!(visible.count(), 1);
    }

    #[test]
    fn limit_without_order_stops_pulling_candidates_after_offset_plus_limit() {
        let pulled = std::cell::Cell::new(0usize);
        let candidates = (1..=1_000i64).map(|id| {
            pulled.set(pulled.get() + 1);
            key_only_candidate(id)
        });

        let batch = DatastoreStorage::get_entities_from_candidates(
            candidates,
            None,
            false,
            Some(10),
            5,
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        )
        .unwrap();

        assert_eq!(batch.skipped_results, 5);
        assert_eq!(batch.entity_results.len(), 10);
        assert_eq!(
            batch.more_results,
            MoreResultsType::MoreResultsAfterLimit as i32
        );
        // offset + limit, plus one to learn that more results exist.
        assert_eq!(pulled.get(), 16);
    }

    #[test]
    fn limit_without_order_reports_no_more_results_when_exhausted() {
        let candidates = (1..=15i64).map(key_only_candidate);

        let batch = DatastoreStorage::get_entities_from_candidates(
            candidates,
            None,
            false,
            Some(10),
            5,
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        )
        .unwrap();

        assert_eq!(batch.entity_results.len(), 10);
        assert_eq!(batch.more_results, MoreResultsType::NoMoreResults as i32);
    }
    use crate::google::datastore::import_export::datastore_v3::{
        Path as LegacyPath, Property, PropertyValue, path, property_value,
        property_value::reference_value,
    };
    use crate::google::datastore::import_export::dsbackups::{
        ExportMetadataEntry, ExportMetadataEntryKind, ExportMetadataItems,
    };
    use std::path::{Path, PathBuf};

    const LOCAL_PROJECT: &str = "local-project";
    const SOURCE_PROJECT: &str = "source-project";

    fn legacy_path(kind: &str, name: &str) -> LegacyPath {
        LegacyPath {
            element: vec![path::Element {
                r#type: kind.to_string(),
                id: None,
                name: Some(name.to_string()),
            }],
        }
    }

    fn legacy_reference(app: &str, kind: &str, name: &str) -> Reference {
        Reference {
            app: app.to_string(),
            name_space: None,
            path: legacy_path(kind, name),
        }
    }

    fn legacy_entity(app: &str, kind: &str, name: &str) -> EntityProto {
        EntityProto {
            key: legacy_reference(app, kind, name),
            entity_group: LegacyPath { element: vec![] },
            owner: None,
            kind: None,
            kind_uri: None,
            property: vec![],
            raw_property: vec![],
            rank: None,
        }
    }

    fn reference_property(name: &str, app: &str, kind: &str, entity_name: &str) -> Property {
        Property {
            meaning: None,
            meaning_uri: None,
            name: name.to_string(),
            value: PropertyValue {
                int64_value: None,
                boolean_value: None,
                string_value: None,
                double_value: None,
                pointvalue: None,
                uservalue: None,
                referencevalue: Some(property_value::ReferenceValue {
                    app: app.to_string(),
                    name_space: None,
                    pathelement: vec![reference_value::PathElement {
                        r#type: kind.to_string(),
                        id: None,
                        name: Some(entity_name.to_string()),
                    }],
                }),
            },
            multiple: false,
            searchable: Some(true),
            fts_tokenization_option: None,
            locale: None,
        }
    }

    fn legacy_string_property(name: &str, meaning: Option<i32>, value: &[u8]) -> Property {
        Property {
            meaning,
            meaning_uri: None,
            name: name.to_string(),
            value: PropertyValue {
                int64_value: None,
                boolean_value: None,
                string_value: Some(value.to_vec()),
                double_value: None,
                pointvalue: None,
                uservalue: None,
                referencevalue: None,
            },
            multiple: false,
            searchable: Some(true),
            fts_tokenization_option: None,
            locale: None,
        }
    }

    fn key_project_id(entity: &Entity) -> &str {
        &entity
            .key
            .as_ref()
            .unwrap()
            .partition_id
            .as_ref()
            .unwrap()
            .project_id
    }

    fn key_property_project_id<'a>(entity: &'a Entity, name: &str) -> &'a str {
        let Some(ValueType::KeyValue(key)) =
            entity.properties.get(name).unwrap().value_type.as_ref()
        else {
            panic!("expected key-valued property {name}");
        };
        &key.partition_id.as_ref().unwrap().project_id
    }

    fn temp_export_root() -> PathBuf {
        std::env::temp_dir().join(format!(
            "datastore-emulator-import-project-test-{}",
            uuid::Uuid::new_v4()
        ))
    }

    fn append_leveldb_full_record(record: &mut Vec<u8>, data: &[u8]) {
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(&(data.len() as u16).to_le_bytes());
        record.push(1);
        record.extend_from_slice(data);
    }

    fn write_leveldb_full_record(path: &Path, data: &[u8]) {
        let mut record = Vec::with_capacity(7 + data.len());
        append_leveldb_full_record(&mut record, data);
        std::fs::write(path, record).unwrap();
    }

    fn write_leveldb_full_records(path: &Path, records: &[&[u8]]) {
        let mut record = Vec::new();
        for data in records {
            append_leveldb_full_record(&mut record, data);
        }
        std::fs::write(path, record).unwrap();
    }

    fn write_minimal_export_with_paths(
        root: &Path,
        entity: EntityProto,
        metadata_path: &str,
        output_path: &str,
        include_items: bool,
    ) {
        let exports_dir = root.join("exports");
        let kind_dir = exports_dir.join("all_namespaces");
        std::fs::create_dir_all(&kind_dir).unwrap();

        let overall = OverallExportMetadata {
            exports: vec![ExportMetadataEntry {
                kind: Some(ExportMetadataEntryKind {
                    unknown1: 0,
                    kind: "Task".to_string(),
                    unknown2: 0,
                }),
                path: metadata_path.to_string(),
                count: 1,
                size: 0,
            }],
        };
        write_leveldb_full_record(
            &exports_dir.join("all_namespaces_kind_Task.overall_export_metadata"),
            &overall.encode_to_vec(),
        );

        let export_metadata = ExportMetadata {
            operation: None,
            items: include_items.then(|| ExportMetadataItems {
                kind: "Task".to_string(),
                outputs: vec![output_path.to_string()],
            }),
        };
        std::fs::write(
            exports_dir.join(metadata_path),
            export_metadata.encode_to_vec(),
        )
        .unwrap();

        write_leveldb_full_record(&kind_dir.join("output-0"), &entity.encode_to_vec());
    }

    fn write_minimal_export(root: &Path, entity: EntityProto) {
        write_minimal_export_with_paths(
            root,
            entity,
            "all_namespaces/kind_Task.export_metadata",
            "output-0",
            true,
        );
    }

    #[test]
    fn read_overall_metadata_skips_leading_non_metadata_record() {
        let root = temp_export_root();
        let exports_dir = root.join("exports");
        std::fs::create_dir_all(&exports_dir).unwrap();
        let overall = OverallExportMetadata {
            exports: vec![ExportMetadataEntry {
                kind: Some(ExportMetadataEntryKind {
                    unknown1: 0,
                    kind: "Task".to_string(),
                    unknown2: 0,
                }),
                path: "all_namespaces/kind_Task.export_metadata".to_string(),
                count: 1,
                size: 0,
            }],
        };
        let metadata = overall.encode_to_vec();

        write_leveldb_full_records(
            &exports_dir.join("all_namespaces_kind_Task.overall_export_metadata"),
            &[b"3".as_slice(), metadata.as_slice()],
        );

        let decoded = read_overall_metadata(root.to_str().unwrap()).unwrap();

        assert_eq!(decoded.exports.len(), 1);
        assert_eq!(
            decoded.exports[0].path,
            "all_namespaces/kind_Task.export_metadata"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn convert_property_value_preserves_utf8_legacy_string_values() {
        let prop = legacy_string_property("title", None, b"hello");

        let value = convert_property_value(&prop).unwrap();

        match value {
            ValueType::StringValue(value) => assert_eq!(value, "hello"),
            other => panic!("expected string value, got {other:?}"),
        }
    }

    #[test]
    fn convert_property_value_treats_binary_legacy_string_values_as_blob() {
        let prop = legacy_string_property("payload", Some(14), &[0xff, 0x00, 0x61]);

        let value = convert_property_value(&prop).unwrap();

        match value {
            ValueType::BlobValue(value) => assert_eq!(value, vec![0xff, 0x00, 0x61]),
            other => panic!("expected blob value, got {other:?}"),
        }
    }

    #[test]
    fn converter_dump_normalizes_legacy_app_ids_for_keys_and_reference_properties() {
        let mut entity = legacy_entity("s~source-project", "Task", "one");
        entity.property.push(reference_property(
            "owner",
            "s~source-project",
            "Owner",
            "owner-one",
        ));

        let converted = converter_dump(vec![entity]);
        let entity = &converted[0].entity;

        assert_eq!(key_project_id(entity), SOURCE_PROJECT);
        assert_eq!(key_property_project_id(entity, "owner"), SOURCE_PROJECT);
    }

    #[test]
    fn import_dump_for_project_rewrites_entity_and_reference_property_projects() {
        let root = temp_export_root();
        let mut entity = legacy_entity(SOURCE_PROJECT, "Task", "one");
        entity.property.push(reference_property(
            "owner",
            SOURCE_PROJECT,
            "Owner",
            "owner-one",
        ));
        write_minimal_export(&root, entity);

        let mut storage = DatastoreStorage::default();
        storage
            .import_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT))
            .unwrap();

        let stored_entity = &storage.entities.values().next().unwrap().entity;
        assert_eq!(storage.entities.len(), 1);
        assert_eq!(key_project_id(stored_entity), LOCAL_PROJECT);
        assert_eq!(
            key_property_project_id(stored_entity, "owner"),
            LOCAL_PROJECT
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepare_dump_for_project_rewrites_projects_without_a_storage() {
        let root = temp_export_root();
        let mut entity = legacy_entity(SOURCE_PROJECT, "Task", "one");
        entity.property.push(reference_property(
            "owner",
            SOURCE_PROJECT,
            "Owner",
            "owner-one",
        ));
        write_minimal_export(&root, entity);

        let prepared =
            prepare_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT)).unwrap();

        assert_eq!(prepared.len(), 1);
        assert_eq!(key_project_id(&prepared[0].entity), LOCAL_PROJECT);
        assert_eq!(
            key_property_project_id(&prepared[0].entity, "owner"),
            LOCAL_PROJECT
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn insert_imported_stores_prepared_entities() {
        let root = temp_export_root();
        write_minimal_export(&root, legacy_entity(SOURCE_PROJECT, "Task", "one"));
        let prepared = prepare_dump_for_project(root.to_str().unwrap(), None).unwrap();

        let mut storage = DatastoreStorage::default();
        storage.insert_imported(prepared).unwrap();

        assert_eq!(storage.entities.len(), 1);
        let stored = &storage.entities.values().next().unwrap().entity;
        assert_eq!(key_project_id(stored), SOURCE_PROJECT);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn insert_imported_rejects_an_entity_without_a_key() {
        let mut storage = DatastoreStorage::default();
        let entity = EntityWithMetadata {
            entity: Entity::default(),
            version: 1,
            create_time: pbjson_types::Timestamp::default(),
            update_time: pbjson_types::Timestamp::default(),
        };

        let err = storage.insert_imported(vec![entity]).unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(storage.entities.is_empty());
    }

    #[test]
    fn import_dump_for_project_skips_entries_with_missing_kind_metadata() {
        let root = temp_export_root();
        let exports_dir = root.join("exports");
        let kind_dir = exports_dir.join("all_namespaces");
        std::fs::create_dir_all(&kind_dir).unwrap();

        let overall = OverallExportMetadata {
            exports: vec![
                ExportMetadataEntry {
                    kind: Some(ExportMetadataEntryKind {
                        unknown1: 0,
                        kind: "ExcludedTask".to_string(),
                        unknown2: 0,
                    }),
                    path: "all_namespaces/kind_ExcludedTask.export_metadata".to_string(),
                    count: 1,
                    size: 0,
                },
                ExportMetadataEntry {
                    kind: Some(ExportMetadataEntryKind {
                        unknown1: 0,
                        kind: "Task".to_string(),
                        unknown2: 0,
                    }),
                    path: "all_namespaces/kind_Task.export_metadata".to_string(),
                    count: 1,
                    size: 0,
                },
            ],
        };
        write_leveldb_full_record(
            &exports_dir.join("all_namespaces_kind_Task.overall_export_metadata"),
            &overall.encode_to_vec(),
        );

        let export_metadata = ExportMetadata {
            operation: None,
            items: Some(ExportMetadataItems {
                kind: "Task".to_string(),
                outputs: vec!["output-0".to_string()],
            }),
        };
        std::fs::write(
            exports_dir.join("all_namespaces/kind_Task.export_metadata"),
            export_metadata.encode_to_vec(),
        )
        .unwrap();
        write_leveldb_full_record(
            &kind_dir.join("output-0"),
            &legacy_entity(SOURCE_PROJECT, "Task", "one").encode_to_vec(),
        );

        let mut storage = DatastoreStorage::default();
        storage
            .import_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT))
            .unwrap();

        assert_eq!(storage.entities.len(), 1);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn import_dump_for_project_fails_when_export_metadata_is_missing() {
        let root = temp_export_root();
        std::fs::create_dir_all(root.join("exports")).unwrap();

        let mut storage = DatastoreStorage::default();
        let status = storage
            .import_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT))
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status
                .message()
                .contains("Could not find .overall_export_metadata")
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn import_dump_for_project_fails_when_kind_metadata_items_are_missing() {
        let root = temp_export_root();
        write_minimal_export_with_paths(
            &root,
            legacy_entity(SOURCE_PROJECT, "Task", "one"),
            "all_namespaces/kind_Task.export_metadata",
            "output-0",
            false,
        );

        let mut storage = DatastoreStorage::default();
        let status = storage
            .import_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT))
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("missing items"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn import_dump_for_project_rejects_unsafe_export_metadata_path() {
        let root = temp_export_root();
        write_minimal_export_with_paths(
            &root,
            legacy_entity(SOURCE_PROJECT, "Task", "one"),
            "../kind_Task.export_metadata",
            "output-0",
            true,
        );

        let mut storage = DatastoreStorage::default();
        let status = storage
            .import_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT))
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Export metadata path"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn import_dump_for_project_rejects_unsafe_output_path() {
        let root = temp_export_root();
        write_minimal_export_with_paths(
            &root,
            legacy_entity(SOURCE_PROJECT, "Task", "one"),
            "all_namespaces/kind_Task.export_metadata",
            "../output-0",
            true,
        );

        let mut storage = DatastoreStorage::default();
        let status = storage
            .import_dump_for_project(root.to_str().unwrap(), Some(LOCAL_PROJECT))
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Export output path"));

        std::fs::remove_dir_all(root).unwrap();
    }
}
