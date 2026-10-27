use crate::google::datastore::v1::{
    ArrayValue, Entity, Mutation, Value, mutation::Operation, property_transform::TransformType,
    value::ValueType,
};
use pbjson_types::Timestamp;
use std::cmp::Ordering;
use std::collections::HashMap;
use tonic::Status;

fn invalid(message: &str) -> Status {
    Status::invalid_argument(message.to_owned())
}

/// Datastore paths escape literal dots and backslashes with a backslash.
fn path_parts(path: &str) -> Result<Vec<String>, Status> {
    let mut parts = vec![String::new()];
    let mut chars = path.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => match chars.next() {
                Some(escaped @ ('.' | '\\')) => parts.last_mut().unwrap().push(escaped),
                _ => return Err(invalid("Invalid property path escape")),
            },
            '.' => parts.push(String::new()),
            _ => parts.last_mut().unwrap().push(ch),
        }
    }
    if parts.iter().any(|part| part.is_empty()) {
        return Err(invalid("Property paths must not be empty"));
    }
    if parts
        .iter()
        .any(|part| part.starts_with("__") && part.ends_with("__") && part != "__key__")
    {
        return Err(invalid("Property path contains a reserved name"));
    }
    Ok(parts)
}

fn get_property<'a>(entity: &'a Entity, path: &[String]) -> Result<Option<&'a Value>, Status> {
    let Some(value) = entity.properties.get(&path[0]) else {
        return Ok(None);
    };
    if path.len() == 1 {
        return Ok(Some(value));
    }
    match value.value_type.as_ref() {
        Some(ValueType::EntityValue(nested)) => get_property(nested, &path[1..]),
        Some(ValueType::ArrayValue(_)) => Err(invalid("Property paths cannot traverse arrays")),
        _ => Ok(None),
    }
}

fn set_property(entity: &mut Entity, path: &[String], value: Option<Value>) -> Result<(), Status> {
    if path.len() == 1 {
        match value {
            Some(value) => {
                entity.properties.insert(path[0].clone(), value);
            }
            None => {
                entity.properties.remove(&path[0]);
            }
        }
        return Ok(());
    }
    if !entity.properties.contains_key(&path[0]) && value.is_none() {
        return Ok(());
    }
    let parent = entity.properties.entry(path[0].clone()).or_default();
    if matches!(parent.value_type, Some(ValueType::ArrayValue(_))) {
        return Err(invalid("Property paths cannot traverse arrays"));
    }
    if !matches!(parent.value_type, Some(ValueType::EntityValue(_))) {
        if value.is_none() {
            return Ok(());
        }
        parent.value_type = Some(ValueType::EntityValue(Entity::default()));
    }
    let Some(ValueType::EntityValue(nested)) = parent.value_type.as_mut() else {
        unreachable!()
    };
    set_property(nested, &path[1..], value)
}

fn number(value: &Value) -> Option<f64> {
    match value.value_type.as_ref()? {
        ValueType::IntegerValue(n) => Some(*n as f64),
        ValueType::DoubleValue(n) => Some(*n),
        _ => None,
    }
}

// Compare an integer with a double without rounding integers above 2^53.
fn compare_int_double(integer: i64, double: f64) -> Option<Ordering> {
    if double.is_nan() {
        return None;
    }
    if double >= -(i64::MIN as f64) {
        return Some(Ordering::Less);
    }
    if double < i64::MIN as f64 {
        return Some(Ordering::Greater);
    }
    Some(integer.cmp(&(double as i64)).then_with(|| {
        if double.fract() > 0.0 {
            Ordering::Less
        } else if double.fract() < 0.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }))
}

fn compare_numbers(a: &Value, b: &Value) -> Option<Ordering> {
    match (a.value_type.as_ref()?, b.value_type.as_ref()?) {
        (ValueType::IntegerValue(a), ValueType::IntegerValue(b)) => Some(a.cmp(b)),
        (ValueType::IntegerValue(a), ValueType::DoubleValue(b)) => compare_int_double(*a, *b),
        (ValueType::DoubleValue(a), ValueType::IntegerValue(b)) => {
            compare_int_double(*b, *a).map(Ordering::reverse)
        }
        (ValueType::DoubleValue(a), ValueType::DoubleValue(b)) => a.partial_cmp(b),
        _ => None,
    }
}

fn equivalent(a: &Value, b: &Value) -> bool {
    if let (Some(a_number), Some(b_number)) = (number(a), number(b)) {
        return (a_number.is_nan() && b_number.is_nan())
            || compare_numbers(a, b) == Some(Ordering::Equal);
    }
    match (a.value_type.as_ref(), b.value_type.as_ref()) {
        (Some(ValueType::ArrayValue(a)), Some(ValueType::ArrayValue(b))) => {
            a.values.len() == b.values.len()
                && a.values
                    .iter()
                    .zip(&b.values)
                    .all(|(a, b)| equivalent(a, b))
        }
        (Some(ValueType::EntityValue(a)), Some(ValueType::EntityValue(b))) => {
            a.key == b.key
                && a.properties.len() == b.properties.len()
                && a.properties
                    .iter()
                    .all(|(key, a)| b.properties.get(key).is_some_and(|b| equivalent(a, b)))
        }
        _ => a.value_type == b.value_type,
    }
}

fn transformed(
    current: Option<&Value>,
    transform: &TransformType,
    time: &Timestamp,
) -> Result<(Value, Value), Status> {
    let mut value = current.cloned().unwrap_or_default();
    let value_type = match transform {
        TransformType::SetToServerValue(1) => ValueType::TimestampValue(Timestamp {
            seconds: time.seconds,
            nanos: time.nanos / 1_000_000 * 1_000_000,
        }),
        TransformType::SetToServerValue(_) => {
            return Err(invalid("Invalid server value transform"));
        }
        TransformType::Increment(operand)
        | TransformType::Maximum(operand)
        | TransformType::Minimum(operand) => {
            let rhs = number(operand)
                .ok_or_else(|| invalid("Numeric transforms require an integer or double"))?;
            match current.and_then(number) {
                None => operand.value_type.clone().unwrap(),
                Some(lhs) => match transform {
                    TransformType::Increment(_) => {
                        match (value.value_type.as_ref(), operand.value_type.as_ref()) {
                            (
                                Some(ValueType::IntegerValue(a)),
                                Some(ValueType::IntegerValue(b)),
                            ) => ValueType::IntegerValue(a.saturating_add(*b)),
                            _ => ValueType::DoubleValue(lhs + rhs),
                        }
                    }
                    _ if lhs.is_nan() || rhs.is_nan() => ValueType::DoubleValue(f64::NAN),
                    TransformType::Maximum(_)
                        if compare_numbers(&value, operand) == Some(Ordering::Less) =>
                    {
                        operand.value_type.clone().unwrap()
                    }
                    TransformType::Minimum(_)
                        if compare_numbers(&value, operand) == Some(Ordering::Greater) =>
                    {
                        operand.value_type.clone().unwrap()
                    }
                    _ => value.value_type.clone().unwrap(),
                },
            }
        }
        TransformType::AppendMissingElements(operand)
        | TransformType::RemoveAllFromArray(operand) => {
            let mut array = match value.value_type.take() {
                Some(ValueType::ArrayValue(array)) => array,
                _ => ArrayValue::default(),
            };
            if matches!(transform, TransformType::AppendMissingElements(_)) {
                for item in &operand.values {
                    if !array.values.iter().any(|stored| equivalent(stored, item)) {
                        array.values.push(item.clone());
                    }
                }
            } else {
                array
                    .values
                    .retain(|stored| !operand.values.iter().any(|item| equivalent(stored, item)));
            }
            value.value_type = Some(ValueType::ArrayValue(array));
            return Ok((
                value,
                Value {
                    value_type: Some(ValueType::NullValue(0)),
                    ..Default::default()
                },
            ));
        }
    };
    value.value_type = Some(value_type);
    Ok((value.clone(), value))
}

/// Materialize the write before it reaches storage. Masks run before transforms.
pub(crate) fn prepare(
    mutation: &mut Mutation,
    previous: Option<&Entity>,
    time: &Timestamp,
) -> Result<Vec<Value>, Status> {
    let Some(operation) = mutation.operation.as_mut() else {
        return Err(invalid("Mutation requires an operation"));
    };
    let entity = match operation {
        Operation::Insert(entity) | Operation::Update(entity) | Operation::Upsert(entity) => entity,
        Operation::Delete(_) => {
            if !mutation.property_transforms.is_empty() {
                return Err(invalid("Delete mutations cannot have property transforms"));
            }
            return Ok(Vec::new());
        }
    };
    if let Some(mask) = &mutation.property_mask {
        let input = std::mem::replace(
            entity,
            Entity {
                key: entity.key.clone(),
                properties: previous.map_or_else(HashMap::new, |entity| entity.properties.clone()),
            },
        );
        for path in &mask.paths {
            let path = path_parts(path)?;
            if path == ["__key__"] {
                continue;
            }
            if path.iter().any(|part| part == "__key__") {
                return Err(invalid("Invalid key property path"));
            }
            set_property(entity, &path, get_property(&input, &path)?.cloned())?;
        }
    }
    let mut results = Vec::with_capacity(mutation.property_transforms.len());
    for transform in &mutation.property_transforms {
        let path = path_parts(&transform.property)?;
        if path.iter().any(|part| part == "__key__") {
            return Err(invalid("Cannot transform the entity key"));
        }
        let transform_type = transform
            .transform_type
            .as_ref()
            .ok_or_else(|| invalid("Missing property transform type"))?;
        let (value, result) = transformed(get_property(entity, &path)?, transform_type, time)?;
        set_property(entity, &path, Some(value))?;
        results.push(result);
    }
    Ok(results)
}
