use prost_wkt_build::{FileDescriptorSet, Message, add_serde};
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        // Derive Serde for your own messages
        .type_attribute(".", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute(".", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.Query", "#[serde(default)]")
        .type_attribute("google.datastore.v1.RunQueryRequest.QueryType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.RunQueryRequest.QueryType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.RunAggregationQueryRequest.QueryType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.RunAggregationQueryRequest.QueryType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.AggregationQuery.QueryType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.AggregationQuery.QueryType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.TransactionOptions.Mode", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.TransactionOptions.Mode", "#[serde(rename_all = \"camelCase\")]")
        .field_attribute("google.datastore.v1.Key.PathElement.id_type", "#[serde(flatten)]")
        .type_attribute("google.datastore.v1.Key.PathElement.IdType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.Key.PathElement.IdType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.Value.ValueType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.Value.ValueType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.ReadOptions.ConsistencyType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.ReadOptions.ConsistencyType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.Mutation.Operation", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.Mutation.Operation", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.Mutation.ConflictDetectionStrategy", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.Mutation.ConflictDetectionStrategy", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.GqlQueryParameter.ParameterType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.GqlQueryParameter.ParameterType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.Filter.FilterType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.Filter.FilterType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.PropertyTransform.TransformType", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.PropertyTransform.TransformType", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.AggregationQuery.Aggregation.Operator", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.AggregationQuery.Aggregation.Operator", "#[serde(rename_all = \"camelCase\")]")
        .type_attribute("google.datastore.v1.CommitRequest.TransactionSelector", "#[derive(serde::Serialize,serde::Deserialize)]")
        .type_attribute("google.datastore.v1.CommitRequest.TransactionSelector", "#[serde(rename_all = \"camelCase\")]")
        // Map WKTs to prost-wkt-types versions
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Value", "::prost_wkt_types::Value")
        .extern_path(".google.protobuf.Struct", "::prost_wkt_types::Struct")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .file_descriptor_set_path(out_dir.join("descriptor.bin"))
        .compile_protos(
            &[
                "proto/google/datastore/v1/datastore.proto",
                "proto/google/datastore/import_export/dsbackups.proto",
                "proto/google/datastore/import_export/datastore_v3.proto",
                "proto/google/type/latlng.proto",
            ],
            &["proto/"],
        )?;

    // Finally, inject Serde impls into prost-wkt-types
    let bytes = std::fs::read(out_dir.join("descriptor.bin"))?;
    let fds = FileDescriptorSet::decode(&bytes[..])?;
    add_serde(out_dir, fds);

    Ok(())
}
