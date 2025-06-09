fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/");

    tonic_build::configure().build_server(true).compile(
        &[
            "proto/google/datastore/v1/datastore.proto",
            "proto/google/datastore/import_export/dsbackups.proto",
            "proto/google/datastore/import_export/datastore_v3.proto",
        ],
        &["proto"],
    )?;

    Ok(())
}
