use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        PathBuf::from("proto/google/datastore/v1/datastore.proto"),
        PathBuf::from("proto/google/datastore/import_export/datastore_v3.proto"),
        PathBuf::from("proto/google/datastore/import_export/dsbackups.proto"),
    ];

    let mut includes = vec![PathBuf::from("proto")];
    println!("cargo:rerun-if-env-changed=PROTOC_INCLUDE");

    if let Ok(path) = std::env::var("PROTOC_INCLUDE") {
        includes.push(PathBuf::from(path));
    }

    for path in [
        "/usr/include",
        "/usr/local/include",
        "/opt/homebrew/include",
    ] {
        let include_path = Path::new(path);
        if include_path
            .join("google/protobuf/timestamp.proto")
            .exists()
        {
            includes.push(include_path.to_path_buf());
        }
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("descriptor.bin");

    tonic_build::configure()
        .build_server(true)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .file_descriptor_set_path(&descriptor_path)
        .compile(&protos, &includes)?;

    let descriptor_set = std::fs::read(&descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[".google.datastore.v1"])?;

    Ok(())
}
