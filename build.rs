fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/");
    
    tonic_build::configure()
        .build_server(true)
        .compile(
            &["proto/google/datastore/v1/datastore.proto"],
            &["proto"],
        )?;
    
    Ok(())
}
