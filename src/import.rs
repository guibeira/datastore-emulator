use crate::database::DatastoreStorage;
use std::sync::{Arc, Mutex};
use std::time::Instant;

pub async fn bg_import_data(storage: Arc<Mutex<DatastoreStorage>>, input_url: String) {
    // only supports zip files at same machine
    println!("Importing data from {:?} in the background...", input_url);
    // check if the file exists,
    // todo: download the file if it does not exist
    let path = std::path::Path::new(&input_url);
    if !path.exists() {
        println!("File {:?} does not exist.", input_url);
        return;
    }
    let folder_name = "dump";
    // unzip the file into the dump directory at dump path as this lib create folder
    if let Ok(_) = extract_file(input_url.clone()) {
        println!("File {:?} extracted successfully.", input_url);

        let start = Instant::now();
        let mut storage = storage.lock().unwrap();
        if let Ok(_) = storage.import_dump(folder_name) {
            println!("Entities imported successfully from dump directory.");
        } else {
            eprintln!("Failed to import entities from dump directory.");
        }
        let duration = start.elapsed();
        let humanized_duration = duration.as_secs_f64();
        let minutes = humanized_duration / 60.0;
        let seconds = humanized_duration % 60.0;
        println!(
            "Imported entities in {:02}:{:02} minutes",
            minutes as u64, seconds as u64
        );
        // Clean up the dump directory
        if let Err(e) = std::fs::remove_dir_all(folder_name) {
            eprintln!("Failed to clean up dump directory: {}", e);
        } else {
            println!("Dump directory cleaned up successfully.");
        }
    } else {
        println!("Failed to extract file {:?}", input_url);
        return;
    }

    println!("Background import completed.");
}

fn extract_file(input_url: String) -> Result<(), Box<dyn std::error::Error>> {
    let dump_path = std::path::Path::new("dump");
    if !dump_path.exists() {
        std::fs::create_dir_all(dump_path).expect("Failed to create dump directory");
    }
    let zip_file = std::fs::File::open(&input_url).expect("Failed to open zip file");
    let mut archive = zip::ZipArchive::new(zip_file).expect("Failed to read zip file");
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).expect("Failed to read file from zip");
        let outpath = dump_path.join(file.mangled_name());
        if file.name().ends_with('/') {
            std::fs::create_dir_all(&outpath).expect("Failed to create directory");
        } else {
            if let Some(p) = outpath.parent() {
                std::fs::create_dir_all(p).expect("Failed to create parent directory");
            }
            let mut outfile =
                std::fs::File::create(&outpath).expect("Failed to create output file");
            std::io::copy(&mut file, &mut outfile).expect("Failed to copy file contents");
        }
    }
    Ok(())
}
