use crate::database::DatastoreStorage;
use crate::operation::{OperationStatus, Operations};
use chrono::Utc;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tracing;

async fn download_gcs_file(gcs_url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let (bucket, object) = gcs_url
        .strip_prefix("gs://")
        .and_then(|s| s.split_once('/'))
        .ok_or("Invalid GCS URL format")?;

    tracing::info!("Downloading gs://{}/{}...", bucket, object);

    // Create a client.
    // The `with_auth()` method tries to authenticate using multiple sources.
    let config = ClientConfig::default().with_auth().await?;
    let client = Client::new(config);

    // Get the object
    let file_bytes = client
        .download_object(
            &GetObjectRequest {
                bucket: bucket.to_string(),
                object: object.to_string(),
                ..Default::default()
            },
            &Range::default(),
        )
        .await?;

    // Create a temporary file to store the download
    let file_name = object
        .split('/')
        .next_back()
        .unwrap_or("datastore_export.zip");
    let mut temp_path = std::env::temp_dir();
    temp_path.push(file_name);

    let local_path_str = temp_path.to_str().ok_or("Invalid temp path")?.to_string();

    let mut dest = tokio::fs::File::create(&temp_path).await?;

    // Write the content to the file
    dest.write_all(&file_bytes).await?;
    dest.flush().await?;

    tracing::info!("Downloaded to {}", &local_path_str);

    Ok(local_path_str)
}

pub async fn bg_import_data(
    storage: Arc<Mutex<DatastoreStorage>>,
    operations: Operations,
    operation_id: String,
    input_url: String,
) {
    tracing::info!("Importing data from {:?} in the background...", input_url);

    let local_file_path = if input_url.starts_with("gs://") {
        match download_gcs_file(&input_url).await {
            Ok(path) => path,
            Err(e) => {
                tracing::error!("Failed to download file from GCS: {}", e);
                let mut operations = operations.lock().unwrap();
                if let Some(state) = operations.get_mut(&operation_id) {
                    state.status = OperationStatus::Failed;
                    state.end_time = Some(Utc::now());
                    state.error = Some(e.to_string());
                }
                return;
            }
        }
    } else {
        // It's a local file, check if it exists
        tracing::info!("Using local file path: {:?}", input_url);
        let path = std::path::Path::new(&input_url);
        if !path.exists() {
            tracing::warn!("File {:?} does not exist.", input_url);
            let mut operations = operations.lock().unwrap();
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Failed;
                state.end_time = Some(Utc::now());
                state.error = Some(format!("File not found: {}", input_url));
            }
            return;
        }
        input_url.clone()
    };

    let folder_name = "dump";
    // unzip the file into the dump directory at dump path as this lib create folder
    if let Ok(_) = extract_file(local_file_path.clone()) {
        tracing::info!("File {:?} extracted successfully.", local_file_path);

        let start = Instant::now();
        let mut storage = storage.lock().unwrap();
        if let Ok(_) = storage.import_dump(folder_name) {
            tracing::info!("Entities imported successfully from dump directory.");
            let mut operations = operations.lock().unwrap();
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Successful;
                state.end_time = Some(Utc::now());
            }
        } else {
            tracing::error!("Failed to import entities from dump directory.");
            let mut operations = operations.lock().unwrap();
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Failed;
                state.end_time = Some(Utc::now());
                state.error = Some("Failed to import entities".to_string());
            }
        }
        let duration = start.elapsed();
        let humanized_duration = duration.as_secs_f64();
        let minutes = humanized_duration / 60.0;
        let seconds = humanized_duration % 60.0;
        tracing::info!(
            "Imported entities in {:02}:{:02} minutes",
            minutes as u64,
            seconds as u64
        );
        // Clean up the dump directory
        if let Err(e) = std::fs::remove_dir_all(folder_name) {
            tracing::error!("Failed to clean up dump directory: {}", e);
        } else {
            tracing::info!("Dump directory cleaned up successfully.");
        }
    } else {
        tracing::error!("Failed to extract file {:?}", local_file_path);
        let mut operations = operations.lock().unwrap();
        if let Some(state) = operations.get_mut(&operation_id) {
            state.status = OperationStatus::Failed;
            state.end_time = Some(Utc::now());
            state.error = Some(format!("Failed to extract file {}", local_file_path));
        }
        return;
    }

    // Clean up downloaded file if it was from GCS
    if input_url.starts_with("gs://") {
        if let Err(e) = std::fs::remove_file(&local_file_path) {
            tracing::warn!(
                "Failed to remove temporary downloaded file {}: {}",
                local_file_path,
                e
            );
        } else {
            tracing::info!("Temporary downloaded file {} removed.", local_file_path);
        }
    }

    tracing::info!("Background import completed.");
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
