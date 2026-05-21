use crate::database::DatastoreStorage;
use crate::operation::{OperationStatus, Operations};
use chrono::Utc;
use futures::StreamExt;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use std::sync::Arc;
use std::time::Instant;
use tokio::{io::AsyncWriteExt, sync::RwLock, task};
use tracing;

async fn download_gcs_file(
    gcs_url: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
    let mut stream = client
        .download_streamed_object(
            &GetObjectRequest {
                bucket: bucket.to_string(),
                object: object.to_string(),
                ..Default::default()
            },
            &Range::default(),
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

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
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        dest.write_all(&chunk).await?;
    }
    dest.flush().await?;

    tracing::info!("Downloaded to {}", &local_path_str);

    Ok(local_path_str)
}

pub async fn bg_import_data(
    storage: Arc<RwLock<DatastoreStorage>>,
    operations: Operations,
    operation_id: String,
    input_url: String,
    target_project_id: Option<String>,
) {
    tracing::info!("Importing data from {:?} in the background...", input_url);

    let downloaded_from_gcs = input_url.starts_with("gs://");
    let local_file_path = if downloaded_from_gcs {
        match download_gcs_file(&input_url).await {
            Ok(path) => path,
            Err(e) => {
                tracing::error!("Failed to download file from GCS: {}", e);
                let mut operations = operations.write().await;
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
            let mut operations = operations.write().await;
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Failed;
                state.end_time = Some(Utc::now());
                state.error = Some(format!("File not found: {}", input_url));
            }
            return;
        }
        input_url.clone()
    };

    let cleanup_downloaded_file = |path: String| async move {
        let cleanup_path = path.clone();
        match task::spawn_blocking(move || std::fs::remove_file(&cleanup_path)).await {
            Ok(Ok(())) => {
                tracing::info!("Temporary downloaded file {} removed.", path);
            }
            Ok(Err(e)) => {
                tracing::warn!("Failed to remove temporary downloaded file {}: {}", path, e);
            }
            Err(join_err) => {
                tracing::warn!(
                    "Cleanup task for temporary file {} failed: {}",
                    path,
                    join_err
                );
            }
        }
    };

    let prepare_result = task::spawn_blocking({
        let path = local_file_path.clone();
        move || prepare_export_dir(path)
    })
    .await;

    let (export_dir, extracted_dump) = match prepare_result {
        Ok(Ok(result)) => {
            tracing::info!(
                "Prepared export dir {:?} from input {:?} (extracted_dump={})",
                result.0,
                local_file_path,
                result.1
            );
            result
        }
        Ok(Err(e)) => {
            tracing::error!(
                "Failed to prepare export dir from {:?}: {}",
                local_file_path,
                e
            );
            let mut operations = operations.write().await;
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Failed;
                state.end_time = Some(Utc::now());
                state.error = Some(format!(
                    "Failed to prepare export dir from {}: {}",
                    local_file_path, e
                ));
            }
            if downloaded_from_gcs {
                cleanup_downloaded_file(local_file_path.clone()).await;
            }
            return;
        }
        Err(join_err) => {
            tracing::error!(
                "Preparation task panicked for {:?}: {}",
                local_file_path,
                join_err
            );
            let mut operations = operations.write().await;
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Failed;
                state.end_time = Some(Utc::now());
                state.error = Some("Background preparation task failed".to_string());
            }
            if downloaded_from_gcs {
                cleanup_downloaded_file(local_file_path.clone()).await;
            }
            return;
        }
    };

    let start = Instant::now();
    let import_result = {
        let mut storage_guard = storage.write().await;
        storage_guard.import_dump_for_project(&export_dir, target_project_id.as_deref())
    };

    let duration = start.elapsed();
    let humanized_duration = duration.as_secs_f64();
    let minutes = humanized_duration / 60.0;
    let seconds = humanized_duration % 60.0;

    match import_result {
        Ok(_) => {
            tracing::info!("Entities imported successfully from dump directory.");
            tracing::info!(
                "Import completed in {:02}:{:02} minutes",
                minutes as u64,
                seconds as u64
            );
            let mut operations = operations.write().await;
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Successful;
                state.end_time = Some(Utc::now());
            }
        }
        Err(status) => {
            tracing::error!("Failed to import entities from dump directory: {}", status);
            tracing::info!(
                "Import attempt finished in {:02}:{:02} minutes",
                minutes as u64,
                seconds as u64
            );
            let mut operations = operations.write().await;
            if let Some(state) = operations.get_mut(&operation_id) {
                state.status = OperationStatus::Failed;
                state.end_time = Some(Utc::now());
                state.error = Some("Failed to import entities".to_string());
            }
        }
    }

    if extracted_dump {
        let folder = export_dir.clone();
        match task::spawn_blocking(move || std::fs::remove_dir_all(&folder)).await {
            Ok(Ok(())) => {
                tracing::info!("Dump directory cleaned up successfully.");
            }
            Ok(Err(e)) => {
                tracing::error!("Failed to clean up dump directory: {}", e);
            }
            Err(join_err) => {
                tracing::error!("Cleanup task for dump directory failed: {}", join_err);
            }
        }
    }

    if downloaded_from_gcs {
        cleanup_downloaded_file(local_file_path.clone()).await;
    }

    tracing::info!("Background import completed.");
}

/// Inspect `input_url` and return `(export_dir, extracted_dump)`.
///
/// `export_dir` is a path suitable for `DatastoreStorage::import_dump`, i.e. it
/// satisfies `<export_dir>/exports/<name>.overall_export_metadata`.
///
/// `extracted_dump` is `true` when this function extracted a zip archive into a
/// temporary directory and the caller is expected to clean it up.
///
/// Accepted input formats:
///   * A zip archive (`*.zip` or magic-byte detected) that contains an
///     `exports/<name>.overall_export_metadata` tree at the archive root.
///   * A `.overall_export_metadata` LevelDB log file. The export root is the
///     grandparent directory (metadata files live in `<root>/exports/`).
///   * A directory that either already is the export root or contains the
///     metadata file directly.
fn prepare_export_dir(
    input_url: String,
) -> Result<(String, bool), Box<dyn std::error::Error + Send + Sync>> {
    let path = std::path::Path::new(&input_url);

    if !path.exists() {
        return Err(format!("Input path does not exist: {}", input_url).into());
    }

    if path.is_file() {
        if input_url.ends_with(".overall_export_metadata") {
            let exports_dir = path
                .parent()
                .ok_or("Metadata file has no parent directory")?;
            let export_root = exports_dir
                .parent()
                .ok_or("Metadata file parent has no parent directory")?;
            return Ok((export_root.to_string_lossy().into_owned(), false));
        }

        if is_zip_file(path)? {
            let dump_path = unique_dump_dir();
            if let Err(e) = extract_zip_to_dir(path, &dump_path) {
                let _ = std::fs::remove_dir_all(&dump_path);
                return Err(e);
            }
            return Ok((dump_path.to_string_lossy().into_owned(), true));
        }

        return Err(format!(
            "Unsupported file input (expected .zip or .overall_export_metadata): {}",
            input_url
        )
        .into());
    }

    if path.is_dir() {
        let exports_subdir = path.join("exports");
        if exports_subdir.is_dir() && contains_overall_metadata(&exports_subdir)? {
            return Ok((input_url, false));
        }
        if contains_overall_metadata(path)? {
            let parent = path
                .parent()
                .ok_or("Export directory has no parent directory")?;
            return Ok((parent.to_string_lossy().into_owned(), false));
        }
        return Err(format!(
            "Directory {} does not contain a *.overall_export_metadata file",
            input_url
        )
        .into());
    }

    Err(format!("Unsupported input path: {}", input_url).into())
}

fn is_zip_file(path: &std::path::Path) -> Result<bool, std::io::Error> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut magic = [0u8; 4];
    let read = file.read(&mut magic)?;
    Ok(read == 4 && &magic == b"PK\x03\x04")
}

fn contains_overall_metadata(dir: &std::path::Path) -> Result<bool, std::io::Error> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".overall_export_metadata")
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn unique_dump_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("datastore-emulator-dump-{}", uuid::Uuid::new_v4()))
}

fn extract_zip_to_dir(
    zip_path: &std::path::Path,
    dump_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if dump_path.exists() {
        std::fs::remove_dir_all(dump_path)?;
    }
    std::fs::create_dir_all(dump_path)?;

    let zip_file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(zip_file)?;
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = dump_path.join(file.mangled_name());
        if file.name().ends_with('/') {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(p) = outpath.parent() {
                std::fs::create_dir_all(p)?;
            }
            let mut outfile = std::fs::File::create(&outpath)?;
            std::io::copy(&mut file, &mut outfile)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    fn temp_export_root() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "datastore-emulator-import-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("exports")).unwrap();
        std::fs::write(
            root.join("exports")
                .join("all_namespaces_kind_Task.overall_export_metadata"),
            b"",
        )
        .unwrap();
        root
    }

    fn assert_prepared_path_eq(actual: String, expected: &Path) {
        assert_eq!(PathBuf::from(actual), expected);
    }

    fn temp_zip_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "datastore-emulator-import-test-{}.zip",
            uuid::Uuid::new_v4()
        ))
    }

    fn write_export_zip(path: &Path) {
        let file = std::fs::File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        zip.start_file(
            "exports/all_namespaces_kind_Task.overall_export_metadata",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
        zip.write_all(b"metadata").unwrap();
        zip.finish().unwrap();
    }

    #[test]
    fn prepare_export_dir_accepts_export_root_directory() {
        let root = temp_export_root();

        let (export_dir, extracted_dump) =
            prepare_export_dir(root.to_string_lossy().into_owned()).unwrap();

        assert_prepared_path_eq(export_dir, &root);
        assert!(!extracted_dump);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepare_export_dir_accepts_exports_directory() {
        let root = temp_export_root();
        let exports_dir = root.join("exports");

        let (export_dir, extracted_dump) =
            prepare_export_dir(exports_dir.to_string_lossy().into_owned()).unwrap();

        assert_prepared_path_eq(export_dir, &root);
        assert!(!extracted_dump);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepare_export_dir_accepts_overall_metadata_file() {
        let root = temp_export_root();
        let metadata_file = root
            .join("exports")
            .join("all_namespaces_kind_Task.overall_export_metadata");

        let (export_dir, extracted_dump) =
            prepare_export_dir(metadata_file.to_string_lossy().into_owned()).unwrap();

        assert_prepared_path_eq(export_dir, &root);
        assert!(!extracted_dump);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn prepare_export_dir_extracts_zip_to_unique_temp_directory() {
        let zip_path = temp_zip_path();
        write_export_zip(&zip_path);

        let (first_dir, first_extracted) =
            prepare_export_dir(zip_path.to_string_lossy().into_owned()).unwrap();
        let (second_dir, second_extracted) =
            prepare_export_dir(zip_path.to_string_lossy().into_owned()).unwrap();

        let first_path = PathBuf::from(&first_dir);
        let second_path = PathBuf::from(&second_dir);
        assert!(first_extracted);
        assert!(second_extracted);
        assert_ne!(first_path, PathBuf::from("dump"));
        assert_ne!(first_path, second_path);
        assert!(
            first_path
                .join("exports")
                .join("all_namespaces_kind_Task.overall_export_metadata")
                .exists()
        );
        assert!(
            second_path
                .join("exports")
                .join("all_namespaces_kind_Task.overall_export_metadata")
                .exists()
        );

        std::fs::remove_dir_all(first_path).unwrap();
        std::fs::remove_dir_all(second_path).unwrap();
        std::fs::remove_file(zip_path).unwrap();
    }
}
