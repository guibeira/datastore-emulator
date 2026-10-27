use crate::core::system_time_to_timestamp;
use crate::database::{DatastoreStorage, TransactionState, timestamp_key};
use crate::google::datastore::v1::{
    ReadOptions, TransactionOptions, read_options::ConsistencyType, transaction_options,
};
use parking_lot::RwLock;
use pbjson_types::Timestamp;
use std::sync::Arc;
use std::time::SystemTime;
use tonic::Status;

pub(crate) struct ReadContext {
    pub time: Option<Timestamp>,
    pub transaction: Vec<u8>,
    pub id: Option<String>,
}

pub(crate) fn validate_time(storage: &DatastoreStorage, time: &Timestamp) -> Result<(), Status> {
    let now = system_time_to_timestamp(SystemTime::now());
    if !(0..1_000_000_000).contains(&time.nanos)
        || time.nanos % 1000 != 0
        || timestamp_key(time) > timestamp_key(&now)
        || timestamp_key(time) < (now.seconds - 3600, now.nanos)
        || storage
            .history_floor
            .as_ref()
            .is_some_and(|floor| timestamp_key(time) < timestamp_key(floor))
    {
        return Err(Status::invalid_argument(
            "read_time must be available, within the past hour, and have microsecond precision",
        ));
    }
    Ok(())
}

pub(crate) fn start_transaction(
    storage: &mut DatastoreStorage,
    options: Option<&TransactionOptions>,
    project: &str,
    database: &str,
) -> Result<Vec<u8>, Status> {
    let read_only_options = options.and_then(|options| match &options.mode {
        Some(transaction_options::Mode::ReadOnly(options)) => Some(options),
        _ => None,
    });
    let time = if let Some(time) = read_only_options.and_then(|options| options.read_time.as_ref())
    {
        validate_time(storage, time)?;
        Some(*time)
    } else {
        None
    };
    let id = crate::core::next_transaction_id("tx");
    storage.transactions.insert(
        id.clone(),
        TransactionState {
            read_only: read_only_options.is_some(),
            read_time: time,
            project_id: project.to_owned(),
            database_id: database.to_owned(),
            ..Default::default()
        },
    );
    Ok(id.into_bytes())
}

pub(crate) fn context(
    storage: &Arc<RwLock<DatastoreStorage>>,
    options: Option<&ReadOptions>,
    project: &str,
    database: &str,
) -> Result<ReadContext, Status> {
    let mut result = ReadContext {
        time: None,
        transaction: Vec::new(),
        id: None,
    };
    match options.and_then(|options| options.consistency_type.as_ref()) {
        Some(ConsistencyType::ReadTime(time)) => {
            validate_time(&storage.read(), time)?;
            result.time = Some(*time);
        }
        Some(ConsistencyType::Transaction(id)) => {
            result.id = Some(
                std::str::from_utf8(id)
                    .map_err(|_| Status::invalid_argument("Invalid transaction"))?
                    .to_owned(),
            );
        }
        Some(ConsistencyType::NewTransaction(options)) => {
            result.transaction =
                start_transaction(&mut storage.write(), Some(options), project, database)?;
            result.id = Some(
                String::from_utf8(result.transaction.clone()).expect("transaction IDs are UTF-8"),
            );
        }
        _ => {}
    }
    if let Some(id) = &result.id {
        let mut storage = storage.write();
        let transaction = storage
            .transactions
            .get_mut(id)
            .ok_or_else(|| Status::invalid_argument("Invalid transaction"))?;
        if transaction.project_id != project || transaction.database_id != database {
            return Err(Status::invalid_argument(
                "Transaction belongs to a different project or database",
            ));
        }
        // A transaction without an explicit read_time takes its snapshot at
        // its first read, so writes completed before that read remain visible.
        let time = *transaction
            .read_time
            .get_or_insert_with(|| system_time_to_timestamp(SystemTime::now()));
        validate_time(&storage, &time)?;
        result.time = Some(time);
    }
    Ok(result)
}
