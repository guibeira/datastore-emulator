# Datastore Emulator in Rust

This project is a custom implementation of the Google Cloud Datastore emulator, written in Rust. It is designed to be a lightweight, high-performance alternative to the official Google emulator for local development and testing.

The repository includes a comprehensive test suite that runs against both this custom emulator and the official Google Datastore emulator to ensure a high degree of API compatibility.

## Features

*   **Core Datastore API**: Supports standard gRPC operations like `Lookup`, `RunQuery`, and `Commit`.
*   **Mutations**: Full support for `Insert`, `Update`, `Upsert`, and `Delete` operations.
*   **Transactions**: Implements `BeginTransaction`, `Commit`, and `Rollback`.
*   **Querying**: Supports property filters (`=`, `!=`, `<`, `>`, etc.) and composite filters.
*   **Aggregation Queries**: Supports `COUNT`, `SUM`, and `AVG` aggregations.
*   **ID Management**: Implements `AllocateIds` and `ReserveIds`.
*   **Data Import**: An HTTP endpoint to import data from official Datastore exports.


## How to use

Make sure you have the following prerequisites installed on your system:
 * Rust toolchain (using `rustup`)
 * protobuf compiler (`protoc`)

### 2. Run the Rust Emulator
To build the Rust emulator, navigate to the project root and run:

```bash
cargo run --release
```


## Testing


### Prerequisites

- Docker & Docker Compose V2 (`docker compose`)
- Python 3.x & `pip`

### 1. Run the Emulators

```bash
docker compose up
```

The `docker-compose.yml` file orchestrates two emulator instances:

1.  **`datastore-emulator-rust`**: The custom Rust implementation.
    *   gRPC: `localhost:8042`
    *   HTTP: `localhost:8043`
2.  **`datastore-emulator-google`**: The official Google emulator.
    *   gRPC: `localhost:8044`

With the both emulators running, you can run the tests against both.

```bash
poetry run pytest
```
