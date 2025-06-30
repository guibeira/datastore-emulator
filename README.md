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

See the [test](https://github.com/guibeira/datastore-emulator/blob/main/tests/README.md) instructions


## Benchmarks

Here some results with 30 clients and 10 runs each:

Operation: Single Insert
  - Rust (30 clients, 10 runs each):
    - Total time: 1.6883 seconds
    - Avg time per client: 0.0563 seconds
  - Java (30 clients, 10 runs each):
    - Total time: 95.1637 seconds
    - Avg time per client: 3.1721 seconds
  - Verdict: Rust was 56.37x faster overall.

Operation: Bulk Insert (50)
  - Rust (30 clients, 10 runs each):
    - Total time: 17.7474 seconds
    - Avg time per client: 0.5916 seconds
  - Java (30 clients, 10 runs each):
    - Total time: 375.3370 seconds
    - Avg time per client: 12.5112 seconds
  - Verdict: Rust was 21.15x faster overall.

Operation: Simple Query
  - Rust (30 clients, 10 runs each):
    - Total time: 20.5427 seconds
    - Avg time per client: 0.6848 seconds
  - Java (30 clients, 10 runs each):
    - Total time: 62.8813 seconds
    - Avg time per client: 2.0960 seconds
  - Verdict: Rust was 3.06x faster overall.


If you want to run the benchmarks yourself, run the following commands:

```bash
    docker compose up --build -d

```
install python dependencies:

```bash

    poetry install --no-root && poetry env activate

```
    and then run the benchmarks:

```bash
    python benchmark/test_benchmark.py --num-clients 30 --num-runs 10

