# Datastore Emulator in Rust
![Crates.io Version](https://img.shields.io/crates/v/datastore-emulator)
![Docker Pulls](https://img.shields.io/docker/pulls/guibeira/datastore-emulator)
<p align="center">
  <img src="https://github.com/guibeira/datastore-emulator/blob/main/logo.png?raw=true" alt="Project's logo"/>
</p>

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
*   **HTTP REST API**: Implements the public Google Datastore REST API v1 at `POST /v1/projects/{projectId}:{method}` for `lookup`, `runQuery`, `runAggregationQuery`, `commit`, `beginTransaction`, `rollback`, `allocateIds`, `reserveIds`, and `import`. Browser clients are supported via permissive CORS.


## How to use

### Using Cargo
Make sure you have Rust and Cargo installed on your system. You can install the Rust toolchain by following the instructions at [rustup.rs](https://rustup.rs/). Then, you can install the Rust Datastore emulator using Cargo:

```bash
cargo install datastore-emulator
```

### Using Homebrew

After a stable release is published:

```bash
brew tap guibeira/datastore-emulator https://github.com/guibeira/datastore-emulator
brew install datastore-emulator
```

### Using Docker Compose
You can use the following `docker-compose.yml` file to run the Rust Datastore emulator: 

```yaml
services:
  datastore-emulator:
    image: guibeira/datastore-emulator:latest
    command: [
      "--host-port", "0.0.0.0:8042",
      "--no-store-on-disk",
    ]
    ports:
      - "8042:8042"
```

### Using the pre-built Docker image
You can run the Rust Datastore emulator using the pre-built Docker image available on Docker Hub.   

```bash
docker run -d \
  --name datastore-emulator \
  -p 8042:8042 \
  guibeira/datastore-emulator:latest \
  --host-port 0.0.0.0:8042 \
  --no-store-on-disk
```


## Building from source

Make sure you have the following prerequisites installed on your system:
 * Rust toolchain (using `rustup`)
 * protobuf compiler (`protoc`)

### Run the Rust Emulator
To build the Rust emulator, navigate to the project root and run:

```bash
cargo run --release
```


## Testing

See the [test](https://github.com/guibeira/datastore-emulator/blob/main/tests/README.md) instructions

## Releasing

1. Open GitHub Actions.
2. Run `Manual Release`.
3. Use a version without leading `v`, for example `0.1.1`.
4. Run first with `dry_run=true`.
5. If the diff is correct, run again with `dry_run=false`.
6. The pushed tag `vX.Y.Z` triggers the release pipeline.

Stable releases publish GitHub Release assets, crates.io, Homebrew and Docker Hub.
Prereleases publish GitHub Release assets and Docker prerelease tags, but skip crates.io and Homebrew.


## Benchmarks

Here are some results with 30 clients and 10 runs each:

| Operation | Total Time (Rust) | Total Time (Java) | Avg Time/Client (Rust) | Avg Time/Client (Java) | Verdict |
|---|---|---|---|---|---|
| Single Insert | 1.6883s | 95.1637s | 0.0563s | 3.1721s | Rust 56.37x faster |
| Bulk Insert (50) | 17.7474s | 375.3370s | 0.5916s | 12.5112s | Rust 21.15x faster |
| Simple Query | 20.5427s | 62.8813s | 0.6848s | 2.0960s | Rust 3.06x faster |


If you want to run the benchmarks yourself, [here](https://github.com/guibeira/datastore-emulator/tree/main/benchmark#readme) are the instructions:
