FROM rust:1.88.0-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY benches ./benches

RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/datastore-emulator /usr/local/bin/datastore-emulator

EXPOSE 8042

ENTRYPOINT ["/usr/local/bin/datastore-emulator"]
CMD ["--host-port", "0.0.0.0:8042", "--no-store-on-disk"]
