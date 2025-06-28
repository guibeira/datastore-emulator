# Stage 1: Build the application
FROM rust:1.88.0-bullseye as builder

# Install proto compiler
RUN apt-get update && apt-get install -y protobuf-compiler

# Create working directory
WORKDIR /usr/src/app

# Copy source code to the container
COPY . .

# Build the application in release mode for optimization
RUN cargo build --release

# Stage 2: Create the final and smaller image
FROM debian:bullseye-slim

# Copy the compiled binary from the build stage
COPY --from=builder /usr/src/app/target/release/datastore-emulator /usr/local/bin/datastore-emulator-rust

# Expose the gRPC and HTTP ports used by the application
EXPOSE 8042
EXPOSE 8043

# Set the entry point to execute the binary.
# Arguments (host and port) will be provided by docker-compose.yml.
ENTRYPOINT ["/usr/local/bin/datastore-emulator-rust"]

