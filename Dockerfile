# syntax=docker/dockerfile:1.7
# The builder runs on the build machine's own architecture and cross-compiles
# for the target, so a multi-platform build never runs the compiler under
# QEMU. Dependencies are cooked by cargo-chef into their own layer, which the
# CI cache keeps until Cargo.lock changes.

FROM --platform=$BUILDPLATFORM rust:1.88.0-bookworm AS chef
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /usr/src/app

FROM chef AS planner
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY benches ./benches
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETARCH
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-gnu > /rust-target; cross=x86-64 ;; \
      arm64) echo aarch64-unknown-linux-gnu > /rust-target; cross=aarch64 ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    host="$(dpkg --print-architecture)"; \
    packages="protobuf-compiler libprotobuf-dev"; \
    if [ "$host" != "$TARGETARCH" ]; then \
      packages="$packages gcc-${cross}-linux-gnu libc6-dev-${TARGETARCH}-cross"; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends $packages; \
    rm -rf /var/lib/apt/lists/*; \
    rustup target add "$(cat /rust-target)"
# Linker and C compiler per target: the native names exist on a native host too.
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar
COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN cargo chef cook --release --locked --target "$(cat /rust-target)" --recipe-path recipe.json
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY benches ./benches
RUN cargo build --release --locked --target "$(cat /rust-target)" \
    && install -D "target/$(cat /rust-target)/release/datastore-emulator" /out/datastore-emulator

FROM gcr.io/distroless/cc-debian12
COPY --from=builder /out/datastore-emulator /usr/local/bin/datastore-emulator
EXPOSE 8042
ENTRYPOINT ["/usr/local/bin/datastore-emulator"]
CMD ["--host-port", "0.0.0.0:8042", "--no-store-on-disk"]
