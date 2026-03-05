FROM rust:1.89.0-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get -y install --no-install-recommends \
  pkg-config \
  libssl-dev \
  build-essential \
  ca-certificates \
  curl \
  unzip \
  && apt-get autoclean && rm -rf /var/lib/apt/lists/*

COPY scripts/install_duckdb_libs.sh /tmp/
RUN /tmp/install_duckdb_libs.sh /opt/duckdb /opt/duckdb_extensions
ENV RUSTFLAGS="-L /opt/duckdb"

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get -y install --no-install-recommends \
    libssl3 \
    ca-certificates \
    libcurl4-openssl-dev \
    procps \
    && apt-get autoclean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/duckdb/libduckdb* /usr/lib/
COPY --from=builder /app/target/release/altertable-mock /usr/local/bin/altertable-mock

EXPOSE 15000
EXPOSE 15002
ENV RUST_LOG=info

ENTRYPOINT ["altertable-mock"]
