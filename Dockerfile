FROM rust:1.89.0-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get -y install --no-install-recommends \
  pkg-config \
  libssl-dev \
  build-essential \
  ca-certificates \
  && apt-get autoclean && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
# duckdb-rs caches the shared library under target/duckdb-download, not target/release/deps.
RUN cargo build --release --locked \
 && find target/duckdb-download -type f -name libduckdb.so -exec cp {} /tmp/libduckdb.so \; \
 && test -s /tmp/libduckdb.so

FROM debian:bookworm-slim

ARG TARGETARCH

WORKDIR /app

RUN apt-get update && apt-get -y install --no-install-recommends \
    libssl3 \
    ca-certificates \
    libcurl4-openssl-dev \
    libstdc++6 \
    procps \
    curl \
    gzip \
    && apt-get autoclean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/libduckdb.so /usr/local/lib/libduckdb.so
RUN ldconfig

COPY --from=builder /app/target/release/altertable-mock /usr/local/bin/altertable-mock

# Install DuckLake and semantic_layer for the linked DuckDB version into the default extension directory.
# The DuckDB CLI installs them so it also writes the `.info` files: without one recording the
# community repository, DuckDB rejects semantic_layer's community signature on LOAD.
RUN set -eu; \
    case "${TARGETARCH:-}" in \
      amd64|arm64) arch="${TARGETARCH}" ;; \
      "") \
        case "$(uname -m)" in \
          x86_64) arch=amd64 ;; \
          aarch64|arm64) arch=arm64 ;; \
          *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;; \
        esac ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-${arch}.tar.gz" \
      | tar -xz -C /tmp duckdb; \
    /tmp/duckdb -c "INSTALL ducklake; INSTALL semantic_layer FROM community;"; \
    rm /tmp/duckdb

EXPOSE 15000
EXPOSE 15002
ENV RUST_LOG=info

ENTRYPOINT ["altertable-mock"]
