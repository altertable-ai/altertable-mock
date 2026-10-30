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
RUN set -eu; \
    case "${TARGETARCH:-}" in \
      amd64) platform=linux_amd64 ;; \
      arm64) platform=linux_arm64 ;; \
      "") \
        case "$(uname -m)" in \
          x86_64) platform=linux_amd64 ;; \
          aarch64|arm64) platform=linux_arm64 ;; \
          *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;; \
        esac ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    dir="/root/.duckdb/extensions/v2.0.0/${platform}"; \
    mkdir -p "$dir"; \
    curl -fsSL "http://extensions.duckdb.org/v2.0.0/${platform}/ducklake.duckdb_extension.gz" \
      | gzip -dc > "$dir/ducklake.duckdb_extension"; \
    curl -fsSL "http://community-extensions.duckdb.org/v2.0.0/${platform}/semantic_layer.duckdb_extension.gz" \
      | gzip -dc > "$dir/semantic_layer.duckdb_extension"

EXPOSE 15000
EXPOSE 15002
ENV RUST_LOG=info

ENTRYPOINT ["altertable-mock"]
