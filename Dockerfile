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
RUN cargo build --release --locked --features bundled

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get -y install --no-install-recommends \
    libssl3 \
    ca-certificates \
    libcurl4-openssl-dev \
    procps \
    && apt-get autoclean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/altertable-mock /usr/local/bin/altertable-mock

EXPOSE 15000
EXPOSE 15002
ENV RUST_LOG=info

ENTRYPOINT ["altertable-mock"]
