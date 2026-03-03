# altertable-mock

A minimal mock server that implements the [Altertable](https://altertable.ai) Arrow Flight SQL API, backed by an in-memory [DuckDB](https://duckdb.org) instance. Intended for local development and testing.

## What it is

Altertable exposes a [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) (gRPC) interface. This server speaks the same protocol and behaves similarly, without requiring a real Altertable account or any external infrastructure.

Each authenticated user gets their own isolated in-memory DuckDB database. Sessions within a user share that database. The server supports:

- Ad-hoc SQL queries and DML statements
- Prepared statements with parameter binding
- Bulk Arrow record batch ingestion (including upsert / MERGE semantics)
- Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
- Schema introspection (catalogs, schemas, tables, SQL info)

## Usages

- Run your integration tests against a real Flight SQL endpoint without hitting production
- Develop locally without network access or credentials
- Deterministic, ephemeral state — the in-memory database resets when the server stops
- Fast and self-contained — no external services required

## Usage

### Docker (recommended)

A pre-built image is published at `ghcr.io/altertable-ai/altertable-mock`:

```bash
docker run -p 15002:15002 \
  -e ALTERTABLE_MOCK_USERS="alice:secret,bob:hunter2" \
  ghcr.io/altertable-ai/altertable-mock
```

The server listens on port `15002` by default.

### Binary

```bash
cargo build --release
./target/release/altertable-mock --user alice:secret --user bob:hunter2
```

### Options

| Flag            | Short | Env var                       | Default | Description                                                            |
| --------------- | ----- | ----------------------------- | ------- | ---------------------------------------------------------------------- |
| `--flight-port` | `-f`  | `ALTERTABLE_MOCK_FLIGHT_PORT` | `15002` | Port to listen on                                                      |
| `--user`        | `-u`  | `ALTERTABLE_MOCK_USERS`       | —       | `username:password` pairs (repeatable, or comma-separated via env var) |

**Examples:**

```bash
# Custom port, multiple users via flags
altertable-mock --flight-port 9090 --user alice:s3cr3t --user bob:hunter2

# Via environment variables
ALTERTABLE_MOCK_FLIGHT_PORT=9090 ALTERTABLE_MOCK_USERS=alice:s3cr3t,bob:hunter2 altertable-mock
```

Log verbosity is controlled via the standard `RUST_LOG` environment variable (default: `info`).

## Connecting

Connect using any Arrow Flight SQL client pointed at `localhost:15002`. The [altertable-flightsql](https://github.com/altertable-ai/altertable-flightsql-python) Python library is the recommended client:

```bash
pip install altertable-flightsql
```

```python
from altertable_flightsql import Client

with Client(username="alice", password="secret", host="localhost", port=15002) as client:
    reader = client.query("SELECT * FROM my_table")
    for batch in reader:
        print(batch.data.to_pandas())
```

## Building from source

DuckDB is linked as a shared library. The `scripts/install_duckdb_libs.sh` script downloads the prebuilt `libduckdb` for Linux x86_64. See the `Dockerfile` for the exact build steps.

```bash
bash scripts/install_duckdb_libs.sh /usr/local/lib
cargo build --release
```
