# altertable-mock

A minimal mock server that implements the [Altertable](https://altertable.ai) Arrow Flight SQL API, backed by an in-memory [DuckDB](https://duckdb.org) instance. Intended for local development and testing.

## What it is

Altertable exposes two interfaces — an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) (gRPC) endpoint and a REST/HTTP endpoint. This server speaks both protocols and behaves similarly to the real service, without requiring a real Altertable account or any external infrastructure.

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
docker run -p 15000:15000 -p 15002:15002 \
  -e ALTERTABLE_MOCK_USERS="alice:secret,bob:hunter2" \
  ghcr.io/altertable-ai/altertable-mock
```

The server listens on two ports by default:

| Port    | Protocol            | Description                        |
| ------- | ------------------- | ---------------------------------- |
| `15000` | HTTP (REST)         | Lakehouse REST API                 |
| `15002` | gRPC (Flight SQL)   | Arrow Flight SQL API               |

### Binary

```bash
cargo build --release
./target/release/altertable-mock --user alice:secret --user bob:hunter2
```

### Options

| Flag                | Short | Env var                         | Default | Description                                                            |
| ------------------- | ----- | ------------------------------- | ------- | ---------------------------------------------------------------------- |
| `--flight-port`     | `-f`  | `ALTERTABLE_MOCK_FLIGHT_PORT`   | `15002` | Port for the Arrow Flight SQL (gRPC) server                            |
| `--lakehouse-port`  | `-p`  | `ALTERTABLE_MOCK_LAKEHOUSE_PORT`| `15000` | Port for the REST (HTTP) server                                        |
| `--user`            | `-u`  | `ALTERTABLE_MOCK_USERS`         | —       | `username:password` pairs (repeatable, or comma-separated via env var) |

**Examples:**

```bash
# Custom ports, multiple users via flags
altertable-mock --flight-port 9090 --lakehouse-port 9091 --user alice:s3cr3t --user bob:hunter2

# Via environment variables
ALTERTABLE_MOCK_FLIGHT_PORT=9090 ALTERTABLE_MOCK_LAKEHOUSE_PORT=9091 ALTERTABLE_MOCK_USERS=alice:s3cr3t,bob:hunter2 altertable-mock
```

Log verbosity is controlled via the standard `RUST_LOG` environment variable (default: `info`).

## Connecting

### Flight SQL (gRPC) — port 15002

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

### REST API (HTTP) — port 15000

The server also exposes a REST interface on `localhost:15000`. Authenticate with HTTP Basic Auth and use the following endpoints:

| Method   | Path                    | Description                          |
| -------- | ----------------------- | ------------------------------------ |
| `POST`   | `/query`                | Submit an async SQL query            |
| `GET`    | `/query/{query_id}`     | Poll / fetch results for a query     |
| `DELETE` | `/query/{query_id}`     | Cancel a query                       |
| `POST`   | `/validate`             | Validate a SQL statement             |
| `POST`   | `/upload`               | Upload (upsert) Arrow data           |
| `POST`   | `/append`               | Append Arrow data                    |

```bash
curl -u alice:secret -X POST http://localhost:15000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 42 AS answer"}'
```

## Building from source

DuckDB is compiled in via the `bundled` feature, so no external shared library is required.

```bash
cargo build --release
```
