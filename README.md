# altertable-mock

A minimal mock server that implements the [Altertable](https://altertable.ai) Arrow Flight SQL API, backed by an in-memory [DuckDB](https://duckdb.org) instance. Intended for local development and testing.

## What it is

Altertable exposes three interfaces — an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) (gRPC) endpoint, a Lakehouse REST/HTTP endpoint, and a Product Analytics REST/HTTP endpoint. This server speaks all three protocols and behaves similarly to the real service, without requiring a real Altertable account or any external infrastructure.

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
docker run -p 15000:15000 -p 15001:15001 -p 15002:15002 \
  -e ALTERTABLE_MOCK_USERS="alice:secret,bob:hunter2" \
  -e ALTERTABLE_MOCK_API_KEYS="myapikey" \
  -e ALTERTABLE_MOCK_ENVIRONMENTS="production,staging" \
  ghcr.io/altertable-ai/altertable-mock
```

The server listens on three ports by default:

| Port    | Protocol          | Description           |
| ------- | ----------------- | --------------------- |
| `15000` | HTTP (REST)       | Lakehouse REST API    |
| `15001` | HTTP (REST)       | Product Analytics API |
| `15002` | gRPC (Flight SQL) | Arrow Flight SQL API  |

### Binary

```bash
cargo build --release
./target/release/altertable-mock --user alice:secret --user bob:hunter2
```

### Options

| Flag               | Short | Env var                          | Default      | Description                                                                                                                                                                                 |
| ------------------ | ----- | -------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--flight-port`    | `-f`  | `ALTERTABLE_MOCK_FLIGHT_PORT`    | `15002`      | Port for the Arrow Flight SQL (gRPC) server                                                                                                                                                 |
| `--lakehouse-port` | `-p`  | `ALTERTABLE_MOCK_LAKEHOUSE_PORT` | `15000`      | Port for the Lakehouse REST (HTTP) server                                                                                                                                                   |
| `--analytics-port` | `-a`  | `ALTERTABLE_MOCK_ANALYTICS_PORT` | `15001`      | Port for the Product Analytics REST (HTTP) server                                                                                                                                           |
| `--user`           | `-u`  | `ALTERTABLE_MOCK_USERS`          | —            | `username:password` pairs (repeatable, or comma-separated via env var)                                                                                                                      |
| `--api-key`        | `-k`  | `ALTERTABLE_MOCK_API_KEYS`       | —            | API keys for the Product Analytics server (repeatable, or comma-separated via env var)                                                                                                      |
| `--environment`    | `-e`  | `ALTERTABLE_MOCK_ENVIRONMENTS`   | `production` | Environment names to pre-create for the Product Analytics server (repeatable, or comma-separated via env var). Requests for unknown environments are rejected with `environment-not-found`. |

**Examples:**

```bash
# Custom ports, multiple users and an API key via flags
altertable-mock --flight-port 9090 --lakehouse-port 9091 --analytics-port 9092 \
  --user alice:s3cr3t --user bob:hunter2 --api-key myapikey \
  --environment production --environment staging

# Via environment variables
ALTERTABLE_MOCK_FLIGHT_PORT=9090 ALTERTABLE_MOCK_LAKEHOUSE_PORT=9091 \
  ALTERTABLE_MOCK_ANALYTICS_PORT=9092 ALTERTABLE_MOCK_USERS=alice:s3cr3t,bob:hunter2 \
  ALTERTABLE_MOCK_API_KEYS=myapikey ALTERTABLE_MOCK_ENVIRONMENTS=production,staging \
  altertable-mock
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

| Method   | Path                | Description                      |
| -------- | ------------------- | -------------------------------- |
| `POST`   | `/query`            | Submit an async SQL query        |
| `GET`    | `/query/{query_id}` | Poll / fetch results for a query |
| `DELETE` | `/query/{query_id}` | Cancel a query                   |
| `POST`   | `/validate`         | Validate a SQL statement         |
| `POST`   | `/upload`           | Upload (upsert) Arrow data       |
| `POST`   | `/append`           | Append Arrow data                |

```bash
curl -u alice:secret -X POST http://localhost:15000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 42 AS answer"}'
```

### Product Analytics API (HTTP) — port 15001

The product analytics server listens on `localhost:15001`. Authenticate with an API key passed as either an `X-API-Key` header or an `Authorization: Bearer <token>` header. Each API key + environment combination gets its own isolated in-memory DuckDB database.

Environments must be declared at startup via `--environment` (or `ALTERTABLE_MOCK_ENVIRONMENTS`). If no environments are specified, a single `"production"` environment is created automatically. Requests that reference an undeclared environment are rejected with `{"ok": false, "error_code": "environment-not-found"}`.

If the `environment` field is omitted from a request payload, it defaults to `"production"`.

| Method | Path        | Description                                  |
| ------ | ----------- | -------------------------------------------- |
| `POST` | `/track`    | Ingest one or a batch of analytics events    |
| `POST` | `/identify` | Create or update a user identity with traits |

Both endpoints accept either a single object or a JSON array of objects.

**`POST /track`**

```bash
curl -H "X-API-Key: myapikey" -X POST http://localhost:15001/track \
  -H "Content-Type: application/json" \
  -d '{
    "event": "Button Clicked",
    "environment": "production",
    "properties": {"button": "signup"},
    "distinct_id": "user_42",
    "anonymous_id": "anon_abc",
    "timestamp": "2025-06-15T12:00:00Z"
  }'
```

| Field          | Type             | Required | Description                                                        |
| -------------- | ---------------- | -------- | ------------------------------------------------------------------ |
| `event`        | string           | yes      | Event name                                                         |
| `environment`  | string           | no       | Environment name; defaults to `"production"` if omitted            |
| `properties`   | object           | yes      | Arbitrary event properties                                         |
| `distinct_id`  | string           | no       | Identified user ID                                                 |
| `anonymous_id` | string           | no       | Anonymous / device ID (linked to `distinct_id` if both provided)   |
| `device_id`    | string           | no       | Device identifier                                                  |
| `session_id`   | string           | no       | Session identifier                                                 |
| `timestamp`    | string or number | no       | ISO 8601 string or Unix timestamp in milliseconds; defaults to now |

**`POST /identify`**

```bash
curl -H "X-API-Key: myapikey" -X POST http://localhost:15001/identify \
  -H "Content-Type: application/json" \
  -d '{
    "distinct_id": "user_42",
    "environment": "production",
    "traits": {"email": "alice@example.com", "plan": "pro"},
    "anonymous_id": "anon_abc"
  }'
```

| Field          | Type   | Required | Description                                             |
| -------------- | ------ | -------- | ------------------------------------------------------- |
| `distinct_id`  | string | yes      | Identified user ID                                      |
| `environment`  | string | no       | Environment name; defaults to `"production"` if omitted |
| `traits`       | object | no       | User traits to shallow-merge into existing traits       |
| `anonymous_id` | string | no       | Anonymous ID to link to this identity                   |

Both endpoints return `{"ok": true}` on success or `{"ok": false, "error_code": "..."}` on failure.

## Building from source

DuckDB is compiled in via the `bundled` feature, so no external shared library is required.

```bash
cargo build --release
```
