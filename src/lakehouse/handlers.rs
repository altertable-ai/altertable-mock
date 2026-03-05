use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::{
    Extension,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::Utc;
use duckdb::Connection;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::flight::layers::auth::Identity;
use crate::session::create_schema_if_not_exists;
use crate::utils::{escape_identifier, escape_literal};

use super::state::LakehouseState;
use super::types::{
    AppendResponse, CancelQueryResponse, QueryLog, QueryRequest, QueryStreamHeader,
    ValidateRequest, ValidateResponse,
};

// ── /query ────────────────────────────────────────────────────────────────────

pub async fn post_query(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    axum::Json(req): axum::Json<QueryRequest>,
) -> Response {
    let conn = state.get_or_create_connection(&identity).await;
    let session_id = req
        .session_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let query_id = req
        .query_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::new_v4);

    let statement = req.statement.clone();
    let catalog = req.catalog.clone();
    let schema_name = req.schema.clone();
    let limit = req.limit;
    let offset = req.offset;
    let visible = req.visible.unwrap_or(true);
    let requested_by = req.requested_by.clone();

    let start_time = Utc::now();

    let mut log = QueryLog {
        uuid: query_id,
        start_time,
        end_time: None,
        duration_ms: None,
        query: statement.clone(),
        client_interface: "HttpQuery".to_owned(),
        visible,
        session_id: session_id.clone(),
        error: None,
        requested_by: requested_by.clone(),
        user_agent: None,
    };

    // Execute query in blocking thread
    let result = execute_query(
        conn,
        &statement,
        catalog.as_deref(),
        schema_name.as_deref(),
        limit,
        offset,
    )
    .await;

    let end_time = Utc::now();
    let duration_ms = (end_time - start_time).num_milliseconds();
    log.end_time = Some(end_time);
    log.duration_ms = Some(duration_ms);

    state
        .query_store
        .write()
        .await
        .insert(query_id, log.clone());

    match result {
        Err(e) => {
            let mut log = log.clone();
            log.error = Some(e.to_string());
            state.query_store.write().await.insert(query_id, log);
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
        Ok((columns, rows)) => {
            // Stream NDJSON: header line, column names line, then data rows
            let header = QueryStreamHeader {
                statement: statement.clone(),
                rows_limit: limit,
                connections_errors: HashMap::new(),
                session_id: session_id.clone(),
            };
            let mut body = serde_json::to_string(&header).unwrap();
            body.push('\n');

            // Column names as JSON array
            let col_names: Vec<Value> = columns.iter().map(|c| Value::String(c.clone())).collect();
            body.push_str(&serde_json::to_string(&col_names).unwrap());
            body.push('\n');

            // Data rows
            for row in rows {
                body.push_str(&serde_json::to_string(&row).unwrap());
                body.push('\n');
            }

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/x-ndjson")
                .body(Body::from(body))
                .unwrap()
        }
    }
}

// ── /query/{query_id} ─────────────────────────────────────────────────────────

pub async fn get_query(
    State(state): State<LakehouseState>,
    Path(query_id): Path<Uuid>,
    Extension(_identity): Extension<Identity>,
) -> Response {
    let store = state.query_store.read().await;
    match store.get(&query_id) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(log) => axum::Json(log).into_response(),
    }
}

#[derive(Deserialize)]
pub struct CancelQueryParams {
    pub session_id: String,
}

pub async fn delete_query(
    State(state): State<LakehouseState>,
    Path(query_id): Path<Uuid>,
    Query(params): Query<CancelQueryParams>,
    Extension(_identity): Extension<Identity>,
) -> Response {
    let store = state.query_store.read().await;
    match store.get(&query_id) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(log) if log.session_id != params.session_id => axum::Json(CancelQueryResponse {
            cancelled: false,
            message: "Session ID does not match".to_owned(),
        })
        .into_response(),
        Some(_) => axum::Json(CancelQueryResponse {
            cancelled: true,
            message: "Query cancelled".to_owned(),
        })
        .into_response(),
    }
}

// ── /validate ─────────────────────────────────────────────────────────────────

pub async fn post_validate(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    axum::Json(req): axum::Json<ValidateRequest>,
) -> axum::Json<ValidateResponse> {
    let conn = state.get_or_create_connection(&identity).await;
    let statement = req.statement.clone();
    let catalog = req.catalog.clone();
    let schema_name = req.schema.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock connection"))?;

        set_catalog_schema(&conn, catalog.as_deref(), schema_name.as_deref())?;

        // Use EXPLAIN to validate without executing
        let explain_query = format!("EXPLAIN {statement}");
        conn.execute(&explain_query, duckdb::params![])
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("{e}"))
    })
    .await;

    match result {
        Ok(Ok(())) => axum::Json(ValidateResponse {
            valid: true,
            statement: req.statement,
            connections_errors: HashMap::new(),
            error: None,
        }),
        Ok(Err(e)) => axum::Json(ValidateResponse {
            valid: false,
            statement: req.statement,
            connections_errors: HashMap::new(),
            error: Some(e.to_string()),
        }),
        Err(e) => axum::Json(ValidateResponse {
            valid: false,
            statement: req.statement,
            connections_errors: HashMap::new(),
            error: Some(e.to_string()),
        }),
    }
}

// ── /upload ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct UploadParams {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub format: String,
    pub mode: String,
    pub primary_key: Option<String>,
}

pub async fn post_upload(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    Query(params): Query<UploadParams>,
    body: axum::body::Bytes,
) -> Response {
    let conn = state.get_or_create_connection(&identity).await;

    let result = do_upload(conn, params, body).await;
    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn do_upload(
    conn: Arc<Mutex<Connection>>,
    params: UploadParams,
    body: axum::body::Bytes,
) -> anyhow::Result<()> {
    let catalog = params.catalog.clone();
    let schema = params.schema.clone();
    let table = params.table.clone();
    let format = params.format.to_lowercase();
    let mode = params.mode.to_lowercase();
    let primary_key = params.primary_key.clone();

    tokio::task::spawn_blocking(move || {
        let conn = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock connection"))?;

        create_schema_if_not_exists(&conn, &catalog, &schema)?;

        let full_table = format!(
            r#""{}"."{}"."{}""#,
            escape_identifier(&catalog),
            escape_identifier(&schema),
            escape_identifier(&table)
        );

        // Write body to a temp file so DuckDB can read it
        let ext = match format.as_str() {
            "csv" => "csv",
            "json" => "json",
            "parquet" => "parquet",
            _ => return Err(anyhow::anyhow!("Unsupported format: {format}")),
        };

        let tmp_path = std::env::temp_dir().join(format!("altertable_upload_{}.{}", Uuid::new_v4(), ext));
        std::fs::write(&tmp_path, &body)?;
        let tmp_path_str = tmp_path.to_string_lossy().to_string();

        let read_expr = match format.as_str() {
            "csv" => format!("read_csv('{tmp_path_str}', auto_detect=true)"),
            "json" => format!("read_json_auto('{tmp_path_str}')"),
            "parquet" => format!("read_parquet('{tmp_path_str}')"),
            _ => unreachable!(),
        };

        let query = match mode.as_str() {
            "create" => {
                format!("CREATE TABLE {full_table} AS SELECT * FROM {read_expr}")
            }
            "append" => {
                format!("INSERT INTO {full_table} SELECT * FROM {read_expr}")
            }
            "overwrite" => {
                format!(
                    "DROP TABLE IF EXISTS {full_table}; CREATE TABLE {full_table} AS SELECT * FROM {read_expr}"
                )
            }
            "upsert" => {
                let pk = primary_key
                    .ok_or_else(|| anyhow::anyhow!("primary_key required for upsert mode"))?;
                format!(
                    "INSERT INTO {full_table} SELECT * FROM {read_expr} ON CONFLICT ({pk}) DO UPDATE SET *"
                )
            }
            _ => return Err(anyhow::anyhow!("Unsupported mode: {mode}")),
        };

        conn.execute_batch(&query)
            .map_err(|e| anyhow::anyhow!("Failed to execute upload: {e}"))?;

        let _ = std::fs::remove_file(&tmp_path);
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {e}"))?
}

// ── /append ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct AppendParams {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

pub async fn post_append(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    Query(params): Query<AppendParams>,
    axum::Json(req): axum::Json<serde_json::Value>,
) -> axum::Json<AppendResponse> {
    let conn = state.get_or_create_connection(&identity).await;

    let rows: Vec<serde_json::Map<String, Value>> = match req {
        Value::Object(map) => {
            // Single variant: {"Single": {...}} or raw object
            if let Some(Value::Object(inner)) = map.get("Single") {
                vec![inner.clone()]
            } else if let Some(Value::Array(arr)) = map.get("Batch") {
                arr.iter().filter_map(|v| v.as_object().cloned()).collect()
            } else {
                vec![map]
            }
        }
        Value::Array(arr) => arr.into_iter().filter_map(|v| v.into_object()).collect(),
        _ => {
            return axum::Json(AppendResponse {
                ok: false,
                error_code: Some("invalid-data".to_owned()),
            });
        }
    };

    if rows.is_empty() {
        return axum::Json(AppendResponse {
            ok: true,
            error_code: None,
        });
    }

    let catalog = params.catalog.clone();
    let schema = params.schema.clone();
    let table = params.table.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock connection"))?;

        create_schema_if_not_exists(&conn, &catalog, &schema)?;

        let full_table = format!(
            r#""{}"."{}"."{}""#,
            escape_identifier(&catalog),
            escape_identifier(&schema),
            escape_identifier(&table)
        );

        for row in &rows {
            let cols: Vec<String> = row
                .keys()
                .map(|k| format!(r#""{}""#, escape_identifier(k)))
                .collect();
            let vals: Vec<String> = row
                .values()
                .map(|v| match v {
                    Value::Null => "NULL".to_owned(),
                    Value::Bool(b) => b.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => format!("'{}'", escape_literal(s)),
                    other => format!("'{}'", escape_literal(&other.to_string())),
                })
                .collect();

            let query = format!(
                "INSERT INTO {full_table} ({}) VALUES ({})",
                cols.join(", "),
                vals.join(", ")
            );

            conn.execute(&query, duckdb::params![])
                .map_err(|e| anyhow::anyhow!("Failed to insert row: {e}"))?;
        }

        Ok::<_, anyhow::Error>(())
    })
    .await;

    match result {
        Ok(Ok(())) => axum::Json(AppendResponse {
            ok: true,
            error_code: None,
        }),
        _ => axum::Json(AppendResponse {
            ok: false,
            error_code: Some("invalid-data".to_owned()),
        }),
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn set_catalog_schema(
    conn: &Connection,
    catalog: Option<&str>,
    schema: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(c) = catalog {
        conn.execute(&format!("USE {}", escape_identifier(c)), duckdb::params![])?;
    }
    if let Some(s) = schema {
        conn.execute(&format!("USE {}", escape_identifier(s)), duckdb::params![])?;
    }
    Ok(())
}

async fn execute_query(
    conn: Arc<Mutex<Connection>>,
    statement: &str,
    catalog: Option<&str>,
    schema: Option<&str>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> anyhow::Result<(Vec<String>, Vec<Vec<Value>>)> {
    let statement = statement.to_owned();
    let catalog = catalog.map(str::to_owned);
    let schema = schema.map(str::to_owned);

    tokio::task::spawn_blocking(move || {
        let conn = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock connection"))?;

        set_catalog_schema(&conn, catalog.as_deref(), schema.as_deref())?;

        let mut sql = statement.clone();

        // Apply limit/offset wrapping if requested
        if limit.is_some() || offset.is_some() {
            let limit_clause = limit.map(|l| format!(" LIMIT {l}")).unwrap_or_default();
            let offset_clause = offset.map(|o| format!(" OFFSET {o}")).unwrap_or_default();
            sql = format!("SELECT * FROM ({sql}){limit_clause}{offset_clause}");
        }

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| anyhow::anyhow!("Failed to prepare statement: {e}"))?;

        // Use query_arrow which executes the statement and gives schema + data
        let arrow_batches: Vec<duckdb::arrow::array::RecordBatch> = stmt
            .query_arrow(duckdb::params![])
            .map_err(|e| anyhow::anyhow!("Failed to execute query: {e}"))?
            .collect();

        if arrow_batches.is_empty() {
            return Ok((vec![], vec![]));
        }

        let schema_ref = arrow_batches[0].schema();
        let columns: Vec<String> = schema_ref
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let mut buf = Vec::new();
        let mut writer = arrow_json::WriterBuilder::new()
            .with_struct_mode(arrow_json::StructMode::ListOnly)
            .build::<_, arrow_json::writer::LineDelimited>(&mut buf);
        for batch in &arrow_batches {
            writer
                .write(batch)
                .map_err(|e| anyhow::anyhow!("Failed to serialize batch: {e}"))?;
        }
        writer
            .finish()
            .map_err(|e| anyhow::anyhow!("Failed to finish JSON writer: {e}"))?;

        let rows: Vec<Vec<Value>> = buf
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_slice::<Value>(line)
                    .map(|v| match v {
                        Value::Array(arr) => arr,
                        other => vec![other],
                    })
                    .unwrap_or_default()
            })
            .collect();

        Ok((columns, rows))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {e}"))?
}

trait IntoObject {
    fn into_object(self) -> Option<serde_json::Map<String, Value>>;
}

impl IntoObject for Value {
    fn into_object(self) -> Option<serde_json::Map<String, Value>> {
        match self {
            Self::Object(m) => Some(m),
            _ => None,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use axum::{Router, middleware, routing};
    use base64::{Engine, prelude::BASE64_STANDARD};
    use tower::ServiceExt;

    use crate::flight::layers::auth::Identity;
    use crate::lakehouse::auth::auth_middleware;
    use crate::lakehouse::state::LakehouseState;

    use super::*;

    fn make_state() -> LakehouseState {
        let user = Identity {
            username: "testuser".to_owned().into(),
            password: "testpass".to_owned().into(),
        };
        let mut set = HashSet::new();
        set.insert(user);
        LakehouseState::new(Arc::new(set))
    }

    fn basic_auth_header() -> String {
        let encoded = BASE64_STANDARD.encode("testuser:testpass");
        format!("Basic {encoded}")
    }

    fn make_router(state: LakehouseState) -> Router {
        Router::new()
            .route("/query", routing::post(post_query))
            .route("/query/{query_id}", routing::get(get_query))
            .route("/query/{query_id}", routing::delete(delete_query))
            .route("/validate", routing::post(post_validate))
            .route("/upload", routing::post(post_upload))
            .route("/append", routing::post(post_append))
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    // ── auth ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn missing_auth_is_rejected() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_password_is_rejected() {
        let app = make_router(make_state());
        let bad_auth = format!("Basic {}", BASE64_STANDARD.encode("testuser:wrong"));
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, bad_auth)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── POST /query ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn post_query_simple_select() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SELECT 42 AS n"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-ndjson"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        // line 0: stream header, line 1: column names, line 2: first data row
        assert!(lines.len() >= 3, "expected at least 3 NDJSON lines");
        let header: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["statement"], "SELECT 42 AS n");
        let cols: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(cols[0], "n");
        let row: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(row[0], 42);
    }

    #[tokio::test]
    async fn post_query_invalid_sql_returns_400() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SELECT FROM WHERE"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── GET /query/{query_id} ─────────────────────────────────────────────────

    #[tokio::test]
    async fn get_query_unknown_id_returns_404() {
        let app = make_router(make_state());
        let unknown = uuid::Uuid::new_v4();
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/query/{unknown}"))
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_query_returns_log_after_execution() {
        let state = make_state();
        // Run a query to populate the store
        let query_id = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "statement": "SELECT 1",
            "query_id": query_id,
            "session_id": "sess-1"
        });

        let app = make_router(state.clone());
        app.oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/query")
                .header(header::AUTHORIZATION, basic_auth_header())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/query/{query_id}"))
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let log: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(log["query"], "SELECT 1");
    }

    // ── DELETE /query/{query_id} ──────────────────────────────────────────────

    #[tokio::test]
    async fn delete_query_unknown_id_returns_404() {
        let app = make_router(make_state());
        let unknown = uuid::Uuid::new_v4();
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/query/{unknown}?session_id=x"))
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_query_session_mismatch_returns_cancelled_false() {
        let state = make_state();
        let query_id = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "statement": "SELECT 1",
            "query_id": query_id,
            "session_id": "real-session"
        });

        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/query/{query_id}?session_id=wrong-session"))
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["cancelled"], false);
    }

    #[tokio::test]
    async fn delete_query_matching_session_returns_cancelled_true() {
        let state = make_state();
        let query_id = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "statement": "SELECT 1",
            "query_id": query_id,
            "session_id": "my-session"
        });

        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!("/query/{query_id}?session_id=my-session"))
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["cancelled"], true);
    }

    // ── POST /validate ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn validate_valid_sql() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/validate")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["valid"], true);
        assert!(result.get("error").is_none() || result["error"].is_null());
    }

    #[tokio::test]
    async fn validate_invalid_sql() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/validate")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"NOT VALID SQL !!!"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["valid"], false);
        assert!(result["error"].is_string());
    }

    // ── POST /upload ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn upload_csv_create_mode() {
        let csv = "id,name\n1,Alice\n2,Bob\n";
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=people&format=csv&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::from(csv))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn upload_unsupported_format_returns_400() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=t&format=xml&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::from("data"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── POST /append ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn append_rows_to_existing_table() {
        let state = make_state();
        let csv = "id,name\n1,Alice\n";

        // First create the table via upload
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=append_test&format=csv&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .body(Body::from(csv))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then append a row
        let row = serde_json::json!({"id": 2, "name": "Bob"});
        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/append?catalog=memory&schema=main&table=append_test")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(row.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["ok"], true);
    }

    #[tokio::test]
    async fn append_invalid_body_returns_ok_false() {
        let app = make_router(make_state());
        // Send a plain string (not an object or array)
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/append?catalog=memory&schema=main&table=t")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#""just a string""#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["ok"], false);
    }
}
