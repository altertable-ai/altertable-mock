use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::{
    Extension, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    middleware,
    response::{IntoResponse, Response},
    routing,
};
use chrono::Utc;
use duckdb::Connection;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::flight::layers::auth::Identity;
use crate::lakehouse::auth::auth_middleware;
use crate::session::create_schema_if_not_exists;
use crate::utils::{escape_identifier, escape_literal};

pub fn router(state: super::state::LakehouseState) -> Router {
    Router::new()
        .route("/query", routing::post(post_query))
        .route("/query/{query_id}", routing::get(get_query))
        .route("/query/{query_id}", routing::delete(delete_query))
        .route("/validate", routing::post(post_validate))
        .route("/explain", routing::post(post_explain))
        .route("/autocomplete", routing::post(post_autocomplete))
        .route(
            "/upload",
            routing::post(post_upload).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/upsert",
            routing::post(post_upsert).layer(DefaultBodyLimit::disable()),
        )
        .route("/append", routing::post(post_append))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state)
}

use super::format::{
    ALTERTABLE_ORIGINAL_TYPE_JSON, ALTERTABLE_ORIGINAL_TYPE_VARIANT, OutputFormat,
    record_batch_to_csv, record_batch_to_default_rows, record_batch_to_jsonl,
    record_batches_to_parquet,
};
use super::state::LakehouseState;
use super::types::{
    AppendRequest, AppendResponse, AutocompleteRequest, AutocompleteResponse,
    AutocompleteSuggestion, CancelQueryResponse, ExplainRequest, ExplainResponse, QueryLog,
    QueryRequest, QueryStreamError, QueryStreamHeader, TableScanEstimate, ValidateRequest,
    ValidateResponse,
};

const MOCK_WORKER_SLUG: &str = "altertable-mock";

// ── /query ────────────────────────────────────────────────────────────────────

pub async fn post_query(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    axum::Json(req): axum::Json<QueryRequest>,
) -> Response {
    let format = match OutputFormat::parse(req.format.as_deref()) {
        Ok(format) => format,
        Err(msg) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header(CONTENT_TYPE, "text/plain")
                .body(Body::from(msg))
                .unwrap();
        }
    };

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
    let request_start = Instant::now();

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

    let init_time_ms = std::cmp::max(1, request_start.elapsed().as_millis() as u32);
    let metadata = build_query_metadata(
        statement.clone(),
        limit,
        offset,
        init_time_ms,
        session_id.clone(),
        query_id,
    );

    match result {
        Err(e) => {
            let mut log = log.clone();
            log.error = Some(e.to_string());
            state.query_store.write().await.insert(query_id, log);

            if !format.is_default() {
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(CONTENT_TYPE, format.content_type())
                    .body(Body::from(e.to_string()))
                    .unwrap();
            }

            let mut body = serde_json::to_string(&metadata).unwrap();
            body.push('\n');
            body.push_str(
                &serde_json::to_string(&QueryStreamError {
                    error: e.to_string(),
                })
                .unwrap(),
            );
            body.push('\n');

            ndjson_response(body)
        }
        Ok((columns, batches)) => {
            match encode_query_response(format, &metadata, &columns, &batches) {
                Ok(response) => response,
                Err(e) => Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from(e.to_string()))
                    .unwrap(),
            }
        }
    }
}

fn encode_query_response(
    format: OutputFormat,
    metadata: &QueryStreamHeader,
    columns: &[String],
    batches: &[duckdb::arrow::array::RecordBatch],
) -> anyhow::Result<Response> {
    match format {
        OutputFormat::Default => {
            let mut body = serde_json::to_string(metadata)?;
            body.push('\n');

            let col_names: Vec<Value> = columns.iter().map(|c| Value::String(c.clone())).collect();
            body.push_str(&serde_json::to_string(&col_names)?);
            body.push('\n');

            for batch in batches {
                for row in record_batch_to_default_rows(batch)? {
                    body.push_str(&serde_json::to_string(&row)?);
                    body.push('\n');
                }
            }

            Ok(ndjson_response(body))
        }
        OutputFormat::Csv => {
            let mut body = Vec::new();
            if let Some(first) = batches.first() {
                body.extend(record_batch_to_csv(
                    &duckdb::arrow::array::RecordBatch::new_empty(first.schema()),
                    true,
                )?);
                for batch in batches {
                    body.extend(record_batch_to_csv(batch, false)?);
                }
            } else {
                // Empty result: still emit a header line from column names if present.
                let schema = Arc::new(arrow_schema::Schema::new(
                    columns
                        .iter()
                        .map(|name| {
                            arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true)
                        })
                        .collect::<Vec<_>>(),
                ));
                let empty = duckdb::arrow::array::RecordBatch::new_empty(schema);
                body.extend(record_batch_to_csv(&empty, true)?);
            }
            Ok(bytes_response(StatusCode::OK, format.content_type(), body))
        }
        OutputFormat::Jsonl => {
            let mut body = Vec::new();
            for batch in batches {
                body.extend(record_batch_to_jsonl(batch)?);
            }
            Ok(bytes_response(StatusCode::OK, format.content_type(), body))
        }
        OutputFormat::Parquet => {
            let schema = if let Some(first) = batches.first() {
                first.schema()
            } else {
                Arc::new(arrow_schema::Schema::new(
                    columns
                        .iter()
                        .map(|name| {
                            arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true)
                        })
                        .collect::<Vec<_>>(),
                ))
            };
            let body = record_batches_to_parquet(schema, batches)?;
            Ok(bytes_response(StatusCode::OK, format.content_type(), body))
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

// ── /explain ──────────────────────────────────────────────────────────────────

fn sum_optional(values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    values
        .collect::<Option<Vec<_>>>()
        .map(|v| v.into_iter().sum())
}

pub async fn post_explain(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    axum::Json(req): axum::Json<ExplainRequest>,
) -> axum::Json<ExplainResponse> {
    let conn = state.get_or_create_connection(&identity).await;
    let statement = req.statement.trim().to_owned();
    let include_plan = req.include_plan;
    let catalog = req.catalog.clone();
    let schema_name = req.schema.clone();
    let statement_for_explain = statement.clone();

    let result = tokio::task::spawn_blocking(move || {
        let conn = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock connection"))?;

        set_catalog_schema(&conn, catalog.as_deref(), schema_name.as_deref())?;
        super::explain::explain_statement(&conn, &statement_for_explain)
    })
    .await;

    match result {
        Ok(Ok(plan)) => {
            let tables: Vec<TableScanEstimate> = plan
                .table_scans()
                .into_iter()
                .map(|scan| TableScanEstimate {
                    table_name: scan.table_name,
                    estimated_rows: scan.estimated_rows,
                    filters: scan.filters,
                    total_files: None,
                    total_bytes: None,
                    scanned_files_estimate: None,
                    scanned_bytes_estimate: None,
                })
                .collect();

            axum::Json(ExplainResponse {
                total_files: sum_optional(tables.iter().map(|t| t.total_files)),
                total_bytes: sum_optional(tables.iter().map(|t| t.total_bytes)),
                scanned_files_estimate: sum_optional(
                    tables.iter().map(|t| t.scanned_files_estimate),
                ),
                scanned_bytes_estimate: sum_optional(
                    tables.iter().map(|t| t.scanned_bytes_estimate),
                ),
                plan: if include_plan { Some(vec![plan]) } else { None },
                error: None,
                statement,
                tables,
                connections_errors: HashMap::new(),
            })
        }
        Ok(Err(e)) => axum::Json(ExplainResponse {
            tables: vec![],
            total_files: None,
            total_bytes: None,
            scanned_files_estimate: None,
            scanned_bytes_estimate: None,
            statement,
            plan: None,
            error: Some(e.to_string()),
            connections_errors: HashMap::new(),
        }),
        Err(e) => axum::Json(ExplainResponse {
            tables: vec![],
            total_files: None,
            total_bytes: None,
            scanned_files_estimate: None,
            scanned_bytes_estimate: None,
            statement,
            plan: None,
            error: Some(e.to_string()),
            connections_errors: HashMap::new(),
        }),
    }
}

// ── /autocomplete ───────────────────────────────────────────────────────────

const DEFAULT_MAX_SUGGESTIONS: u32 = 20;
const MAX_SUGGESTIONS_CAP: u32 = 200;

fn resolve_max_suggestions(max: Option<u32>) -> u32 {
    match max {
        None | Some(0) => DEFAULT_MAX_SUGGESTIONS,
        Some(n) => n.min(MAX_SUGGESTIONS_CAP),
    }
}

pub async fn post_autocomplete(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    axum::Json(req): axum::Json<AutocompleteRequest>,
) -> Response {
    let statement = req.statement.trim().to_owned();
    let statement_for_query = statement.clone();
    let limit = resolve_max_suggestions(req.max_suggestions);
    let limit_i64 = i64::from(limit);
    let catalog = req.catalog.clone();
    let schema_name = req.schema.clone();
    let conn = state.get_or_create_connection(&identity).await;

    let result = tokio::task::spawn_blocking(move || {
        let conn = conn
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to lock connection"))?;

        set_catalog_schema(&conn, catalog.as_deref(), schema_name.as_deref())?;

        let sql = format!(
            "SELECT suggestion, suggestion_start FROM sql_auto_complete('{}') \
             ORDER BY suggestion LIMIT {}",
            escape_literal(&statement_for_query),
            limit_i64
        );

        let mut stmt = conn.prepare(&sql).map_err(|e| anyhow::anyhow!("{e}"))?;

        stmt.query_map(duckdb::params![], |row| {
            let start: i32 = row.get(1)?;
            Ok(AutocompleteSuggestion {
                suggestion: row.get(0)?,
                suggestion_start: start,
                suggestion_type: String::new(),
                suggestion_score: 0,
                extra_char: None,
            })
        })
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("{e}"))
    })
    .await;

    match result {
        Ok(Ok(suggestions)) => (
            StatusCode::OK,
            axum::Json(AutocompleteResponse {
                suggestions,
                statement,
                connections_errors: HashMap::new(),
            }),
        )
            .into_response(),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

// ── /upload and /upsert ───────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, PartialEq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum UploadMode {
    Create,
    #[default]
    Append,
    #[serde(rename = "create_append")]
    CreateAppend,
    Overwrite,
}

#[derive(Deserialize)]
pub struct UploadParams {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub mode: UploadMode,
}

#[derive(Deserialize)]
pub struct UpsertParams {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub primary_key: String,
    pub cursor_field: Option<String>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum IngestMode {
    Create,
    Append,
    CreateAppend,
    Overwrite,
    Upsert,
}

impl From<UploadMode> for IngestMode {
    fn from(mode: UploadMode) -> Self {
        match mode {
            UploadMode::Create => Self::Create,
            UploadMode::Append => Self::Append,
            UploadMode::CreateAppend => Self::CreateAppend,
            UploadMode::Overwrite => Self::Overwrite,
        }
    }
}

struct IngestTarget {
    catalog: String,
    schema: String,
    table: String,
    mode: IngestMode,
    primary_key: Option<String>,
    cursor_field: Option<String>,
}

pub async fn post_upload(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    Query(params): Query<UploadParams>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let conn = state.get_or_create_connection(&identity).await;

    let result = do_ingest(
        conn,
        IngestTarget {
            catalog: params.catalog,
            schema: params.schema,
            table: params.table,
            mode: params.mode.into(),
            primary_key: None,
            cursor_field: None,
        },
        &headers,
        body,
    )
    .await;
    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

pub async fn post_upsert(
    State(state): State<LakehouseState>,
    Extension(identity): Extension<Identity>,
    Query(params): Query<UpsertParams>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let conn = state.get_or_create_connection(&identity).await;

    let result = do_ingest(
        conn,
        IngestTarget {
            catalog: params.catalog,
            schema: params.schema,
            table: params.table,
            mode: IngestMode::Upsert,
            primary_key: Some(params.primary_key),
            cursor_field: params.cursor_field,
        },
        &headers,
        body,
    )
    .await;
    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

fn detect_format_from_content_type(headers: &HeaderMap) -> Option<&'static str> {
    let ct = headers.get(CONTENT_TYPE)?.to_str().ok()?;
    let ct = ct
        .split(';')
        .next()
        .unwrap_or(ct)
        .trim()
        .to_ascii_lowercase();
    if ct == "application/json" || ct.ends_with("+json") {
        Some("json")
    } else if ct == "text/csv" || ct == "application/csv" {
        Some("csv")
    } else if ct == "application/parquet" {
        Some("parquet")
    } else {
        None
    }
}

fn detect_format_from_bytes(bytes: &[u8]) -> &'static str {
    if bytes.len() >= 4 && bytes.starts_with(b"PAR1") {
        "parquet"
    } else if bytes
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| *b == b'{' || *b == b'[')
    {
        "json"
    } else {
        "csv"
    }
}

fn parse_quoted_columns(raw: &str, parameter: &str) -> anyhow::Result<Vec<String>> {
    let columns: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(|column| format!(r#""{}""#, escape_identifier(column)))
        .collect();
    if columns.is_empty() {
        anyhow::bail!("{parameter} must name at least one column");
    }
    Ok(columns)
}

fn detect_data_format(headers: &HeaderMap, body: &[u8]) -> &'static str {
    detect_format_from_content_type(headers).unwrap_or_else(|| detect_format_from_bytes(body))
}

fn pinned_json_columns(
    conn: &Connection,
    catalog: &str,
    schema: &str,
    table: &str,
    mode: IngestMode,
) -> anyhow::Result<Option<String>> {
    if !matches!(
        mode,
        IngestMode::Append | IngestMode::CreateAppend | IngestMode::Upsert
    ) {
        return Ok(None);
    }

    let mut stmt = conn.prepare(
        "SELECT column_name, data_type FROM duckdb_columns()
         WHERE database_name = ? AND schema_name = ? AND table_name = ?
         ORDER BY column_index",
    )?;
    let entries = stmt
        .query_map(duckdb::params![catalog, schema, table], |row| {
            let name: String = row.get(0)?;
            let data_type: String = row.get(1)?;
            Ok(format!(
                "'{}': '{}'",
                escape_literal(&name),
                escape_literal(&data_type)
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok((!entries.is_empty()).then(|| entries.join(", ")))
}

fn source_cursor_wins_condition(cursor_columns: &[String]) -> String {
    cursor_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let mut clauses: Vec<String> = cursor_columns[..index]
                .iter()
                .map(|outranking| {
                    format!("source.{outranking} IS NOT DISTINCT FROM target.{outranking}")
                })
                .collect();
            clauses.push(format!(
                "(source.{column} > target.{column} OR (source.{column} IS NOT NULL AND target.{column} IS NULL))"
            ));
            format!("({})", clauses.join(" AND "))
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

async fn do_ingest(
    conn: Arc<Mutex<Connection>>,
    target: IngestTarget,
    headers: &HeaderMap,
    body: axum::body::Bytes,
) -> anyhow::Result<()> {
    let IngestTarget {
        catalog,
        schema,
        table,
        mode,
        primary_key,
        cursor_field,
    } = target;
    let format = detect_data_format(headers, &body);

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

        let tmp_path = std::env::temp_dir().join(format!(
            "altertable_ingest_{}.{}",
            Uuid::new_v4(),
            format
        ));
        std::fs::write(&tmp_path, &body)?;
        let tmp_path_str = tmp_path.to_string_lossy().to_string();

        let read_expr = match format {
            "csv" => format!("read_csv('{tmp_path_str}', auto_detect=true)"),
            "json" => match pinned_json_columns(&conn, &catalog, &schema, &table, mode)? {
                Some(columns) => format!("read_json('{tmp_path_str}', columns={{{columns}}})"),
                None => format!("read_json_auto('{tmp_path_str}')"),
            },
            "parquet" => format!("read_parquet('{tmp_path_str}')"),
            _ => unreachable!(),
        };

        let query = match mode {
            IngestMode::Create => {
                format!("CREATE TABLE {full_table} AS SELECT * FROM {read_expr}")
            }
            IngestMode::Append => {
                format!("INSERT INTO {full_table} BY NAME SELECT * FROM {read_expr}")
            }
            IngestMode::CreateAppend => {
                format!(
                    "CREATE TABLE IF NOT EXISTS {full_table} AS SELECT * FROM {read_expr} LIMIT 0; INSERT INTO {full_table} BY NAME SELECT * FROM {read_expr}"
                )
            }
            IngestMode::Overwrite => {
                format!(
                    "DROP TABLE IF EXISTS {full_table}; CREATE TABLE {full_table} AS SELECT * FROM {read_expr}"
                )
            }
            IngestMode::Upsert => {
                let raw_pk = primary_key.expect("primary_key must be set for upsert mode");
                let pk_columns = parse_quoted_columns(&raw_pk, "primary_key")?;
                let cursor_columns = cursor_field
                    .as_deref()
                    .map(|raw| parse_quoted_columns(raw, "cursor_field"))
                    .transpose()?
                    .unwrap_or_default();

                let on_clause = pk_columns
                    .iter()
                    .map(|column| format!("target.{column} = source.{column}"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let source = if cursor_columns.is_empty() {
                    format!("SELECT * FROM {read_expr}")
                } else {
                    format!(
                        "SELECT DISTINCT ON ({pk}) * FROM {read_expr} ORDER BY {pk}, {cursor}",
                        pk = pk_columns.join(", "),
                        cursor = cursor_columns
                            .iter()
                            .map(|column| format!("{column} DESC NULLS LAST"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                let matched = if cursor_columns.is_empty() {
                    "WHEN MATCHED THEN UPDATE SET *".to_owned()
                } else {
                    format!(
                        "WHEN MATCHED AND ({}) THEN UPDATE SET *",
                        source_cursor_wins_condition(&cursor_columns)
                    )
                };
                format!(
                    "CREATE TABLE IF NOT EXISTS {full_table} AS SELECT * FROM {read_expr} LIMIT 0; MERGE INTO {full_table} AS target USING ({source}) AS source ON {on_clause} {matched} WHEN NOT MATCHED THEN INSERT BY NAME"
                )
            }
        };

        conn.execute_batch(&query)
            .map_err(|e| anyhow::anyhow!("Failed to execute ingest: {e}"))?;

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
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AppendRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(AppendResponse {
                    ok: false,
                    error_code: Some("invalid-data".to_owned()),
                }),
            );
        }
    };

    let conn = state.get_or_create_connection(&identity).await;
    let rows = req.into_vec();

    if rows.is_empty() {
        return (
            StatusCode::OK,
            axum::Json(AppendResponse {
                ok: true,
                error_code: None,
            }),
        );
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
        Ok(Ok(())) => (
            StatusCode::OK,
            axum::Json(AppendResponse {
                ok: true,
                error_code: None,
            }),
        ),
        _ => (
            StatusCode::OK,
            axum::Json(AppendResponse {
                ok: false,
                error_code: Some("invalid-data".to_owned()),
            }),
        ),
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn build_query_metadata(
    statement: String,
    rows_limit: Option<u64>,
    rows_offset: Option<u64>,
    init_time_ms: u32,
    session_id: String,
    query_id: Uuid,
) -> QueryStreamHeader {
    QueryStreamHeader {
        statement,
        rows_limit,
        rows_offset,
        init_time_ms,
        connections_errors: HashMap::new(),
        session_id,
        query_id,
        worker_slug: MOCK_WORKER_SLUG.to_owned(),
    }
}

fn ndjson_response(body: String) -> Response {
    bytes_response(StatusCode::OK, "application/x-ndjson", body.into_bytes())
}

fn bytes_response(status: StatusCode, content_type: &'static str, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap()
}

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
) -> anyhow::Result<(Vec<String>, Vec<duckdb::arrow::array::RecordBatch>)> {
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

        // Materialize JSON/VARIANT as JSON Utf8 (VARIANT otherwise arrives as binary struct).
        let column_types = describe_column_types(&conn, &sql).unwrap_or_default();
        let json_columns: std::collections::HashSet<String> = column_types
            .iter()
            .filter(|(_, ty)| is_json_or_variant_type(ty))
            .map(|(name, _)| name.clone())
            .collect();
        let sql = wrap_json_variant_casts(&sql, &column_types);

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| anyhow::anyhow!("Failed to prepare statement: {e}"))?;

        // Use query_arrow which executes the statement and gives schema + data
        let arrow_batches: Vec<duckdb::arrow::array::RecordBatch> = stmt
            .query_arrow(duckdb::params![])
            .map_err(|e| anyhow::anyhow!("Failed to execute query: {e}"))?
            .collect();

        // Annotate JSON Utf8 fields so default/jsonl encoding parses them as JSON objects.
        let arrow_batches = annotate_json_utf8_fields(arrow_batches, &json_columns);

        let columns = if let Some(first) = arrow_batches.first() {
            first
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        } else {
            vec![]
        };

        Ok((columns, arrow_batches))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Task join error: {e}"))?
}

fn describe_column_types(conn: &Connection, sql: &str) -> anyhow::Result<Vec<(String, String)>> {
    let describe_sql = format!("DESCRIBE {sql}");
    let mut stmt = conn
        .prepare(&describe_sql)
        .map_err(|e| anyhow::anyhow!("Failed to DESCRIBE query: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| anyhow::anyhow!("Failed to run DESCRIBE: {e}"))?;

    let mut columns = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| anyhow::anyhow!("Failed to read DESCRIBE row: {e}"))?
    {
        let name: String = row.get(0)?;
        let type_name: String = row.get(1)?;
        columns.push((name, type_name));
    }
    Ok(columns)
}

/// Cast JSON/VARIANT columns to JSON so Arrow export sees Utf8 JSON payloads (not VARIANT structs).
fn wrap_json_variant_casts(sql: &str, column_types: &[(String, String)]) -> String {
    if column_types.is_empty() {
        return sql.to_owned();
    }

    let mut needs_wrap = false;
    let mut select_list = Vec::with_capacity(column_types.len());
    for (name, type_name) in column_types {
        if is_json_or_variant_type(type_name) {
            needs_wrap = true;
            select_list.push(format!(
                "{}::JSON AS {}",
                escape_identifier(name),
                escape_identifier(name)
            ));
        } else {
            select_list.push(escape_identifier(name));
        }
    }

    if !needs_wrap {
        return sql.to_owned();
    }

    format!("SELECT {} FROM ({sql})", select_list.join(", "))
}

fn is_json_or_variant_type(type_name: &str) -> bool {
    let ty = type_name.to_ascii_uppercase();
    ty == ALTERTABLE_ORIGINAL_TYPE_JSON || ty == ALTERTABLE_ORIGINAL_TYPE_VARIANT
}

/// Mark known JSON/VARIANT Utf8 fields with `arrow.json` so export parses stringified JSON cells.
fn annotate_json_utf8_fields(
    batches: Vec<duckdb::arrow::array::RecordBatch>,
    json_columns: &std::collections::HashSet<String>,
) -> Vec<duckdb::arrow::array::RecordBatch> {
    use arrow_schema::extension::{EXTENSION_TYPE_NAME_KEY, ExtensionType, Json};
    use duckdb::arrow::datatypes::{DataType, Field, Schema};

    if batches.is_empty() || json_columns.is_empty() {
        return batches;
    }

    let schema = batches[0].schema();
    let new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            if crate::lakehouse::format::field_is_json_utf8(field) {
                return field.as_ref().clone();
            }
            if json_columns.contains(field.name()) && matches!(field.data_type(), DataType::Utf8) {
                let mut metadata = field.metadata().clone();
                metadata.insert(EXTENSION_TYPE_NAME_KEY.to_owned(), Json::NAME.to_owned());
                return Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                    .with_metadata(metadata);
            }
            field.as_ref().clone()
        })
        .collect();

    let changed = new_fields
        .iter()
        .zip(schema.fields().iter())
        .any(|(new_f, old_f)| new_f.metadata() != old_f.metadata());
    if !changed {
        return batches;
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    batches
        .into_iter()
        .filter_map(|batch| {
            duckdb::arrow::array::RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                .ok()
        })
        .collect()
}
// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use base64::{Engine, prelude::BASE64_STANDARD};
    use tower::ServiceExt;

    use crate::flight::layers::auth::Identity;
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

    async fn post_json(state: &LakehouseState, uri: &str, body: &str) -> StatusCode {
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    async fn query_rows(state: &LakehouseState, statement: &str) -> Vec<Value> {
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({ "statement": statement }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        text.trim_end_matches('\n')
            .split('\n')
            .skip(2)
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn make_router(state: LakehouseState) -> Router {
        super::router(state)
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
        assert!(header["init_time_ms"].as_u64().is_some_and(|ms| ms > 0));
        assert_eq!(header["rows_limit"], Value::Null);
        assert_eq!(header["rows_offset"], Value::Null);
        assert_eq!(header["connections_errors"], serde_json::json!({}));
        assert_eq!(header["worker_slug"], "altertable-mock");
        let cols: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(cols[0], "n");
        let row: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(row[0], 42);
    }

    #[tokio::test]
    async fn post_query_response_contains_query_id() {
        let app = make_router(make_state());
        let query_id = uuid::Uuid::new_v4().to_string();
        let body = serde_json::json!({
            "statement": "SELECT 1",
            "query_id": query_id,
        });

        let resp = app
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

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let header_line = text.lines().next().expect("expected NDJSON header line");
        let header: Value = serde_json::from_str(header_line).unwrap();
        assert_eq!(header["query_id"], query_id);
    }

    #[tokio::test]
    async fn post_query_invalid_sql_returns_ndjson_error() {
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
        assert_eq!(lines.len(), 2, "expected metadata + error lines");

        let metadata: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(metadata["statement"], "SELECT FROM WHERE");
        assert!(metadata["init_time_ms"].as_u64().is_some_and(|ms| ms > 0));
        assert_eq!(metadata["worker_slug"], "altertable-mock");

        let error: Value = serde_json::from_str(lines[1]).unwrap();
        assert!(error["error"].is_string());
    }

    async fn query_with_format(statement: &str, format: &str) -> axum::http::Response<Body> {
        let app = make_router(make_state());
        let body = serde_json::json!({
            "statement": statement,
            "format": format,
        });
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
        .unwrap()
    }

    #[tokio::test]
    async fn post_query_explicit_default_format_preserves_envelope() {
        let resp = query_with_format("SELECT 1 AS n", "default").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-ndjson"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert_eq!(text.lines().count(), 3);
    }

    #[tokio::test]
    async fn post_query_unknown_format_returns_400() {
        let resp = query_with_format("SELECT 1", "not-a-format").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("Unsupported query format"));
    }

    #[tokio::test]
    async fn post_query_csv_contains_only_csv_data() {
        let app = make_router(make_state());
        // Seed a small table, then query it as CSV.
        let setup = r#"{"statement":"CREATE TABLE users (id INTEGER, name VARCHAR); INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"}"#;
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(setup))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = serde_json::json!({
            "statement": "SELECT id, name FROM users ORDER BY id",
            "format": "csv",
        });
        let resp = app
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

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/csv"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert_eq!(text, "id,name\n1,Alice\n2,Bob\n");
    }

    #[tokio::test]
    async fn post_query_jsonl_contains_named_objects_only() {
        let app = make_router(make_state());
        let setup = r#"{"statement":"CREATE TABLE users_jsonl (id INTEGER, name VARCHAR); INSERT INTO users_jsonl VALUES (1, 'Alice'), (2, 'Bob')"}"#;
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(setup))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = serde_json::json!({
            "statement": "SELECT id, name FROM users_jsonl ORDER BY id",
            "format": "jsonl",
        });
        let resp = app
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

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-ndjson"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).expect("invalid JSONL row"))
            .collect();
        assert_eq!(
            lines,
            vec![
                serde_json::json!({"id": 1, "name": "Alice"}),
                serde_json::json!({"id": 2, "name": "Bob"}),
            ]
        );
    }

    #[tokio::test]
    async fn post_query_parquet_contains_only_parquet_data() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let app = make_router(make_state());
        let setup = r#"{"statement":"CREATE TABLE users_parquet (id INTEGER, name VARCHAR); INSERT INTO users_parquet VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"}"#;
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(setup))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = serde_json::json!({
            "statement": "SELECT id, name FROM users_parquet ORDER BY id",
            "format": "parquet",
        });
        let resp = app
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

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/parquet"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("invalid Parquet body")
            .build()
            .expect("failed to build Parquet reader");
        let batches: Vec<_> = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read Parquet batches");
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
    }

    async fn ndjson_query_succeeded(resp: axum::http::Response<Body>) -> bool {
        if resp.status() != StatusCode::OK {
            return false;
        }
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        !text.lines().any(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .is_some_and(|v| v.get("error").is_some())
        })
    }

    #[tokio::test]
    async fn post_query_json_and_variant_cells_are_parsed_objects() {
        let state = make_state();
        let app = make_router(state);

        let setup_variant = r#"{"statement":"CREATE TABLE json_rows (id INTEGER, payload JSON, attributes VARIANT); INSERT INTO json_rows VALUES (1, '{\"a\":1}', json('{\"b\":2}')::VARIANT), (2, '[1,2,3]', json('{\"nested\":{\"x\":true}}')::VARIANT)"}"#;
        let setup_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(setup_variant))
                    .unwrap(),
            )
            .await
            .unwrap();

        let (statement, expect_variant) = if ndjson_query_succeeded(setup_resp).await {
            (
                "SELECT id, payload, attributes FROM json_rows ORDER BY id",
                true,
            )
        } else {
            let setup_json = r#"{"statement":"CREATE TABLE json_rows (id INTEGER, payload JSON); INSERT INTO json_rows VALUES (1, '{\"a\":1}'), (2, '[1,2,3]')"}"#;
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/query")
                        .header(header::AUTHORIZATION, basic_auth_header())
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(setup_json))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(ndjson_query_succeeded(resp).await);
            ("SELECT id, payload FROM json_rows ORDER BY id", false)
        };

        let body = serde_json::json!({ "statement": statement });
        let resp = app
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
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert!(lines.len() >= 4);
        let row1: Value = serde_json::from_str(lines[2]).unwrap();
        let row2: Value = serde_json::from_str(lines[3]).unwrap();
        assert_eq!(row1[0], 1);
        assert_eq!(row1[1], serde_json::json!({"a": 1}));
        assert_eq!(row2[0], 2);
        assert_eq!(row2[1], serde_json::json!([1, 2, 3]));
        if expect_variant {
            assert_eq!(row1[2], serde_json::json!({"b": 2}));
            assert_eq!(row2[2], serde_json::json!({"nested": {"x": true}}));
        }
    }

    #[tokio::test]
    async fn post_query_non_default_format_sql_error_has_no_ndjson_envelope() {
        let resp = query_with_format("SELECT * FROM unknown_table", "csv").await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/csv"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("\"error\""));
        assert!(!text.starts_with('{'));
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

    // ── POST /explain ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn explain_requires_auth() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/explain")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SELECT 1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn explain_simple_select_has_no_table_scans() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/explain")
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
        assert!(result.get("error").is_none() || result["error"].is_null());
        assert_eq!(result["tables"], serde_json::json!([]));
        assert_eq!(result["statement"], "SELECT 1");
        assert_eq!(result["connections_errors"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn explain_table_scan_includes_estimates() {
        let state = make_state();
        {
            let conn = state
                .get_or_create_connection(&Identity {
                    username: "testuser".to_owned().into(),
                    password: "testpass".to_owned().into(),
                })
                .await;
            let conn = conn.lock().unwrap();
            conn.execute(
                "CREATE TABLE events (id INTEGER, category VARCHAR)",
                duckdb::params![],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO events SELECT i, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END FROM generate_series(1, 100) t(i)",
                duckdb::params![],
            )
            .unwrap();
        }

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/explain")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"statement":"SELECT * FROM events WHERE id > 50"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(result.get("error").is_none() || result["error"].is_null());
        assert_eq!(result["tables"].as_array().unwrap().len(), 1);
        assert!(
            result["tables"][0]["table_name"]
                .as_str()
                .is_some_and(|name| name.ends_with("events"))
        );
        assert_eq!(result["tables"][0]["filters"], "id>50");
        assert!(result["tables"][0]["estimated_rows"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn explain_invalid_sql_returns_error_in_body() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/explain")
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
        assert!(result["error"].is_string());
        assert_eq!(result["tables"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn explain_include_plan_returns_plan() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/explain")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"statement":"SELECT 1","include_plan":true}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(result.get("error").is_none() || result["error"].is_null());
        assert!(result["plan"].is_array());
        assert_eq!(result["plan"].as_array().unwrap().len(), 1);
        assert!(result["plan"][0]["name"].is_string());
    }

    // ── POST /autocomplete ───────────────────────────────────────────────────

    #[tokio::test]
    async fn autocomplete_requires_auth() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/autocomplete")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SEL"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn autocomplete_includes_select_for_sel() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/autocomplete")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SEL","max_suggestions":50}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        let suggestions = result["suggestions"].as_array().unwrap();
        assert!(
            suggestions.iter().any(|s| {
                s["suggestion"]
                    .as_str()
                    .is_some_and(|t| t.trim() == "SELECT")
            }),
            "expected SELECT among suggestions: {suggestions:?}"
        );
        assert_eq!(result["statement"], "SEL");
    }

    #[tokio::test]
    async fn autocomplete_respects_max_suggestions() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/autocomplete")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"statement":"SEL","max_suggestions":3}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["suggestions"].as_array().unwrap().len(), 3);
        assert_eq!(result["statement"], "SEL");
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
                    .uri("/upload?catalog=memory&schema=main&table=people&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "text/csv")
                    .body(Body::from(csv))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn upload_bad_parquet_returns_400() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=t&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/parquet")
                    .body(Body::from("not parquet"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── POST /upsert ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn upsert_without_primary_key_returns_400() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upsert?catalog=memory&schema=main&table=t")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/parquet")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn upsert_json_by_primary_key() {
        let state = make_state();
        let create_body = r#"[{"id":1,"value":100},{"id":2,"value":200},{"id":3,"value":300}]"#;
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=upsert_test&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(create_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let upsert_body = r#"[{"id":2,"value":250},{"id":4,"value":400}]"#;
        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upsert?catalog=memory&schema=main&table=upsert_test&primary_key=id")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(upsert_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn upload_accepts_bodies_over_the_default_extractor_limit() {
        let state = make_state();
        let mut csv = String::with_capacity(3_500_000);
        csv.push_str("id\n");
        for row in 0..400_000 {
            csv.push_str(&format!("{row}\n"));
        }
        assert!(csv.len() > 2 * 1024 * 1024);

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=big_upload&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "text/csv")
                    .body(Body::from(csv))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn upsert_creates_the_table_when_missing() {
        let state = make_state();
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upsert?catalog=memory&schema=main&table=fresh_upsert&primary_key=id")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"[{"id":1,"value":100}]"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"statement":"SELECT count(*) FROM memory.main.fresh_upsert"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        let row: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(row[0], 1);
    }

    #[tokio::test]
    async fn upsert_composite_primary_key_with_cursor() {
        let state = make_state();
        let create_body = r#"[
            {"id":1,"region":"eu","updated_at":10,"value":100},
            {"id":2,"region":"us","updated_at":10,"value":200}
        ]"#;
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upload?catalog=memory&schema=main&table=cursor_test&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(create_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let stale_newer_and_new_rows = r#"[
            {"id":1,"region":"eu","updated_at":5,"value":999},
            {"id":2,"region":"us","updated_at":20,"value":250},
            {"id":3,"region":"ap","updated_at":1,"value":300}
        ]"#;
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/upsert?catalog=memory&schema=main&table=cursor_test&primary_key=id,region&cursor_field=updated_at")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(stale_newer_and_new_rows))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"statement":"SELECT value FROM memory.main.cursor_test ORDER BY id"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        let values: Vec<i64> = lines[2..]
            .iter()
            .map(|line| {
                serde_json::from_str::<Value>(line).unwrap()[0]
                    .as_i64()
                    .unwrap()
            })
            .collect();
        assert_eq!(values, vec![100, 250, 300]);
    }

    #[tokio::test]
    async fn upload_create_append_creates_then_appends() {
        let state = make_state();

        for body in [r#"[{"id":1}]"#, r#"[{"id":2}]"#] {
            let resp = make_router(state.clone())
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/upload?catalog=memory&schema=main&table=ca_test&mode=create_append")
                        .header(header::AUTHORIZATION, basic_auth_header())
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        let resp = make_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"statement":"SELECT count(*) FROM memory.main.ca_test"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        let row: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(row[0], 2);
    }

    #[test]
    fn parse_quoted_columns_trims_and_drops_empty_elements() {
        assert_eq!(
            parse_quoted_columns(" id, , region ", "primary_key").unwrap(),
            vec![r#""id""#, r#""region""#]
        );
        assert!(parse_quoted_columns(" , ", "primary_key").is_err());
    }

    #[tokio::test]
    async fn upsert_cursor_field_naming_no_column_returns_400() {
        let state = make_state();
        let status = post_json(
            &state,
            "/upsert?catalog=memory&schema=main&table=blank_cursor&primary_key=id&cursor_field=%20,%20",
            r#"[{"id":1}]"#,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn upsert_null_cursor_never_wins() {
        let state = make_state();
        assert_eq!(
            post_json(
                &state,
                "/upload?catalog=memory&schema=main&table=null_cursor&mode=create",
                r#"[{"id":1,"updated_at":null,"value":100},{"id":9,"updated_at":5,"value":50}]"#,
            )
            .await,
            StatusCode::OK
        );

        let upsert_uri = "/upsert?catalog=memory&schema=main&table=null_cursor&primary_key=id&cursor_field=updated_at";
        assert_eq!(
            post_json(
                &state,
                upsert_uri,
                r#"[{"id":1,"updated_at":10,"value":200}]"#
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            post_json(
                &state,
                upsert_uri,
                r#"[{"id":1,"updated_at":null,"value":999}]"#
            )
            .await,
            StatusCode::OK
        );

        let rows = query_rows(
            &state,
            "SELECT id, updated_at, value FROM memory.main.null_cursor ORDER BY id",
        )
        .await;
        assert_eq!(
            rows,
            vec![
                serde_json::json!([1, 10, 200]),
                serde_json::json!([9, 5, 50])
            ]
        );
    }

    #[tokio::test]
    async fn upsert_in_batch_dedup_ranks_null_cursor_lowest() {
        let state = make_state();
        assert_eq!(
            post_json(
                &state,
                "/upload?catalog=memory&schema=main&table=batch_null_cursor&mode=create",
                r#"[{"id":1,"updated_at":1,"value":100}]"#,
            )
            .await,
            StatusCode::OK
        );

        assert_eq!(
            post_json(
                &state,
                "/upsert?catalog=memory&schema=main&table=batch_null_cursor&primary_key=id&cursor_field=updated_at",
                r#"[{"id":1,"updated_at":null,"value":999},{"id":1,"updated_at":5,"value":150},{"id":1,"updated_at":20,"value":200}]"#,
            )
            .await,
            StatusCode::OK
        );

        let rows = query_rows(
            &state,
            "SELECT id, updated_at, value FROM memory.main.batch_null_cursor ORDER BY id",
        )
        .await;
        assert_eq!(rows, vec![serde_json::json!([1, 20, 200])]);
    }

    #[tokio::test]
    async fn upsert_null_leading_cursor_defers_to_the_trailing_one() {
        let state = make_state();
        assert_eq!(
            post_json(
                &state,
                "/upload?catalog=memory&schema=main&table=composite_cursor&mode=create",
                r#"[{"id":1,"version":null,"updated_at":1,"value":100},{"id":2,"version":null,"updated_at":5,"value":200},{"id":3,"version":1,"updated_at":1,"value":300}]"#,
            )
            .await,
            StatusCode::OK
        );

        let upsert_uri = "/upsert?catalog=memory&schema=main&table=composite_cursor&primary_key=id&cursor_field=version,updated_at";
        assert_eq!(
            post_json(
                &state,
                upsert_uri,
                r#"[{"id":1,"version":null,"updated_at":2,"value":111},{"id":2,"version":1,"updated_at":1,"value":222},{"id":3,"version":null,"updated_at":9,"value":333}]"#,
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            post_json(
                &state,
                upsert_uri,
                r#"[{"id":1,"version":null,"updated_at":2,"value":999}]"#
            )
            .await,
            StatusCode::OK
        );

        let rows = query_rows(
            &state,
            "SELECT id, version, updated_at, value FROM memory.main.composite_cursor ORDER BY id",
        )
        .await;
        assert_eq!(
            rows,
            vec![
                serde_json::json!([1, null, 2, 111]),
                serde_json::json!([2, 1, 1, 222]),
                serde_json::json!([3, 1, 1, 300]),
            ]
        );
    }

    #[tokio::test]
    async fn key_only_upsert_inserts_new_rows_and_leaves_matched_ones() {
        let state = make_state();
        assert_eq!(
            post_json(
                &state,
                "/upload?catalog=memory&schema=main&table=key_only&mode=create",
                r#"[{"id":1,"region":"eu"},{"id":2,"region":"us"}]"#,
            )
            .await,
            StatusCode::OK
        );

        assert_eq!(
            post_json(
                &state,
                "/upsert?catalog=memory&schema=main&table=key_only&primary_key=id,region",
                r#"[{"id":1,"region":"eu"},{"id":3,"region":"ap"}]"#,
            )
            .await,
            StatusCode::OK
        );

        let rows = query_rows(
            &state,
            "SELECT id, region FROM memory.main.key_only ORDER BY id",
        )
        .await;
        assert_eq!(
            rows,
            vec![
                serde_json::json!([1, "eu"]),
                serde_json::json!([2, "us"]),
                serde_json::json!([3, "ap"]),
            ]
        );
    }

    #[tokio::test]
    async fn create_append_pins_json_types_to_the_existing_table() {
        let state = make_state();
        query_rows(
            &state,
            "CREATE TABLE memory.main.pinned_append (id VARCHAR, title VARCHAR)",
        )
        .await;

        assert_eq!(
            post_json(
                &state,
                "/upload?catalog=memory&schema=main&table=pinned_append&mode=create_append",
                r#"[{"id":"550E8400-E29B-41D4-A716-446655440000","title":"First Doc"}]"#,
            )
            .await,
            StatusCode::OK
        );

        let rows = query_rows(
            &state,
            "SELECT id, title FROM memory.main.pinned_append ORDER BY id",
        )
        .await;
        assert_eq!(
            rows,
            vec![serde_json::json!([
                "550E8400-E29B-41D4-A716-446655440000",
                "First Doc"
            ])]
        );
    }

    #[tokio::test]
    async fn upsert_pins_json_types_to_the_existing_table() {
        let state = make_state();
        query_rows(
            &state,
            "CREATE TABLE memory.main.pinned_upsert (id VARCHAR, title VARCHAR)",
        )
        .await;

        assert_eq!(
            post_json(
                &state,
                "/upsert?catalog=memory&schema=main&table=pinned_upsert&primary_key=id",
                r#"[{"id":"550E8400-E29B-41D4-A716-446655440000","title":"First Doc"}]"#,
            )
            .await,
            StatusCode::OK
        );

        let rows = query_rows(
            &state,
            "SELECT id, title FROM memory.main.pinned_upsert ORDER BY id",
        )
        .await;
        assert_eq!(
            rows,
            vec![serde_json::json!([
                "550E8400-E29B-41D4-A716-446655440000",
                "First Doc"
            ])]
        );
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
                    .uri("/upload?catalog=memory&schema=main&table=append_test&mode=create")
                    .header(header::AUTHORIZATION, basic_auth_header())
                    .header(header::CONTENT_TYPE, "text/csv")
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
    async fn append_invalid_body_returns_400() {
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

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["ok"], false);
        assert_eq!(result["error_code"], "invalid-data");
    }
}
