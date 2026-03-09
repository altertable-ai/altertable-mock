use axum::{Extension, extract::State, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use duckdb::params;
use tracing::warn;

use crate::product_analytics::auth::ApiKey;

use super::{
    state::{EnvironmentState, ProductAnalyticsState, new_uuid},
    types::{
        AliasPayload, AliasRequest, AliasResponse, IdentifyPayload, IdentifyRequest,
        IdentifyResponse, TrackPayload, TrackRequest, TrackResponse,
    },
};

// ── /track ────────────────────────────────────────────────────────────────────

pub async fn post_track(
    State(state): State<ProductAnalyticsState>,
    Extension(api_key): Extension<ApiKey>,
    axum::Json(req): axum::Json<TrackRequest>,
) -> impl IntoResponse {
    let payloads = req.into_vec();

    for payload in payloads {
        let Some(env_state) = state
            .get_environment(&api_key.0, &payload.environment)
            .await
        else {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(TrackResponse {
                    ok: false,
                    error_code: Some("environment-not-found".to_owned()),
                }),
            );
        };

        let result = tokio::task::spawn_blocking(move || {
            let env = env_state.lock().unwrap();
            insert_event(&env, &payload)
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("Failed to insert event: {}", e);
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(TrackResponse {
                        ok: false,
                        error_code: Some("insert-failed".to_owned()),
                    }),
                );
            }
            Err(e) => {
                warn!("Spawn blocking error: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(TrackResponse {
                        ok: false,
                        error_code: Some("internal-error".to_owned()),
                    }),
                );
            }
        }
    }

    (
        StatusCode::OK,
        axum::Json(TrackResponse {
            ok: true,
            error_code: None,
        }),
    )
}

// ── /identify ─────────────────────────────────────────────────────────────────

pub async fn post_identify(
    State(state): State<ProductAnalyticsState>,
    Extension(api_key): Extension<ApiKey>,
    axum::Json(req): axum::Json<IdentifyRequest>,
) -> impl IntoResponse {
    let payloads = req.into_vec();

    for payload in payloads {
        let Some(env_state) = state
            .get_environment(&api_key.0, &payload.environment)
            .await
        else {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(IdentifyResponse {
                    ok: false,
                    error_code: Some("environment-not-found".to_owned()),
                }),
            );
        };

        let result = tokio::task::spawn_blocking(move || {
            let env = env_state.lock().unwrap();
            upsert_identity(&env, &payload)
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("Failed to identify user: {}", e);
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(IdentifyResponse {
                        ok: false,
                        error_code: Some("identify-failed".to_owned()),
                    }),
                );
            }
            Err(e) => {
                warn!("Spawn blocking error: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(IdentifyResponse {
                        ok: false,
                        error_code: Some("internal-error".to_owned()),
                    }),
                );
            }
        }
    }

    (
        StatusCode::OK,
        axum::Json(IdentifyResponse {
            ok: true,
            error_code: None,
        }),
    )
}

// ── /alias ────────────────────────────────────────────────────────────────────

pub async fn post_alias(
    State(state): State<ProductAnalyticsState>,
    Extension(api_key): Extension<ApiKey>,
    axum::Json(req): axum::Json<AliasRequest>,
) -> impl IntoResponse {
    let payloads = req.into_vec();

    for payload in payloads {
        let Some(env_state) = state
            .get_environment(&api_key.0, &payload.environment)
            .await
        else {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(AliasResponse {
                    ok: false,
                    error_code: Some("environment-not-found".to_owned()),
                }),
            );
        };

        let result = tokio::task::spawn_blocking(move || {
            let env = env_state.lock().unwrap();
            merge_identities(&env, &payload.distinct_id, &payload.new_user_id)
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!("Failed to alias identities: {}", e);
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(AliasResponse {
                        ok: false,
                        error_code: Some("alias-failed".to_owned()),
                    }),
                );
            }
            Err(e) => {
                warn!("Spawn blocking error: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(AliasResponse {
                        ok: false,
                        error_code: Some("internal-error".to_owned()),
                    }),
                );
            }
        }
    }

    (
        StatusCode::OK,
        axum::Json(AliasResponse {
            ok: true,
            error_code: None,
        }),
    )
}

// ── Internal logic ────────────────────────────────────────────────────────────

/// Insert a single event, resolving identity if a `distinct_id` is provided.
fn insert_event(env: &EnvironmentState, payload: &TrackPayload) -> Result<(), anyhow::Error> {
    let now = Utc::now().to_rfc3339();
    let event_uuid = new_uuid().to_string();

    let timestamp = resolve_timestamp(payload.timestamp.as_ref(), &now);

    // Resolve identity for the distinct_id
    let (identity_uuid, identity_traits) = if let Some(ref distinct_id) = payload.distinct_id {
        let (uuid, traits) = get_or_create_identity(env, distinct_id)?;
        (Some(uuid), Some(traits))
    } else {
        (None, None)
    };

    // If we have both a distinct_id and an anonymous_id, link them
    if let (Some(identity_uuid), Some(anonymous_id)) = (&identity_uuid, &payload.anonymous_id)
        && payload
            .distinct_id
            .as_deref()
            .is_none_or(|d| d != anonymous_id)
    {
        link_distinct_id_to_identity(env, anonymous_id, identity_uuid)?;
    }

    let properties_json = serde_json::to_string(&payload.properties)?;
    let identity_traits_json = identity_traits
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;

    let conn = env.conn.lock().unwrap();
    conn.execute(
        "INSERT INTO events (uuid, timestamp, event, properties, distinct_id, anonymous_id, device_id, session_id, identity_uuid, identity_traits)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            event_uuid,
            timestamp,
            payload.event,
            properties_json,
            payload.distinct_id,
            payload.anonymous_id,
            payload.device_id,
            payload.session_id,
            identity_uuid,
            identity_traits_json,
        ],
    )?;

    Ok(())
}

/// Upsert an identity record: create or merge traits, mark as identified,
/// and optionally link an anonymous_id.
fn upsert_identity(env: &EnvironmentState, payload: &IdentifyPayload) -> Result<(), anyhow::Error> {
    let (identity_uuid, existing_traits) = get_or_create_identity(env, &payload.distinct_id)?;

    // Shallow-merge new traits over existing traits
    let merged_traits = if let Some(ref new_traits) = payload.traits {
        let mut merged = existing_traits;
        for (k, v) in new_traits {
            merged.insert(k.clone(), v.clone());
        }
        merged
    } else {
        existing_traits
    };

    let merged_traits_json = serde_json::to_string(&merged_traits)?;
    let now = Utc::now().to_rfc3339();

    // Update the identity record: bump version, set is_identified, merge traits
    {
        let conn = env.conn.lock().unwrap();
        conn.execute(
            "UPDATE identities SET traits = ?, is_identified = true, version = version + 1 WHERE uuid = ?",
            params![merged_traits_json, identity_uuid],
        )?;

        // Also update identity_distinct_ids version for the primary distinct_id
        conn.execute(
            "UPDATE identity_distinct_ids SET version = version + 1 WHERE distinct_id = ? AND identity_uuid = ?",
            params![payload.distinct_id, identity_uuid],
        )?;

        conn.execute(
            "UPDATE identity_distinct_id_overrides SET version = version + 1 WHERE distinct_id = ? AND identity_uuid = ?",
            params![payload.distinct_id, identity_uuid],
        )?;
    }

    // Link anonymous_id if provided and different from distinct_id
    if let Some(ref anonymous_id) = payload.anonymous_id
        && anonymous_id != &payload.distinct_id
    {
        link_distinct_id_to_identity(env, anonymous_id, &identity_uuid)?;
    }

    // Update all existing events for this identity with the fresh traits
    {
        let conn = env.conn.lock().unwrap();
        conn.execute(
            "UPDATE events SET identity_traits = ?, identity_uuid = ? WHERE distinct_id = ?",
            params![merged_traits_json, identity_uuid, payload.distinct_id],
        )?;

        // Also update events that were tracked with the anonymous_id prior to identification
        if let Some(ref anonymous_id) = payload.anonymous_id {
            conn.execute(
                "UPDATE events SET identity_traits = ?, identity_uuid = ? WHERE anonymous_id = ? AND identity_uuid IS NULL",
                params![merged_traits_json, identity_uuid, anonymous_id],
            )?;
        }
    }

    let _ = now;
    Ok(())
}

/// Merge the identity of `distinct_id` (source) into the identity of `new_user_id` (target).
/// All distinct_id mappings and events from the source are re-pointed to the target, then
/// the source identity row is deleted.
fn merge_identities(
    env: &EnvironmentState,
    distinct_id: &str,
    new_user_id: &str,
) -> Result<(), anyhow::Error> {
    let (target_uuid, _) = get_or_create_identity(env, new_user_id)?;
    let (source_uuid, _) = get_or_create_identity(env, distinct_id)?;

    if source_uuid == target_uuid {
        return Ok(());
    }

    let conn = env.conn.lock().unwrap();

    conn.execute(
        "UPDATE identity_distinct_ids SET identity_uuid = ? WHERE identity_uuid = ?",
        params![target_uuid, source_uuid],
    )?;
    conn.execute(
        "UPDATE identity_distinct_id_overrides SET identity_uuid = ? WHERE identity_uuid = ?",
        params![target_uuid, source_uuid],
    )?;
    conn.execute(
        "UPDATE events SET identity_uuid = ? WHERE identity_uuid = ?",
        params![target_uuid, source_uuid],
    )?;
    conn.execute(
        "UPDATE identities SET is_identified = true, version = version + 1 WHERE uuid = ?",
        params![target_uuid],
    )?;
    conn.execute(
        "DELETE FROM identities WHERE uuid = ?",
        params![source_uuid],
    )?;

    Ok(())
}

/// Retrieve existing identity by distinct_id, or create a new one.
/// Returns `(identity_uuid, current_traits)`.
fn get_or_create_identity(
    env: &EnvironmentState,
    distinct_id: &str,
) -> Result<(String, serde_json::Map<String, serde_json::Value>), anyhow::Error> {
    // Look up existing mapping
    let existing = {
        let conn = env.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT d.identity_uuid, i.traits
             FROM identity_distinct_ids d
             JOIN identities i ON d.identity_uuid = i.uuid
             WHERE d.distinct_id = ?
             LIMIT 1",
        )?;

        struct Row {
            identity_uuid: String,
            traits_json: Option<String>,
        }

        let rows: Vec<Row> = stmt
            .query_map(params![distinct_id], |row| {
                Ok(Row {
                    identity_uuid: row.get(0)?,
                    traits_json: row.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        rows.into_iter().next()
    };

    if let Some(row) = existing {
        let traits: serde_json::Map<String, serde_json::Value> = row
            .traits_json
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        return Ok((row.identity_uuid, traits));
    }

    // Create a new identity
    let new_uuid = new_uuid().to_string();
    let now = Utc::now().to_rfc3339();
    let version = 1u64;

    {
        let conn = env.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO identities (uuid, traits, is_identified, is_deleted, version, created_at)
             VALUES (?, '{}', false, false, ?, ?)",
            params![new_uuid, version, now],
        )?;
        conn.execute(
            "INSERT INTO identity_distinct_ids (distinct_id, identity_uuid, version, created_at)
             VALUES (?, ?, ?, ?)",
            params![distinct_id, new_uuid, version, now],
        )?;
        conn.execute(
            "INSERT INTO identity_distinct_id_overrides (distinct_id, identity_uuid, version, created_at)
             VALUES (?, ?, ?, ?)",
            params![distinct_id, new_uuid, version, now],
        )?;
    }

    Ok((new_uuid, serde_json::Map::new()))
}

/// Link an additional distinct_id (e.g., anonymous_id) to an existing identity.
/// No-ops if the distinct_id is already linked.
fn link_distinct_id_to_identity(
    env: &EnvironmentState,
    distinct_id: &str,
    identity_uuid: &str,
) -> Result<(), anyhow::Error> {
    let already_exists = {
        let conn = env.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT 1 FROM identity_distinct_ids WHERE distinct_id = ? LIMIT 1")?;
        let rows: Vec<i32> = stmt
            .query_map(params![distinct_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        !rows.is_empty()
    };

    if already_exists {
        return Ok(());
    }

    let now = Utc::now().to_rfc3339();
    let version = 1u64;

    let conn = env.conn.lock().unwrap();
    conn.execute(
        "INSERT INTO identity_distinct_ids (distinct_id, identity_uuid, version, created_at)
         VALUES (?, ?, ?, ?)",
        params![distinct_id, identity_uuid, version, now],
    )?;
    conn.execute(
        "INSERT INTO identity_distinct_id_overrides (distinct_id, identity_uuid, version, created_at)
         VALUES (?, ?, ?, ?)",
        params![distinct_id, identity_uuid, version, now],
    )?;

    Ok(())
}

/// Resolve a FlexibleTimestamp to an ISO 8601 string, defaulting to now.
fn resolve_timestamp(ts: Option<&super::types::FlexibleTimestamp>, now: &str) -> String {
    match ts {
        Some(super::types::FlexibleTimestamp::Millis(ms)) => {
            let secs = ms / 1000;
            let nanos = ((ms % 1000) * 1_000_000) as u32;
            chrono::DateTime::<Utc>::from_timestamp(secs, nanos)
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| now.to_owned())
        }
        Some(super::types::FlexibleTimestamp::Iso(s)) => s.clone(),
        None => now.to_owned(),
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
    use tower::ServiceExt;

    use crate::product_analytics::auth::auth_middleware;
    use crate::product_analytics::state::ProductAnalyticsState;

    use super::*;

    const TEST_API_KEY: &str = "test-api-key-abc123";

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_state() -> ProductAnalyticsState {
        let mut set = HashSet::new();
        set.insert(TEST_API_KEY.to_owned());
        ProductAnalyticsState::with_environments(
            Arc::new(set),
            &[
                "production".to_owned(),
                "prod".to_owned(),
                "staging".to_owned(),
            ],
        )
    }

    fn api_key_header() -> (&'static str, &'static str) {
        ("X-API-Key", TEST_API_KEY)
    }

    fn make_router(state: ProductAnalyticsState) -> Router {
        Router::new()
            .route("/track", routing::post(post_track))
            .route("/identify", routing::post(post_identify))
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    /// Query the in-memory DuckDB for `env` and return rows as JSON values.
    async fn query_env(
        state: &ProductAnalyticsState,
        environment: &str,
        sql: &str,
    ) -> Vec<serde_json::Value> {
        let env_state = state
            .get_environment(TEST_API_KEY, environment)
            .await
            .expect("environment not found in test state");
        let sql = sql.to_owned();
        tokio::task::spawn_blocking(move || {
            let env = env_state.lock().unwrap();
            let conn = env.conn.lock().unwrap();
            let mut stmt = conn.prepare(&sql).unwrap();
            let batches: Vec<duckdb::arrow::array::RecordBatch> =
                stmt.query_arrow(duckdb::params![]).unwrap().collect();

            if batches.is_empty() {
                return vec![];
            }

            let schema = batches[0].schema();
            let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

            batches
                .iter()
                .flat_map(|batch| {
                    let col_names = col_names.clone();
                    (0..batch.num_rows()).map(move |row_idx| {
                        let mut map = serde_json::Map::new();
                        for (col_idx, name) in col_names.iter().enumerate() {
                            use duckdb::arrow::array::Array;
                            let col = batch.column(col_idx);
                            // Cast everything to String via debug representation for simplicity
                            let val = if col.is_null(row_idx) {
                                serde_json::Value::Null
                            } else {
                                use duckdb::arrow::array::*;
                                use duckdb::arrow::datatypes::DataType;
                                let s = match col.data_type() {
                                    DataType::Utf8 => col
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .map(|a| a.value(row_idx).to_owned()),
                                    DataType::LargeUtf8 => col
                                        .as_any()
                                        .downcast_ref::<LargeStringArray>()
                                        .map(|a| a.value(row_idx).to_owned()),
                                    DataType::Boolean => col
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::Int8 => col
                                        .as_any()
                                        .downcast_ref::<Int8Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::Int16 => col
                                        .as_any()
                                        .downcast_ref::<Int16Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::Int32 => col
                                        .as_any()
                                        .downcast_ref::<Int32Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::Int64 => col
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::UInt8 => col
                                        .as_any()
                                        .downcast_ref::<UInt8Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::UInt16 => col
                                        .as_any()
                                        .downcast_ref::<UInt16Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::UInt32 => col
                                        .as_any()
                                        .downcast_ref::<UInt32Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    DataType::UInt64 => col
                                        .as_any()
                                        .downcast_ref::<UInt64Array>()
                                        .map(|a| a.value(row_idx).to_string()),
                                    _ => {
                                        // Fallback: use arrow_json to serialize
                                        Some(format!("{col:?}"))
                                    }
                                };
                                s.map(serde_json::Value::String)
                                    .unwrap_or(serde_json::Value::Null)
                            };
                            map.insert(name.clone(), val);
                        }
                        serde_json::Value::Object(map)
                    })
                })
                .collect()
        })
        .await
        .unwrap()
    }

    // ── auth ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn missing_auth_is_rejected() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"e","environment":"prod","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_api_key_is_rejected() {
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header("X-API-Key", "wrong-api-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"e","environment":"prod","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── POST /track ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn track_unknown_environment_returns_environment_not_found() {
        let (key, val) = api_key_header();
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"e","environment":"non-existent-environment","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "ok": false, "error_code": "environment-not-found" })
        );
    }

    #[tokio::test]
    async fn identify_unknown_environment_returns_environment_not_found() {
        let (key, val) = api_key_header();
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_1","environment":"non-existent-environment"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body,
            serde_json::json!({ "ok": false, "error_code": "environment-not-found" })
        );
    }

    #[tokio::test]
    async fn track_single_event_returns_ok() {
        let (key, val) = api_key_header();
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"Button Clicked","environment":"production","properties":{"button":"signup"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"], true);
        assert!(body["error_code"].is_null());
    }

    #[tokio::test]
    async fn track_batch_events_returns_ok() {
        let (key, val) = api_key_header();
        let app = make_router(make_state());
        let batch = serde_json::json!([
            {"event": "Page View", "environment": "production", "properties": {"url": "/home"}},
            {"event": "Click", "environment": "production", "properties": {"target": "btn"}}
        ]);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(batch.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"], true);
    }

    #[tokio::test]
    async fn track_persists_event_to_duckdb() {
        let state = make_state();
        let (key, val) = api_key_header();
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"Signup","environment":"production","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(&state, "production", "SELECT event FROM events").await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["event"], "Signup");
    }

    #[tokio::test]
    async fn track_with_distinct_id_creates_identity() {
        let state = make_state();
        let (key, val) = api_key_header();
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"Purchase","environment":"prod","distinct_id":"user_42","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(
            &state,
            "prod",
            "SELECT distinct_id FROM identity_distinct_ids WHERE distinct_id = 'user_42'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["distinct_id"], "user_42");
    }

    #[tokio::test]
    async fn track_with_distinct_id_stores_identity_uuid_on_event() {
        let state = make_state();
        let (key, val) = api_key_header();
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"Login","environment":"prod","distinct_id":"user_99","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(
            &state,
            "prod",
            "SELECT identity_uuid FROM events WHERE distinct_id = 'user_99'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        assert!(
            !rows[0]["identity_uuid"].is_null(),
            "identity_uuid should be set on the event"
        );
    }

    #[tokio::test]
    async fn track_links_anonymous_id_to_distinct_id_identity() {
        let state = make_state();
        let (key, val) = api_key_header();
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"AddToCart","environment":"prod","distinct_id":"user_1","anonymous_id":"anon_abc","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Both user_1 and anon_abc should be linked to the same identity
        let rows = query_env(
            &state,
            "prod",
            "SELECT DISTINCT identity_uuid FROM identity_distinct_ids WHERE distinct_id IN ('user_1', 'anon_abc')",
        )
        .await;
        assert_eq!(
            rows.len(),
            1,
            "user_1 and anon_abc should share one identity"
        );
    }

    #[tokio::test]
    async fn track_with_unix_millis_timestamp() {
        let state = make_state();
        let (key, val) = api_key_header();
        // 2024-01-01T00:00:00Z in millis
        let ts_ms: i64 = 1704067200000;
        let body = serde_json::json!({
            "event": "TimedEvent",
            "environment": "prod",
            "properties": {},
            "timestamp": ts_ms
        });
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let rows = query_env(
            &state,
            "prod",
            "SELECT timestamp FROM events WHERE event = 'TimedEvent'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        let ts_str = rows[0]["timestamp"].as_str().unwrap();
        assert!(
            ts_str.contains("2024"),
            "timestamp should reflect 2024-01-01, got {ts_str}"
        );
    }

    #[tokio::test]
    async fn track_with_iso_timestamp() {
        let state = make_state();
        let (key, val) = api_key_header();
        let body = serde_json::json!({
            "event": "IsoEvent",
            "environment": "prod",
            "properties": {},
            "timestamp": "2025-06-15T12:00:00Z"
        });
        let resp = make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let rows = query_env(
            &state,
            "prod",
            "SELECT timestamp FROM events WHERE event = 'IsoEvent'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        let ts_str = rows[0]["timestamp"].as_str().unwrap();
        assert!(
            ts_str.contains("2025"),
            "timestamp should reflect 2025, got {ts_str}"
        );
    }

    // ── POST /identify ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn identify_single_user_returns_ok() {
        let (key, val) = api_key_header();
        let app = make_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_1","environment":"production","traits":{"email":"alice@example.com","plan":"pro"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"], true);
        assert!(body["error_code"].is_null());
    }

    #[tokio::test]
    async fn identify_batch_returns_ok() {
        let (key, val) = api_key_header();
        let app = make_router(make_state());
        let batch = serde_json::json!([
            {"distinct_id": "user_a", "environment": "prod"},
            {"distinct_id": "user_b", "environment": "prod", "traits": {"plan": "free"}}
        ]);
        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(batch.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"], true);
    }

    #[tokio::test]
    async fn identify_creates_identity_record() {
        let state = make_state();
        let (key, val) = api_key_header();
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"new_user","environment":"prod","traits":{"plan":"free"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(&state, "prod", "SELECT is_identified FROM identities").await;
        assert_eq!(rows.len(), 1);
        // is_identified should be TRUE after identify call
        assert_eq!(rows[0]["is_identified"], "true");
    }

    #[tokio::test]
    async fn identify_merges_traits_shallowly() {
        let state = make_state();
        let (key, val) = api_key_header();

        // First identify: set plan
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_1","environment":"prod","traits":{"plan":"free","name":"Alice"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Second identify: update plan, leave name intact
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_1","environment":"prod","traits":{"plan":"pro"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(&state, "prod", "SELECT traits FROM identities").await;
        assert_eq!(rows.len(), 1);
        let traits_str = rows[0]["traits"].as_str().unwrap();
        let traits: serde_json::Value = serde_json::from_str(traits_str).unwrap();
        assert_eq!(traits["plan"], "pro", "plan should be updated");
        assert_eq!(traits["name"], "Alice", "name should be preserved");
    }

    #[tokio::test]
    async fn identify_reuses_existing_identity_for_same_distinct_id() {
        let state = make_state();
        let (key, val) = api_key_header();

        for _ in 0..3 {
            make_router(state.clone())
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/identify")
                        .header(key, val)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            r#"{"distinct_id":"stable_user","environment":"prod"}"#,
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        // Should have exactly one identity row, not three
        let rows = query_env(&state, "prod", "SELECT uuid FROM identities").await;
        assert_eq!(
            rows.len(),
            1,
            "repeated identifies should not create duplicate identity rows"
        );
    }

    #[tokio::test]
    async fn identify_links_anonymous_id() {
        let state = make_state();
        let (key, val) = api_key_header();

        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_1","environment":"prod","anonymous_id":"anon_xyz"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(
            &state,
            "prod",
            "SELECT DISTINCT identity_uuid FROM identity_distinct_ids WHERE distinct_id IN ('user_1', 'anon_xyz')",
        )
        .await;
        assert_eq!(
            rows.len(),
            1,
            "user_1 and anon_xyz should share one identity"
        );
    }

    // ── identify + track interactions ─────────────────────────────────────────

    #[tokio::test]
    async fn identify_back_fills_traits_on_prior_events() {
        let state = make_state();
        let (key, val) = api_key_header();

        // Track an event first (no traits yet)
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"PageView","environment":"prod","distinct_id":"user_1","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Now identify with traits
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_1","environment":"prod","traits":{"email":"user@example.com"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(
            &state,
            "prod",
            "SELECT identity_traits FROM events WHERE event = 'PageView'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        let traits_json = rows[0]["identity_traits"].as_str().unwrap();
        let traits: serde_json::Value = serde_json::from_str(traits_json).unwrap();
        assert_eq!(
            traits["email"], "user@example.com",
            "identify should back-fill traits on prior events"
        );
    }

    #[tokio::test]
    async fn track_after_identify_carries_traits() {
        let state = make_state();
        let (key, val) = api_key_header();

        // Identify first
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_2","environment":"prod","traits":{"plan":"enterprise"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then track
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"Dashboard Loaded","environment":"prod","distinct_id":"user_2","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let rows = query_env(
            &state,
            "prod",
            "SELECT identity_traits FROM events WHERE event = 'Dashboard Loaded'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        let traits_json = rows[0]["identity_traits"].as_str().unwrap();
        let traits: serde_json::Value = serde_json::from_str(traits_json).unwrap();
        assert_eq!(traits["plan"], "enterprise");
    }

    #[tokio::test]
    async fn anonymous_events_back_filled_when_identified() {
        let state = make_state();
        let (key, val) = api_key_header();

        // Anonymous event tracked before identification
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"Browse","environment":"prod","anonymous_id":"anon_1","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Identify, linking the anonymous_id
        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/identify")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"distinct_id":"user_3","environment":"prod","anonymous_id":"anon_1","traits":{"plan":"starter"}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The anonymous event should now have an identity_uuid assigned
        let rows = query_env(
            &state,
            "prod",
            "SELECT identity_uuid FROM events WHERE anonymous_id = 'anon_1'",
        )
        .await;
        assert_eq!(rows.len(), 1);
        assert!(
            !rows[0]["identity_uuid"].is_null(),
            "anonymous event should be back-filled with identity_uuid after identify"
        );
    }

    #[tokio::test]
    async fn environments_are_isolated() {
        let state = make_state();
        let (key, val) = api_key_header();

        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"ProdEvent","environment":"production","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        make_router(state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/track")
                    .header(key, val)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"event":"StagingEvent","environment":"staging","properties":{}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let prod_rows = query_env(&state, "production", "SELECT event FROM events").await;
        let staging_rows = query_env(&state, "staging", "SELECT event FROM events").await;

        assert_eq!(prod_rows.len(), 1);
        assert_eq!(prod_rows[0]["event"], "ProdEvent");
        assert_eq!(staging_rows.len(), 1);
        assert_eq!(staging_rows[0]["event"], "StagingEvent");
    }
}
