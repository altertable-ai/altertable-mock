use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

// --- Request types ---

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct QueryRequest {
    pub statement: String,
    pub session_id: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub sanitize: Option<bool>,
    pub visible: Option<bool>,
    pub ephemeral: Option<bool>,
    pub requested_by: Option<String>,
    pub query_id: Option<String>,
    pub timezone: Option<String>,
    pub compute_size: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ValidateRequest {
    pub statement: String,
    pub session_id: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum AppendRequest {
    Single(serde_json::Map<String, serde_json::Value>),
    Batch(Vec<serde_json::Map<String, serde_json::Value>>),
}

// --- Response types ---

#[derive(Debug, Serialize)]
pub struct ValidateResponse {
    pub valid: bool,
    pub statement: String,
    pub connections_errors: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AppendResponse {
    pub ok: bool,
    pub error_code: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CancelQueryResponse {
    pub cancelled: bool,
    pub message: String,
}

// --- Query log ---

#[derive(Debug, Clone, Serialize)]
pub struct QueryLog {
    pub uuid: Uuid,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub query: String,
    pub client_interface: String,
    pub visible: bool,
    pub session_id: String,
    pub error: Option<String>,
    pub requested_by: Option<String>,
    pub user_agent: Option<String>,
}

// --- Query streaming header (first NDJSON line) ---

#[derive(Debug, Serialize)]
pub struct QueryStreamHeader {
    pub statement: String,
    pub rows_limit: Option<u64>,
    pub connections_errors: HashMap<String, String>,
    pub session_id: String,
}
