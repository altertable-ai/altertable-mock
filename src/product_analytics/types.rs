use serde::{Deserialize, Serialize};

// ── Shared timestamp ─────────────────────────────────────────────────────────

/// Accepts either a Unix timestamp in milliseconds or an ISO 8601 string.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FlexibleTimestamp {
    Millis(i64),
    Iso(String),
}

// ── Track ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct TrackPayload {
    pub event: String,
    pub environment: String,
    pub properties: serde_json::Map<String, serde_json::Value>,
    pub distinct_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub device_id: Option<String>,
    pub session_id: Option<String>,
    pub timestamp: Option<FlexibleTimestamp>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TrackRequest {
    Single(TrackPayload),
    Batch(Vec<TrackPayload>),
}

impl TrackRequest {
    pub fn into_vec(self) -> Vec<TrackPayload> {
        match self {
            Self::Single(p) => vec![p],
            Self::Batch(v) => v,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TrackResponse {
    pub ok: bool,
    pub error_code: Option<String>,
}

// ── Identify ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct IdentifyPayload {
    pub distinct_id: String,
    pub environment: String,
    pub anonymous_id: Option<String>,
    pub traits: Option<serde_json::Map<String, serde_json::Value>>,
    #[allow(dead_code)]
    pub timestamp: Option<FlexibleTimestamp>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum IdentifyRequest {
    Single(IdentifyPayload),
    Batch(Vec<IdentifyPayload>),
}

impl IdentifyRequest {
    pub fn into_vec(self) -> Vec<IdentifyPayload> {
        match self {
            Self::Single(p) => vec![p],
            Self::Batch(v) => v,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct IdentifyResponse {
    pub ok: bool,
    pub error_code: Option<String>,
}

// ── Alias ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct AliasPayload {
    pub environment: String,
    /// The source distinct_id to be merged away.
    pub distinct_id: String,
    /// The target user id that survives the merge.
    pub new_user_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum AliasRequest {
    Single(AliasPayload),
    Batch(Vec<AliasPayload>),
}

impl AliasRequest {
    pub fn into_vec(self) -> Vec<AliasPayload> {
        match self {
            Self::Single(p) => vec![p],
            Self::Batch(v) => v,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct AliasResponse {
    pub ok: bool,
    pub error_code: Option<String>,
}
