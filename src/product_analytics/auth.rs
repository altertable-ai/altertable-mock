use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};

use super::state::ProductAnalyticsState;

#[derive(Clone)]
pub struct ApiKey(pub String);

pub async fn auth_middleware(
    State(state): State<ProductAnalyticsState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let api_key = extract_api_key(req.headers())?;

    if !state.allowed_api_keys.contains(api_key.as_str()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    req.extensions_mut().insert(ApiKey(api_key));
    Ok(next.run(req).await)
}

/// Accepts either `Authorization: Bearer <token>` or `X-API-Key: <token>`.
fn extract_api_key(headers: &axum::http::HeaderMap) -> Result<String, StatusCode> {
    // Prefer the dedicated header.
    if let Some(value) = headers.get("X-API-Key") {
        return value
            .to_str()
            .map(|s| s.to_owned())
            .map_err(|_| StatusCode::UNAUTHORIZED);
    }

    // Fall back to Bearer token.
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    auth_str
        .strip_prefix("Bearer ")
        .map(|token| token.trim().to_owned())
        .ok_or(StatusCode::UNAUTHORIZED)
}
