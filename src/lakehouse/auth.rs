use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use base64::{Engine, prelude::BASE64_STANDARD};

use crate::flight::layers::auth::Identity;

use super::state::LakehouseState;

pub async fn auth_middleware(
    State(state): State<LakehouseState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let identity = extract_basic_auth(req.headers())?;

    if !state.allowed_users.contains(&identity) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    req.extensions_mut().insert(identity);
    Ok(next.run(req).await)
}

fn extract_basic_auth(headers: &axum::http::HeaderMap) -> Result<Identity, StatusCode> {
    let auth_header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let auth_str = auth_header.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    let encoded = auth_str
        .strip_prefix("Basic ")
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let decoded = BASE64_STANDARD
        .decode(encoded.trim())
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let decoded_str = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;

    let (username, password) = decoded_str
        .split_once(':')
        .ok_or(StatusCode::UNAUTHORIZED)?;

    Ok(Identity {
        username: username.to_owned().into(),
        password: password.to_owned().into(),
    })
}
