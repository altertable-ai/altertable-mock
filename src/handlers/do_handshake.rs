use arrow_flight::{HandshakeRequest, HandshakeResponse};
use base64::{Engine, prelude::BASE64_STANDARD};
use futures::stream::{self, BoxStream};
use tonic::{Request, Response, Status, Streaming};

use crate::layers::auth::{Identity, SessionID};

type Result<T> = std::result::Result<T, Status>;

pub async fn handshake(
    req: Request<Streaming<HandshakeRequest>>,
) -> Result<Response<BoxStream<'static, Result<HandshakeResponse>>>> {
    let identity = req
        .extensions()
        .get::<Identity>()
        .ok_or_else(|| Status::internal("Missing identity"))?;

    let session = req
        .extensions()
        .get::<SessionID>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let payload = BASE64_STANDARD
        .encode(format!("{}:{}:{}", identity.username, identity.password, session.0).as_bytes());

    let token_header = format!("Bearer {payload}");
    let stream: BoxStream<'static, Result<HandshakeResponse>> =
        Box::pin(stream::once(async move {
            Ok(HandshakeResponse {
                protocol_version: 0,
                payload: payload.into(),
            })
        }));

    let mut response = Response::new(stream);
    response.metadata_mut().insert(
        "authorization",
        token_header.try_into().map_err(|e| {
            Status::internal(format!("Failed to convert token header to metadata: {e}"))
        })?,
    );

    Ok(response)
}
