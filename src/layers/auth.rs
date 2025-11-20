use std::{borrow::Cow, collections::HashSet, sync::Arc};

use base64::{
    Engine,
    prelude::{BASE64_STANDARD, BASE64_STANDARD_NO_PAD},
};
use tonic::{
    Request, Status,
    service::{Interceptor, InterceptorLayer},
};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Identity {
    pub username: Cow<'static, str>,
    pub password: Cow<'static, str>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SessionID(pub Cow<'static, str>);

pub fn layer(tokens: Arc<HashSet<Identity>>) -> InterceptorLayer<impl Interceptor + Clone> {
    InterceptorLayer::new(interceptor(tokens))
}

pub fn interceptor(tokens: Arc<HashSet<Identity>>) -> impl Interceptor + Clone {
    AuthMiddleware { tokens }
}

#[derive(Clone)]
struct AuthMiddleware {
    tokens: Arc<HashSet<Identity>>,
}

impl AuthMiddleware {
    #[allow(clippy::result_large_err)]
    fn authenticate_with_scheme(
        &self,
        req: &Request<()>,
        scheme: &[u8],
    ) -> Result<(Identity, SessionID), Status> {
        let (identity, session) = extract_auth_token(req, scheme)?;
        if !self.tokens.contains(&identity) {
            return Err(Status::unauthenticated("Invalid token"));
        }

        let session = session.unwrap_or_else(|| {
            let new_session = Uuid::new_v4().to_string();
            info!(
                "Creating new session {new_session} for user: {}",
                identity.username
            );
            SessionID(new_session.into())
        });

        Ok((identity, session))
    }
}

impl Interceptor for AuthMiddleware {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let (identity, session) = self
            .authenticate_with_scheme(&req, b"Basic")
            .or_else(|_| self.authenticate_with_scheme(&req, b"Bearer"))?;

        info!(
            "Authenticated request for user: {} with session: {}",
            identity.username, session.0
        );

        req.extensions_mut().insert(identity);
        req.extensions_mut().insert(session);

        Ok(req)
    }
}

#[allow(clippy::result_large_err)]
fn extract_auth_token<T>(
    req: &Request<T>,
    scheme: &[u8],
) -> Result<(Identity, Option<SessionID>), Status> {
    let metadata = req.metadata();

    let auth_header = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("Missing authorization header"))?;

    let Some(encoded) = auth_header
        .as_bytes()
        .strip_prefix(scheme)
        .map(|s| s.trim_ascii())
    else {
        return Err(Status::unauthenticated(format!(
            "Authorization header must use {} scheme",
            String::from_utf8_lossy(scheme),
        )));
    };

    let decoded = BASE64_STANDARD
        .decode(encoded)
        .or_else(|_| BASE64_STANDARD_NO_PAD.decode(encoded))
        .map_err(|_| Status::unauthenticated("Invalid authorization header encoding"))?;

    let decoded = String::from_utf8(decoded)
        .map_err(|_| Status::unauthenticated("Invalid authorization header encoding"))?;

    let parts = decoded.splitn(3, ':').collect::<Vec<_>>();

    if parts.len() < 2 {
        return Err(Status::unauthenticated(
            "Invalid authorization header format",
        ));
    }

    let identity = Identity {
        username: parts[0].to_owned().into(),
        password: parts[1].to_owned().into(),
    };

    let session = parts.get(2).map(|&s| SessionID(s.to_owned().into()));

    Ok((identity, session))
}
