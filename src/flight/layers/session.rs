use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use duckdb::Connection;
use futures::future::BoxFuture;
use tokio::sync::RwLock;
use tonic::{Request, Status};
use tonic_async_interceptor::{AsyncInterceptor, AsyncInterceptorLayer, async_interceptor};
use tracing::info;

use crate::session::Session;

use super::auth::{Identity, SessionID};

pub fn layer() -> AsyncInterceptorLayer<
    impl AsyncInterceptor<Future = BoxFuture<'static, Result<Request<()>, Status>>> + Clone,
> {
    async_interceptor(interceptor())
}

pub fn interceptor()
-> impl AsyncInterceptor<Future = BoxFuture<'static, Result<Request<()>, Status>>> + Clone {
    let lakehouse_store = Arc::new(RwLock::new(HashMap::new()));
    let session_store = Arc::new(RwLock::new(HashMap::new()));
    SessionMiddleware {
        lakehouse_store,
        session_store,
    }
}

type LakehouseKey = Identity;
type LakehouseStore = Arc<RwLock<HashMap<LakehouseKey, Arc<Mutex<Connection>>>>>;

type SessionKey = (Identity, SessionID);
type SessionStore = Arc<RwLock<HashMap<SessionKey, Session>>>;

#[derive(Clone)]
struct SessionMiddleware {
    lakehouse_store: LakehouseStore,
    session_store: SessionStore,
}

impl AsyncInterceptor for SessionMiddleware {
    type Future = BoxFuture<'static, Result<Request<()>, Status>>;

    fn call(&mut self, mut req: Request<()>) -> Self::Future {
        let lakehouse_store = self.lakehouse_store.clone();
        let session_store = self.session_store.clone();

        Box::pin(async move {
            let identity = req
                .extensions()
                .get::<Identity>()
                .ok_or_else(|| Status::internal("Missing identity"))?
                .clone();

            let session_id = req
                .extensions()
                .get::<SessionID>()
                .ok_or_else(|| Status::unauthenticated("Missing session"))?
                .clone();

            let key = (identity, session_id);

            let session = if let Some(session) = session_store.read().await.get(&key) {
                info!(
                    "Reusing connection for user: {} with session: {}",
                    key.0.username, key.1.0
                );

                session.clone()
            } else {
                info!(
                    "Creating new connection for user: {} with session: {}",
                    key.0.username, key.1.0
                );

                let lakehouse =
                    get_or_create_lakehouse(key.0.clone(), lakehouse_store.clone()).await;
                #[allow(clippy::result_large_err)]
                let new_conn = tokio::task::spawn_blocking(move || {
                    lakehouse
                        .lock()
                        .map_err(|e| Status::internal(format!("Failed to lock connection: {e}")))?
                        .try_clone()
                        .map_err(|e| Status::internal(format!("Failed to clone connection: {e}")))
                })
                .await
                .map_err(|e| Status::internal(format!("Failed to clone connection: {e}")))??;

                let conn = Arc::new(Mutex::new(new_conn));
                let session = Session {
                    connection: conn,
                    statements: Arc::new(RwLock::new(HashMap::new())),
                    catalog: Arc::new(RwLock::new(None)),
                    schema: Arc::new(RwLock::new(None)),
                };

                session_store.write().await.insert(key, session.clone());
                session
            };

            req.extensions_mut().insert(session);

            Ok(req)
        })
    }
}

async fn get_or_create_lakehouse(
    key: LakehouseKey,
    store: LakehouseStore,
) -> Arc<Mutex<Connection>> {
    if let Some(connection) = store.read().await.get(&key).cloned() {
        connection
    } else {
        info!("Creating new lakehouse for user: {}", key.username);
        let connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        store.write().await.insert(key, connection.clone());
        connection
    }
}
