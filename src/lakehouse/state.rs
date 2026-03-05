use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::flight::layers::auth::Identity;

use super::types::QueryLog;

pub type LakehouseStore = Arc<RwLock<HashMap<Identity, Arc<Mutex<Connection>>>>>;
pub type QueryStore = Arc<RwLock<HashMap<Uuid, QueryLog>>>;

#[derive(Clone)]
pub struct LakehouseState {
    pub lakehouse_store: LakehouseStore,
    pub query_store: QueryStore,
    pub allowed_users: Arc<std::collections::HashSet<Identity>>,
}

impl LakehouseState {
    pub fn new(allowed_users: Arc<std::collections::HashSet<Identity>>) -> Self {
        Self {
            lakehouse_store: Arc::new(RwLock::new(HashMap::new())),
            query_store: Arc::new(RwLock::new(HashMap::new())),
            allowed_users,
        }
    }

    pub async fn get_or_create_connection(&self, identity: &Identity) -> Arc<Mutex<Connection>> {
        {
            let store = self.lakehouse_store.read().await;
            if let Some(conn) = store.get(identity) {
                return conn.clone();
            }
        }

        let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        self.lakehouse_store
            .write()
            .await
            .insert(identity.clone(), conn.clone());
        conn
    }
}
