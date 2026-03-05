use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug)]
pub struct EnvironmentState {
    /// DuckDB connection for this environment.
    pub conn: Arc<Mutex<Connection>>,
}

impl EnvironmentState {
    pub fn new() -> Self {
        let conn = Connection::open_in_memory().expect("Failed to open DuckDB in memory");
        let state = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        state.initialize_schema();
        state
    }

    fn initialize_schema(&self) {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS events (
                uuid VARCHAR NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                event VARCHAR NOT NULL,
                properties JSON NOT NULL,
                distinct_id VARCHAR,
                anonymous_id VARCHAR,
                device_id VARCHAR,
                session_id VARCHAR,
                identity_uuid VARCHAR,
                identity_traits JSON
            );

            CREATE TABLE IF NOT EXISTS identities (
                uuid VARCHAR NOT NULL,
                traits JSON NOT NULL,
                is_identified BOOLEAN NOT NULL,
                is_deleted BOOLEAN NOT NULL,
                version BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS identity_distinct_ids (
                distinct_id VARCHAR NOT NULL,
                identity_uuid VARCHAR NOT NULL,
                version BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );

            CREATE TABLE IF NOT EXISTS identity_distinct_id_overrides (
                distinct_id VARCHAR NOT NULL,
                identity_uuid VARCHAR NOT NULL,
                version BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            ",
        )
        .expect("Failed to initialize product analytics schema");
    }
}

/// Key: (api_key, environment_name)
pub type EnvironmentStore = Arc<RwLock<HashMap<(String, String), Arc<Mutex<EnvironmentState>>>>>;

#[derive(Clone)]
pub struct ProductAnalyticsState {
    pub environment_store: EnvironmentStore,
    pub allowed_api_keys: Arc<std::collections::HashSet<String>>,
}

impl ProductAnalyticsState {
    pub fn new(allowed_api_keys: Arc<std::collections::HashSet<String>>) -> Self {
        Self {
            environment_store: Arc::new(RwLock::new(HashMap::new())),
            allowed_api_keys,
        }
    }

    pub async fn get_or_create_environment(
        &self,
        username: &str,
        environment: &str,
    ) -> Arc<Mutex<EnvironmentState>> {
        let key = (username.to_owned(), environment.to_owned());

        {
            let store = self.environment_store.read().await;
            if let Some(env_state) = store.get(&key) {
                return env_state.clone();
            }
        }

        let env_state = Arc::new(Mutex::new(EnvironmentState::new()));
        self.environment_store
            .write()
            .await
            .insert(key, env_state.clone());
        env_state
    }
}

pub fn new_uuid() -> Uuid {
    Uuid::now_v7()
}
