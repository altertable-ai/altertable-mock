use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use arrow_schema::{DataType, Schema};
use duckdb::{
    Connection, Params,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
};
use tokio::sync::RwLock;

use crate::{
    flight::schema_extractor::{extract_parameter_schema, extract_schema},
    utils::{SendableString, TEMP_DB, empty_params, escape_identifier, escape_literal},
};

#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub query: String,
    pub parameters: Vec<String>,
}

#[derive(Clone)]
pub struct Session {
    pub catalog: Arc<RwLock<Option<String>>>,
    pub schema: Arc<RwLock<Option<String>>>,
    pub connection: Arc<Mutex<Connection>>,
    pub statements: Arc<RwLock<HashMap<Vec<u8>, PreparedStatement>>>,
}

impl Session {
    pub async fn extract_prepared_statement_schema(
        &self,
        statement_handle: &[u8],
    ) -> anyhow::Result<Schema> {
        let statement = self
            .statements
            .read()
            .await
            .get(statement_handle)
            .cloned()
            .ok_or_else(|| anyhow!("Statement not found"))?;

        let schema = self.extract_schema(statement.query).await?;
        Ok(schema)
    }

    pub async fn extract_schema(&self, query: impl SendableString) -> anyhow::Result<Schema> {
        let schema = self
            .spawn_blocking(move |connection| {
                extract_schema(connection, query.as_str())
                    .map_err(|e| anyhow!("Failed to extract schema: {e}"))
            })
            .await??;

        Ok(schema)
    }

    pub async fn extract_parameter_schema(
        &self,
        query: impl SendableString,
    ) -> anyhow::Result<Option<Schema>> {
        self.spawn_blocking(move |connection| extract_parameter_schema(connection, query.as_str()))
            .await?
    }

    pub async fn query_arrow<P>(
        &self,
        query: impl SendableString,
        params: P,
    ) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)>
    where
        P: Params + Send + 'static,
    {
        let data = self
            .spawn_blocking(move |connection| {
                tracing::debug!("Executing query: {}", query.as_str());
                let mut stmt = connection.prepare(query.as_str())?;
                let batches = stmt.query_arrow(params)?.collect();
                Ok::<(SchemaRef, Vec<RecordBatch>), anyhow::Error>((stmt.schema(), batches))
            })
            .await??;

        Ok(data)
    }

    pub async fn execute(&self, query: impl SendableString) -> anyhow::Result<i64> {
        self.spawn_blocking(move |connection| {
            tracing::debug!("Executing query: {}", query.as_str());
            let res = connection
                .execute(query.as_str(), empty_params())
                .map_err(|e| anyhow!("Failed to execute query: {e}"))?;

            Ok(res as i64)
        })
        .await
        .map_err(|_| anyhow!("Failed to spawn blocking task"))?
    }

    pub async fn create_table_from_schema(
        &self,
        catalog_name: impl SendableString,
        schema_name: impl SendableString,
        table_name: impl SendableString,
        schema: SchemaRef,
    ) -> anyhow::Result<()> {
        self.spawn_blocking(move |connection| {
            create_schema_if_not_exists(connection, catalog_name.as_str(), schema_name.as_str())
                .map_err(|e| anyhow!("Failed to create schema for table: {e}"))?;

            let full_table_name = format!(
                r#""{}"."{}"."{}""#,
                escape_identifier(catalog_name.as_str()),
                escape_identifier(schema_name.as_str()),
                escape_identifier(table_name.as_str())
            );

            let column_defs: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| {
                    let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                    let column_def = format!(
                        "\"{}\" {}{}",
                        escape_identifier(field.name()),
                        arrow_type_to_duckdb_type(field.data_type()).map_err(|e| anyhow!(
                            "Failed to convert arrow type to DuckDB type: {e}"
                        ))?,
                        nullable
                    );
                    Ok(column_def)
                })
                .collect::<Result<Vec<String>, anyhow::Error>>()?;

            let temporary = if catalog_name.as_str() == TEMP_DB {
                "TEMPORARY"
            } else {
                ""
            };

            let create_table_query = format!(
                "CREATE {} TABLE {} ({})",
                temporary,
                full_table_name,
                column_defs.join(", ")
            );

            tracing::debug!("Executing query: {}", create_table_query);
            connection
                .execute(&create_table_query, empty_params())
                .and(Ok(()))
                .map_err(|e| anyhow!("Failed to create table: {e}"))
        })
        .await
        .map_err(|_| anyhow!("Failed to spawn blocking task"))??;

        Ok(())
    }

    pub async fn table_exists(
        &self,
        catalog_name: impl SendableString,
        schema_name: impl SendableString,
        table_name: impl SendableString,
    ) -> anyhow::Result<bool> {
        self.spawn_blocking(move |connection| {
            let table_exists_statement = format!(
                "SELECT 1 FROM information_schema.tables WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}'",
                escape_literal(catalog_name.as_str()),
                escape_literal(schema_name.as_str()),
                escape_literal(table_name.as_str())
            );

            let mut stmt = connection.prepare(&table_exists_statement)?;
            let rows = stmt.query_map(empty_params(),  |row| row.get::<_, usize>(0))?
                .collect::<Result<Vec<_>, _>>()?;

            Ok(!rows.is_empty())
        })
        .await
        .map_err(|_| anyhow!("Failed to spawn blocking task"))?
    }

    pub async fn drop_table_if_exists(
        &self,
        catalog: impl SendableString,
        schema: impl SendableString,
        table: impl SendableString,
    ) -> anyhow::Result<()> {
        self.spawn_blocking(move |connection| {
            connection
                .execute_batch(&format!(
                    "DROP TABLE IF EXISTS {catalog}.{schema}.{table}",
                    catalog = catalog.as_str(),
                    schema = schema.as_str(),
                    table = table.as_str()
                ))
                .map_err(|e| anyhow!("Failed to drop table: {e}"))?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow!("Failed to spawn blocking task"))?
    }

    pub async fn spawn_blocking<F, R>(&self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&duckdb::Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        let connection = self.connection.clone();
        let catalog = self.catalog.read().await.clone();
        let schema = self.schema.read().await.clone();

        let result = tokio::task::spawn_blocking(move || {
            let connection = connection
                .lock()
                .map_err(|_| anyhow!("Failed to lock connection"))?;

            if let Some(catalog) = catalog {
                let query = format!("USE {}", escape_identifier(&catalog));
                tracing::debug!("Executing query: {}", query);
                connection.execute(&query, empty_params())?;
            }

            if let Some(schema) = schema {
                let query = format!("USE {}", escape_identifier(&schema));
                tracing::debug!("Executing query: {}", query);
                connection.execute(&query, empty_params())?;
            }

            Ok::<R, anyhow::Error>(f(&connection))
        })
        .await
        .map_err(|_| anyhow!("Failed to spawn blocking task"))??;

        Ok(result)
    }
}

pub fn create_schema_if_not_exists(
    connection: &Connection,
    catalog: &str,
    schema: &str,
) -> anyhow::Result<()> {
    connection.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS {}.{};",
        escape_identifier(catalog),
        escape_identifier(schema)
    ))?;

    Ok(())
}

#[allow(clippy::match_same_arms)]
pub fn arrow_type_to_duckdb_type(data_type: &DataType) -> anyhow::Result<&'static str> {
    let duckdb_type = match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 => "DATE",
        DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        _ => return Err(anyhow::anyhow!("unsupported arrow type: {data_type}")),
    };
    Ok(duckdb_type)
}
