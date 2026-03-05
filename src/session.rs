use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use arrow_schema::{DataType, Schema};
use duckdb::{
    Connection, Params,
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    core::{LogicalTypeHandle, LogicalTypeId},
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

pub fn arrow_type_to_duckdb_type(
    data_type: &DataType,
) -> anyhow::Result<std::borrow::Cow<'static, str>> {
    let logical_type = duckdb::vtab::to_duckdb_logical_type(data_type)
        .map_err(|e| anyhow::anyhow!("failed to convert arrow type to duckdb logical type: {e}"))?;
    duckdb_logical_type_to_string(&logical_type)
}

fn duckdb_logical_type_to_string(
    logical_type: &LogicalTypeHandle,
) -> anyhow::Result<std::borrow::Cow<'static, str>> {
    match logical_type.id() {
        LogicalTypeId::Boolean => Ok("BOOLEAN".into()),
        LogicalTypeId::Tinyint => Ok("TINYINT".into()),
        LogicalTypeId::Smallint => Ok("SMALLINT".into()),
        LogicalTypeId::Integer => Ok("INTEGER".into()),
        LogicalTypeId::Bigint => Ok("BIGINT".into()),
        LogicalTypeId::UTinyint => Ok("UTINYINT".into()),
        LogicalTypeId::USmallint => Ok("USMALLINT".into()),
        LogicalTypeId::UInteger => Ok("UINTEGER".into()),
        LogicalTypeId::UBigint => Ok("UBIGINT".into()),
        LogicalTypeId::Float => Ok("FLOAT".into()),
        LogicalTypeId::Double => Ok("DOUBLE".into()),
        LogicalTypeId::Timestamp => Ok("TIMESTAMP".into()),
        LogicalTypeId::Date => Ok("DATE".into()),
        LogicalTypeId::Time => Ok("TIME".into()),
        LogicalTypeId::Interval => Ok("INTERVAL".into()),
        LogicalTypeId::Hugeint => Ok("HUGEINT".into()),
        LogicalTypeId::Varchar => Ok("VARCHAR".into()),
        LogicalTypeId::Blob => Ok("BLOB".into()),
        LogicalTypeId::Decimal => {
            let precision = logical_type.decimal_width();
            let scale = logical_type.decimal_scale();
            Ok(format!("DECIMAL({precision}, {scale})").into())
        }
        LogicalTypeId::TimestampS => Ok("TIMESTAMP_S".into()),
        LogicalTypeId::TimestampMs => Ok("TIMESTAMP_MS".into()),
        LogicalTypeId::TimestampNs => Ok("TIMESTAMP_NS".into()),
        LogicalTypeId::Enum => Ok("ENUM".into()),
        LogicalTypeId::List => Ok("LIST".into()),
        LogicalTypeId::Struct => {
            let num_children = logical_type.num_children();
            let field_strs: anyhow::Result<Vec<_>> = (0..num_children)
                .map(|i| {
                    let field_name = logical_type.child_name(i);
                    let field_type = logical_type.child(i);
                    let type_str = duckdb_logical_type_to_string(&field_type)?;
                    Ok(format!("{field_name} {type_str}"))
                })
                .collect();
            Ok(format!("STRUCT({})", field_strs?.join(", ")).into())
        }
        LogicalTypeId::Map => Ok("MAP".into()),
        LogicalTypeId::Uuid => Ok("UUID".into()),
        LogicalTypeId::Union => Ok("UNION".into()),
        LogicalTypeId::TimestampTZ => Ok("TIMESTAMP WITH TIME ZONE".into()),
    }
}
