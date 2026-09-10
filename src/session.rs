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
    compute_size::ComputeSize,
    flight::schema_extractor::{extract_parameter_schema, extract_schema},
    transaction::{ActiveTransaction, EndAction, TransactionId, to_status},
    utils::{SendableString, TEMP_DB, empty_params, escape_identifier, escape_literal},
};
use tonic::Status;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub query: String,
    pub parameters: Vec<String>,
    pub transaction_id: Option<TransactionId>,
}

#[derive(Clone)]
pub struct Session {
    pub catalog: Arc<RwLock<Option<String>>>,
    pub schema: Arc<RwLock<Option<String>>>,
    pub compute_size: Arc<RwLock<ComputeSize>>,
    pub connection: Arc<Mutex<Connection>>,
    pub statements: Arc<RwLock<HashMap<Vec<u8>, PreparedStatement>>>,
    pub active_transaction: ActiveTransaction,
}

impl Session {
    pub fn new(connection: Connection) -> Self {
        Self {
            catalog: Arc::new(RwLock::new(None)),
            schema: Arc::new(RwLock::new(None)),
            compute_size: Arc::new(RwLock::new(ComputeSize::default())),
            connection: Arc::new(Mutex::new(connection)),
            statements: Arc::new(RwLock::new(HashMap::new())),
            active_transaction: ActiveTransaction::default(),
        }
    }
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

        self.extract_schema_with_transaction(statement.query, statement.transaction_id)
            .await
    }

    pub async fn extract_schema(&self, query: impl SendableString) -> anyhow::Result<Schema> {
        self.extract_schema_with_transaction(query, None).await
    }

    pub async fn extract_schema_with_transaction(
        &self,
        query: impl SendableString,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<Schema> {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
            extract_schema(connection, query.as_str())
                .map_err(|e| anyhow!("Failed to extract schema: {e}"))
        })
        .await?
    }

    pub async fn extract_parameter_schema_with_transaction(
        &self,
        query: impl SendableString,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<Option<Schema>> {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
            extract_parameter_schema(connection, query.as_str())
        })
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
        self.query_arrow_with_transaction(query, params, None).await
    }

    pub async fn query_arrow_with_transaction<P>(
        &self,
        query: impl SendableString,
        params: P,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)>
    where
        P: Params + Send + 'static,
    {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
            tracing::debug!("Executing query: {}", query.as_str());
            let mut stmt = connection.prepare(query.as_str())?;
            let batches = stmt.query_arrow(params)?.collect();
            Ok::<(SchemaRef, Vec<RecordBatch>), anyhow::Error>((stmt.schema(), batches))
        })
        .await?
    }

    pub async fn execute(&self, query: impl SendableString) -> anyhow::Result<i64> {
        self.execute_with_transaction(query, None).await
    }

    pub async fn execute_with_transaction(
        &self,
        query: impl SendableString,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<i64> {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
            tracing::debug!("Executing query: {}", query.as_str());
            let res = connection
                .execute(query.as_str(), empty_params())
                .map_err(|e| anyhow!("Failed to execute query: {e}"))?;

            Ok(res as i64)
        })
        .await?
    }

    pub async fn begin_transaction(&self) -> Result<TransactionId, Status> {
        let transaction_id: TransactionId = Uuid::now_v7().as_bytes().to_vec().into();
        let connection = self.connection.clone();
        let active_transaction = self.active_transaction.clone();
        let id_for_task = transaction_id.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let conn = connection
                .lock()
                .map_err(|e| anyhow!("Failed to lock connection: {e}"))?;
            active_transaction.require_absent()?;
            conn.execute_batch("BEGIN TRANSACTION")?;
            active_transaction.insert(id_for_task)?;
            Ok(())
        })
        .await
        .map_err(|e| Status::internal(format!("Begin transaction task failed: {e}")))?
        .map_err(|e| to_status(&e, "Failed to begin transaction"))?;

        Ok(transaction_id)
    }

    pub async fn end_transaction(
        &self,
        id: &TransactionId,
        action: EndAction,
    ) -> Result<(), Status> {
        let connection = self.connection.clone();
        let active_transaction = self.active_transaction.clone();
        let id = id.clone();
        tokio::task::spawn_blocking(move || -> Result<(), Status> {
            let conn = connection.lock().map_err(|e| {
                Status::internal(format!("Failed to lock connection for transaction: {e}"))
            })?;
            if !active_transaction
                .contains(&id)
                .map_err(|e| to_status(&e, "Failed to check transaction"))?
            {
                return Err(Status::not_found("Transaction not found"));
            }

            let sql = match action {
                EndAction::Commit => "COMMIT",
                EndAction::Rollback => "ROLLBACK",
            };
            conn.execute_batch(sql).map_err(|e| {
                let verb = match action {
                    EndAction::Commit => "Commit",
                    EndAction::Rollback => "Rollback",
                };
                Status::internal(format!("{verb} failed: {e}"))
            })?;
            active_transaction
                .remove(&id)
                .map_err(|e| to_status(&e, "Failed to clear transaction"))?;
            Ok(())
        })
        .await
        .map_err(|e| Status::internal(format!("End transaction task failed: {e}")))??;

        Ok(())
    }

    pub async fn create_table_from_schema_with_transaction(
        &self,
        catalog_name: impl SendableString,
        schema_name: impl SendableString,
        table_name: impl SendableString,
        schema: SchemaRef,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<()> {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
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
        .await?
    }

    pub async fn table_exists_with_transaction(
        &self,
        catalog_name: impl SendableString,
        schema_name: impl SendableString,
        table_name: impl SendableString,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<bool> {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
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
        .await?
    }

    pub async fn drop_table_if_exists_with_transaction(
        &self,
        catalog: impl SendableString,
        schema: impl SendableString,
        table: impl SendableString,
        transaction_id: Option<TransactionId>,
    ) -> anyhow::Result<()> {
        self.spawn_blocking_in_transaction(transaction_id, move |connection| {
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
        .await?
    }

    pub async fn spawn_blocking_in_transaction<F, R>(
        &self,
        transaction_id: Option<TransactionId>,
        f: F,
    ) -> anyhow::Result<R>
    where
        F: FnOnce(&duckdb::Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        let connection = self.connection.clone();
        let active_transaction = self.active_transaction.clone();
        let catalog = self.catalog.read().await.clone();
        let schema = self.schema.read().await.clone();

        let result = tokio::task::spawn_blocking(move || {
            let connection = connection
                .lock()
                .map_err(|_| anyhow!("Failed to lock connection"))?;

            match &transaction_id {
                Some(id) => active_transaction.require(id)?,
                None => active_transaction.require_absent()?,
            }

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
        LogicalTypeId::Integer | LogicalTypeId::IntegerLiteral => Ok("INTEGER".into()),
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
        LogicalTypeId::Varchar | LogicalTypeId::StringLiteral => Ok("VARCHAR".into()),
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
        LogicalTypeId::Invalid => Err(anyhow::anyhow!("invalid DuckDB type")),
        LogicalTypeId::Bit => Ok("BIT".into()),
        LogicalTypeId::TimeTZ => Ok("TIMETZ".into()),
        LogicalTypeId::UHugeint => Ok("UHUGEINT".into()),
        LogicalTypeId::Array => {
            let element_type = logical_type.child(0);
            let element_str = duckdb_logical_type_to_string(&element_type)?;
            Ok(format!("{element_str}[]").into())
        }
        LogicalTypeId::Any => Ok("ANY".into()),
        LogicalTypeId::Bignum => Ok("DECIMAL".into()),
        LogicalTypeId::SqlNull => Ok("NULL".into()),
        LogicalTypeId::TimeNs => Ok("TIME_NS".into()),
        _ => Err(anyhow::anyhow!(
            "unsupported DuckDB type (id={})",
            logical_type.raw_id()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::TRANSACTION_ALREADY_ACTIVE_MESSAGE;

    fn test_session() -> Session {
        Session::new(Connection::open_in_memory().unwrap())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_sees_session_temp_table() {
        let session = test_session();
        session
            .execute("CREATE TEMP TABLE temp_source (id INTEGER, value VARCHAR)")
            .await
            .unwrap();
        session
            .execute("INSERT INTO temp_source VALUES (1, 'from_temp')")
            .await
            .unwrap();

        let tx_id = session.begin_transaction().await.unwrap();
        session
            .execute_with_transaction(
                "CREATE TABLE persisted_from_temp AS SELECT * FROM temp_source",
                Some(tx_id.clone()),
            )
            .await
            .unwrap();
        session
            .end_transaction(&tx_id, EndAction::Commit)
            .await
            .unwrap();

        let (_, batches) = session
            .query_arrow("SELECT * FROM persisted_from_temp", empty_params())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn transaction_rollback_discards_table_created_from_temp() {
        let session = test_session();
        session
            .execute("CREATE TEMP TABLE temp_source (id INTEGER, value VARCHAR)")
            .await
            .unwrap();
        session
            .execute("INSERT INTO temp_source VALUES (1, 'from_temp')")
            .await
            .unwrap();

        let tx_id = session.begin_transaction().await.unwrap();
        session
            .execute_with_transaction(
                "CREATE TABLE rolled_back_from_temp AS SELECT * FROM temp_source",
                Some(tx_id.clone()),
            )
            .await
            .unwrap();
        session
            .end_transaction(&tx_id, EndAction::Rollback)
            .await
            .unwrap();

        assert!(
            session
                .query_arrow("SELECT * FROM rolled_back_from_temp", empty_params())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn begin_transaction_rejects_second_active_transaction() {
        let session = test_session();
        let tx_id = session.begin_transaction().await.unwrap();

        let err = session.begin_transaction().await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), TRANSACTION_ALREADY_ACTIVE_MESSAGE);

        session
            .end_transaction(&tx_id, EndAction::Rollback)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_without_transaction_id_rejected_while_active() {
        let session = test_session();
        let tx_id = session.begin_transaction().await.unwrap();

        let err = session.execute("SELECT 1").await.unwrap_err();
        assert_eq!(err.to_string(), TRANSACTION_ALREADY_ACTIVE_MESSAGE);

        session
            .end_transaction(&tx_id, EndAction::Rollback)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_with_unknown_transaction_id_is_rejected() {
        let session = test_session();
        let tx_id = session.begin_transaction().await.unwrap();
        let unknown: TransactionId = uuid::Uuid::now_v7().as_bytes().to_vec().into();

        let err = session
            .execute_with_transaction("SELECT 1", Some(unknown))
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "transaction not found");

        session
            .end_transaction(&tx_id, EndAction::Rollback)
            .await
            .unwrap();
    }
}
