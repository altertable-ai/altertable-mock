use arrow_flight::{
    FlightData, Ticket,
    encode::FlightDataEncoderBuilder,
    sql::{
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandPreparedStatementQuery, SqlInfo, TicketStatementQuery,
        metadata::SqlInfoDataBuilder,
    },
};
use arrow_schema::Schema;
use duckdb::params_from_iter;
use futures::{
    TryStreamExt,
    stream::{self, BoxStream},
};
use tonic::{Request, Response, Status};

use crate::{
    session::Session,
    utils::{empty_params, escape_identifier, escape_literal},
};

type Result<T> = std::result::Result<T, Status>;
type FlightDataStream = BoxStream<'static, Result<FlightData>>;

pub async fn statement(
    query: TicketStatementQuery,
    req: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let session = req
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let handle = &query.statement_handle;
    let query = String::from_utf8_lossy(handle);

    tracing::debug!("Executing query: {}", query);
    let (schema, batches) = session
        .query_arrow(query.into_owned(), empty_params())
        .await
        .map_err(|_| Status::invalid_argument("Failed to query arrow"))?;

    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(Status::from);

    Ok(Response::new(Box::pin(flight_data_stream)))
}

pub async fn catalogs(
    _query: CommandGetCatalogs,
    request: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let sql = "SELECT DISTINCT catalog_name FROM information_schema.schemata";
    tracing::debug!("Executing query: {}", sql);
    let (schema, batches) = session
        .query_arrow(sql.to_owned(), empty_params())
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(Status::from);

    Ok(Response::new(Box::pin(flight_data_stream)))
}

pub async fn schemas(
    query: CommandGetDbSchemas,
    request: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let mut sql = "SELECT catalog_name, schema_name AS db_schema_name FROM information_schema.schemata WHERE 1=1"
        .to_owned();

    if let Some(ref catalog) = query.catalog {
        sql.push_str(&format!(
            " AND catalog_name = '{}'",
            escape_literal(catalog)
        ));
    }

    if let Some(ref schema_pattern) = query.db_schema_filter_pattern {
        sql.push_str(&format!(
            " AND schema_name LIKE '{}'",
            escape_literal(schema_pattern)
        ));
    }

    tracing::debug!("Executing query: {sql}");
    let (schema, batches) = session
        .query_arrow(sql, empty_params())
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(Status::from);

    Ok(Response::new(Box::pin(flight_data_stream)))
}

pub async fn tables(
    query: CommandGetTables,
    request: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let mut sql = "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE 1=1".to_owned();

    if let Some(ref catalog) = query.catalog {
        sql.push_str(&format!(
            " AND table_catalog = '{}'",
            escape_literal(catalog)
        ));
    }

    if let Some(ref schema_pattern) = query.db_schema_filter_pattern {
        sql.push_str(&format!(
            " AND table_schema LIKE '{}'",
            escape_literal(schema_pattern)
        ));
    }

    if let Some(ref table_pattern) = query.table_name_filter_pattern {
        sql.push_str(&format!(
            " AND table_name LIKE '{}'",
            escape_literal(table_pattern)
        ));
    }

    sql.push_str(" ORDER BY table_catalog, table_schema, table_name");

    let include_schema = query.include_schema;

    let mut builder = query.into_builder();

    tracing::debug!("Executing query: {sql}");
    let (_, batches) = session
        .query_arrow(sql, empty_params())
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    for batch in batches {
        let catalog_col = batch
            .column_by_name("table_catalog")
            .ok_or_else(|| Status::internal("Missing table_catalog column"))?;
        let schema_col = batch
            .column_by_name("table_schema")
            .ok_or_else(|| Status::internal("Missing table_schema column"))?;
        let table_col = batch
            .column_by_name("table_name")
            .ok_or_else(|| Status::internal("Missing table_name column"))?;
        let type_col = batch
            .column_by_name("table_type")
            .ok_or_else(|| Status::internal("Missing table_type column"))?;

        let catalog_arr = catalog_col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| Status::internal("Invalid catalog column type"))?;
        let schema_arr = schema_col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| Status::internal("Invalid schema column type"))?;
        let table_arr = table_col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| Status::internal("Invalid table column type"))?;
        let type_arr = type_col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| Status::internal("Invalid type column type"))?;

        for i in 0..batch.num_rows() {
            let catalog = catalog_arr.value(i);
            let schema = schema_arr.value(i);
            let table = table_arr.value(i);
            let table_type = type_arr.value(i);

            let table_schema = if include_schema {
                let schema_query = format!(
                    r#"SELECT * FROM "{}"."{}"."{}""#,
                    escape_identifier(catalog),
                    escape_identifier(schema),
                    escape_identifier(table)
                );
                Some(
                    session
                        .extract_schema(schema_query)
                        .await
                        .map_err(|e| Status::internal(format!("Failed to extract schema: {e}")))?,
                )
            } else {
                None
            };

            if let Some(ref ts) = table_schema {
                builder
                    .append(catalog, schema, table, table_type, ts)
                    .map_err(Status::from)?;
            } else {
                builder
                    .append(catalog, schema, table, table_type, &Schema::empty())
                    .map_err(Status::from)?;
            }
        }
    }

    let schema = builder.schema();
    let batch = builder.build();

    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures::stream::once(async { batch }))
        .map_err(Status::from);

    Ok(Response::new(Box::pin(stream)))
}

pub async fn prepared_statement(
    query: CommandPreparedStatementQuery,
    request: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let handle = query.prepared_statement_handle.to_vec();

    let (sql, params) = {
        let prepared_statements = session.statements.read().await;
        let prepared_statement = prepared_statements
            .get(&handle)
            .ok_or_else(|| Status::not_found("Prepared statement not found"))?;
        (
            prepared_statement.query.clone(),
            prepared_statement.parameters.clone(),
        )
    };

    tracing::debug!("Executing query: {} with params: {:?}", sql, params);
    let (schema, batches) = session
        .query_arrow(sql, params_from_iter(params))
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(Status::from);

    Ok(Response::new(Box::pin(flight_data_stream)))
}

pub async fn table_types(
    _query: CommandGetTableTypes,
    request: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let sql = "SELECT DISTINCT table_type FROM information_schema.tables ORDER BY table_type";

    tracing::debug!("Executing query: {}", sql);
    let (schema, batches) = session
        .query_arrow(sql.to_owned(), empty_params())
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(Status::from);

    Ok(Response::new(Box::pin(flight_data_stream)))
}

pub async fn sql_info(
    query: CommandGetSqlInfo,
    _request: Request<Ticket>,
) -> Result<Response<FlightDataStream>> {
    let mut builder = SqlInfoDataBuilder::new();

    let requested_infos: Vec<u32> = if query.info.is_empty() {
        [
            SqlInfo::FlightSqlServerName,
            SqlInfo::FlightSqlServerVersion,
            SqlInfo::FlightSqlServerArrowVersion,
            SqlInfo::FlightSqlServerReadOnly,
            SqlInfo::FlightSqlServerSql,
            SqlInfo::FlightSqlServerSubstrait,
            SqlInfo::FlightSqlServerTransaction,
            SqlInfo::FlightSqlServerCancel,
            SqlInfo::FlightSqlServerStatementTimeout,
            SqlInfo::FlightSqlServerTransactionTimeout,
            SqlInfo::SqlDdlCatalog,
            SqlInfo::SqlDdlSchema,
            SqlInfo::SqlDdlTable,
            SqlInfo::SqlIdentifierCase,
            SqlInfo::SqlIdentifierQuoteChar,
            SqlInfo::SqlQuotedIdentifierCase,
            SqlInfo::SqlAllTablesAreSelectable,
            SqlInfo::SqlNullOrdering,
            SqlInfo::SqlKeywords,
            SqlInfo::SqlNumericFunctions,
            SqlInfo::SqlStringFunctions,
            SqlInfo::SqlSystemFunctions,
            SqlInfo::SqlDatetimeFunctions,
            SqlInfo::SqlSearchStringEscape,
            SqlInfo::SqlExtraNameCharacters,
            SqlInfo::SqlSupportsColumnAliasing,
            SqlInfo::SqlMaxColumnsInGroupBy,
            SqlInfo::SqlMaxColumnsInOrderBy,
            SqlInfo::SqlMaxColumnsInSelect,
            SqlInfo::SqlMaxStatementLength,
            SqlInfo::SqlMaxUsernameLength,
            SqlInfo::SqlMaxCatalogNameLength,
            SqlInfo::SqlMaxTableNameLength,
            SqlInfo::SqlMaxTablesInSelect,
            SqlInfo::SqlDefaultTransactionIsolation,
            SqlInfo::SqlTransactionsSupported,
            SqlInfo::SqlSupportedTransactionsIsolationLevels,
            SqlInfo::SqlDataDefinitionCausesTransactionCommit,
            SqlInfo::SqlDataDefinitionsInTransactionsIgnored,
        ]
        .iter()
        .map(|info| u32::try_from(i32::from(*info)).unwrap_or(0))
        .collect()
    } else {
        query.info
    };

    for info_id in &requested_infos {
        match SqlInfo::try_from(*info_id as i32) {
            Ok(SqlInfo::FlightSqlServerName) => {
                builder.append(SqlInfo::FlightSqlServerName, "Altertable Flight SQL Server");
            }
            Ok(SqlInfo::FlightSqlServerVersion) => {
                builder.append(SqlInfo::FlightSqlServerVersion, "1.0.0");
            }
            Ok(SqlInfo::FlightSqlServerArrowVersion) => {
                builder.append(SqlInfo::FlightSqlServerArrowVersion, "56.0.0");
            }
            Ok(SqlInfo::FlightSqlServerReadOnly) => {
                builder.append(SqlInfo::FlightSqlServerReadOnly, false);
            }
            Ok(SqlInfo::FlightSqlServerSql) => {
                builder.append(SqlInfo::FlightSqlServerSql, true);
            }
            Ok(SqlInfo::FlightSqlServerSubstrait) => {
                builder.append(SqlInfo::FlightSqlServerSubstrait, false);
            }
            Ok(SqlInfo::FlightSqlServerTransaction) => {
                // Bitmask for transaction support:
                // SQL_TRANSACTION_NONE = 0
                // SQL_TRANSACTION_TRANSACTION = 1
                // SQL_TRANSACTION_SAVEPOINT = 2
                builder.append(SqlInfo::FlightSqlServerTransaction, 1i32);
            }
            Ok(SqlInfo::FlightSqlServerCancel) => {
                builder.append(SqlInfo::FlightSqlServerCancel, false);
            }
            Ok(SqlInfo::FlightSqlServerBulkIngestion) => {
                builder.append(SqlInfo::FlightSqlServerBulkIngestion, false);
            }
            Ok(SqlInfo::FlightSqlServerIngestTransactionsSupported) => {
                builder.append(SqlInfo::FlightSqlServerIngestTransactionsSupported, false);
            }
            Ok(SqlInfo::FlightSqlServerStatementTimeout) => {
                // Default timeout in milliseconds
                builder.append(SqlInfo::FlightSqlServerStatementTimeout, 0i32);
            }
            Ok(SqlInfo::FlightSqlServerTransactionTimeout) => {
                // Default timeout in milliseconds
                builder.append(SqlInfo::FlightSqlServerTransactionTimeout, 0i32);
            }
            Ok(SqlInfo::SqlDdlCatalog) => {
                builder.append(SqlInfo::SqlDdlCatalog, true);
            }
            Ok(SqlInfo::SqlDdlSchema) => {
                builder.append(SqlInfo::SqlDdlSchema, true);
            }
            Ok(SqlInfo::SqlDdlTable) => {
                builder.append(SqlInfo::SqlDdlTable, true);
            }
            Ok(SqlInfo::SqlIdentifierCase) => {
                builder.append(SqlInfo::SqlIdentifierCase, 0i32); // Case insensitive
            }
            Ok(SqlInfo::SqlIdentifierQuoteChar) => {
                builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
            }
            Ok(SqlInfo::SqlQuotedIdentifierCase) => {
                builder.append(SqlInfo::SqlQuotedIdentifierCase, 0i32); // Case insensitive
            }
            Ok(SqlInfo::SqlAllTablesAreSelectable) => {
                builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
            }
            Ok(SqlInfo::SqlNullOrdering) => {
                builder.append(SqlInfo::SqlNullOrdering, 0i32); // NULL values sorted high
            }
            Ok(SqlInfo::SqlKeywords) => {
                builder.append(SqlInfo::SqlKeywords, &["SELECT", "FROM", "WHERE"][..]);
            }
            Ok(SqlInfo::SqlNumericFunctions) => {
                builder.append(
                    SqlInfo::SqlNumericFunctions,
                    &["ABS", "CEIL", "FLOOR", "ROUND"][..],
                );
            }
            Ok(SqlInfo::SqlStringFunctions) => {
                builder.append(
                    SqlInfo::SqlStringFunctions,
                    &["CONCAT", "SUBSTRING", "UPPER", "LOWER"][..],
                );
            }
            Ok(SqlInfo::SqlSystemFunctions) => {
                builder.append(SqlInfo::SqlSystemFunctions, &["DATABASE", "USER"][..]);
            }
            Ok(SqlInfo::SqlDatetimeFunctions) => {
                builder.append(
                    SqlInfo::SqlDatetimeFunctions,
                    &["CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP"][..],
                );
            }
            Ok(SqlInfo::SqlSearchStringEscape) => {
                builder.append(SqlInfo::SqlSearchStringEscape, "\\");
            }
            Ok(SqlInfo::SqlExtraNameCharacters) => {
                builder.append(SqlInfo::SqlExtraNameCharacters, "");
            }
            Ok(SqlInfo::SqlSupportsColumnAliasing) => {
                builder.append(SqlInfo::SqlSupportsColumnAliasing, true);
            }
            Ok(SqlInfo::SqlMaxColumnsInGroupBy) => {
                builder.append(SqlInfo::SqlMaxColumnsInGroupBy, 0i64); // no limit
            }
            Ok(SqlInfo::SqlMaxColumnsInOrderBy) => {
                builder.append(SqlInfo::SqlMaxColumnsInOrderBy, 0i64);
            }
            Ok(SqlInfo::SqlMaxColumnsInSelect) => {
                builder.append(SqlInfo::SqlMaxColumnsInSelect, 0i64);
            }
            Ok(SqlInfo::SqlMaxStatementLength) => {
                builder.append(SqlInfo::SqlMaxStatementLength, 0i64);
            }
            Ok(SqlInfo::SqlMaxUsernameLength) => {
                builder.append(SqlInfo::SqlMaxUsernameLength, 0i64);
            }
            Ok(SqlInfo::SqlMaxCatalogNameLength) => {
                builder.append(SqlInfo::SqlMaxCatalogNameLength, 0i64);
            }
            Ok(SqlInfo::SqlMaxTableNameLength) => {
                builder.append(SqlInfo::SqlMaxTableNameLength, 0i64);
            }
            Ok(SqlInfo::SqlMaxTablesInSelect) => {
                builder.append(SqlInfo::SqlMaxTablesInSelect, 0i64);
            }
            Ok(SqlInfo::SqlDefaultTransactionIsolation) => {
                // SQL_TRANSACTION_READ_UNCOMMITTED = 1
                // SQL_TRANSACTION_READ_COMMITTED = 2
                // SQL_TRANSACTION_REPEATABLE_READ = 4
                // SQL_TRANSACTION_SERIALIZABLE = 8
                builder.append(SqlInfo::SqlDefaultTransactionIsolation, 8i32);
            }
            Ok(SqlInfo::SqlTransactionsSupported) => {
                builder.append(SqlInfo::SqlTransactionsSupported, true);
            }
            Ok(SqlInfo::SqlSupportedTransactionsIsolationLevels) => {
                builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 8i32);
                // SERIALIZABLE
            }
            Ok(SqlInfo::SqlDataDefinitionCausesTransactionCommit) => {
                builder.append(SqlInfo::SqlDataDefinitionCausesTransactionCommit, false);
            }
            Ok(SqlInfo::SqlDataDefinitionsInTransactionsIgnored) => {
                builder.append(SqlInfo::SqlDataDefinitionsInTransactionsIgnored, false);
            }
            Ok(info) => {
                tracing::info!("Unhandled SqlInfo ID: {:?} ({}), skipping", info, info_id);
            }
            Err(_) => {
                tracing::info!("Unknown SqlInfo ID requested: {}, skipping", info_id);
            }
        }
    }

    let sql_info_data = builder
        .build()
        .map_err(|e| Status::internal(format!("Failed to build SqlInfo data: {e}")))?;

    let batch = sql_info_data
        .record_batch(requested_infos)
        .map_err(|e| Status::internal(format!("Failed to filter SqlInfo data: {e}")))?;

    let schema = batch.schema();
    let batch_stream = stream::iter(vec![Ok(batch)]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(batch_stream)
        .map_err(|e| Status::internal(format!("Failed to encode flight data: {e}")));

    Ok(Response::new(Box::pin(flight_data_stream)))
}
