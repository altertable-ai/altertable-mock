use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_flight::{
    decode::FlightRecordBatchStream,
    error::FlightError,
    sql::{
        CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest,
        CommandStatementUpdate, DoPutPreparedStatementResult, TableExistsOption,
        TableNotExistOption, server::PeekableFlightDataStream,
    },
};
use arrow_schema::{DataType, SchemaRef};
use chrono;
use futures::{Stream, StreamExt, TryStreamExt};
use serde_json;
use tonic::{Request, Status};

use crate::{
    session::Session,
    utils::{MAIN_SCHEMA, MEMORY_DB, SendableString, TEMP_DB, escape_identifier},
};

#[derive(Debug)]
struct OptionsParsingError {
    errors: HashMap<String, String>,
}

impl std::error::Error for OptionsParsingError {}

impl std::fmt::Display for OptionsParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error parsing command options: ")?;
        let errors = serde_json::to_string(&self.errors).or(Err(std::fmt::Error))?;
        f.write_str(&errors)
    }
}

const CURSOR_FIELD_OPTION: &str = "cursor_field";
const PRIMARY_KEY_OPTION: &str = "primary_key";
const INGEST_CHANNEL_BUFFER: usize = 5;

#[derive(Debug, Clone, Default)]
struct StatementIngestOptions {
    primary_key: Vec<String>,
    cursor_field: Vec<String>,
}

impl StatementIngestOptions {
    fn is_upsert(&self) -> bool {
        !self.primary_key.is_empty()
    }
}

impl TryFrom<HashMap<String, String>> for StatementIngestOptions {
    type Error = OptionsParsingError;

    fn try_from(value: HashMap<String, String>) -> Result<Self, Self::Error> {
        let primary_key = value
            .get(PRIMARY_KEY_OPTION)
            .map(|json| serde_json::from_str(json))
            .transpose()
            .map_err(|e| OptionsParsingError {
                errors: HashMap::from([(
                    PRIMARY_KEY_OPTION.to_owned(),
                    format!("parsing error: {e}"),
                )]),
            })?
            .unwrap_or_else(std::vec::Vec::new);

        let cursor_field = value
            .get(CURSOR_FIELD_OPTION)
            .map(|json| serde_json::from_str(json))
            .transpose()
            .map_err(|e| OptionsParsingError {
                errors: HashMap::from([(
                    CURSOR_FIELD_OPTION.to_owned(),
                    format!("parsing error: {e}"),
                )]),
            })?
            .unwrap_or_else(std::vec::Vec::new);

        Ok(Self {
            primary_key,
            cursor_field,
        })
    }
}

pub async fn statement_update(
    ticket: CommandStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    tracing::debug!("Executing query: {}", ticket.query);
    let res = session
        .execute(ticket.query)
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    Ok(res)
}

pub async fn prepared_statement_query(
    query: CommandPreparedStatementQuery,
    request: Request<PeekableFlightDataStream>,
) -> Result<DoPutPreparedStatementResult, Status> {
    let session = request
        .extensions()
        .get::<Session>()
        .cloned()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let handle = query.prepared_statement_handle.to_vec();
    let stream = request.into_inner();

    let parameters = extract_parameters_from_stream(stream).await?;

    let mut stmts = session.statements.write().await;
    let stmt = stmts
        .get_mut(&handle)
        .ok_or_else(|| Status::not_found("Prepared statement not found"))?;

    stmt.parameters = parameters;

    Ok(DoPutPreparedStatementResult {
        prepared_statement_handle: Some(query.prepared_statement_handle),
    })
}

pub async fn prepared_statement_update(
    query: CommandPreparedStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?
        .clone();

    let handle = query.prepared_statement_handle.to_vec();
    let stream = request.into_inner();

    let parameters = extract_parameters_from_stream(stream).await?;

    let sql = {
        let mut prepared_statements = session.statements.write().await;
        let prepared_statement = prepared_statements
            .get_mut(&handle)
            .ok_or_else(|| Status::not_found("Prepared statement not found"))?;
        prepared_statement.parameters = parameters;
        prepared_statement.query.clone()
    };

    tracing::debug!("Executing query: {}", sql);
    let res = session
        .execute(sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    Ok(res)
}

// NOTE: we should probably do the ingest in a transaction to avoid race conditions
pub async fn statement_ingest(
    command: CommandStatementIngest,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?
        .clone();

    let catalog_name = Arc::new(command.catalog.as_deref().unwrap_or(MEMORY_DB).to_owned());
    let schema_name = Arc::new(command.schema.as_deref().unwrap_or(MAIN_SCHEMA).to_owned());
    let table_name = Arc::new(command.table);

    let full_table_name = format!(
        "\"{}\".\"{}\".\"{}\"",
        escape_identifier(&catalog_name),
        escape_identifier(&schema_name),
        escape_identifier(&table_name)
    );

    let mut table_exists = session
        .table_exists(
            catalog_name.clone(),
            schema_name.clone(),
            table_name.clone(),
        )
        .await
        .map_err(|_| Status::internal("Failed to check if table exists"))?;

    if let Some(table_def_options) = &command.table_definition_options {
        let if_not_exist = TableNotExistOption::try_from(table_def_options.if_not_exist)
            .map_err(|_| Status::invalid_argument("Invalid if_not_exist option"))?;
        let if_exists = TableExistsOption::try_from(table_def_options.if_exists)
            .map_err(|_| Status::invalid_argument("Invalid if_exists option"))?;

        if matches!(if_not_exist, TableNotExistOption::Unspecified)
            || matches!(if_exists, TableExistsOption::Unspecified)
        {
            return Err(Status::invalid_argument(
                "Table definition options must be specified",
            ));
        }

        match (table_exists, if_not_exist, if_exists) {
            (_, TableNotExistOption::Unspecified, _) => {
                return Err(Status::invalid_argument(
                    "Table not exist option must be specified",
                ));
            }
            (_, _, TableExistsOption::Unspecified) => {
                return Err(Status::invalid_argument(
                    "Table exists option must be specified",
                ));
            }
            (false, TableNotExistOption::Fail, _) => {
                return Err(Status::failed_precondition(format!(
                    "Table {full_table_name} does not exist"
                )));
            }
            (true, _, TableExistsOption::Fail) => {
                return Err(Status::already_exists(format!(
                    "Table {full_table_name} already exists"
                )));
            }
            (true, _, TableExistsOption::Replace) => {
                session
                    .drop_table_if_exists(
                        catalog_name.clone(),
                        schema_name.clone(),
                        table_name.clone(),
                    )
                    .await
                    .map_err(|_| Status::internal("Failed to drop table"))?;
                table_exists = false;
            }
            _ => {}
        }
    }

    let options: StatementIngestOptions = command
        .options
        .try_into()
        .map_err(|e| Status::invalid_argument(format!("Malformed options: {e}")))?;

    let stream = request.into_inner();
    let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(arrow_flight::error::FlightError::from),
    )
    .peekable();

    let schema = {
        // Get the first batch to extract schema
        let Some(first_batch) = Pin::new(&mut record_batch_stream).peek().await else {
            return Ok(0); // Empty stream
        };

        let first_batch = first_batch
            .as_ref()
            .map_err(|e| Status::internal(format!("Failed to read batch from stream: {e}")))?;

        first_batch.schema()
    };

    if !table_exists {
        session
            .create_table_from_schema(
                catalog_name.clone(),
                schema_name.clone(),
                table_name.clone(),
                schema.clone(),
            )
            .await
            .map_err(|_| Status::internal("Failed to create table"))?;
    }

    if options.is_upsert() {
        upsert_record_batches(
            &session,
            catalog_name,
            schema_name,
            table_name,
            &options,
            schema,
            record_batch_stream,
        )
        .await
    } else {
        insert_record_batches(
            &session,
            catalog_name,
            schema_name,
            table_name,
            record_batch_stream,
        )
        .await
    }
}

async fn upsert_record_batches<S>(
    session: &Session,
    catalog_name: impl SendableString,
    schema_name: impl SendableString,
    table_name: impl SendableString,
    options: &StatementIngestOptions,
    schema: SchemaRef,
    record_batch_stream: S,
) -> Result<i64, Status>
where
    S: Stream<Item = Result<RecordBatch, FlightError>> + Unpin,
{
    let temp_table_name = Arc::new(format!(
        "temp_upsert_{}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Status::internal(format!("System time error: {e}")))?
            .as_nanos(),
        std::process::id()
    ));

    session
        .create_table_from_schema(
            TEMP_DB,
            MAIN_SCHEMA,
            temp_table_name.clone(),
            schema.clone(),
        )
        .await
        .map_err(|_| Status::internal("Failed to create temporary table"))?;

    let _rows_inserted = insert_record_batches(
        session,
        TEMP_DB,
        MAIN_SCHEMA,
        temp_table_name.clone(),
        record_batch_stream,
    )
    .await?;

    let full_temp_table = format!(
        "{}.{}.\"{}\"",
        TEMP_DB,
        MAIN_SCHEMA,
        escape_identifier(&temp_table_name)
    );
    let full_target_table = format!(
        "\"{}\".\"{}\".\"{}\"",
        escape_identifier(catalog_name.as_str()),
        escape_identifier(schema_name.as_str()),
        escape_identifier(table_name.as_str())
    );

    let on_clause = options
        .primary_key
        .iter()
        .map(|col| {
            format!(
                "target.\"{}\" = source.\"{}\"",
                escape_identifier(col),
                escape_identifier(col)
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    let distinct_on_clause = options
        .primary_key
        .iter()
        .map(|col| format!("source.\"{}\"", escape_identifier(col)))
        .collect::<Vec<_>>()
        .join(", ");

    let all_columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let distinct_select_columns = all_columns
        .iter()
        .map(|col| format!("\"{}\"", escape_identifier(col)))
        .collect::<Vec<_>>()
        .join(", ");

    // Build ORDER BY clause for DISTINCT ON to control which row is kept
    let distinct_order_by = if !options.cursor_field.is_empty() {
        // Order by primary key columns first, then by cursor_field(s) DESC to get the latest
        let pk_order = options
            .primary_key
            .iter()
            .map(|col| format!("\"{}\"", escape_identifier(col)))
            .collect::<Vec<_>>()
            .join(", ");
        let cursor_order = options
            .cursor_field
            .iter()
            .map(|col| format!("\"{}\" DESC", escape_identifier(col)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{pk_order}, {cursor_order}")
    } else {
        // Just order by primary key columns
        options
            .primary_key
            .iter()
            .map(|col| format!("\"{}\"", escape_identifier(col)))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Build the UPDATE SET clause (all columns except primary key)
    let update_set_clause = all_columns
        .iter()
        .filter(|&col| !options.primary_key.iter().any(|pk| pk == col))
        .map(|col| {
            format!(
                "\"{}\" = source.\"{}\"",
                escape_identifier(col),
                escape_identifier(col)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    // Build additional condition for WHEN MATCHED clause based on cursor_field
    let when_matched_condition = if !options.cursor_field.is_empty() {
        // Use tuple comparison for multiple cursor fields
        let source_fields = options
            .cursor_field
            .iter()
            .map(|col| format!("source.\"{}\"", escape_identifier(col)))
            .collect::<Vec<_>>()
            .join(", ");
        let target_fields = options
            .cursor_field
            .iter()
            .map(|col| format!("target.\"{}\"", escape_identifier(col)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("AND ({source_fields}) > ({target_fields})")
    } else {
        String::new()
    };

    // Build the INSERT columns and values
    let insert_columns = all_columns
        .iter()
        .map(|col| format!("\"{}\"", escape_identifier(col)))
        .collect::<Vec<_>>()
        .join(", ");

    let insert_values = all_columns
        .iter()
        .map(|col| format!("source.\"{}\"", escape_identifier(col)))
        .collect::<Vec<_>>()
        .join(", ");

    let merge_sql = format!(
        r#"
        MERGE INTO {full_target_table} AS target
             USING (
                SELECT DISTINCT ON ({distinct_on_clause}) {distinct_select_columns} 
                FROM {full_temp_table} AS source
                ORDER BY {distinct_order_by}
             ) AS source
             ON {on_clause}
             WHEN MATCHED {when_matched_condition} THEN UPDATE SET {update_set_clause}
             WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        "#
    );

    let affected_rows = session
        .execute(merge_sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to execute MERGE: {e}")))?;

    session
        .drop_table_if_exists(TEMP_DB, MAIN_SCHEMA, temp_table_name)
        .await
        .map_err(|_| Status::internal("Failed to drop temporary table"))?;

    Ok(affected_rows)
}

async fn insert_record_batches<S>(
    session: &Session,
    catalog_name: impl SendableString,
    schema_name: impl SendableString,
    table_name: impl SendableString,
    mut record_batch_stream: S,
) -> Result<i64, Status>
where
    S: Stream<Item = Result<RecordBatch, FlightError>> + Unpin,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<RecordBatch>(INGEST_CHANNEL_BUFFER);

    let connection = session.connection.clone();
    #[allow(clippy::result_large_err)]
    let writer_handle = tokio::task::spawn_blocking(move || {
        let conn = connection
            .lock()
            .map_err(|_| Status::internal("Failed to lock connection"))?;

        let mut appender = conn
            .appender_to_catalog_and_db(
                table_name.as_str(),
                catalog_name.as_str(),
                schema_name.as_str(),
            )
            .map_err(|e| Status::internal(format!("Failed to create appender: {e}")))?;

        let mut total_rows = 0usize;

        while let Some(batch) = rx.blocking_recv() {
            total_rows += batch.num_rows();
            appender
                .append_record_batch(batch)
                .map_err(|e| Status::internal(format!("Failed to append batch: {e}")))?;
        }

        appender
            .flush()
            .map_err(|e| Status::internal(format!("Failed to flush appender: {e}")))?;

        Ok::<_, Status>(total_rows)
    });

    while let Some(batch) = record_batch_stream
        .try_next()
        .await
        .map_err(|e| Status::internal(format!("Failed to read batch from stream: {e}")))?
    {
        tx.send(batch)
            .await
            .map_err(|_| Status::internal("Failed to send batch to writer"))?;
    }

    drop(tx);

    let total_rows = writer_handle
        .await
        .map_err(|_| Status::internal("Failed to join writer task"))?
        .map_err(|e| Status::internal(format!("Append failed: {e}")))?;

    Ok(total_rows as i64)
}

async fn extract_parameters_from_stream(
    stream: PeekableFlightDataStream,
) -> Result<Vec<String>, Status> {
    let mut parameters = Vec::new();

    let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(arrow_flight::error::FlightError::from),
    );

    while let Some(batch) = record_batch_stream
        .try_next()
        .await
        .map_err(|e| Status::internal(format!("Failed to read batch from stream: {e}")))?
    {
        let schema = batch.schema();

        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                let field = schema.field(column_index);
                let column = batch.column(column_index);
                let value_str =
                    array_value_to_string(column.as_ref(), field.data_type(), row_index);
                parameters.push(value_str);
            }
        }
    }

    Ok(parameters)
}

fn array_value_to_string(array: &dyn Array, data_type: &DataType, idx: usize) -> String {
    if array.is_null(idx) {
        return String::from("NULL");
    }

    match data_type {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            String::from(arr.value(idx))
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = arr.value(idx);
            format!("{bytes:?}")
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(idx);
            // Date32 is days since Unix epoch (1970-01-01)
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719163 + days)
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            date.format("%Y-%m-%d").to_string()
        }
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            let arr = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            let micros = arr.value(idx);
            let seconds = micros / 1_000_000;
            let remaining_micros = micros % 1_000_000;
            let hours = seconds / 3600;
            let minutes = (seconds % 3600) / 60;
            let secs = seconds % 60;
            format!("{hours:02}:{minutes:02}:{secs:02}.{remaining_micros:06}")
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let timestamp = arr.value(idx);
            if let Some(dt) = chrono::DateTime::from_timestamp(timestamp, 0) {
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                String::from("1970-01-01 00:00:00")
            }
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let timestamp_ms = arr.value(idx);
            let secs = timestamp_ms / 1000;
            let millis = (timestamp_ms % 1000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, millis * 1_000_000) {
                dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            } else {
                String::from("1970-01-01 00:00:00.000")
            }
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let timestamp_us = arr.value(idx);
            let secs = timestamp_us / 1_000_000;
            let micros = (timestamp_us % 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, micros * 1000) {
                dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            } else {
                String::from("1970-01-01 00:00:00.000000")
            }
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let timestamp_ns = arr.value(idx);
            let secs = timestamp_ns / 1_000_000_000;
            let nanos = (timestamp_ns % 1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string()
            } else {
                String::from("1970-01-01 00:00:00.000000000")
            }
        }
        _ => {
            // For unsupported types, return a placeholder
            format!("UNSUPPORTED_TYPE_{data_type:?}")
        }
    }
}
