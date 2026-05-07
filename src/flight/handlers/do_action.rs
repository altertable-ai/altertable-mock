use arrow_flight::{
    Action,
    sql::{
        ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest,
        ActionCancelQueryResult, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
        ActionEndTransactionRequest,
    },
};
use arrow_ipc::writer::IpcWriteOptions;
use futures::{self, stream::BoxStream};
use prost::Message;
use std::{collections::HashMap, sync::Arc};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{
    flight::handlers::messages::{
        CloseSessionResult, SetSessionOptionsRequest, SetSessionOptionsResult,
        SetSessionOptionsResultError, close_session_result, session_option_value,
        set_session_options_result_error,
    },
    session::{PreparedStatement, Session},
    utils::SendableString,
};

const SESSION_CATALOG_KEY: &str = "catalog";
const SESSION_SCHEMA_KEY: &str = "schema";

type Result<T> = std::result::Result<T, Status>;

async fn extract_parameter_schema(
    session: &Session,
    sql: impl SendableString,
) -> anyhow::Result<Option<Vec<u8>>> {
    let parameter_schema = session
        .extract_parameter_schema(sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to extract parameter schema: {e}")))?;

    match parameter_schema {
        Some(schema) => {
            let ipc_options = IpcWriteOptions::default();
            let schema_ipc = arrow_flight::SchemaAsIpc::new(&schema, &ipc_options);
            let ipc_message: arrow_flight::IpcMessage = schema_ipc.try_into().map_err(|e| {
                Status::internal(format!("Failed to convert parameter schema to IPC: {e}"))
            })?;
            Ok(Some(ipc_message.0.into()))
        }
        None => Ok(None),
    }
}

pub async fn create_prepared_statement(
    query: ActionCreatePreparedStatementRequest,
    request: Request<Action>,
) -> Result<ActionCreatePreparedStatementResult> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let handle = Uuid::new_v4().as_bytes().to_vec();

    let sql = Arc::new(query.query);
    let schema = session
        .extract_schema(sql.clone())
        .await
        .map_err(|e| Status::internal(format!("Failed to extract schema: {e}")))?;

    let ipc_options = IpcWriteOptions::default();
    let schema_ipc = arrow_flight::SchemaAsIpc::new(&schema, &ipc_options);
    let ipc_message: arrow_flight::IpcMessage = schema_ipc
        .try_into()
        .map_err(|e| Status::internal(format!("Failed to convert schema to IPC: {e}")))?;
    let schema_bytes = ipc_message.0;

    let parameter_schema_bytes = extract_parameter_schema(session, sql.clone())
        .await
        .map_err(|e| Status::internal(format!("Failed to extract parameter schema: {e}")))?;

    let prepared_statement = PreparedStatement {
        query: sql.as_str().to_owned(),
        parameters: Vec::new(),
    };

    session
        .statements
        .write()
        .await
        .insert(handle.clone(), prepared_statement);

    Ok(ActionCreatePreparedStatementResult {
        prepared_statement_handle: handle.into(),
        dataset_schema: schema_bytes,
        parameter_schema: parameter_schema_bytes.unwrap_or_default().into(),
    })
}

pub async fn close_prepared_statement(
    query: ActionClosePreparedStatementRequest,
    request: Request<Action>,
) -> Result<()> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let handle = query.prepared_statement_handle.to_vec();

    session.statements.write().await.remove(&handle);

    Ok(())
}

pub async fn cancel_query(
    _query: ActionCancelQueryRequest,
    _request: Request<Action>,
) -> Result<ActionCancelQueryResult> {
    // Not implemented yet.
    Ok(ActionCancelQueryResult {
        result: 3, // NOT_CANCELLABLE = 3
    })
}

pub async fn begin_transaction(
    _query: ActionBeginTransactionRequest,
    request: Request<Action>,
) -> Result<ActionBeginTransactionResult> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let transaction_id = Uuid::new_v4().to_string();

    tracing::debug!("Executing query: BEGIN TRANSACTION");
    session
        .execute("BEGIN TRANSACTION")
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    Ok(ActionBeginTransactionResult {
        transaction_id: transaction_id.as_bytes().to_vec().into(),
    })
}

pub async fn end_transaction(
    query: ActionEndTransactionRequest,
    request: Request<Action>,
) -> Result<()> {
    let session = request
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let sql = match query.action {
        1 => "COMMIT",
        2 => "ROLLBACK",
        _ => return Err(Status::invalid_argument("Invalid transaction action")),
    };

    tracing::debug!("Executing query: {}", sql);
    session
        .execute(sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to execute query: {e}")))?;

    Ok(())
}

pub async fn set_session_options(request: Request<Action>) -> Result<SetSessionOptionsResult> {
    let cmd = SetSessionOptionsRequest::decode(&*request.get_ref().body).map_err(|e| {
        Status::invalid_argument(format!("Failed to decode SetSessionOptionsRequest: {e}"))
    })?;

    let mut errors = HashMap::new();

    for (key, value) in cmd.session_options {
        match key.as_str() {
            SESSION_CATALOG_KEY => {
                if let Some(session_option_value::OptionValue::String(catalog_name)) =
                    value.option_value
                {
                    let session = request
                        .extensions()
                        .get::<Session>()
                        .ok_or_else(|| Status::internal("Missing session"))?;

                    session.catalog.write().await.replace(catalog_name);
                } else {
                    errors.insert(
                        key.clone(),
                        SetSessionOptionsResultError {
                            value: set_session_options_result_error::ErrorValue::InvalidValue
                                as i32,
                        },
                    );
                }
            }
            SESSION_SCHEMA_KEY => {
                if let Some(session_option_value::OptionValue::String(schema_name)) =
                    value.option_value
                {
                    let session = request
                        .extensions()
                        .get::<Session>()
                        .ok_or_else(|| Status::internal("Missing session"))?;

                    session.schema.write().await.replace(schema_name);
                } else {
                    errors.insert(
                        key.clone(),
                        SetSessionOptionsResultError {
                            value: set_session_options_result_error::ErrorValue::InvalidValue
                                as i32,
                        },
                    );
                }
            }
            _ => {
                errors.insert(
                    key.clone(),
                    SetSessionOptionsResultError {
                        value: set_session_options_result_error::ErrorValue::InvalidName as i32,
                    },
                );
            }
        }
    }

    Ok(SetSessionOptionsResult { errors })
}

pub async fn close_session(_request: Request<Action>) -> Result<CloseSessionResult> {
    // Not implemented yet.
    Ok(CloseSessionResult {
        status: close_session_result::Status::Closed as i32,
    })
}

pub async fn fallback(
    request: Request<Action>,
) -> Result<Response<BoxStream<'static, Result<arrow_flight::Result>>>> {
    let action = request.get_ref();

    // We implement these actions as fallbacks for now until
    // https://github.com/apache/arrow-rs/issues/6516 is addressed
    match action.r#type.as_str() {
        "SetSessionOptions" => {
            let result = set_session_options(request).await?;
            let encoded_result = result.encode_to_vec();

            let response = arrow_flight::Result {
                body: encoded_result.into(),
            };

            let stream = futures::stream::iter(vec![Ok(response)]);
            Ok(Response::new(Box::pin(stream)))
        }
        "CloseSession" => {
            let result = close_session(request).await?;
            let encoded_result = result.encode_to_vec();

            let response = arrow_flight::Result {
                body: encoded_result.into(),
            };

            let stream = futures::stream::iter(vec![Ok(response)]);
            Ok(Response::new(Box::pin(stream)))
        }
        _ => {
            tracing::warn!(
                "Received unsupported action: type='{}', body={:?}",
                action.r#type,
                action.body
            );

            Err(Status::unimplemented(format!(
                "Action '{}' is not supported",
                action.r#type
            )))
        }
    }
}
