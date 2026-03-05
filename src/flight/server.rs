use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
    sql::{
        ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest,
        ActionCancelQueryResult, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
        ActionEndTransactionRequest, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo,
        CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery,
        CommandStatementUpdate, DoPutPreparedStatementResult, SqlInfo, TicketStatementQuery,
        server::FlightSqlService,
    },
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

use crate::flight::handlers::{do_action, do_get, do_handshake::handshake, do_put, get_flight_info};

type Result<T> = std::result::Result<T, Status>;
type FlightDataStream = BoxStream<'static, Result<FlightData>>;

#[derive(Clone, Debug, Default)]
pub struct MockServer;

impl MockServer {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FlightSqlService for MockServer {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<BoxStream<'static, Result<HandshakeResponse>>>> {
        tracing::debug!("do_handshake");
        handshake(request).await
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_statement");
        get_flight_info::statement(query, request).await
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_prepared_statement");
        get_flight_info::prepared_statement(cmd, request).await
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_catalogs");
        get_flight_info::catalogs(query, request).await
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_schemas");
        get_flight_info::schemas(query, request).await
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_tables");
        get_flight_info::tables(query, request).await
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_table_types");
        get_flight_info::table_types(query, request).await
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>> {
        tracing::debug!("get_flight_info_sql_info");
        get_flight_info::sql_info(query, request).await
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_statement");
        do_get::statement(ticket, request).await
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_catalogs");
        do_get::catalogs(query, request).await
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_schemas");
        do_get::schemas(query, request).await
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_tables");
        do_get::tables(query, request).await
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_prepared_statement");
        do_get::prepared_statement(query, request).await
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_table_types");
        do_get::table_types(query, request).await
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<FlightDataStream>> {
        tracing::debug!("do_get_sql_info");
        do_get::sql_info(query, request).await
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64> {
        tracing::debug!("do_put_statement_update");
        do_put::statement_update(ticket, request).await
    }

    async fn do_put_statement_ingest(
        &self,
        command: CommandStatementIngest,
        request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64> {
        tracing::debug!("do_put_statement_ingest");
        do_put::statement_ingest(command, request).await
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult> {
        tracing::debug!("do_put_prepared_statement_query");
        do_put::prepared_statement_query(query, request).await
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<arrow_flight::sql::server::PeekableFlightDataStream>,
    ) -> Result<i64> {
        tracing::debug!("do_put_prepared_statement_update");
        do_put::prepared_statement_update(query, request).await
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult> {
        tracing::debug!("do_action_create_prepared_statement");
        do_action::create_prepared_statement(query, request).await
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<()> {
        tracing::debug!("do_action_close_prepared_statement");
        do_action::close_prepared_statement(query, request).await
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult> {
        tracing::debug!("do_action_cancel_query");
        do_action::cancel_query(query, request).await
    }

    async fn do_action_begin_transaction(
        &self,
        query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult> {
        tracing::debug!("do_action_begin_transaction");
        do_action::begin_transaction(query, request).await
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<()> {
        tracing::debug!("do_action_end_transaction (action={:?})", query.action);
        do_action::end_transaction(query, request).await
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<BoxStream<'static, Result<arrow_flight::Result>>>> {
        tracing::debug!("do_action_fallback");
        do_action::fallback(request).await
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}
