use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo,
    sql::{
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandPreparedStatementQuery, CommandStatementQuery, ProstMessageExt,
        TicketStatementQuery, metadata::SqlInfoDataBuilder,
    },
};
use arrow_schema::Schema;
use bytes::Bytes;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::session::Session;

type Result<T> = std::result::Result<T, Status>;

#[allow(clippy::result_large_err)]
fn make_flight_info<T>(
    descriptor: FlightDescriptor,
    ticket: &T,
    schema: &Schema,
) -> Result<FlightInfo>
where
    T: ProstMessageExt,
{
    let endpoint = FlightEndpoint::new()
        .with_ticket(arrow_flight::Ticket::new(ticket.as_any().encode_to_vec()));

    let flight_info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(format!("Unable to encode schema: {e}")))?
        .with_endpoint(endpoint)
        .with_descriptor(descriptor);

    Ok(flight_info)
}

pub async fn statement(
    query: CommandStatementQuery,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let session = req
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let schema = session
        .extract_schema(query.query.clone())
        .await
        .map_err(|e| Status::internal(format!("Failed to extract schema: {e}")))?;

    let ticket = TicketStatementQuery {
        statement_handle: Bytes::from(query.query.clone()),
    };

    let flight_info = make_flight_info(req.into_inner(), &ticket, &schema)?;

    Ok(Response::new(flight_info))
}

pub async fn prepared_statement(
    cmd: CommandPreparedStatementQuery,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let session = req
        .extensions()
        .get::<Session>()
        .ok_or_else(|| Status::internal("Missing session"))?;

    let schema = session
        .extract_prepared_statement_schema(cmd.prepared_statement_handle.as_ref())
        .await
        .map_err(|e| {
            Status::invalid_argument(format!("Failed to extract prepared statement schema: {e}"))
        })?;

    let flight_info = make_flight_info(req.into_inner(), &cmd, &schema)?;

    Ok(Response::new(flight_info))
}

pub async fn catalogs(
    query: CommandGetCatalogs,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let schema = query.into_builder().schema();
    let flight_info = make_flight_info(req.into_inner(), &query, &schema)?;

    Ok(Response::new(flight_info))
}

pub async fn schemas(
    query: CommandGetDbSchemas,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let ticket = query.clone();
    let schema = query.into_builder().schema();
    let flight_info = make_flight_info(req.into_inner(), &ticket, &schema)?;

    Ok(Response::new(flight_info))
}

pub async fn tables(
    query: CommandGetTables,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let ticket = query.clone();
    let schema = query.into_builder().schema();
    let flight_info = make_flight_info(req.into_inner(), &ticket, &schema)?;

    Ok(Response::new(flight_info))
}

pub async fn table_types(
    query: CommandGetTableTypes,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let schema = query.into_builder().schema();
    let flight_info = make_flight_info(req.into_inner(), &query, &schema)?;

    Ok(Response::new(flight_info))
}

pub async fn sql_info(
    query: CommandGetSqlInfo,
    req: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>> {
    let schema = SqlInfoDataBuilder::schema();
    let flight_info = make_flight_info(req.into_inner(), &query, schema)?;

    Ok(Response::new(flight_info))
}
