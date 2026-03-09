mod flight;
mod lakehouse;
mod product_analytics;
mod session;
mod utils;

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{Router, middleware, routing};
use clap::Parser;
use flight::server::MockServer;
use tonic::transport::Server;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{self, EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use lakehouse::{auth::auth_middleware, handlers as lh, state::LakehouseState};
use product_analytics::{
    auth::auth_middleware as pa_auth_middleware, handlers as pa, state::ProductAnalyticsState,
};

#[derive(Parser, Debug)]
#[command(name = "altertable-mock")]
#[command(about = "Mock Altertable server", long_about = None)]
struct Args {
    #[arg(
        short = 'f',
        long,
        default_value_t = 15002,
        env = "ALTERTABLE_MOCK_FLIGHT_PORT"
    )]
    flight_port: u16,

    #[arg(
        short = 'p',
        long,
        default_value_t = 15000,
        env = "ALTERTABLE_MOCK_LAKEHOUSE_PORT"
    )]
    lakehouse_port: u16,

    #[arg(
        short = 'a',
        long,
        default_value_t = 15001,
        env = "ALTERTABLE_MOCK_ANALYTICS_PORT"
    )]
    analytics_port: u16,

    #[arg(
        short = 'u',
        long,
        env = "ALTERTABLE_MOCK_USERS",
        value_delimiter = ','
    )]
    user: Vec<String>,

    #[arg(
        short = 'k',
        long,
        env = "ALTERTABLE_MOCK_API_KEYS",
        value_delimiter = ','
    )]
    api_key: Vec<String>,

    /// Pre-created environment names for the Product Analytics server.
    /// Defaults to a single "production" environment when omitted.
    #[arg(
        short = 'e',
        long,
        env = "ALTERTABLE_MOCK_ENVIRONMENTS",
        value_delimiter = ','
    )]
    environment: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    let tokens: Arc<std::collections::HashSet<flight::layers::auth::Identity>> = Arc::new(
        args.user
            .into_iter()
            .map(|token| {
                let parts = token.splitn(2, ':').collect::<Vec<_>>();
                if parts.len() != 2 {
                    panic!("Invalid user token: {token}");
                }

                flight::layers::auth::Identity {
                    username: parts[0].to_owned().into(),
                    password: parts[1].to_owned().into(),
                }
            })
            .collect(),
    );

    // ── Flight SQL server ────────────────────────────────────────────────────
    let flight_addr = format!("0.0.0.0:{}", args.flight_port).parse()?;
    let flight_service = MockServer::new();
    let flight_tokens = tokens.clone();

    let flight_handle = tokio::spawn(async move {
        info!("Starting Flight SQL server on {}", flight_addr);
        Server::builder()
            .layer(flight::layers::correlation::layer())
            .layer(
                TraceLayer::new_for_grpc().make_span_with(flight::layers::correlation::make_span()),
            )
            .layer(flight::layers::auth::layer(flight_tokens))
            .layer(flight::layers::session::layer())
            .add_service(FlightServiceServer::new(flight_service))
            .serve(flight_addr)
            .await
            .expect("Flight SQL server failed");
    });

    // ── Lakehouse HTTP server ─────────────────────────────────────────────────
    let lakehouse_state = LakehouseState::new(tokens.clone());
    let lakehouse_addr =
        format!("0.0.0.0:{}", args.lakehouse_port).parse::<std::net::SocketAddr>()?;

    let lakehouse_router = Router::new()
        .route("/query", routing::post(lh::post_query))
        .route("/query/{query_id}", routing::get(lh::get_query))
        .route("/query/{query_id}", routing::delete(lh::delete_query))
        .route("/validate", routing::post(lh::post_validate))
        .route("/upload", routing::post(lh::post_upload))
        .route("/append", routing::post(lh::post_append))
        .route_layer(middleware::from_fn_with_state(
            lakehouse_state.clone(),
            auth_middleware,
        ))
        .with_state(lakehouse_state);

    let lakehouse_handle = tokio::spawn(async move {
        info!("Starting Lakehouse HTTP server on {}", lakehouse_addr);
        let listener = tokio::net::TcpListener::bind(lakehouse_addr)
            .await
            .expect("Failed to bind lakehouse port");
        axum::serve(listener, lakehouse_router)
            .await
            .expect("Lakehouse HTTP server failed");
    });

    // ── Product Analytics HTTP server ─────────────────────────────────────────
    let api_keys: Arc<std::collections::HashSet<String>> =
        Arc::new(args.api_key.into_iter().collect());
    let analytics_state = ProductAnalyticsState::with_environments(api_keys, &args.environment);
    let analytics_addr =
        format!("0.0.0.0:{}", args.analytics_port).parse::<std::net::SocketAddr>()?;

    let analytics_router = Router::new()
        .route("/track", routing::post(pa::post_track))
        .route("/identify", routing::post(pa::post_identify))
        .route("/alias", routing::post(pa::post_alias))
        .route_layer(middleware::from_fn_with_state(
            analytics_state.clone(),
            pa_auth_middleware,
        ))
        .with_state(analytics_state);

    let analytics_handle = tokio::spawn(async move {
        info!(
            "Starting Product Analytics HTTP server on {}",
            analytics_addr
        );
        let listener = tokio::net::TcpListener::bind(analytics_addr)
            .await
            .expect("Failed to bind analytics port");
        axum::serve(listener, analytics_router)
            .await
            .expect("Product Analytics HTTP server failed");
    });

    tokio::try_join!(
        async {
            flight_handle
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        },
        async {
            lakehouse_handle
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        },
        async {
            analytics_handle
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        },
    )?;

    Ok(())
}
