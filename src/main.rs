mod handlers;
mod layers;
mod schema_extractor;
mod server;
mod session;
mod utils;

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use server::MockServer;
use tonic::transport::Server;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{self, EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "altertable-mock")]
#[command(about = "Mock Altertable server", long_about = None)]
struct Args {
    #[arg(
        short,
        long,
        default_value_t = 15002,
        env = "ALTERTABLE_MOCK_FLIGHT_PORT"
    )]
    flight_port: u16,

    #[arg(short, long, env = "ALTERTABLE_MOCK_USERS", value_delimiter = ',')]
    user: Vec<String>,
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

    let addr = format!("0.0.0.0:{}", args.flight_port).parse()?;
    let service = MockServer::new();

    let tokens = Arc::new(
        args.user
            .into_iter()
            .map(|token| {
                let parts = token.splitn(2, ':').collect::<Vec<_>>();
                if parts.len() != 2 {
                    panic!("Invalid user token: {token}");
                }

                layers::auth::Identity {
                    username: parts[0].to_owned().into(),
                    password: parts[1].to_owned().into(),
                }
            })
            .collect(),
    );

    info!("Starting Flight SQL server on {}", addr);
    Server::builder()
        .layer(layers::correlation::layer())
        .layer(TraceLayer::new_for_grpc().make_span_with(layers::correlation::make_span()))
        .layer(layers::auth::layer(tokens))
        .layer(layers::session::layer())
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
