use std::time::{Duration, Instant};

use altertable_mock_e2e::{MockContainer, PASSWORD};
use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::{Channel, Endpoint};

const USER: &str = "flight";

/// The harness is synchronous, so each test drives the gRPC client on its own runtime.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("failed to start a tokio runtime")
}

/// Connects and authenticates, retrying while the gRPC server is still starting.
async fn connect(mock: &MockContainer) -> FlightSqlServiceClient<Channel> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let attempt = async {
            let channel = Endpoint::from_shared(mock.flight_url.clone())?
                .connect()
                .await?;
            let mut client = FlightSqlServiceClient::new(channel);
            client.handshake(USER, PASSWORD).await?;
            Ok::<_, Box<dyn std::error::Error>>(client)
        };
        match attempt.await {
            Ok(client) => return client,
            Err(e) if Instant::now() > deadline => panic!("Flight SQL never came up: {e}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
}

async fn query(client: &mut FlightSqlServiceClient<Channel>, sql: &str) -> String {
    let info = client.execute(sql.to_owned(), None).await.unwrap();
    let mut batches: Vec<RecordBatch> = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.expect("endpoint has a ticket");
        let stream = client.do_get(ticket).await.unwrap();
        batches.extend(stream.try_collect::<Vec<_>>().await.unwrap());
    }
    pretty_format_batches(&batches).unwrap().to_string()
}

#[test]
fn flight_sql_round_trip() {
    let mock = MockContainer::start(&[USER]);
    runtime().block_on(round_trip(&mock));
}

async fn round_trip(mock: &MockContainer) {
    let mut client = connect(mock).await;

    client
        .execute_update(
            "CREATE TABLE fruits (id INTEGER, name VARCHAR, price DECIMAL(6, 2))".to_owned(),
            None,
        )
        .await
        .unwrap();
    let inserted = client
        .execute_update(
            "INSERT INTO fruits VALUES (1, 'apple', 1.25), (2, 'banana', 0.5), (3, 'cherry', NULL)"
                .to_owned(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(inserted, 3);

    assert_eq!(
        query(&mut client, "SELECT * FROM fruits ORDER BY id").await,
        "\
+----+--------+-------+
| id | name   | price |
+----+--------+-------+
| 1  | apple  | 1.25  |
| 2  | banana | 0.50  |
| 3  | cherry |       |
+----+--------+-------+"
    );

    assert_eq!(
        query(
            &mut client,
            "SELECT count(*) AS n, sum(price) AS total FROM fruits"
        )
        .await,
        "\
+---+-------+
| n | total |
+---+-------+
| 3 | 1.75  |
+---+-------+"
    );
}

#[test]
fn flight_sql_rejects_bad_credentials() {
    let mock = MockContainer::start(&[USER]);
    runtime().block_on(rejects_bad_credentials(&mock));
}

async fn rejects_bad_credentials(mock: &MockContainer) {
    // Wait for the server with valid credentials so the failure below is about auth.
    connect(mock).await;

    let channel = Endpoint::from_shared(mock.flight_url.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel);
    let error = client.handshake(USER, "wrong").await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("valid authentication credentials"),
        "unexpected error: {error}"
    );
}
