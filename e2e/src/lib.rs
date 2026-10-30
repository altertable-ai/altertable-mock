//! Harness for black-box tests against the altertable-mock Docker image.
//!
//! The harness builds the repository's `Dockerfile` with `docker build`, exactly as users and
//! the publish workflow do, then runs it with testcontainers and talks to it only over the
//! published ports.

pub mod cases;

use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use base64::{Engine, prelude::BASE64_STANDARD};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

/// Set to an image reference (such as `ghcr.io/altertable-ai/altertable-mock:latest`) to test
/// that image instead of building the repository's `Dockerfile`.
pub const IMAGE_ENV: &str = "ALTERTABLE_MOCK_IMAGE";
const BUILT_IMAGE: &str = "altertable-mock:latest";

/// Every user the harness creates shares this password.
pub const PASSWORD: &str = "e2e-password";

const LAKEHOUSE_PORT: u16 = 15000;
const FLIGHT_PORT: u16 = 15002;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// A running mock container, removed when dropped.
pub struct MockContainer {
    container: Container<GenericImage>,
    pub lakehouse_url: String,
    pub flight_url: String,
}

impl MockContainer {
    /// Starts the image with one user per name and waits until the Lakehouse API answers.
    pub fn start(users: &[&str]) -> Self {
        assert!(!users.is_empty(), "the mock needs at least one user");
        let users_env = users
            .iter()
            .map(|user| format!("{user}:{PASSWORD}"))
            .collect::<Vec<_>>()
            .join(",");

        let (name, tag) = split_reference(image());
        let container = GenericImage::new(name, tag)
            .with_exposed_port(LAKEHOUSE_PORT.tcp())
            .with_exposed_port(FLIGHT_PORT.tcp())
            .with_env_var("ALTERTABLE_MOCK_USERS", users_env)
            .start()
            .expect("failed to start the mock container");
        let url = |port: u16| {
            let host_port = container
                .get_host_port_ipv4(port)
                .unwrap_or_else(|e| panic!("port {port} is not published: {e}"));
            format!("http://127.0.0.1:{host_port}")
        };
        let mock = Self {
            lakehouse_url: url(LAKEHOUSE_PORT),
            flight_url: url(FLIGHT_PORT),
            container,
        };
        mock.wait_until_ready(users[0]);
        mock
    }

    pub fn lakehouse(&self, user: &str) -> LakehouseClient {
        LakehouseClient::new(&self.lakehouse_url, user, PASSWORD)
    }

    fn wait_until_ready(&self, user: &str) {
        let client = self.lakehouse(user);
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Ok(response) = client.query(&serde_json::json!({ "statement": "SELECT 1" }))
                && response.status == 200
            {
                return;
            }
            let running = self.container.is_running().unwrap_or(false);
            if !running || Instant::now() > deadline {
                let logs = self.container.stdout_to_vec().unwrap_or_default();
                panic!(
                    "the mock {} within {STARTUP_TIMEOUT:?}; its logs:\n{}",
                    if running { "was not ready" } else { "exited" },
                    String::from_utf8_lossy(&logs)
                );
            }
            thread::sleep(Duration::from_millis(250));
        }
    }
}

/// The image under test, building the repository's `Dockerfile` once per test binary.
fn image() -> &'static str {
    static IMAGE: OnceLock<String> = OnceLock::new();
    IMAGE.get_or_init(|| {
        if let Ok(image) = std::env::var(IMAGE_ENV) {
            return image;
        }
        let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the e2e crate lives inside the repository");
        let output = Command::new("docker")
            .args(["build", "--tag", BUILT_IMAGE])
            .arg(repository)
            .output()
            .expect("failed to run docker; is it installed and on the PATH?");
        assert!(
            output.status.success(),
            "docker build failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        BUILT_IMAGE.to_owned()
    })
}

/// Splits `registry/name:tag` into name and tag, defaulting the tag to `latest`.
fn split_reference(reference: &str) -> (&str, &str) {
    let name_start = reference.rfind('/').map_or(0, |slash| slash + 1);
    match reference[name_start..].rfind(':') {
        Some(colon) => (
            &reference[..name_start + colon],
            &reference[name_start + colon + 1..],
        ),
        None => (reference, "latest"),
    }
}

/// Minimal blocking client for the Lakehouse REST API.
pub struct LakehouseClient {
    agent: ureq::Agent,
    base_url: String,
    authorization: String,
}

pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

impl LakehouseClient {
    pub fn new(base_url: &str, user: &str, password: &str) -> Self {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        Self {
            agent,
            base_url: base_url.to_owned(),
            authorization: format!(
                "Basic {}",
                BASE64_STANDARD.encode(format!("{user}:{password}"))
            ),
        }
    }

    /// Posts a `/query` request body as-is.
    pub fn query(&self, request: &serde_json::Value) -> Result<HttpResponse, ureq::Error> {
        let mut response = self
            .agent
            .post(format!("{}/query", self.base_url))
            .header("Authorization", &self.authorization)
            .header("Content-Type", "application/json")
            .send(request.to_string())?;
        Ok(HttpResponse {
            status: response.status().as_u16(),
            body: response.body_mut().read_to_string()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::split_reference;

    #[test]
    fn splits_image_references() {
        assert_eq!(
            split_reference("altertable-mock"),
            ("altertable-mock", "latest")
        );
        assert_eq!(
            split_reference("ghcr.io/altertable-ai/altertable-mock:1.2"),
            ("ghcr.io/altertable-ai/altertable-mock", "1.2")
        );
        assert_eq!(
            split_reference("localhost:5000/altertable-mock"),
            ("localhost:5000/altertable-mock", "latest")
        );
    }
}
