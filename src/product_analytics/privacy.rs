use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

use axum::http::{HeaderMap, header};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

const X_FORWARDED_FOR_HEADER: &str = "x-forwarded-for";

/// Derive an audience visitor id from a daily-rotating salt, so the same visitor
/// is only recognizable within one environment, hostname and day.
pub fn visitor_id(
    salts: &Mutex<HashMap<i64, [u8; 32]>>,
    environment: &str,
    properties: &serde_json::Map<String, serde_json::Value>,
    headers: &HeaderMap,
    ip: Option<IpAddr>,
    now: DateTime<Utc>,
) -> String {
    let day = now.timestamp().div_euclid(86_400);
    let salt = {
        let mut salts = salts.lock().unwrap();
        // Keep yesterday's salt around for events straddling midnight.
        salts.retain(|d, _| *d >= day - 1);
        *salts.entry(day).or_insert_with(|| {
            let mut salt = [0; 32];
            salt[..16].copy_from_slice(Uuid::new_v4().as_bytes());
            salt[16..].copy_from_slice(Uuid::new_v4().as_bytes());
            salt
        })
    };
    let hostname = properties
        .get("$url")
        .and_then(|v| v.as_str())
        .and_then(hostname)
        .or_else(|| {
            headers
                .get(header::ORIGIN)
                .and_then(|v| v.to_str().ok())
                .and_then(hostname)
        })
        .unwrap_or_default();
    let ip = ip
        .map(|ip| ip.to_canonical().to_string())
        .unwrap_or_default();
    let ua = headers
        .get(header::USER_AGENT)
        .map(|v| v.as_bytes())
        .unwrap_or_default();
    hash_visitor(&salt, environment, day, &hostname, &ip, ua)
}

pub fn strip_sensitive_properties(properties: &mut serde_json::Map<String, serde_json::Value>) {
    properties.retain(|key, _| {
        !matches!(
            key.strip_prefix('$').unwrap_or(key),
            "ip" | "country"
                | "region"
                | "city"
                | "user_agent"
                | "browser"
                | "browser_version"
                | "os"
                | "os_version"
                | "device"
                | "device_type"
                | "device_manufacturer"
                | "device_model"
                | "language"
                | "viewport"
        )
    });
}

/// Client IP from a single-valued `X-Forwarded-For` header, falling back to the socket address.
pub fn client_ip(headers: &HeaderMap, socket: Option<SocketAddr>) -> Option<IpAddr> {
    match headers
        .get(X_FORWARDED_FOR_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        Some(forwarded) if forwarded.contains(',') => None,
        Some(forwarded) => forwarded.trim().parse().ok(),
        None => socket.map(|addr| addr.ip()),
    }
}

fn hostname(value: &str) -> Option<String> {
    let url = url::Url::parse(value).ok()?;
    matches!(url.scheme(), "http" | "https")
        .then(|| url.host_str().map(str::to_owned))
        .flatten()
}

fn hash_visitor(
    salt: &[u8],
    environment: &str,
    day: i64,
    hostname: &str,
    ip: &str,
    ua: &[u8],
) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(salt).expect("HMAC accepts any key size");
    for part in [
        environment.as_bytes(),
        &day.to_be_bytes(),
        hostname.as_bytes(),
        ip.as_bytes(),
        ua,
    ] {
        mac.update(&(part.len() as u64).to_be_bytes());
        mac.update(part);
    }
    format!("audience_{}", hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_sensitive_properties_removes_explicit_client_metadata() {
        let keys = [
            "ip",
            "country",
            "region",
            "city",
            "user_agent",
            "browser",
            "browser_version",
            "os",
            "os_version",
            "device",
            "device_type",
            "device_manufacturer",
            "device_model",
            "language",
            "viewport",
        ];
        let mut properties = serde_json::Map::new();
        for key in keys {
            properties.insert(key.into(), "private".into());
            properties.insert(format!("${key}"), "private".into());
        }
        properties.insert("$url".into(), "https://example.com/docs".into());
        properties.insert("$referer".into(), "https://google.com/".into());
        properties.insert("category".into(), "documentation".into());
        strip_sensitive_properties(&mut properties);
        assert_eq!(
            serde_json::Value::Object(properties),
            serde_json::json!({
                "$url": "https://example.com/docs",
                "$referer": "https://google.com/",
                "category": "documentation"
            })
        );
    }

    #[test]
    fn visitor_ids_are_shared_but_rotate_and_isolate_request_context() {
        let salts = Mutex::default();
        let environment = "env-key";
        let now = Utc::now();
        let properties = serde_json::json!({"$url": "https://example.com/docs"})
            .as_object()
            .unwrap()
            .clone();
        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, "browser one".parse().unwrap());
        let ip = Some("1.2.3.4".parse().unwrap());
        let first = visitor_id(&salts, environment, &properties, &headers, ip, now);
        assert_eq!(
            first,
            visitor_id(&salts, environment, &properties, &headers, ip, now)
        );
        let other_host = serde_json::json!({"$url": "https://other.example/docs"})
            .as_object()
            .unwrap()
            .clone();
        for (env, props, hdrs, addr, date) in [
            ("other-key", &properties, &headers, ip, now),
            (environment, &other_host, &headers, ip, now),
            (
                environment,
                &properties,
                &headers,
                Some("1.2.3.5".parse().unwrap()),
                now,
            ),
            (
                environment,
                &properties,
                &headers,
                ip,
                now + chrono::Duration::days(1),
            ),
            (environment, &properties, &HeaderMap::new(), ip, now),
        ] {
            assert_ne!(first, visitor_id(&salts, env, props, hdrs, addr, date));
        }
        let same_host = serde_json::json!({"$url": "https://example.com/another?secret=foo"})
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(
            first,
            visitor_id(&salts, environment, &same_host, &headers, ip, now)
        );
    }

    #[test]
    fn client_ip_prefers_single_forwarded_address() {
        let socket = Some("10.0.0.1:1234".parse().unwrap());
        let mut headers = HeaderMap::new();
        assert_eq!(client_ip(&headers, socket), "10.0.0.1".parse().ok());
        headers.insert(X_FORWARDED_FOR_HEADER, "1.2.3.4".parse().unwrap());
        assert_eq!(client_ip(&headers, socket), "1.2.3.4".parse().ok());
        headers.insert(X_FORWARDED_FOR_HEADER, "1.2.3.4, 5.6.7.8".parse().unwrap());
        assert_eq!(client_ip(&headers, socket), None);
    }
}
