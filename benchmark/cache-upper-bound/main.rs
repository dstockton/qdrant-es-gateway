//! Diagnostic only: immutable preloaded responses, never a production cache.
use axum::{
    body::Bytes,
    extract::State,
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

const SCRIPT: &str = include_str!("hit.lua");
#[derive(Deserialize, Serialize)]
struct Fixtures {
    diagnostic: String,
    entries: Vec<Entry>,
}
#[derive(Deserialize, Serialize)]
struct Entry {
    method: String,
    path: String,
    request_body: String,
    response: Value,
}
#[derive(Clone)]
struct Probe {
    connection: MultiplexedConnection,
    prefix: Arc<str>,
    script: Arc<redis::Script>,
}

fn digest(method: &str, path: &str, body: &[u8]) -> String {
    let mut hash = Sha256::new();
    for part in [method.as_bytes(), path.as_bytes(), body] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        encoded.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        encoded.push(char::from(b"0123456789abcdef"[(byte & 15) as usize]));
    }
    encoded
}
fn permitted(method: &Method, path: &str) -> bool {
    (method == Method::GET && path.starts_with("/balanced_benchmark/_doc/"))
        || (method == Method::POST && path == "/balanced_benchmark/_search")
}
impl Probe {
    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }
    async fn lookup(&self, key: &str) -> redis::RedisResult<Option<Vec<u8>>> {
        self.script
            .key(self.key("control"))
            .key(self.key("catalog"))
            .key(self.key("admin"))
            .key(self.key(&format!("entry:{key}")))
            .invoke_async(&mut self.connection.clone())
            .await
    }
    async fn initialize(&self, fixtures: Fixtures) -> anyhow::Result<()> {
        anyhow::ensure!(
            fixtures.diagnostic == "synthetic-100-percent-hit-upper-bound",
            "not a probe fixture"
        );
        let identity = digest("fixture", "immutable", &serde_json::to_vec(&fixtures)?);
        let mut connection = self.connection.clone();
        redis::pipe()
            .cmd("HSET")
            .arg(self.key("control"))
            .arg("identity")
            .arg(&identity)
            .arg("epoch")
            .arg("1")
            .arg("writer_count")
            .arg("0")
            .ignore()
            .cmd("SET")
            .arg(self.key("catalog"))
            .arg("synthetic-catalog")
            .ignore()
            .cmd("DEL")
            .arg(self.key("admin"))
            .ignore()
            .query_async::<()>(&mut connection)
            .await?;
        for entry in fixtures.entries {
            anyhow::ensure!(
                permitted(&Method::from_bytes(entry.method.as_bytes())?, &entry.path),
                "fixture contains forbidden route"
            );
            let key = digest(&entry.method, &entry.path, entry.request_body.as_bytes());
            redis::cmd("HSET")
                .arg(self.key(&format!("entry:{key}")))
                .arg("identity")
                .arg(&identity)
                .arg("epoch")
                .arg("1")
                .arg("payload")
                .arg(serde_json::to_vec(&entry.response)?)
                .query_async::<()>(&mut connection)
                .await?;
        }
        // Load before timing so every warm lookup uses one EVALSHA round trip.
        redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(SCRIPT)
            .query_async::<String>(&mut connection)
            .await?;
        Ok(())
    }
}
async fn handle(State(probe): State<Probe>, method: Method, uri: Uri, body: Bytes) -> Response {
    let started = Instant::now();
    if method == Method::GET && uri.path() == "/" {
        return Json(json!({"version":{"number":"synthetic-probe"},"diagnostic":"synthetic-100-percent-hit-upper-bound"})).into_response();
    }
    if !permitted(&method, uri.path()) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            "probe rejects writes and unsupported routes",
        )
            .into_response();
    }
    if !body.is_empty() && serde_json::from_slice::<Value>(&body).is_err() {
        return (StatusCode::BAD_REQUEST, "invalid JSON").into_response();
    }
    let key = digest(method.as_str(), &uri.to_string(), &body);
    let payload = match tokio::time::timeout(Duration::from_secs(5), probe.lookup(&key)).await {
        Ok(Ok(Some(payload))) => payload,
        _ => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "probe miss or guard denied; no backend fallback",
            )
                .into_response()
        }
    };
    let mut response: Value = match serde_json::from_slice(&payload) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
    };
    if response.get("took").is_some() {
        response["took"] = json!(started.elapsed().as_millis() as u64);
    }
    (
        [
            ("x-elastic-product", "Elasticsearch"),
            ("x-qeg-diagnostic", "synthetic-cache-upper-bound"),
        ],
        Json(response),
    )
        .into_response()
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let fixtures: Fixtures = serde_json::from_slice(&std::fs::read(env::var("PROBE_FIXTURES")?)?)?;
    let client = redis::Client::open(env::var("PROBE_VALKEY_URL")?)?;
    let probe = Probe {
        connection: client.get_multiplexed_async_connection().await?,
        prefix: Arc::from("{qeg-cache-upper-bound}"),
        script: Arc::new(redis::Script::new(SCRIPT)),
    };
    probe.initialize(fixtures).await?;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9200").await?;
    eprintln!("DIAGNOSTIC ONLY: synthetic 100% hit cache upper bound; not gateway performance");
    axum::serve(
        listener,
        Router::new().fallback(any(handle)).with_state(probe),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keys_preserve_request_boundaries() {
        assert_ne!(digest("GET", "ab", b"c"), digest("GET", "a", b"bc"));
        assert_ne!(digest("GET", "/x", b""), digest("POST", "/x", b""));
        assert_ne!(
            digest("POST", "/x?a=1", b"{}"),
            digest("POST", "/x?a=2", b"{}")
        );
    }
    #[test]
    fn writes_and_foreign_indices_are_rejected() {
        assert!(!permitted(&Method::PUT, "/balanced_benchmark/_doc/1"));
        assert!(!permitted(&Method::POST, "/balanced_benchmark/_bulk"));
        assert!(!permitted(&Method::GET, "/private/_doc/1"));
        assert!(permitted(&Method::POST, "/balanced_benchmark/_search"));
    }
    #[tokio::test]
    #[ignore = "requires isolated PROBE_TEST_VALKEY_URL"]
    async fn every_guard_denies_unsafe_hits() {
        let client = redis::Client::open(env::var("PROBE_TEST_VALKEY_URL").unwrap()).unwrap();
        let probe = Probe {
            connection: client.get_multiplexed_async_connection().await.unwrap(),
            prefix: Arc::from("{qeg-cache-upper-bound-test}"),
            script: Arc::new(redis::Script::new(SCRIPT)),
        };
        probe
            .initialize(Fixtures {
                diagnostic: "synthetic-100-percent-hit-upper-bound".into(),
                entries: vec![Entry {
                    method: "GET".into(),
                    path: "/balanced_benchmark/_doc/1".into(),
                    request_body: String::new(),
                    response: json!({"found":true}),
                }],
            })
            .await
            .unwrap();
        let key = digest("GET", "/balanced_benchmark/_doc/1", b"");
        assert!(probe.lookup(&key).await.unwrap().is_some());
        assert!(probe.lookup("absent").await.unwrap().is_none());
        let mut c = probe.connection.clone();
        for (field, changed) in [
            ("writer_count", "1"),
            ("epoch", "2"),
            ("identity", "replacement"),
        ] {
            let restored: String = redis::cmd("HGET")
                .arg(probe.key("control"))
                .arg(field)
                .query_async(&mut c)
                .await
                .unwrap();
            redis::cmd("HSET")
                .arg(probe.key("control"))
                .arg(field)
                .arg(changed)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
            assert!(probe.lookup(&key).await.unwrap().is_none());
            redis::cmd("HSET")
                .arg(probe.key("control"))
                .arg(field)
                .arg(restored)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
        }
        redis::cmd("SET")
            .arg(probe.key("admin"))
            .arg("claim")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(probe.lookup(&key).await.unwrap().is_none());
        redis::cmd("DEL")
            .arg(probe.key("admin"))
            .arg(probe.key("catalog"))
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(probe.lookup(&key).await.unwrap().is_none());
        redis::cmd("SET")
            .arg(probe.key("catalog"))
            .arg("restored")
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(probe.lookup(&key).await.unwrap().is_some());
        for field in ["identity", "epoch", "writer_count"] {
            let previous: String = redis::cmd("HGET")
                .arg(probe.key("control"))
                .arg(field)
                .query_async(&mut c)
                .await
                .unwrap();
            redis::cmd("HDEL")
                .arg(probe.key("control"))
                .arg(field)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
            assert!(probe.lookup(&key).await.unwrap().is_none());
            redis::cmd("HSET")
                .arg(probe.key("control"))
                .arg(field)
                .arg(previous)
                .query_async::<()>(&mut c)
                .await
                .unwrap();
        }
        redis::cmd("DEL")
            .arg(probe.key("control"))
            .query_async::<()>(&mut c)
            .await
            .unwrap();
        assert!(probe.lookup(&key).await.unwrap().is_none());
    }
}
