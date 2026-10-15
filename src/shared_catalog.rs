//! An opt-in authoritative catalog for replicas; SQLite is only a request-local view.
use super::*;
use serde::{Deserialize, Serialize};

const MAX_BYTES: usize = 8 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(5);
const SCHEMA: &str = "CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);";

#[derive(Clone)]
pub(crate) struct SharedCatalog {
    client: redis::Client,
    key: String,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    indices: Vec<(String, String, String)>,
    aliases: Vec<(String, String)>,
}

fn unavailable() -> GatewayError {
    // Never put connection URLs or Redis authentication errors in client responses.
    GatewayError::upstream("shared catalog unavailable or administrative claim requires recovery")
}

impl SharedCatalog {
    #[cfg(test)]
    pub(crate) fn test_handle(client: redis::Client, key: String) -> Self {
        Self { client, key }
    }
    pub(crate) async fn from_env() -> anyhow::Result<Option<Self>> {
        let Ok(url) = env::var("CATALOG_REDIS_URL") else {
            return Ok(None);
        };
        let client = redis::Client::open(url.as_str())
            .map_err(|_| anyhow::anyhow!("invalid CATALOG_REDIS_URL"))?;
        let catalog = Self {
            client,
            key: env::var("CATALOG_REDIS_KEY").unwrap_or_else(|_| "qeg:{catalog}:v1".into()),
        };
        if env::var("CATALOG_INITIALIZE").as_deref() == Ok("true") {
            let empty = serde_json::to_string(&Snapshot::default())?;
            tokio::time::timeout(TIMEOUT, async {
                let mut connection = catalog.client.get_multiplexed_async_connection().await?;
                redis::cmd("SET")
                    .arg(&catalog.key)
                    .arg(empty)
                    .arg("NX")
                    .query_async::<Option<String>>(&mut connection)
                    .await
            })
            .await
            .map_err(|_| anyhow::anyhow!("shared catalog initialization timed out"))?
            .map_err(|_| anyhow::anyhow!("shared catalog initialization failed"))?;
        }
        catalog.begin(false).await.map_err(|_| {
            anyhow::anyhow!("shared catalog missing, unavailable, or awaiting recovery")
        })?;
        Ok(Some(catalog))
    }

    pub(crate) async fn begin(
        &self,
        admin: bool,
    ) -> Result<(Connection, Option<String>), GatewayError> {
        self.begin_tracked(admin).await.map_err(|(error, _)| error)
    }

    // Error boolean proves this call did not acquire a claim. Transport failure
    // after an administrative SET is ambiguous and must retain the writer fence.
    pub(crate) async fn begin_tracked(
        &self,
        admin: bool,
    ) -> Result<(Connection, Option<String>), (GatewayError, bool)> {
        let claim = admin.then(|| uuid::Uuid::new_v4().to_string());
        let result: Vec<String> = tokio::time::timeout(TIMEOUT, async {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            redis::Script::new(
                r#"
                if redis.call('EXISTS', KEYS[2]) == 1 then return {'rejected',''} end
                if redis.call('EXISTS', KEYS[1]) == 0 then return {'rejected',''} end
                if redis.call('STRLEN', KEYS[1]) > tonumber(ARGV[2]) then return {'rejected',''} end
                local snapshot = redis.call('GET', KEYS[1])
                if ARGV[1] ~= '' then redis.call('SET', KEYS[2], ARGV[1]) end
                return {'ok',snapshot}
            "#,
            )
            .key(&self.key)
            .key(format!("{}:admin", self.key))
            .arg(claim.as_deref().unwrap_or(""))
            .arg(MAX_BYTES)
            .invoke_async(&mut connection)
            .await
        })
        .await
        .map_err(|_| (unavailable(), !admin))?
        .map_err(|_| (unavailable(), !admin))?;
        if result.first().map(String::as_str) == Some("rejected") {
            return Err((unavailable(), true));
        }
        if result.len() != 2 || result[0] != "ok" {
            return Err((unavailable(), !admin));
        }
        let snapshot: Snapshot =
            serde_json::from_str(&result[1]).map_err(|_| (unavailable(), !admin))?;
        let db = snapshot.into_db().map_err(|error| (error, !admin))?;
        Ok((db, claim))
    }

    pub(crate) async fn commit(&self, state: &AppState, claim: &str) -> Result<(), GatewayError> {
        let snapshot = {
            let db = state.db.lock().map_err(|_| unavailable())?;
            Snapshot::read(&db)?
        };
        let payload = serde_json::to_string(&snapshot).map_err(|_| unavailable())?;
        self.publish(claim, payload).await
    }

    pub(crate) async fn release(&self, claim: &str) -> Result<(), GatewayError> {
        let result: i32 = tokio::time::timeout(TIMEOUT, async {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            redis::Script::new("if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end; return redis.call('DEL', KEYS[1])")
                .key(format!("{}:admin", self.key)).arg(claim)
                .invoke_async(&mut connection).await
        }).await.map_err(|_| unavailable())?.map_err(|_| unavailable())?;
        if result == 1 {
            Ok(())
        } else {
            Err(unavailable())
        }
    }

    async fn publish(&self, claim: &str, payload: String) -> Result<(), GatewayError> {
        if payload.len() > MAX_BYTES {
            return Err(unavailable());
        }
        let result: i32 = tokio::time::timeout(TIMEOUT, async {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            redis::Script::new(
                r#"
                if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
                redis.call('SET', KEYS[1], ARGV[2])
                redis.call('DEL', KEYS[2])
                return 1
            "#,
            )
            .key(&self.key)
            .key(format!("{}:admin", self.key))
            .arg(claim)
            .arg(payload)
            .invoke_async(&mut connection)
            .await
        })
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        if result == 1 {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
}

impl Snapshot {
    fn into_db(self) -> Result<Connection, GatewayError> {
        let mut db = Connection::open_in_memory().map_err(|_| unavailable())?;
        db.execute_batch(SCHEMA).map_err(|_| unavailable())?;
        let tx = db.transaction().map_err(|_| unavailable())?;
        for (name, mapping, vectors) in self.indices {
            // Reject corrupt serialized metadata before any upstream request.
            serde_json::from_str::<Value>(&mapping).map_err(|_| unavailable())?;
            serde_json::from_str::<Vec<String>>(&vectors).map_err(|_| unavailable())?;
            tx.execute(
                "INSERT INTO indices VALUES (?1,?2,?3)",
                params![name, mapping, vectors],
            )
            .map_err(|_| unavailable())?;
        }
        for (alias, index) in self.aliases {
            tx.execute("INSERT INTO aliases VALUES (?1,?2)", params![alias, index])
                .map_err(|_| unavailable())?;
        }
        tx.commit().map_err(|_| unavailable())?;
        Ok(db)
    }
    fn read(db: &Connection) -> Result<Self, GatewayError> {
        let mut indices = db
            .prepare("SELECT name,mapping,vectors FROM indices ORDER BY name")
            .map_err(|_| unavailable())?;
        let indices = indices
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .map_err(|_| unavailable())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| unavailable())?;
        let mut aliases = db
            .prepare("SELECT alias,index_name FROM aliases ORDER BY alias")
            .map_err(|_| unavailable())?;
        let aliases = aliases
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|_| unavailable())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| unavailable())?;
        Ok(Self { indices, aliases })
    }
}

/// Check the exact serialized prospective snapshot before creating/updating Qdrant state.
pub(crate) fn validate_index_capacity(
    state: &AppState,
    index: &str,
    mapping: &Value,
    vectors: &[String],
) -> Result<(), GatewayError> {
    if state.shared_catalog.is_none() {
        return Ok(());
    }
    let db = state.db.lock().map_err(|_| unavailable())?;
    let mut snapshot = Snapshot::read(&db)?;
    snapshot.indices.retain(|(name, _, _)| name != index);
    snapshot.indices.push((
        index.to_owned(),
        mapping.to_string(),
        serde_json::to_string(vectors).map_err(|_| unavailable())?,
    ));
    validate_snapshot_capacity(&snapshot)
}

pub(crate) fn validate_catalog_capacity(
    state: &AppState,
    db: &Connection,
) -> Result<(), GatewayError> {
    if state.shared_catalog.is_none() {
        return Ok(());
    }
    validate_snapshot_capacity(&Snapshot::read(db)?)
}

fn validate_snapshot_capacity(snapshot: &Snapshot) -> Result<(), GatewayError> {
    if serde_json::to_vec(snapshot)
        .map_err(|_| unavailable())?
        .len()
        > MAX_BYTES
    {
        return Err(GatewayError::bad("catalog.size", "shared metadata catalog would exceed its fixed 8 MiB capacity; no upstream changes were attempted"));
    }
    Ok(())
}

pub(crate) fn is_admin(method: &Method, path: &str) -> Result<bool, GatewayError> {
    if is_bulk_request(method, path) {
        return Ok(false);
    }
    let parts = decode_path_segments(path)?;
    Ok(
        (parts.len() == 1 && matches!(*method, Method::PUT | Method::DELETE))
            || (path == "/_aliases" && *method == Method::POST)
            || (parts.len() == 2 && parts[1] == "_mapping" && *method == Method::PUT),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Run against an isolated local Redis/Valkey instance, never a production catalog.
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn shared_catalog_serializes_replicas_and_retains_uncertain_claims() {
        let url = env::var("CATALOG_TEST_REDIS_URL").unwrap();
        let client = redis::Client::open(url).unwrap();
        let key = format!("qeg:{{test-{}}}:v1", uuid::Uuid::new_v4());
        let first = SharedCatalog {
            client: client.clone(),
            key: key.clone(),
        };
        let second = SharedCatalog {
            client,
            key: key.clone(),
        };
        let mut connection = first
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let empty = serde_json::to_string(&Snapshot::default()).unwrap();
        assert!(
            first.begin(false).await.is_err(),
            "missing catalog must not silently initialize"
        );
        redis::cmd("SET")
            .arg(&key)
            .arg(&empty)
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let (_, validation_claim) = first.begin(true).await.unwrap();
        assert!(second.release("wrong-owner").await.is_err());
        first
            .release(validation_claim.as_deref().unwrap())
            .await
            .unwrap();
        assert!(
            second.begin(false).await.is_ok(),
            "pre-mutation validation failure can safely release ownership"
        );
        let (_, claim) = first.begin(true).await.unwrap();
        assert!(second.begin(false).await.is_err());
        assert!(second.begin(true).await.is_err());
        let ttl: i64 = redis::cmd("TTL")
            .arg(format!("{key}:admin"))
            .query_async(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            ttl, -1,
            "ownership must not expire while Qdrant may still be mutating"
        );
        assert!(second.publish("wrong-owner", empty).await.is_err());
        let updated = Snapshot {
            indices: vec![("books".into(), "{}".into(), "[]".into())],
            aliases: vec![("live".into(), "books".into())],
        };
        first
            .publish(
                claim.as_deref().unwrap(),
                serde_json::to_string(&updated).unwrap(),
            )
            .await
            .unwrap();
        let (db, claim) = second.begin(false).await.unwrap();
        assert!(claim.is_none());
        assert_eq!(Snapshot::read(&db).unwrap().aliases, updated.aliases);
        let _ = first.begin(true).await.unwrap();
        drop(first);
        assert!(
            second.begin(false).await.is_err(),
            "dropping a failed writer must retain ownership"
        );
        redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:admin"))
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn dispatch_releases_validation_errors_but_retains_upstream_failures() {
        let client = redis::Client::open(env::var("CATALOG_TEST_REDIS_URL").unwrap()).unwrap();
        let key = format!("qeg:{{test-{}}}:v1", uuid::Uuid::new_v4());
        let catalog = SharedCatalog {
            client,
            key: key.clone(),
        };
        let mut connection = catalog
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        redis::cmd("SET")
            .arg(&key)
            .arg(serde_json::to_string(&Snapshot::default()).unwrap())
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            )
            .await
            .unwrap();
        });
        let state = AppState {
            cfg: Config::from_env(),
            qdrant: Qdrant {
                client: Client::new(),
                base,
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
                admin_mutation_started: None,
                mutation_completion: None,
            },
            db: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            shared_catalog: Some(catalog.clone()),
            cache_runtime: None,
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
        };
        let response = dispatch(
            state.clone(),
            Method::PUT,
            "/books".into(),
            None,
            HeaderMap::new(),
            "not JSON".into(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(catalog.begin(false).await.is_ok());
        let response = dispatch(
            state,
            Method::PUT,
            "/books".into(),
            None,
            HeaderMap::new(),
            "{}".into(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(catalog.begin(false).await.is_err());
        server.abort();
        redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:admin"))
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn dispatch_publishes_create_mapping_and_aliases_across_replicas() {
        let client = redis::Client::open(env::var("CATALOG_TEST_REDIS_URL").unwrap()).unwrap();
        let key = format!("qeg:{{test-{}}}:v1", uuid::Uuid::new_v4());
        let catalog = SharedCatalog {
            client,
            key: key.clone(),
        };
        let mut connection = catalog
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        redis::cmd("SET")
            .arg(&key)
            .arg(serde_json::to_string(&Snapshot::default()).unwrap())
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = writes.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(move || {
                    let observed = observed.clone();
                    async move {
                        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Json(json!({"status":"ok","result":true}))
                    }
                }),
            )
            .await
            .unwrap();
        });
        let replica = || AppState {
            cfg: Config::from_env(),
            qdrant: Qdrant {
                client: Client::new(),
                base: base.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
                admin_mutation_started: None,
                mutation_completion: None,
            },
            db: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            shared_catalog: Some(catalog.clone()),
            cache_runtime: None,
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
        };
        let first = replica();
        let second = replica();
        for (method, path, body) in [
            (
                Method::PUT,
                "/books",
                json!({"mappings":{"properties":{"title":{"type":"text"}}}}),
            ),
            (
                Method::PUT,
                "/books/_mapping",
                json!({"properties":{"year":{"type":"integer"}}}),
            ),
            (
                Method::POST,
                "/_aliases",
                json!({"actions":[{"add":{"index":"books","alias":"current"}}]}),
            ),
        ] {
            let response = dispatch(
                first.clone(),
                method,
                path.into(),
                None,
                HeaderMap::new(),
                body.to_string(),
            )
            .await;
            assert!(
                response.status().is_success(),
                "{path}: {}",
                response.status()
            );
            let response = dispatch(
                second.clone(),
                Method::GET,
                "/books/_mapping".into(),
                None,
                HeaderMap::new(),
                String::new(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                result["books"]["mappings"]["properties"]["title"]["type"],
                "text"
            );
            if path != "/books" {
                assert_eq!(
                    result["books"]["mappings"]["properties"]["year"]["type"],
                    "integer"
                );
            }
        }
        let response = dispatch(
            second.clone(),
            Method::GET,
            "/current".into(),
            None,
            HeaderMap::new(),
            String::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&bytes)
            .unwrap()
            .get("books")
            .is_some());
        // Exact prospective-size checks must reject before any Qdrant mutation,
        // and release the claim so the other replica remains usable.
        for (path, body) in [
            (
                "/oversized",
                json!({"mappings":{"_meta":{"padding":"x".repeat(MAX_BYTES)}}}),
            ),
            (
                "/books/_mapping",
                json!({"properties":{"large":{"type":"keyword","meta":{"padding":"x".repeat(MAX_BYTES)}}}}),
            ),
            (
                "/_aliases",
                json!({"actions":[{"add":{"index":"books","alias":"x".repeat(MAX_BYTES)}}]}),
            ),
        ] {
            let before = writes.load(std::sync::atomic::Ordering::SeqCst);
            let method = if path == "/_aliases" {
                Method::POST
            } else {
                Method::PUT
            };
            let response = dispatch(
                first.clone(),
                method,
                path.into(),
                None,
                HeaderMap::new(),
                body.to_string(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let error: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(error["error"]["feature"], "catalog.size", "{path}");
            assert_eq!(
                writes.load(std::sync::atomic::Ordering::SeqCst),
                before,
                "{path} must not mutate Qdrant"
            );
            assert!(
                catalog.begin(false).await.is_ok(),
                "{path} must not leave an administrative claim"
            );
        }
        server.abort();
        redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{key}:admin"))
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }

    #[test]
    fn catalog_capacity_counts_serialized_bytes_including_escaping() {
        let mut snapshot = Snapshot {
            indices: vec![],
            aliases: vec![(String::new(), "books".into())],
        };
        let overhead = serde_json::to_vec(&snapshot).unwrap().len();
        snapshot.aliases[0].0 = "x".repeat(MAX_BYTES - overhead);
        assert!(validate_snapshot_capacity(&snapshot).is_ok());
        snapshot.aliases[0].0.push('x');
        assert!(validate_snapshot_capacity(&snapshot).is_err());
        snapshot.aliases[0].0 = "\n".repeat(MAX_BYTES / 2);
        assert!(validate_snapshot_capacity(&snapshot).is_err());
    }

    #[test]
    fn snapshots_preserve_aliases_and_mapping() {
        let original = Snapshot {
            indices: vec![(
                "books".into(),
                "{\"properties\":{}}".into(),
                "[\"text_all\"]".into(),
            )],
            aliases: vec![("current".into(), "books".into())],
        };
        let expected = serde_json::to_value(&original).unwrap();
        let db = original.into_db().unwrap();
        assert_eq!(
            serde_json::to_value(Snapshot::read(&db).unwrap()).unwrap(),
            expected
        );
    }
    #[test]
    fn corrupt_or_duplicate_catalog_is_rejected() {
        assert!(Snapshot {
            indices: vec![("x".into(), "bad".into(), "[]".into())],
            aliases: vec![]
        }
        .into_db()
        .is_err());
        assert!(Snapshot {
            indices: vec![],
            aliases: vec![("x".into(), "a".into()), ("x".into(), "b".into())]
        }
        .into_db()
        .is_err());
    }
    #[test]
    fn metadata_mutations_are_classified_after_decoding() {
        for (method, path) in [
            (Method::PUT, "/books"),
            (Method::DELETE, "/books"),
            (Method::POST, "/_aliases"),
            (Method::PUT, "/books/%5Fmapping"),
        ] {
            assert!(is_admin(&method, path).unwrap());
        }
        for (method, path) in [
            (Method::GET, "/books/_mapping"),
            (Method::PUT, "/books/_doc/1"),
            (Method::POST, "/_bulk"),
            (Method::PUT, "/_bulk"),
            (Method::POST, "/books/_bulk"),
            (Method::PUT, "/books/_bulk"),
        ] {
            assert!(!is_admin(&method, path).unwrap());
        }
    }
}
