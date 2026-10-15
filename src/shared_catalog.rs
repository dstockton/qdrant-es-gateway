//! An opt-in authoritative catalog for replicas; SQLite is only a request-local view.
use super::*;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_BYTES: usize = 8 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(5);
const SCHEMA: &str = "CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);";

#[derive(Clone)]
pub(crate) struct SharedCatalog {
    client: redis::Client,
    key: String,
    connection: Arc<Mutex<Option<Arc<redis::aio::MultiplexedConnection>>>>,
    connect_gate: Arc<AsyncMutex<()>>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    indices: Vec<(String, String, String)>,
    aliases: Vec<(String, String)>,
}

/// Immutable metadata for one request, rebuilt from its authoritative snapshot.
/// This is never retained across requests or used by metadata mutation handlers.
pub(crate) struct ReadView {
    indices: HashMap<String, IndexMetadata>,
    aliases: HashMap<String, String>,
}

struct IndexMetadata {
    mapping: Value,
    vectors: Vec<String>,
}

impl ReadView {
    fn resolve(&self, name: &str) -> Result<(&str, &IndexMetadata), GatewayError> {
        // Match SQLite COALESCE exactly: concrete indices take precedence, and
        // an alias resolves only one hop to an existing concrete index.
        self.indices
            .get_key_value(name)
            .or_else(|| {
                self.aliases
                    .get(name)
                    .and_then(|target| self.indices.get_key_value(target))
            })
            .map(|(index, metadata)| (index.as_str(), metadata))
            .ok_or_else(|| GatewayError::NotFound(format!("no such index [{name}]")))
    }

    pub(crate) fn resolve_index(&self, name: &str) -> Result<(String, String), GatewayError> {
        let (index, _) = self.resolve(name)?;
        Ok((index.to_owned(), collection(index)))
    }

    pub(crate) fn get_index(
        &self,
        name: &str,
    ) -> Result<(String, String, Value, Vec<String>), GatewayError> {
        let (index, metadata) = self.resolve(name)?;
        Ok((
            index.to_owned(),
            collection(index),
            metadata.mapping.clone(),
            metadata.vectors.clone(),
        ))
    }
}

fn unavailable() -> GatewayError {
    // Never put connection URLs or Redis authentication errors in client responses.
    GatewayError::upstream("shared catalog unavailable or administrative claim requires recovery")
}

impl SharedCatalog {
    #[cfg(test)]
    pub(crate) fn test_handle(client: redis::Client, key: String) -> Self {
        Self {
            client,
            key,
            connection: Default::default(),
            connect_gate: Default::default(),
        }
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
            connection: Default::default(),
            connect_gate: Default::default(),
        };
        if env::var("CATALOG_INITIALIZE").as_deref() == Ok("true") {
            let empty = serde_json::to_string(&Snapshot::default())?;
            catalog
                .run::<Option<String>>(
                    redis::Script::new("return redis.call('SET', KEYS[1], ARGV[1], 'NX')")
                        .key(&catalog.key)
                        .arg(empty),
                )
                .await
                .map_err(|_| anyhow::anyhow!("shared catalog initialization failed"))?;
        }
        catalog.begin(false).await.map_err(|_| {
            anyhow::anyhow!("shared catalog missing, unavailable, or awaiting recovery")
        })?;
        Ok(Some(catalog))
    }

    // Share transport only: every operation still reads the authoritative catalog.
    // Never retry a failed command: an administrative claim may already exist.
    async fn run<T: redis::FromRedisValue>(
        &self,
        invocation: &redis::ScriptInvocation<'_>,
    ) -> Result<T, GatewayError> {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let shared = tokio::time::timeout_at(deadline, async {
            if let Some(connection) = self.connection.lock().map_err(|_| unavailable())?.as_ref() {
                return Ok(connection.clone());
            }
            let _gate = self.connect_gate.lock().await;
            // Another caller may have established transport while we waited.
            if let Some(connection) = self.connection.lock().map_err(|_| unavailable())?.as_ref() {
                return Ok(connection.clone());
            }
            let connection = Arc::new(
                self.client
                    .get_multiplexed_async_connection()
                    .await
                    .map_err(|_| unavailable())?,
            );
            *self.connection.lock().map_err(|_| unavailable())? = Some(connection.clone());
            Ok::<_, GatewayError>(connection)
        })
        .await
        .map_err(|_| unavailable())??;
        let mut connection = (*shared).clone();
        match tokio::time::timeout_at(deadline, invocation.invoke_async(&mut connection)).await {
            Ok(Ok(value)) => Ok(value),
            _ => {
                // A delayed failure from an old connection must not discard a
                // replacement established by another request.
                let mut slot = self.connection.lock().map_err(|_| unavailable())?;
                if slot
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &shared))
                {
                    *slot = None;
                }
                Err(unavailable())
            }
        }
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
        let (snapshot, claim) = self.begin_snapshot(admin).await?;
        let db = snapshot.into_db().map_err(|error| (error, !admin))?;
        Ok((db, claim))
    }

    pub(crate) async fn read_view(&self) -> Result<ReadView, GatewayError> {
        let (snapshot, _) = self
            .begin_snapshot(false)
            .await
            .map_err(|(error, _)| error)?;
        snapshot.into_read_view()
    }

    async fn begin_snapshot(
        &self,
        admin: bool,
    ) -> Result<(Snapshot, Option<String>), (GatewayError, bool)> {
        let claim = admin.then(|| uuid::Uuid::new_v4().to_string());
        let result: Vec<String> = self
            .run(
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
                .arg(MAX_BYTES),
            )
            .await
            .map_err(|error| (error, !admin))?;
        if result.first().map(String::as_str) == Some("rejected") {
            return Err((unavailable(), true));
        }
        if result.len() != 2 || result[0] != "ok" {
            return Err((unavailable(), !admin));
        }
        let snapshot = parse_snapshot(result[1].as_bytes()).map_err(|error| (error, !admin))?;
        Ok((snapshot, claim))
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
        let result: i32 = self.run(
            redis::Script::new("if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end; return redis.call('DEL', KEYS[1])")
                .key(format!("{}:admin", self.key)).arg(claim)
        ).await?;
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
        let result: i32 = self
            .run(
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
                .arg(payload),
            )
            .await?;
        if result == 1 {
            Ok(())
        } else {
            Err(unavailable())
        }
    }
}

/// Materialize the same validated request-local catalog for either transport path.
pub(crate) fn snapshot_db(bytes: &[u8]) -> Result<Connection, GatewayError> {
    parse_snapshot(bytes)?.into_db()
}

pub(crate) fn snapshot_read_view(bytes: &[u8]) -> Result<ReadView, GatewayError> {
    parse_snapshot(bytes)?.into_read_view()
}

fn parse_snapshot(bytes: &[u8]) -> Result<Snapshot, GatewayError> {
    if bytes.len() > MAX_BYTES {
        return Err(unavailable());
    }
    serde_json::from_slice(bytes).map_err(|_| unavailable())
}

impl Snapshot {
    fn into_read_view(self) -> Result<ReadView, GatewayError> {
        let mut indices = HashMap::new();
        for (name, mapping, vectors) in self.indices {
            // Validate all rows, including metadata unrelated to this request.
            // Keep the existing permissive JSON shape and vector ordering.
            let metadata = IndexMetadata {
                mapping: serde_json::from_str(&mapping).map_err(|_| unavailable())?,
                vectors: serde_json::from_str(&vectors).map_err(|_| unavailable())?,
            };
            if indices.insert(name, metadata).is_some() {
                return Err(unavailable());
            }
        }
        let mut aliases = HashMap::new();
        for (alias, index) in self.aliases {
            if aliases.insert(alias, index).is_some() {
                return Err(unavailable());
            }
        }
        Ok(ReadView { indices, aliases })
    }

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

/// Only handlers whose metadata access goes through get_index/resolve_index.
/// Body validity and response-cache eligibility do not change this routing.
pub(crate) fn uses_read_view(method: &Method, path: &str) -> bool {
    let Ok(parts) = decode_path_segments(path) else {
        return false;
    };
    match parts.as_slice() {
        [_, endpoint, _] => *method == Method::GET && endpoint == "_doc",
        [index, endpoint] => {
            endpoint == "_search"
                && matches!(*method, Method::GET | Method::POST)
                // dispatch_inner gives GET /_alias/<name> precedence over search.
                && !(*method == Method::GET && index == "_alias")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata_state(db: Connection) -> AppState {
        AppState {
            cfg: Config::from_env(),
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
                admin_mutation_started: None,
                mutation_completion: None,
            },
            db: Arc::new(Mutex::new(db)),
            shared_catalog: None,
            read_catalog: None,
            cache_runtime: None,
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
        }
    }

    #[test]
    fn read_view_preserves_sqlite_resolution_and_metadata_contracts() {
        let bytes = serde_json::to_vec(&json!({
            "indices": [
                ["books", "{\"large\":18446744073709551615,\"small\":-9223372036854775808}", "[\"z\",\"a\",\"z\"]"],
                ["collision", "7", "[]"],
                ["null", "null", "[]"],
                ["array", "[true,1]", "[]"],
                ["boolean", "false", "[]"],
                ["string", "\"value\"", "[]"],
                ["", "{}", "[]"],
                ["é", "{}", "[]"]
            ],
            "aliases": [
                ["current", "books"], ["collision", "books"],
                ["dangling", "missing"], ["chain", "current"],
                ["cycle-a", "cycle-b"], ["cycle-b", "cycle-a"], ["self", "self"]
            ]
        })).unwrap();
        let sqlite = metadata_state(snapshot_db(&bytes).unwrap());
        // No SQLite tables exist in this state: metadata must come entirely
        // from the supplied view, including not-found decisions.
        let mut typed = metadata_state(Connection::open_in_memory().unwrap());
        typed.read_catalog = Some(Arc::new(snapshot_read_view(&bytes).unwrap()));
        for name in [
            "books",
            "current",
            "collision",
            "null",
            "array",
            "boolean",
            "string",
            "",
            "é",
            "É",
            "BOOKS",
            "missing",
            "dangling",
            "chain",
            "cycle-a",
            "self",
        ] {
            assert_eq!(
                get_index(&typed, name).map_err(|error| error.to_string()),
                get_index(&sqlite, name).map_err(|error| error.to_string()),
                "get_index({name})"
            );
            assert_eq!(
                resolve_index(&typed, name).map_err(|error| error.to_string()),
                resolve_index(&sqlite, name).map_err(|error| error.to_string()),
                "resolve_index({name})"
            );
        }
        typed.db = Arc::new(Mutex::new(
            snapshot_db(br#"{"indices":[["stale-local","{}","[]"]],"aliases":[]}"#).unwrap(),
        ));
        assert!(matches!(
            get_index(&typed, "stale-local"),
            Err(GatewayError::NotFound(_))
        ));
        assert!(matches!(
            resolve_index(&typed, "stale-local"),
            Err(GatewayError::NotFound(_))
        ));
    }

    #[test]
    fn read_view_validates_the_complete_snapshot_like_sqlite() {
        let invalid = [
            "invalid json".to_owned(),
            r#"{"indices":[],"aliases":[],"unknown":true}"#.into(),
            r#"{"indices":[],"indices":[],"aliases":[]}"#.into(),
            r#"{"indices":[]}"#.into(),
            json!({"indices":[["wanted","{}","[]"],["unrelated","invalid","[]"]],"aliases":[]})
                .to_string(),
            json!({"indices":[["wanted","{}","[]"],["unrelated","{}","[1]"]],"aliases":[]})
                .to_string(),
            json!({"indices":[["wanted","{}","[]"],["unrelated","{}","{}"]],"aliases":[]})
                .to_string(),
            json!({"indices":[["wanted","{}","[]"],["wanted","{}","[]"]],"aliases":[]}).to_string(),
            json!({"indices":[],"aliases":[["same","x"],["same","x"]]}).to_string(),
            json!({"indices":[],"aliases":[["same","x"],["same","y"]]}).to_string(),
            " ".repeat(MAX_BYTES + 1),
        ];
        for bytes in invalid {
            assert!(snapshot_db(bytes.as_bytes()).is_err());
            assert!(snapshot_read_view(bytes.as_bytes()).is_err());
        }
    }

    #[test]
    fn read_view_allowlist_matches_dispatch_precedence() {
        for (method, path) in [
            (Method::GET, "/books/_doc/a"),
            (Method::GET, "/books/%5Fdoc/a%2Fb"),
            (Method::GET, "/books/_search"),
            (Method::POST, "/books/%5Fsearch"),
            (Method::POST, "/_alias/_search"),
        ] {
            assert!(uses_read_view(&method, path), "{method} {path}");
        }
        for (method, path) in [
            (Method::GET, "/_alias/_search"),
            (Method::GET, "/%5Falias/%5Fsearch"),
            (Method::GET, "/books/_mapping"),
            (Method::GET, "/books"),
            (Method::HEAD, "/books/_doc/a"),
            (Method::PUT, "/books/_doc/a"),
            (Method::POST, "/books/_update/a"),
            (Method::POST, "/books/_bulk"),
            (Method::POST, "/books/_count"),
            (Method::POST, "/books/_msearch"),
            (Method::POST, "/_aliases"),
            (Method::GET, "/%FF/_search"),
        ] {
            assert!(!uses_read_view(&method, path), "{method} {path}");
        }
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn transport_reuse_keeps_fresh_snapshots_and_never_replays_failed_admin() {
        let client = redis::Client::open(env::var("CATALOG_TEST_REDIS_URL").unwrap()).unwrap();
        let key = format!("qeg:{{transport-{}}}:v1", uuid::Uuid::new_v4());
        let catalog = SharedCatalog::test_handle(client.clone(), key.clone());
        let mut observer = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("SET")
            .arg(&key)
            .arg(r#"{"indices":[],"aliases":[]}"#)
            .query_async::<()>(&mut observer)
            .await
            .unwrap();
        catalog.begin(false).await.unwrap();
        let original = catalog.connection.lock().unwrap().as_ref().unwrap().clone();
        let mut transport = (*original).clone();
        let id: i64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut transport)
            .await
            .unwrap();
        redis::cmd("SET")
            .arg(&key)
            .arg(r#"{"indices":[],"aliases":[["fresh","target"]]}"#)
            .query_async::<()>(&mut observer)
            .await
            .unwrap();
        let replica = catalog.clone();
        let (db, _) = replica.begin(false).await.unwrap();
        assert_eq!(
            Snapshot::read(&db).unwrap().aliases,
            vec![("fresh".into(), "target".into())]
        );
        assert!(Arc::ptr_eq(
            catalog.connection.lock().unwrap().as_ref().unwrap(),
            &original
        ));
        let killed: i64 = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(id)
            .query_async(&mut observer)
            .await
            .unwrap();
        assert_eq!(killed, 1);
        let failure = catalog.begin_tracked(true).await;
        assert!(
            matches!(failure, Err((_, false))),
            "failed admin operation must remain ambiguous"
        );
        let claim: Option<String> = redis::cmd("GET")
            .arg(format!("{key}:admin"))
            .query_async(&mut observer)
            .await
            .unwrap();
        assert!(
            claim.is_none(),
            "failed call must not reconnect and replay its claim"
        );
        assert!(catalog.connection.lock().unwrap().is_none());
        let (db, _) = replica.begin(false).await.unwrap();
        assert_eq!(
            Snapshot::read(&db).unwrap().aliases,
            vec![("fresh".into(), "target".into())]
        );
        assert!(!Arc::ptr_eq(
            catalog.connection.lock().unwrap().as_ref().unwrap(),
            &original
        ));
        redis::cmd("DEL")
            .arg(&key)
            .query_async::<()>(&mut observer)
            .await
            .unwrap();
    }

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
            connection: Default::default(),
            connect_gate: Default::default(),
        };
        let second = SharedCatalog {
            client,
            key: key.clone(),
            connection: Default::default(),
            connect_gate: Default::default(),
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
            connection: Default::default(),
            connect_gate: Default::default(),
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
            read_catalog: None,
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
            connection: Default::default(),
            connect_gate: Default::default(),
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
            read_catalog: None,
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
