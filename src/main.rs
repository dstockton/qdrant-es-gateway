use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use futures_util::future::try_join_all;
use percent_encoding::percent_decode_str;
use regex::Regex;
use reqwest::Client;
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tracing::{info, warn};

#[derive(Clone)]
struct Config {
    listen_addr: String,
    qdrant_url: String,
    qdrant_api_key: Option<String>,
    compat_version: String,
    analytics: bool,
    max_body_bytes: usize,
    max_bulk_bytes: usize,
    max_page_size: u64,
    async_payload_writes: bool,
    document_projection: bool,
    async_search_projection: bool,
    qdrant_replication_factor: Option<u64>,
    qdrant_connect_timeout: Duration,
    qdrant_request_timeout: Duration,
    async_write_queue: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:9200".into()),
            qdrant_url: env::var("QDRANT_URL")
                .unwrap_or_else(|_| "http://qdrant:6333".into())
                .trim_end_matches('/')
                .into(),
            qdrant_api_key: env::var("QDRANT_API_KEY").ok().filter(|v| !v.is_empty()),
            compat_version: env::var("ES_COMPAT_VERSION").unwrap_or_else(|_| "8.15.0".into()),
            analytics: env::var("COMPATIBILITY_ANALYTICS")
                .map(|v| v != "0" && v != "false")
                .unwrap_or(true),
            max_body_bytes: env::var("MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10 * 1024 * 1024),
            max_bulk_bytes: env::var("MAX_BULK_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50 * 1024 * 1024),
            max_page_size: env::var("MAX_PAGE_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            async_payload_writes: env::var("ASYNC_PAYLOAD_WRITES")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            document_projection: env::var("DOCUMENT_PROJECTION")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            async_search_projection: env::var("ASYNC_SEARCH_PROJECTION")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            qdrant_replication_factor: env::var("QDRANT_REPLICATION_FACTOR")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|v| *v > 0),
            qdrant_connect_timeout: positive_duration_env(
                "QDRANT_CONNECT_TIMEOUT_MS",
                Duration::from_secs(5),
            ),
            qdrant_request_timeout: positive_duration_env(
                "QDRANT_REQUEST_TIMEOUT_MS",
                Duration::from_secs(180),
            ),
            async_write_queue: positive_usize_env("ASYNC_WRITE_QUEUE", 256),
        }
    }
}

fn positive_duration_env(name: &str, default: Duration) -> Duration {
    match env::var(name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(milliseconds) if milliseconds > 0 => Duration::from_millis(milliseconds),
            _ => {
                warn!(setting = name, "invalid positive duration; using default");
                default
            }
        },
        Err(_) => default,
    }
}

fn positive_usize_env(name: &str, default: usize) -> usize {
    match env::var(name) {
        Ok(value) => match value.parse::<usize>() {
            Ok(number) if number > 0 => number,
            _ => {
                warn!(setting = name, "invalid positive integer; using default");
                default
            }
        },
        Err(_) => default,
    }
}

#[derive(Clone)]
struct AppState {
    cfg: Config,
    qdrant: Qdrant,
    db: Arc<Mutex<Connection>>,
    analytics: Arc<Mutex<Analytics>>,
    index_admin: Arc<AsyncMutex<()>>,
}

#[derive(Default)]
struct Analytics {
    requests: u64,
    supported: u64,
    unsupported: HashMap<String, u64>,
}

#[derive(Clone)]
struct Qdrant {
    client: Client,
    base: String,
    key: Option<String>,
    async_write_queue: Arc<Semaphore>,
}

impl Qdrant {
    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, GatewayError> {
        let url = format!("{}{}", self.base, path);
        let mut req = self.client.request(method, url);
        if let Some(key) = &self.key {
            req = req.header("api-key", key);
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req
            .send()
            .await
            .map_err(|e| GatewayError::upstream(e.without_url().to_string()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| GatewayError::upstream(e.to_string()))?;
        let value: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({"status": String::from_utf8_lossy(&bytes)}));
        if !status.is_success() || value.get("status").and_then(Value::as_str) == Some("error") {
            return Err(GatewayError::upstream(format!(
                "Qdrant returned {status}: {value}"
            )));
        }
        Ok(value)
    }
    async fn health(&self) -> Result<(), GatewayError> {
        self.request(Method::GET, "/", None).await.map(|_| ())
    }

    fn spawn_request(&self, method: Method, path: String, body: Value) -> Result<(), GatewayError> {
        let permit = self
            .async_write_queue
            .clone()
            .try_acquire_owned()
            .map_err(|_| GatewayError::upstream("asynchronous write queue is full"))?;
        let qdrant = self.clone();
        tokio::spawn(async move {
            let result = qdrant.request(method, &path, Some(body)).await;
            drop(permit);
            if let Err(error) = result {
                warn!(%error, "asynchronous Qdrant write failed");
            }
        });
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum GatewayError {
    #[error("{message}")]
    Bad { feature: String, message: String },
    #[error("{0}")]
    NotFound(String),
    #[error("document [{index}]/[{id}] is missing")]
    DocumentNotFound { index: String, id: String },
    #[error("document [{index}]/[{id}] already exists")]
    VersionConflict { index: String, id: String },
    #[error("request body exceeds configured {setting} limit of {limit} bytes")]
    PayloadTooLarge { setting: &'static str, limit: usize },
    #[error("upstream error: {0}")]
    Upstream(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl GatewayError {
    fn bad(feature: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Bad {
            feature: feature.into(),
            message: message.into(),
        }
    }
    fn upstream(s: impl Into<String>) -> Self {
        Self::Upstream(s.into())
    }
    fn payload_too_large(setting: &'static str, limit: usize) -> Self {
        Self::PayloadTooLarge { setting, limit }
    }
    fn status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) | Self::DocumentNotFound { .. } => StatusCode::NOT_FOUND,
            Self::VersionConflict { .. } => StatusCode::CONFLICT,
            Self::Bad { .. } => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn body(&self) -> Value {
        match self {
            Self::Bad { feature, message } => {
                json!({"error":{"type":"qdrant_gateway_unsupported_query","reason":"Unsupported Elasticsearch request","feature":feature,"message":message},"status":400})
            }
            Self::NotFound(message) => {
                json!({"error":{"type":"index_not_found_exception","reason":message},"status":404})
            }
            Self::DocumentNotFound { index, id } => {
                json!({"error":{"type":"document_missing_exception","reason":format!("[{id}]: document missing"),"index":index,"shard":"0"},"status":404})
            }
            Self::VersionConflict { index, id } => {
                json!({"error":{"type":"version_conflict_engine_exception","reason":format!("[{id}]: version conflict, document already exists (current version [1])"),"index":index,"shard":"0"},"status":409})
            }
            Self::PayloadTooLarge { setting, limit } => {
                json!({"error":{"type":"content_too_long_exception","reason":format!("request body exceeds configured {setting} limit of {limit} bytes")},"status":413})
            }
            Self::Upstream(message) => {
                json!({"error":{"type":"qdrant_upstream_error","reason":message},"status":502})
            }
            Self::Internal(message) => {
                json!({"error":{"type":"qdrant_gateway_internal_error","reason":message},"status":500})
            }
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        es_response(self.status(), self.body())
    }
}

fn es_response(status: StatusCode, body: Value) -> Response {
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        "x-elastic-product",
        HeaderValue::from_static("Elasticsearch"),
    );
    response
}

fn es_ok(body: Value) -> Response {
    es_response(StatusCode::OK, body)
}

fn init_db() -> anyhow::Result<Connection> {
    let path = env::var("METADATA_DB").unwrap_or_else(|_| "gateway.db".into());
    let db = Connection::open(path)?;
    db.execute_batch("PRAGMA busy_timeout=5000; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE IF NOT EXISTS aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")?;
    Ok(db)
}

fn point_id(index: &str, id: &str) -> String {
    let mut h = Sha256::new();
    h.update(index.as_bytes());
    h.update([0]);
    h.update(id.as_bytes());
    let b = h.finalize();
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
        u16::from_be_bytes([b[4], b[5]]),
        u16::from_be_bytes([b[6], b[7]]) & 0x0fff | 0x5000,
        u16::from_be_bytes([b[8], b[9]]) & 0x3fff | 0x8000,
        u64::from_be_bytes([b[10], b[11], b[12], b[13], b[14], b[15], 0, 0]) >> 16
    )
}

fn collection(index: &str) -> String {
    format!(
        "es_{}",
        index.replace(
            |c: char| !c.is_ascii_alphanumeric() && c != '_' && c != '-',
            "_"
        )
    )
}

fn document_collection(index: &str) -> String {
    format!("{}_documents", collection(index))
}

fn ensure_collection_namespace_available(
    state: &AppState,
    index: &str,
) -> Result<(), GatewayError> {
    let requested = [collection(index), document_collection(index)];
    let db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let alias_uses_index_name = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM aliases WHERE alias=?1)",
            params![index],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    if alias_uses_index_name {
        return Err(GatewayError::bad(
            "index.name",
            format!("index [{index}] conflicts with an existing alias"),
        ));
    }
    let mut stmt = db
        .prepare("SELECT name FROM indices")
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let existing = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| GatewayError::Internal(e.to_string()))?;

    for existing in existing {
        let existing = existing.map_err(|e| GatewayError::Internal(e.to_string()))?;
        let reserved = [collection(&existing), document_collection(&existing)];
        if requested
            .iter()
            .any(|candidate| reserved.contains(candidate))
        {
            return Err(GatewayError::bad(
                "index.name",
                format!(
                    "index [{index}] conflicts with the Qdrant collection namespace reserved by existing index [{existing}]; choose a different index name"
                ),
            ));
        }
    }
    Ok(())
}

fn get_index(
    state: &AppState,
    name: &str,
) -> Result<(String, String, Value, Vec<String>), GatewayError> {
    let db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let mut stmt = db
        .prepare(
            "SELECT name, mapping, vectors FROM indices
             WHERE name = COALESCE(
                 (SELECT name FROM indices WHERE name=?1),
                 (SELECT index_name FROM aliases WHERE alias=?1)
             )",
        )
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let row = stmt
        .query_row(params![name], |r| {
            let index: String = r.get(0)?;
            let mapping: String = r.get(1)?;
            let vectors: String = r.get(2)?;
            Ok((index, mapping, vectors))
        })
        .map_err(|_| GatewayError::NotFound(format!("no such index [{name}]")))?;
    let vectors =
        serde_json::from_str(&row.2).map_err(|e| GatewayError::Internal(e.to_string()))?;
    Ok((
        row.0.clone(),
        collection(&row.0),
        serde_json::from_str(&row.1).map_err(|e| GatewayError::Internal(e.to_string()))?,
        vectors,
    ))
}

fn mapping_vectors(body: &Value) -> (Value, Vec<String>) {
    let mapping = body.get("mappings").cloned().unwrap_or_else(|| json!({}));
    let mut vectors = Vec::new();
    if let Some(props) = mapping.get("properties").and_then(Value::as_object) {
        for (field, spec) in props {
            if spec.get("type").and_then(Value::as_str) == Some("text") {
                vectors.push(format!("text_{}", field.replace('.', "_")));
            }
        }
    }
    if vectors.is_empty() {
        vectors.push("text_all".into());
    }
    if !vectors.iter().any(|v| v == "text_all") {
        vectors.push("text_all".into());
    }
    vectors.sort();
    (mapping, vectors)
}

fn payload_field_schema(spec: &Value) -> Option<&'static str> {
    match spec.get("type").and_then(Value::as_str) {
        Some("keyword") => Some("keyword"),
        Some("boolean") => Some("bool"),
        Some("byte" | "short" | "integer" | "long") => Some("integer"),
        Some("float" | "double") => Some("float"),
        Some("date") => Some("datetime"),
        _ => None,
    }
}

fn flatten_text(source: &Value, fields: &[String]) -> HashMap<String, String> {
    let obj = source.as_object().cloned().unwrap_or_default();
    let mut out = HashMap::new();
    for vector in fields {
        let field = vector
            .strip_prefix("text_")
            .unwrap_or(vector)
            .replace('_', ".");
        let value = obj
            .get(&field)
            .or_else(|| obj.get(vector))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        out.insert(vector.clone(), value);
    }
    out.insert(
        "text_all".into(),
        obj.values()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
    );
    out
}

async fn root(State(state): State<AppState>) -> Response {
    es_ok(
        json!({"name":"qdrant-es-gateway","cluster_name":"qdrant","cluster_uuid":"qdrant","version":{"number":state.cfg.compat_version,"build_flavor":"default","build_type":"gateway","lucene_version":"qdrant"},"tagline":"You Know, for Search"}),
    )
}
async fn health(State(state): State<AppState>) -> Response {
    match state.qdrant.health().await {
        Ok(_) => es_ok(
            json!({"cluster_name":"qdrant","status":"green","number_of_nodes":1,"active_shards":1}),
        ),
        Err(e) => e.into_response(),
    }
}
async fn healthz() -> Response {
    Json(json!({"status":"ok"})).into_response()
}
async fn readyz(State(state): State<AppState>) -> Response {
    match state.qdrant.health().await {
        Ok(_) => Json(json!({"status":"ready"})).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(e.body())).into_response(),
    }
}

async fn create_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    let _index_admin = state.index_admin.lock().await;
    // Collection names predate an explicit per-index namespace column and
    // normalize punctuation for Qdrant. Reject collisions before creating any
    // upstream state, including collisions with the optional document store.
    ensure_collection_namespace_available(&state, &index)?;
    let (mapping, vectors) = mapping_vectors(&body);
    let mut sparse = Map::new();
    for v in &vectors {
        sparse.insert(v.clone(), json!({}));
    }
    let mut collection_config = json!({"sparse_vectors": sparse});
    if let Some(replication_factor) = state.cfg.qdrant_replication_factor {
        collection_config["replication_factor"] = json!(replication_factor);
        collection_config["shard_number"] = json!(replication_factor);
    }
    state
        .qdrant
        .request(
            Method::PUT,
            &format!("/collections/{}", collection(&index)),
            Some(collection_config),
        )
        .await?;
    if state.cfg.document_projection {
        let mut document_config = json!({"vectors":{"size":1,"distance":"Dot","on_disk":true},"hnsw_config":{"m":0},"optimizers_config":{"indexing_threshold":0}});
        if let Some(replication_factor) = state.cfg.qdrant_replication_factor {
            document_config["replication_factor"] = json!(replication_factor);
            document_config["shard_number"] = json!(replication_factor);
        }
        if let Err(error) = state
            .qdrant
            .request(
                Method::PUT,
                &format!("/collections/{}", document_collection(&index)),
                // Qdrant 1.15 requires a vector field on each point. A
                // one-dimensional on-disk vector with m=0 keeps this
                // collection payload-first and disables HNSW construction.
                Some(document_config),
            )
            .await
        {
            cleanup_failed_index_creation(&state, &index, false).await;
            return Err(error);
        }
    }
    if let Some(props) = mapping.get("properties").and_then(Value::as_object) {
        for (field, spec) in props {
            let schema = payload_field_schema(spec);
            if let Some(schema) = schema {
                if let Err(error) = state
                    .qdrant
                    .request(
                        Method::PUT,
                        &format!("/collections/{}/index", collection(&index)),
                        Some(json!({"field_name":field,"field_schema":schema,"wait":true})),
                    )
                    .await
                {
                    cleanup_failed_index_creation(&state, &index, state.cfg.document_projection)
                        .await;
                    return Err(error);
                }
            }
        }
    }
    let metadata_result = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))
        .and_then(|db| {
            db.execute(
                "INSERT OR REPLACE INTO indices(name,mapping,vectors) VALUES (?1,?2,?3)",
                params![
                    index,
                    mapping.to_string(),
                    serde_json::to_string(&vectors)
                        .map_err(|e| GatewayError::Internal(e.to_string()))?
                ],
            )
            .map_err(|e| GatewayError::Internal(e.to_string()))?;
            Ok(())
        });
    if let Err(error) = metadata_result {
        cleanup_failed_index_creation(&state, &index, state.cfg.document_projection).await;
        return Err(error);
    }
    Ok(es_ok(
        json!({"acknowledged":true,"shards_acknowledged":true,"index":index}),
    ))
}

async fn cleanup_failed_index_creation(state: &AppState, index: &str, document_created: bool) {
    let mut collections = vec![collection(index)];
    if document_created {
        collections.push(document_collection(index));
    }
    for collection in collections {
        if let Err(error) = state
            .qdrant
            .request(Method::DELETE, &format!("/collections/{collection}"), None)
            .await
        {
            warn!(%collection, %error, "failed to clean up collection after index creation error");
        }
    }
}

async fn delete_index(
    State(state): State<AppState>,
    Path(index): Path<String>,
) -> Result<Response, GatewayError> {
    let _index_admin = state.index_admin.lock().await;
    let (concrete_index, _, _, _) = get_index(&state, &index)?;
    if concrete_index != index {
        return Err(GatewayError::bad(
            "index.name",
            "index deletion requires a concrete index name, not an alias",
        ));
    }
    state
        .qdrant
        .request(
            Method::DELETE,
            &format!("/collections/{}", collection(&index)),
            None,
        )
        .await?;
    if state.cfg.document_projection {
        state
            .qdrant
            .request(
                Method::DELETE,
                &format!("/collections/{}", document_collection(&index)),
                None,
            )
            .await?;
    }
    let mut db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let transaction = db
        .transaction()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    transaction
        .execute("DELETE FROM aliases WHERE index_name=?1", params![index])
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    transaction
        .execute("DELETE FROM indices WHERE name=?1", params![index])
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    transaction
        .commit()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    Ok(es_ok(json!({"acknowledged":true})))
}

async fn get_index_info(
    State(state): State<AppState>,
    Path(index): Path<String>,
) -> Result<Response, GatewayError> {
    let (index, _, mapping, _) = get_index(&state, &index)?;
    Ok(es_ok(json!({index:{"mappings":mapping}})))
}
async fn head_index(State(state): State<AppState>, Path(index): Path<String>) -> Response {
    if get_index(&state, &index).is_ok() {
        StatusCode::OK.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn index_control(
    State(state): State<AppState>,
    Path(index): Path<String>,
    action: &'static str,
) -> Result<Response, GatewayError> {
    get_index(&state, &index)?;
    Ok(es_ok(json!({
        "acknowledged": true,
        "shards_acknowledged": true,
        "index": index,
        "action": action
    })))
}

fn qdrant_payload(source: &Value, id: &str, index: &str, mapping: &Value) -> Value {
    let mut p = projected_search_payload(source, id, index, mapping)
        .as_object()
        .cloned()
        .unwrap_or_default();
    p.insert("_source".into(), source.clone());
    Value::Object(p)
}

fn projected_search_payload(source: &Value, id: &str, index: &str, mapping: &Value) -> Value {
    let mut p = Map::new();
    p.insert("_es_id".into(), json!(id));
    p.insert("_es_index".into(), json!(index));
    if let (Some(source), Some(properties)) = (
        source.as_object(),
        mapping.get("properties").and_then(Value::as_object),
    ) {
        for (field, spec) in properties {
            let is_searchable_payload_field = spec
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind != "text");
            if is_searchable_payload_field {
                if let Some(value) = source.get(field) {
                    p.insert(field.clone(), value.clone());
                }
            }
        }
    }
    Value::Object(p)
}

fn source_payload(source: &Value, id: &str, index: &str) -> Value {
    json!({"_es_id":id,"_es_index":index,"_source":source})
}

fn source_point(source: &Value, id: &str, index: &str) -> Value {
    json!({"id":point_id(index,id),"vector":[0.0],"payload":source_payload(source,id,index)})
}

fn merge_partial_document(target: &mut Value, patch: &Value) -> bool {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            let mut changed = false;
            for (key, value) in patch {
                match target.get_mut(key) {
                    Some(existing) => changed |= merge_partial_document(existing, value),
                    None => {
                        target.insert(key.clone(), value.clone());
                        changed = true;
                    }
                }
            }
            changed
        }
        (target, patch) if target == patch => false,
        (target, patch) => {
            *target = patch.clone();
            true
        }
    }
}

async fn retrieve_sources(
    state: &AppState,
    index: &str,
    ids: &[String],
) -> Result<HashMap<String, Value>, GatewayError> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let result = state
        .qdrant
        .request(
            Method::POST,
            &format!("/collections/{}/points", document_collection(index)),
            Some(json!({"ids":ids,"with_payload":true,"with_vector":false})),
        )
        .await?;
    let mut sources = HashMap::new();
    if let Some(points) = result.get("result").and_then(Value::as_array) {
        for point in points {
            if let Some(payload) = point.get("payload") {
                if let Some(id) = payload.get("_es_id").and_then(Value::as_str) {
                    sources.insert(
                        id.to_string(),
                        payload.get("_source").cloned().unwrap_or_else(|| json!({})),
                    );
                }
            }
        }
    }
    Ok(sources)
}

async fn retrieve_source(
    state: &AppState,
    index: &str,
    collection: &str,
    id: &str,
) -> Result<Option<Value>, GatewayError> {
    if state.cfg.document_projection {
        return Ok(retrieve_sources(state, index, &[point_id(index, id)])
            .await?
            .remove(id));
    }
    let point = state
        .qdrant
        .request(
            Method::GET,
            &format!(
                "/collections/{collection}/points/{}?with_payload=true",
                point_id(index, id)
            ),
            None,
        )
        .await?;
    Ok(point
        .get("result")
        .filter(|result| !result.is_null())
        .and_then(|result| result.get("payload"))
        .and_then(|payload| payload.get("_source"))
        .cloned())
}

async fn retrieve_existing_ids(
    state: &AppState,
    index: &str,
    collection: &str,
    ids: &[String],
) -> Result<HashSet<String>, GatewayError> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let collection = if state.cfg.document_projection {
        document_collection(index)
    } else {
        collection.to_string()
    };
    let point_ids = ids.iter().map(|id| point_id(index, id)).collect::<Vec<_>>();
    let result = state
        .qdrant
        .request(
            Method::POST,
            &format!("/collections/{collection}/points"),
            Some(json!({"ids":point_ids,"with_payload":true,"with_vector":false})),
        )
        .await?;
    Ok(result
        .get("result")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|point| {
            point
                .get("payload")
                .and_then(|payload| payload.get("_es_id"))
                .and_then(Value::as_str)
                .map(String::from)
        })
        .collect())
}

async fn write_docs(
    State(state): State<AppState>,
    index: String,
    docs: Vec<(String, Value)>,
) -> Result<Response, GatewayError> {
    let (index, coll, mapping, vectors) = get_index(&state, &index)?;
    let points = docs
        .iter()
        .map(|(id, source)| {
            let texts = flatten_text(source, &vectors);
            let mut vector = Map::new();
            for (name, text) in texts {
                vector.insert(name, json!({"text":text,"model":"qdrant/bm25"}));
            }
            let payload = if state.cfg.document_projection {
                projected_search_payload(source, id, &index, &mapping)
            } else {
                qdrant_payload(source, id, &index, &mapping)
            };
            json!({"id":point_id(&index,id),"vector":vector,"payload":payload})
        })
        .collect::<Vec<_>>();
    if state.cfg.document_projection {
        let source_points = docs
            .iter()
            .map(|(id, source)| source_point(source, id, &index))
            .collect::<Vec<_>>();
        state
            .qdrant
            .request(
                Method::PUT,
                &format!("/collections/{}/points", document_collection(&index)),
                Some(json!({"points":source_points,"wait":true})),
            )
            .await?;
    }
    state
        .qdrant
        .request(
            Method::PUT,
            &format!("/collections/{coll}/points"),
            Some(json!({"points":points,"wait":!state.cfg.async_search_projection})),
        )
        .await?;
    let (id, _) = docs
        .first()
        .cloned()
        .unwrap_or_else(|| (String::new(), json!({})));
    Ok(es_ok(
        json!({"_index":index,"_id":id,"_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}),
    ))
}

async fn write_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Json(source): Json<Value>,
) -> Result<Response, GatewayError> {
    let (concrete_index, collection, _, _) = get_index(&state, &index)?;
    let existed = retrieve_source(&state, &concrete_index, &collection, &id)
        .await?
        .is_some();
    write_docs(
        State(state),
        concrete_index.clone(),
        vec![(id.clone(), source)],
    )
    .await?;

    let status = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let result = if existed { "updated" } else { "created" };
    Ok(es_response(
        status,
        json!({"_index":concrete_index,"_id":id,"_version":1,"result":result,"_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}),
    ))
}

async fn create_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Json(source): Json<Value>,
) -> Result<Response, GatewayError> {
    let index_admin = state.index_admin.clone();
    let _index_admin = index_admin.lock().await;
    let (index, coll, _, _) = get_index(&state, &index)?;
    if retrieve_source(&state, &index, &coll, &id).await?.is_some() {
        return Err(GatewayError::VersionConflict { index, id });
    }
    let mut response = write_docs(State(state), index, vec![(id, source)]).await?;
    *response.status_mut() = StatusCode::CREATED;
    Ok(response)
}

async fn get_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Response, GatewayError> {
    let (index, coll, _, _) = get_index(&state, &index)?;
    if state.cfg.document_projection {
        let sources = retrieve_sources(&state, &index, &[point_id(&index, &id)]).await?;
        return Ok(match sources.get(&id) {
            Some(source) => {
                es_ok(json!({"_index":index,"_id":id,"found":true,"_source":source,"_version":1}))
            }
            None => es_response(
                StatusCode::NOT_FOUND,
                json!({"_index":index,"_id":id,"found":false}),
            ),
        });
    }
    let p = state
        .qdrant
        .request(
            Method::GET,
            &format!(
                "/collections/{}/points/{}?with_payload=true",
                coll,
                point_id(&index, &id)
            ),
            None,
        )
        .await?;
    let result = p.get("result").cloned().unwrap_or(Value::Null);
    if result.is_null() {
        return Ok(es_response(
            StatusCode::NOT_FOUND,
            json!({"_index":index,"_id":id,"found":false}),
        ));
    }
    Ok(es_ok(
        json!({"_index":index,"_id":id,"found":true,"_source":result.get("payload").and_then(|p|p.get("_source")).cloned().unwrap_or_else(||json!({})),"_version":1}),
    ))
}
async fn head_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    let Ok((index, coll, _, _)) = get_index(&state, &index) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if state.cfg.document_projection {
        return match retrieve_sources(&state, &index, &[point_id(&index, &id)]).await {
            Ok(sources) if sources.contains_key(&id) => StatusCode::OK.into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        };
    }
    match state
        .qdrant
        .request(
            Method::GET,
            &format!(
                "/collections/{}/points/{}?with_payload=false",
                coll,
                point_id(&index, &id)
            ),
            None,
        )
        .await
    {
        Ok(result) if !result.get("result").is_some_and(Value::is_null) => {
            StatusCode::OK.into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn delete_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Response, GatewayError> {
    let (index, coll, _, _) = get_index(&state, &index)?;
    if retrieve_source(&state, &index, &coll, &id).await?.is_none() {
        return Ok(es_response(
            StatusCode::NOT_FOUND,
            json!({"_index":index,"_id":id,"_version":1,"result":"not_found","_shards":{"total":1,"successful":1,"failed":0}}),
        ));
    }
    let point = point_id(&index, &id);
    if state.cfg.document_projection {
        state
            .qdrant
            .request(
                Method::POST,
                &format!("/collections/{}/points/delete", document_collection(&index)),
                Some(json!({"points":[point],"wait":true})),
            )
            .await?;
    }
    let path = format!("/collections/{coll}/points/delete");
    let body = json!({"points":[point],"wait":!state.cfg.async_search_projection && !state.cfg.async_payload_writes});
    state
        .qdrant
        .request(Method::POST, &path, Some(body))
        .await?;
    Ok(es_ok(
        json!({"_index":index,"_id":id,"_version":1,"result":"deleted","_shards":{"total":1,"successful":1,"failed":0}}),
    ))
}

async fn update_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    if body.get("script").is_some() {
        return Err(GatewayError::bad(
            "body.script",
            "scripted updates are not supported; send a partial doc instead",
        ));
    }
    let doc = body
        .get("doc")
        .ok_or_else(|| GatewayError::bad("body.doc", "_update requires a doc object"))?;
    if !doc.is_object() {
        return Err(GatewayError::bad("body.doc", "doc must be an object"));
    }
    let upsert = body.get("upsert");
    if upsert.is_some_and(|source| !source.is_object()) {
        return Err(GatewayError::bad("body.upsert", "upsert must be an object"));
    }
    let detect_noop = match body.get("detect_noop") {
        None => true,
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return Err(GatewayError::bad(
                "body.detect_noop",
                "detect_noop must be a boolean",
            ))
        }
    };
    let (index, coll, mapping, vectors) = get_index(&state, &index)?;
    let current = retrieve_source(&state, &index, &coll, &id).await?;
    let Some(source) = current else {
        let create_source = if body
            .get("doc_as_upsert")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            Some(doc.clone())
        } else {
            upsert.cloned()
        };
        if let Some(create_source) = create_source {
            write_docs(
                State(state),
                index.clone(),
                vec![(id.clone(), create_source)],
            )
            .await?;
            return Ok(es_response(
                StatusCode::CREATED,
                json!({"_index":index,"_id":id,"_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}),
            ));
        }
        return Err(GatewayError::DocumentNotFound { index, id });
    };
    let mut merged = source;
    let changed = merge_partial_document(&mut merged, doc);
    if detect_noop && !changed {
        return Ok(es_ok(
            json!({"_index":index,"_id":id,"_version":1,"result":"noop","_shards":{"total":1,"successful":1,"failed":0}}),
        ));
    }
    if state.cfg.document_projection {
        let point = point_id(&index, &id);
        let changed_text = doc.as_object().unwrap().keys().any(|field| {
            field == "_all" || vectors.contains(&format!("text_{}", field.replace('.', "_")))
        });
        if changed_text {
            return write_doc(State(state), Path((index, id)), Json(merged)).await;
        }

        // Metadata-only updates can stay off the sparse index. Fields used by
        // filters/sorts are mirrored cheaply; unmapped fields only need the
        // authoritative document projection.
        let mapped_non_text = doc.as_object().unwrap().keys().any(|field| {
            mapping
                .get("properties")
                .and_then(Value::as_object)
                .and_then(|properties| properties.get(field))
                .and_then(|spec| spec.get("type"))
                .and_then(Value::as_str)
                .is_some_and(|kind| kind != "text")
        });
        state
            .qdrant
            .request(
                Method::PUT,
                &format!("/collections/{}/points", document_collection(&index)),
                Some(json!({"points":[source_point(&merged,&id,&index)],"wait":true})),
            )
            .await?;
        if mapped_non_text {
            let payload = projected_search_payload(&merged, &id, &index, &mapping);
            state
                .qdrant
                .request(
                    Method::POST,
                    &format!("/collections/{coll}/points/payload"),
                    Some(json!({"payload":payload,"points":[point],"wait":!state.cfg.async_search_projection})),
                )
                .await?;
        }
        return Ok(es_ok(
            json!({"_index":index,"_id":id,"_version":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}),
        ));
    }
    let changes_text = doc.as_object().unwrap().keys().any(|field| {
        field == "_all" || vectors.contains(&format!("text_{}", field.replace('.', "_")))
    });
    if !changes_text {
        let path = format!("/collections/{coll}/points/payload");
        let body = json!({
            "payload": qdrant_payload(&merged, &id, &index, &mapping),
            "points": [point_id(&index, &id)],
            "wait": !state.cfg.async_payload_writes
        });
        if state.cfg.async_payload_writes {
            state.qdrant.spawn_request(Method::POST, path, body)?;
        } else {
            state
                .qdrant
                .request(Method::POST, &path, Some(body))
                .await?;
        }
        return Ok(es_ok(
            json!({"_index":index,"_id":id,"_version":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}),
        ));
    }
    write_doc(State(state), Path((index, id)), Json(merged)).await
}

fn term_condition(field: &str, v: &Value) -> Value {
    let value = v.get("value").unwrap_or(v);
    json!({"key":compatibility_field(field),"match":{"value":value}})
}
type FilterResult = (Option<Value>, Vec<(String, String)>);

#[derive(Clone)]
enum GatewayPattern {
    Regex { field: String, pattern: String },
    Prefix { field: String, prefix: String },
    Wildcard { field: String, pattern: String },
}

fn collect_patterns(q: &Value, patterns: &mut Vec<GatewayPattern>) -> Result<(), GatewayError> {
    let object = q
        .as_object()
        .ok_or_else(|| GatewayError::bad("query", "query clause must be an object"))?;
    for (kind, value) in object {
        match kind.as_str() {
            "regexp" | "prefix" | "wildcard" => {
                let (field, raw) = value
                    .as_object()
                    .and_then(|o| o.iter().next())
                    .ok_or_else(|| GatewayError::bad(kind, "pattern query requires a field"))?;
                let pattern = raw
                    .as_str()
                    .or_else(|| raw.get("value").and_then(Value::as_str))
                    .ok_or_else(|| GatewayError::bad(kind, "pattern must be a string"))?;
                let pattern = match kind.as_str() {
                    "regexp" => GatewayPattern::Regex {
                        field: compatibility_field(field).into(),
                        pattern: pattern.into(),
                    },
                    "prefix" => GatewayPattern::Prefix {
                        field: compatibility_field(field).into(),
                        prefix: pattern.into(),
                    },
                    _ => GatewayPattern::Wildcard {
                        field: compatibility_field(field).into(),
                        pattern: pattern.into(),
                    },
                };
                if let GatewayPattern::Regex { pattern, .. } = &pattern {
                    Regex::new(pattern)
                        .map_err(|e| GatewayError::bad("query.regexp", e.to_string()))?;
                }
                patterns.push(pattern);
            }
            "bool" => {
                if let Some(bool_query) = value.as_object() {
                    for key in ["must", "filter"] {
                        if let Some(clauses) = bool_query.get(key).and_then(Value::as_array) {
                            for clause in clauses {
                                collect_patterns(clause, patterns)?;
                            }
                        } else if let Some(clause) = bool_query.get(key) {
                            collect_patterns(clause, patterns)?;
                        }
                    }
                    for key in ["should", "must_not"] {
                        let mut nested = Vec::new();
                        if let Some(clauses) = bool_query.get(key).and_then(Value::as_array) {
                            for clause in clauses {
                                collect_patterns(clause, &mut nested)?;
                            }
                        } else if let Some(clause) = bool_query.get(key) {
                            collect_patterns(clause, &mut nested)?;
                        }
                        if !nested.is_empty() {
                            return Err(GatewayError::bad(
                                format!("query.bool.{key}"),
                                "gateway-side pattern clauses are supported only in must/filter",
                            ));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn source_field<'a>(source: &'a Value, field: &str) -> Option<&'a Value> {
    field
        .split('.')
        .try_fold(source, |value, part| value.get(part))
}

fn compatibility_field(field: &str) -> &str {
    field.strip_suffix(".keyword").unwrap_or(field)
}

fn sortable_source_field<'a>(source: &'a Value, field: &str) -> Option<&'a Value> {
    source_field(source, compatibility_field(field))
}

fn source_value_exists(source: &Value, field: &str) -> bool {
    source_field(source, field).is_some_and(|value| match value {
        Value::Null => false,
        Value::Array(values) => values.iter().any(|value| !value.is_null()),
        _ => true,
    })
}

fn source_value_matches(value: &Value, predicate: &impl Fn(&Value) -> bool) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| source_value_matches(value, predicate)),
        value => predicate(value),
    }
}

fn single_filter_field<'a>(
    value: &'a Value,
    feature: &str,
) -> Result<(&'a str, &'a Value), GatewayError> {
    let fields = value
        .as_object()
        .ok_or_else(|| GatewayError::bad(feature, "filter body must be an object"))?;
    if fields.len() != 1 {
        return Err(GatewayError::bad(
            feature,
            "filter must contain exactly one field",
        ));
    }
    let (field, value) = fields.iter().next().expect("one filter field");
    if field.is_empty() {
        return Err(GatewayError::bad(feature, "filter field must not be empty"));
    }
    Ok((field, value))
}

fn validate_post_filter(query: &Value) -> Result<(), GatewayError> {
    fn validate(query: &Value, feature: &str) -> Result<(), GatewayError> {
        let clauses = query
            .as_object()
            .ok_or_else(|| GatewayError::bad(feature, "filter clause must be an object"))?;
        if clauses.len() != 1 {
            return Err(GatewayError::bad(
                feature,
                "filter clause must contain exactly one query type",
            ));
        }
        let (kind, value) = clauses.iter().next().expect("one filter clause");
        match kind.as_str() {
            "match_all" => {
                if !value.is_object() {
                    return Err(GatewayError::bad(
                        format!("{feature}.match_all"),
                        "match_all body must be an object",
                    ));
                }
            }
            "term" => {
                let (_, expected) = single_filter_field(value, &format!("{feature}.term"))?;
                if expected.is_array() || expected.is_null() {
                    return Err(GatewayError::bad(
                        format!("{feature}.term"),
                        "term value must be a scalar or an options object",
                    ));
                }
                if let Some(options) = expected.as_object() {
                    if options.get("value").is_none()
                        || options.keys().any(|key| key != "value" && key != "boost")
                        || options.get("boost").is_some_and(|boost| !boost.is_number())
                    {
                        return Err(GatewayError::bad(
                            format!("{feature}.term"),
                            "term options require value and support only a numeric boost",
                        ));
                    }
                }
            }
            "terms" => {
                let (_, values) = single_filter_field(value, &format!("{feature}.terms"))?;
                if !values.is_array() {
                    return Err(GatewayError::bad(
                        format!("{feature}.terms"),
                        "terms value must be an array",
                    ));
                }
            }
            "range" => {
                let (_, bounds) = single_filter_field(value, &format!("{feature}.range"))?;
                let bounds = bounds.as_object().ok_or_else(|| {
                    GatewayError::bad(format!("{feature}.range"), "range body must be an object")
                })?;
                for (operator, value) in bounds {
                    match operator.as_str() {
                        "gt" | "gte" | "lt" | "lte" | "from" | "to" if value.is_number() => {}
                        "include_lower" | "include_upper" if value.is_boolean() => {}
                        "boost" if value.is_number() => {}
                        "gt" | "gte" | "lt" | "lte" | "from" | "to" => {
                            return Err(GatewayError::bad(
                                format!("{feature}.range.{operator}"),
                                "post_filter range bounds must be numeric",
                            ));
                        }
                        "include_lower" | "include_upper" | "boost" => {
                            return Err(GatewayError::bad(
                                format!("{feature}.range.{operator}"),
                                "invalid range option value",
                            ));
                        }
                        _ => {
                            return Err(GatewayError::bad(
                                format!("{feature}.range.{operator}"),
                                "unsupported range operator",
                            ));
                        }
                    }
                }
            }
            "exists" => {
                let options = value.as_object().ok_or_else(|| {
                    GatewayError::bad(format!("{feature}.exists"), "exists body must be an object")
                })?;
                if options.len() != 1
                    || options
                        .get("field")
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                {
                    return Err(GatewayError::bad(
                        format!("{feature}.exists"),
                        "exists requires exactly one non-empty field",
                    ));
                }
            }
            "bool" => {
                let bool_query = value.as_object().ok_or_else(|| {
                    GatewayError::bad(format!("{feature}.bool"), "bool filter must be an object")
                })?;
                for (key, clauses) in bool_query {
                    match key.as_str() {
                        "must" | "filter" | "must_not" | "should" => {
                            if let Some(clauses) = clauses.as_array() {
                                for clause in clauses {
                                    validate(clause, &format!("{feature}.bool.{key}"))?;
                                }
                            } else {
                                validate(clauses, &format!("{feature}.bool.{key}"))?;
                            }
                        }
                        "minimum_should_match" => {
                            let supported = clauses.as_u64().is_some()
                                || clauses.as_str().is_some_and(|value| {
                                    value.parse::<u64>().is_ok()
                                        || value
                                            .strip_suffix('%')
                                            .and_then(|percent| percent.parse::<u64>().ok())
                                            .is_some_and(|percent| percent <= 100)
                                });
                            if !supported {
                                return Err(GatewayError::bad(
                                    format!("{feature}.bool.minimum_should_match"),
                                    "minimum_should_match must be a non-negative integer or percentage from 0% to 100%",
                                ));
                            }
                        }
                        _ => {
                            return Err(GatewayError::bad(
                                format!("{feature}.bool.{key}"),
                                "unsupported bool filter option",
                            ));
                        }
                    }
                }
            }
            _ => {
                return Err(GatewayError::bad(
                    format!("{feature}.{kind}"),
                    format!("{kind} is not supported in post_filter"),
                ));
            }
        }
        Ok(())
    }

    validate(query, "post_filter")
}

fn bool_clauses<'a>(bool_query: &'a Map<String, Value>, key: &str) -> Vec<&'a Value> {
    match bool_query.get(key) {
        Some(value) => value
            .as_array()
            .map(|values| values.iter().collect())
            .unwrap_or_else(|| vec![value]),
        None => Vec::new(),
    }
}

fn minimum_should_match(
    bool_query: &Map<String, Value>,
    should_len: usize,
    has_required_clause: bool,
) -> usize {
    if should_len == 0 {
        return 0;
    }
    let explicit = bool_query.get("minimum_should_match").and_then(|value| {
        value.as_u64().or_else(|| {
            value.as_str().and_then(|text| {
                text.parse::<u64>().ok().or_else(|| {
                    text.strip_suffix('%').and_then(|number| {
                        number
                            .parse::<u64>()
                            .ok()
                            .map(|percent| (should_len as u64 * percent).div_ceil(100))
                    })
                })
            })
        })
    });
    let default = if has_required_clause { 0 } else { 1 };
    explicit
        .unwrap_or(default)
        .max(if has_required_clause { 0 } else { 1 })
        .min(should_len as u64) as usize
}

fn wildcard_regex(pattern: &str) -> Result<Regex, GatewayError> {
    let mut expression = String::from("^");
    for character in pattern.chars() {
        match character {
            '*' => expression.push_str(".*"),
            '?' => expression.push('.'),
            literal => expression.push_str(&regex::escape(&literal.to_string())),
        }
    }
    expression.push('$');
    Regex::new(&expression).map_err(|e| GatewayError::Internal(e.to_string()))
}

fn pattern_matches(source: &Value, pattern: &GatewayPattern) -> bool {
    match pattern {
        GatewayPattern::Regex { field, pattern } => Regex::new(pattern).is_ok_and(|regex| {
            source_field(source, field).is_some_and(|value| {
                source_value_matches(value, &|value| {
                    value.as_str().is_some_and(|value| regex.is_match(value))
                })
            })
        }),
        GatewayPattern::Prefix { field, prefix } => {
            source_field(source, field).is_some_and(|value| {
                source_value_matches(value, &|value| {
                    value
                        .as_str()
                        .is_some_and(|value| value.starts_with(prefix))
                })
            })
        }
        GatewayPattern::Wildcard { field, pattern } => wildcard_regex(pattern).is_ok_and(|regex| {
            source_field(source, field).is_some_and(|value| {
                source_value_matches(value, &|value| {
                    value.as_str().is_some_and(|value| regex.is_match(value))
                })
            })
        }),
    }
}

fn source_matches_query(source: &Value, query: &Value) -> bool {
    let Some(object) = query.as_object() else {
        return false;
    };
    if object.contains_key("match_all") {
        return true;
    }
    if let Some(term) = object.get("term").and_then(Value::as_object) {
        return term.iter().next().is_some_and(|(field, value)| {
            let expected = value.get("value").unwrap_or(value);
            source_field(source, compatibility_field(field))
                .is_some_and(|actual| source_value_matches(actual, &|actual| actual == expected))
        });
    }
    if let Some(terms) = object.get("terms").and_then(Value::as_object) {
        return terms.iter().next().is_some_and(|(field, values)| {
            let Some(values) = values.as_array() else {
                return false;
            };
            source_field(source, compatibility_field(field)).is_some_and(|actual| {
                source_value_matches(actual, &|actual| values.iter().any(|value| value == actual))
            })
        });
    }
    if let Some(range) = object.get("range").and_then(Value::as_object) {
        return range.iter().next().is_some_and(|(field, bounds)| {
            let Some(actual) = source_field(source, compatibility_field(field)) else {
                return false;
            };
            bounds.as_object().is_some_and(|bounds| {
                source_value_matches(actual, &|actual| {
                    actual.as_f64().is_some_and(|actual| {
                        ["gt", "gte", "lt", "lte"].iter().all(|op| {
                            match bounds.get(*op).and_then(Value::as_f64) {
                                None => true,
                                Some(bound) => match *op {
                                    "gt" => actual > bound,
                                    "gte" => actual >= bound,
                                    "lt" => actual < bound,
                                    "lte" => actual <= bound,
                                    _ => true,
                                },
                            }
                        })
                    })
                })
            })
        });
    }
    if let Some(exists) = object.get("exists") {
        return exists
            .get("field")
            .and_then(Value::as_str)
            .is_some_and(|field| source_value_exists(source, compatibility_field(field)));
    }
    if let Some(bool_query) = object.get("bool").and_then(Value::as_object) {
        let has_required_clause = !bool_clauses(bool_query, "must").is_empty()
            || !bool_clauses(bool_query, "filter").is_empty();
        let must = bool_clauses(bool_query, "must")
            .into_iter()
            .chain(bool_clauses(bool_query, "filter"))
            .all(|clause| source_matches_query(source, clause));
        let must_not = bool_clauses(bool_query, "must_not")
            .into_iter()
            .all(|clause| !source_matches_query(source, clause));
        let should = bool_clauses(bool_query, "should");
        let minimum = minimum_should_match(bool_query, should.len(), has_required_clause);
        return must
            && must_not
            && should
                .iter()
                .filter(|clause| source_matches_query(source, clause))
                .count()
                >= minimum;
    }
    false
}

fn query_filter(q: &Value) -> Result<FilterResult, GatewayError> {
    let mut must = Vec::new();
    let mut must_not = Vec::new();
    let mut text = Vec::new();
    let mut sorts = Vec::new();
    fn walk(
        q: &Value,
        must: &mut Vec<Value>,
        must_not: &mut Vec<Value>,
        text: &mut Vec<Value>,
        _sorts: &mut Vec<(String, String)>,
    ) -> Result<(), GatewayError> {
        fn condition(mut must: Vec<Value>, must_not: Vec<Value>) -> Value {
            if must_not.is_empty() && must.len() == 1 {
                return must.remove(0);
            }
            if must.is_empty() && must_not.is_empty() {
                return json!({"must":[]});
            }
            let mut filter = Map::new();
            if !must.is_empty() {
                filter.insert("must".into(), Value::Array(must));
            }
            if !must_not.is_empty() {
                filter.insert("must_not".into(), Value::Array(must_not));
            }
            Value::Object(filter)
        }

        let o = q
            .as_object()
            .ok_or_else(|| GatewayError::bad("query", "query clause must be an object"))?;
        for (kind, v) in o {
            match kind.as_str() {
                "match_all" => {}
                "match" | "match_phrase" | "match_bool_prefix" => text.push(v.clone()),
                "multi_match" | "fuzzy" => text.push(v.clone()),
                "dis_max" => {
                    for clause in v
                        .get("queries")
                        .and_then(Value::as_array)
                        .ok_or_else(|| GatewayError::bad("dis_max", "queries must be an array"))?
                    {
                        walk(clause, must, must_not, text, _sorts)?;
                    }
                }
                "term" => {
                    let (f, val) = v
                        .as_object()
                        .and_then(|o| o.iter().next())
                        .ok_or_else(|| GatewayError::bad("term", "term requires a field"))?;
                    must.push(term_condition(f, val));
                }
                "terms" => {
                    let (f, val) = v
                        .as_object()
                        .and_then(|o| o.iter().next())
                        .ok_or_else(|| GatewayError::bad("terms", "terms requires a field"))?;
                    let arr = val.as_array().ok_or_else(|| {
                        GatewayError::bad("terms", "terms value must be an array")
                    })?;
                    must.push(json!({"key":compatibility_field(f),"match":{"any":arr}}));
                }
                "exists" => {
                    let f = v
                        .get("field")
                        .and_then(Value::as_str)
                        .ok_or_else(|| GatewayError::bad("exists", "exists requires field"))?;
                    must.push(json!({"must_not":[{"is_empty":{"key":compatibility_field(f)}}]}));
                }
                "range" => {
                    let (f, r) = v
                        .as_object()
                        .and_then(|o| o.iter().next())
                        .ok_or_else(|| GatewayError::bad("range", "range requires a field"))?;
                    let mut range = Map::new();
                    for (k, x) in r
                        .as_object()
                        .ok_or_else(|| GatewayError::bad("range", "range body must be an object"))?
                    {
                        let op = match k.as_str() {
                            "gt" => "gt",
                            "gte" => "gte",
                            "lt" => "lt",
                            "lte" => "lte",
                            "from" => {
                                if r.get("include_lower")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(true)
                                {
                                    "gte"
                                } else {
                                    "gt"
                                }
                            }
                            "to" => {
                                if r.get("include_upper")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(true)
                                {
                                    "lte"
                                } else {
                                    "lt"
                                }
                            }
                            "include_lower" | "include_upper" | "boost" => continue,
                            _ => {
                                return Err(GatewayError::bad(
                                    format!("range.{k}"),
                                    "unsupported range operator",
                                ))
                            }
                        };
                        range.insert(op.into(), x.clone());
                    }
                    must.push(json!({"key":compatibility_field(f),"range":range}));
                }
                "ids" => {
                    let vals = v
                        .get("values")
                        .and_then(Value::as_array)
                        .ok_or_else(|| GatewayError::bad("ids", "ids.values must be an array"))?;
                    must.push(json!({"key":"_es_id","match":{"any":vals}}));
                }
                "bool" => {
                    let b = v
                        .as_object()
                        .ok_or_else(|| GatewayError::bad("bool", "bool must be an object"))?;
                    for key in ["must", "filter"] {
                        if let Some(a) = b.get(key).and_then(Value::as_array) {
                            for c in a {
                                walk(c, must, must_not, text, _sorts)?
                            }
                        } else if let Some(c) = b.get(key) {
                            walk(c, must, must_not, text, _sorts)?
                        }
                    }
                    for clause in bool_clauses(b, "must_not") {
                        let mut cm = Vec::new();
                        let mut cn = Vec::new();
                        let mut ct = Vec::new();
                        walk(clause, &mut cm, &mut cn, &mut ct, _sorts)?;
                        if !ct.is_empty() {
                            return Err(GatewayError::bad(
                                "query.bool.must_not",
                                "text must_not is not supported safely",
                            ));
                        }
                        must_not.push(condition(cm, cn));
                    }
                    let clauses = bool_clauses(b, "should");
                    if !clauses.is_empty() {
                        let has_required_clause = !bool_clauses(b, "must").is_empty()
                            || !bool_clauses(b, "filter").is_empty();
                        let minimum = minimum_should_match(b, clauses.len(), has_required_clause);
                        let mut should = Vec::new();
                        for clause in clauses {
                            let mut cm = Vec::new();
                            let mut cn = Vec::new();
                            let mut ct = Vec::new();
                            walk(clause, &mut cm, &mut cn, &mut ct, _sorts)?;
                            if !ct.is_empty() {
                                return Err(GatewayError::bad(
                                    "query.bool.should",
                                    "text should clauses are not supported safely",
                                ));
                            }
                            should.push(condition(cm, cn));
                        }
                        if minimum > 0 {
                            must.push(
                                json!({"min_should":{"conditions":should,"min_count":minimum}}),
                            );
                        }
                    }
                }
                "prefix" => {
                    // Evaluated against hydrated source documents by the gateway.
                }
                "regexp" | "wildcard" => {
                    // Evaluated against hydrated source documents by the gateway.
                }
                other => {
                    return Err(GatewayError::bad(
                        format!("query.{other}"),
                        format!("{other} queries are not supported"),
                    ))
                }
            }
        }
        Ok(())
    }
    walk(q, &mut must, &mut must_not, &mut text, &mut sorts)?;
    let mut all = must;
    for x in must_not {
        all.push(json!({"must_not":[x]}));
    }
    Ok((
        if all.is_empty() {
            None
        } else {
            Some(json!({"must":all}))
        },
        sorts,
    ))
}

fn pick_text(q: &Value) -> Option<(String, Vec<(String, f32)>)> {
    if let Some(m) = q.get("match") {
        let (f, v) = m.as_object()?.iter().next()?;
        return Some((
            v.as_str()
                .or_else(|| v.get("query").and_then(Value::as_str))?
                .into(),
            vec![(f.clone(), 1.0)],
        ));
    }
    if let Some(m) = q.get("match_phrase") {
        let (f, v) = m.as_object()?.iter().next()?;
        return Some((
            v.as_str()
                .or_else(|| v.get("query").and_then(Value::as_str))?
                .into(),
            vec![(f.clone(), 1.0)],
        ));
    }
    if let Some(m) = q.get("match_bool_prefix") {
        let (f, v) = m.as_object()?.iter().next()?;
        return Some((
            v.as_str()
                .or_else(|| v.get("query").and_then(Value::as_str))?
                .into(),
            vec![(
                f.clone(),
                v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32,
            )],
        ));
    }
    if let Some(m) = q.get("fuzzy") {
        let (f, v) = m.as_object()?.iter().next()?;
        return Some((
            v.as_str()
                .or_else(|| v.get("value").and_then(Value::as_str))?
                .into(),
            vec![(
                f.clone(),
                v.get("boost").and_then(Value::as_f64).unwrap_or(1.0) as f32,
            )],
        ));
    }
    if let Some(m) = q.get("multi_match") {
        let text = m.get("query")?.as_str()?.into();
        let fs = m
            .get("fields")?
            .as_array()?
            .iter()
            .filter_map(|v| {
                let s = v.as_str()?;
                let mut p = s.split('^');
                Some((
                    p.next()?.into(),
                    p.next().and_then(|x| x.parse().ok()).unwrap_or(1.0),
                ))
            })
            .collect();
        return Some((text, fs));
    }
    if let Some(d) = q.get("dis_max") {
        let mut combined = None;
        for clause in d.get("queries")?.as_array()? {
            if let Some((text, fields)) = pick_text(clause) {
                if combined.is_none() {
                    combined = Some((text, Vec::new()));
                }
                combined.as_mut()?.1.extend(fields);
            }
        }
        return combined;
    }
    if let Some(b) = q.get("bool").and_then(Value::as_object) {
        for key in ["must", "filter"] {
            if let Some(clauses) = b.get(key).and_then(Value::as_array) {
                for clause in clauses {
                    if let Some(found) = pick_text(clause) {
                        return Some(found);
                    }
                }
            } else if let Some(clause) = b.get(key) {
                if let Some(found) = pick_text(clause) {
                    return Some(found);
                }
            }
        }
    }
    None
}

async fn expand_more_like_this(
    state: &AppState,
    index: &str,
    query: Value,
) -> Result<Value, GatewayError> {
    let Some(mlt) = query.get("more_like_this") else {
        return Ok(query);
    };
    let id = mlt
        .get("like")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            GatewayError::bad(
                "more_like_this.like",
                "expected a document reference with _id",
            )
        })?;
    let fields = mlt
        .get("fields")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let (index, coll, _, _) = get_index(state, index)?;
    let source = if state.cfg.document_projection {
        retrieve_sources(state, &index, &[point_id(&index, id)])
            .await?
            .remove(id)
            .unwrap_or_else(|| json!({}))
    } else {
        let point = state
            .qdrant
            .request(
                Method::GET,
                &format!(
                    "/collections/{}/points/{}?with_payload=true",
                    coll,
                    point_id(&index, id)
                ),
                None,
            )
            .await?;
        point
            .get("result")
            .and_then(|result| result.get("payload"))
            .and_then(|payload| payload.get("_source"))
            .cloned()
            .unwrap_or_else(|| json!({}))
    };
    let text = fields
        .iter()
        .filter_map(|field| source_field(&source, field).and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    Ok(json!({"multi_match":{"query":text,"fields":fields}}))
}

fn project_source(source: &Value, spec: Option<&Value>) -> Option<Value> {
    let Some(spec) = spec else {
        return Some(source.clone());
    };
    if spec.as_bool() == Some(false) {
        return None;
    }
    let object = source.as_object()?;
    let (includes, excludes) = if let Some(fields) = spec.as_array() {
        (
            fields.iter().filter_map(Value::as_str).collect::<Vec<_>>(),
            Vec::new(),
        )
    } else {
        (
            spec.get("includes")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default(),
            spec.get("excludes")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default(),
        )
    };
    if includes.is_empty() && excludes.is_empty() {
        return Some(source.clone());
    }
    let mut result = Map::new();
    for (key, value) in object {
        let included = includes.is_empty()
            || includes.iter().any(|field| {
                *field == key
                    || (*field).ends_with('*') && key.starts_with(&field[..field.len() - 1])
            });
        let excluded = excludes.iter().any(|field| {
            *field == key || (*field).ends_with('*') && key.starts_with(&field[..field.len() - 1])
        });
        if included && !excluded {
            result.insert(key.clone(), value.clone());
        }
    }
    Some(Value::Object(result))
}

fn apply_source_projection(hit: &mut Value, spec: Option<&Value>) {
    let current = hit.get("_source").cloned().unwrap_or_else(|| json!({}));
    match project_source(&current, spec) {
        Some(projected) => hit["_source"] = projected,
        None => {
            hit.as_object_mut()
                .expect("search hit is an object")
                .remove("_source");
        }
    }
}

fn words(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(|word| word.to_lowercase())
        .collect()
}

fn field_text(source: &Value, field: &str) -> String {
    source_field(source, compatibility_field(field))
        .map(|value| match value {
            Value::String(text) => text.clone(),
            Value::Array(values) => values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
            _ => value.to_string(),
        })
        .unwrap_or_default()
        .to_lowercase()
}

fn edit_distance(left: &str, right: &str) -> usize {
    let mut row: Vec<usize> = (0..=right.chars().count()).collect();
    for (i, left_char) in left.chars().enumerate() {
        let mut next = vec![i + 1];
        for (j, right_char) in right.chars().enumerate() {
            next.push(if left_char == right_char {
                row[j]
            } else {
                1 + row[j].min(row[j + 1]).min(next[j])
            });
        }
        row = next;
    }
    row[right.chars().count()]
}

fn fuzzy_matches(text: &str, query: &str) -> bool {
    let candidates = words(text);
    words(query).into_iter().all(|term| {
        let maximum = if term.chars().count() <= 2 {
            0
        } else if term.chars().count() <= 5 {
            1
        } else {
            2
        };
        candidates
            .iter()
            .any(|candidate| edit_distance(&term, candidate) <= maximum)
    })
}

fn text_query_matches(source: &Value, query: &Value) -> bool {
    let Some(object) = query.as_object() else {
        return false;
    };
    if object.contains_key("match_all") {
        return true;
    }
    if let Some(match_query) = object.get("match") {
        return match_query
            .as_object()
            .and_then(|items| items.iter().next())
            .is_some_and(|(field, value)| {
                let query = value
                    .as_str()
                    .or_else(|| value.get("query").and_then(Value::as_str))
                    .unwrap_or("");
                words(query).iter().all(|term| {
                    field_text(source, field)
                        .split_whitespace()
                        .any(|candidate| candidate == term)
                })
            });
    }
    if let Some(prefix_query) = object.get("match_bool_prefix") {
        return prefix_query
            .as_object()
            .and_then(|items| items.iter().next())
            .is_some_and(|(field, value)| {
                let query = value
                    .as_str()
                    .or_else(|| value.get("query").and_then(Value::as_str))
                    .unwrap_or("");
                let terms = words(query);
                let candidates = field_text(source, field)
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                terms.last().is_some_and(|last| {
                    terms[..terms.len().saturating_sub(1)]
                        .iter()
                        .all(|term| candidates.iter().any(|candidate| candidate == term))
                        && candidates
                            .iter()
                            .any(|candidate| candidate.starts_with(last))
                })
            });
    }
    if let Some(fuzzy_query) = object.get("fuzzy") {
        return fuzzy_query
            .as_object()
            .and_then(|items| items.iter().next())
            .is_some_and(|(field, value)| {
                let query = value
                    .as_str()
                    .or_else(|| value.get("value").and_then(Value::as_str))
                    .unwrap_or("");
                fuzzy_matches(&field_text(source, field), query)
            });
    }
    if let Some(multi_match) = object.get("multi_match") {
        let query = multi_match
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("");
        return multi_match
            .get("fields")
            .and_then(Value::as_array)
            .is_some_and(|fields| {
                fields.iter().filter_map(Value::as_str).any(|field| {
                    let text = field_text(source, field);
                    words(query)
                        .iter()
                        .all(|term| text.split_whitespace().any(|candidate| candidate == term))
                })
            });
    }
    if let Some(dis_max) = object.get("dis_max") {
        return dis_max
            .get("queries")
            .and_then(Value::as_array)
            .is_some_and(|queries| {
                queries
                    .iter()
                    .any(|query| text_query_matches(source, query))
            });
    }
    if let Some(bool_query) = object.get("bool").and_then(Value::as_object) {
        let has_required_clause = !bool_clauses(bool_query, "must").is_empty()
            || !bool_clauses(bool_query, "filter").is_empty();
        let must = bool_clauses(bool_query, "must")
            .into_iter()
            .chain(bool_clauses(bool_query, "filter"))
            .all(|clause| text_query_matches(source, clause));
        let must_not = bool_clauses(bool_query, "must_not")
            .into_iter()
            .all(|clause| !text_query_matches(source, clause));
        let should = bool_clauses(bool_query, "should");
        let minimum = minimum_should_match(bool_query, should.len(), has_required_clause);
        return must
            && must_not
            && should
                .iter()
                .filter(|clause| text_query_matches(source, clause))
                .count()
                >= minimum;
    }
    false
}

fn contains_approx_text_query(query: &Value) -> bool {
    let Some(object) = query.as_object() else {
        return false;
    };
    if object.contains_key("match_bool_prefix") || object.contains_key("fuzzy") {
        return true;
    }
    object.values().any(contains_approx_text_query)
}

fn value_cmp(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
        _ => left
            .as_str()
            .unwrap_or(&left.to_string())
            .cmp(right.as_str().unwrap_or(&right.to_string())),
    }
}

fn parse_sort_specs(sort: Option<&Value>) -> Result<Vec<(String, String)>, GatewayError> {
    let Some(sort) = sort else {
        return Ok(Vec::new());
    };
    let clauses = match sort {
        Value::Array(clauses) => clauses.iter().collect::<Vec<_>>(),
        clause => vec![clause],
    };
    clauses
        .into_iter()
        .map(|clause| {
            let (field, options) = match clause {
                Value::String(field) if !field.is_empty() => (field.clone(), None),
                Value::Object(object) if object.len() == 1 => {
                    let (field, options) = object.iter().next().expect("one sort field");
                    if field.is_empty() {
                        return Err(GatewayError::bad("sort", "sort field must not be empty"));
                    }
                    (field.clone(), Some(options))
                }
                _ => {
                    return Err(GatewayError::bad(
                        "sort",
                        "each sort clause must be a field name or a single-field object",
                    ))
                }
            };
            let default_order = if field == "_score" { "desc" } else { "asc" };
            let order = match options {
                None => default_order,
                Some(Value::String(order)) => order,
                Some(Value::Object(options)) => {
                    if options.keys().any(|key| key != "order") {
                        return Err(GatewayError::bad(
                            "sort",
                            "only the basic sort order option is supported",
                        ));
                    }
                    options
                        .get("order")
                        .map(|order| {
                            order.as_str().ok_or_else(|| {
                                GatewayError::bad("sort", "sort order must be asc or desc")
                            })
                        })
                        .transpose()?
                        .unwrap_or(default_order)
                }
                _ => return Err(GatewayError::bad("sort", "sort order must be asc or desc")),
            };
            if order != "asc" && order != "desc" {
                return Err(GatewayError::bad("sort", "sort order must be asc or desc"));
            }
            Ok((field, order.to_string()))
        })
        .collect()
}

fn parse_search_window(body: &Value, max_page_size: u64) -> Result<(u64, u64), GatewayError> {
    fn non_negative_integer(
        body: &Value,
        field: &'static str,
        default: u64,
    ) -> Result<u64, GatewayError> {
        match body.get(field) {
            None => Ok(default),
            Some(value) => value.as_u64().ok_or_else(|| {
                GatewayError::bad(field, format!("{field} must be a non-negative integer"))
            }),
        }
    }

    let from = non_negative_integer(body, "from", 0)?;
    let size = non_negative_integer(body, "size", 10_u64.min(max_page_size))?;
    if size > max_page_size {
        return Err(GatewayError::bad(
            "size",
            format!("size is capped at {max_page_size} by MAX_PAGE_SIZE"),
        ));
    }
    if from.checked_add(size).is_none_or(|window| window > 10_000) {
        return Err(GatewayError::bad(
            "from",
            "from + size is capped at 10000; use search_after for later pages",
        ));
    }
    Ok((from, size))
}

fn validate_search_after(
    body: &Value,
    sort_specs: &[(String, String)],
    from: u64,
) -> Result<(), GatewayError> {
    let Some(cursor) = body.get("search_after") else {
        return Ok(());
    };
    let cursor = cursor.as_array().ok_or_else(|| {
        GatewayError::bad(
            "search_after",
            "search_after must be an array of sort values",
        )
    })?;
    if sort_specs.is_empty() || cursor.len() != sort_specs.len() {
        return Err(GatewayError::bad(
            "search_after",
            "search_after must contain one value for every sort field",
        ));
    }
    if from != 0 {
        return Err(GatewayError::bad(
            "search_after",
            "search_after cannot be combined with a non-zero from offset",
        ));
    }
    if cursor
        .iter()
        .any(|value| value.is_array() || value.is_object())
    {
        return Err(GatewayError::bad(
            "search_after",
            "search_after values must be strings, numbers, booleans, or null",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrackTotalHits {
    Enabled,
    Disabled,
    Threshold(u64),
}

fn parse_track_total_hits(body: &Value) -> Result<TrackTotalHits, GatewayError> {
    match body.get("track_total_hits") {
        None | Some(Value::Bool(true)) => Ok(TrackTotalHits::Enabled),
        Some(Value::Bool(false)) => Ok(TrackTotalHits::Disabled),
        Some(value) => value
            .as_u64()
            .map(TrackTotalHits::Threshold)
            .ok_or_else(|| {
                GatewayError::bad(
                    "track_total_hits",
                    "track_total_hits must be true, false, or a non-negative integer",
                )
            }),
    }
}

fn total_hits_value(mode: TrackTotalHits, total: u64, exact: bool) -> Option<Value> {
    match mode {
        TrackTotalHits::Disabled => None,
        TrackTotalHits::Enabled => Some(json!({
            "value": total,
            "relation": if exact { "eq" } else { "gte" },
        })),
        TrackTotalHits::Threshold(threshold) => Some(json!({
            "value": total.min(threshold),
            "relation": if exact && total <= threshold { "eq" } else { "gte" },
        })),
    }
}

#[derive(Clone)]
struct InnerHitsSpec {
    name: String,
    size: usize,
    source: Option<Value>,
}

#[derive(Clone)]
struct CollapseSpec {
    field: String,
    inner_hits: Option<InnerHitsSpec>,
}

fn parse_collapse_spec(
    collapse: Option<&Value>,
    max_page_size: u64,
) -> Result<Option<CollapseSpec>, GatewayError> {
    let Some(collapse) = collapse else {
        return Ok(None);
    };
    let collapse = collapse
        .as_object()
        .ok_or_else(|| GatewayError::bad("collapse", "collapse must be an object"))?;
    if let Some(option) = collapse
        .keys()
        .find(|key| *key != "field" && *key != "inner_hits")
    {
        return Err(GatewayError::bad(
            format!("collapse.{option}"),
            "unsupported collapse option",
        ));
    }
    let requested_field = collapse
        .get("field")
        .and_then(Value::as_str)
        .filter(|field| !field.is_empty())
        .ok_or_else(|| GatewayError::bad("collapse.field", "collapse requires a field"))?;
    let field = compatibility_field(requested_field).to_string();
    let inner_hits = collapse
        .get("inner_hits")
        .map(|inner_hits| {
            let inner_hits = inner_hits.as_object().ok_or_else(|| {
                GatewayError::bad("collapse.inner_hits", "inner_hits must be an object")
            })?;
            if let Some(option) = inner_hits
                .keys()
                .find(|key| !matches!(key.as_str(), "name" | "size" | "fields" | "_source"))
            {
                return Err(GatewayError::bad(
                    format!("collapse.inner_hits.{option}"),
                    "unsupported inner_hits option",
                ));
            }
            let name = match inner_hits.get("name") {
                None => requested_field.to_string(),
                Some(name) => name
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| {
                        GatewayError::bad(
                            "collapse.inner_hits.name",
                            "inner_hits name must be a non-empty string",
                        )
                    })?
                    .to_string(),
            };
            let size = match inner_hits.get("size") {
                None => 3,
                Some(size) => size.as_u64().ok_or_else(|| {
                    GatewayError::bad(
                        "collapse.inner_hits.size",
                        "inner_hits size must be a non-negative integer",
                    )
                })?,
            };
            if size > max_page_size {
                return Err(GatewayError::bad(
                    "collapse.inner_hits.size",
                    format!("inner_hits size is capped at {max_page_size} by MAX_PAGE_SIZE"),
                ));
            }
            if let Some(fields) = inner_hits.get("fields") {
                let valid = fields
                    .as_array()
                    .is_some_and(|fields| fields.iter().all(|field| field.as_str() == Some("_id")));
                if !valid {
                    return Err(GatewayError::bad(
                        "collapse.inner_hits.fields",
                        "only the _id metadata field is supported in inner_hits",
                    ));
                }
            }
            Ok(InnerHitsSpec {
                name,
                size: size as usize,
                source: inner_hits.get("_source").cloned(),
            })
        })
        .transpose()?;
    Ok(Some(CollapseSpec { field, inner_hits }))
}

fn hit_sort_value(hit: &Value, field: &str) -> Value {
    match field {
        "_score" => hit.get("_score").cloned().unwrap_or(Value::Null),
        "_id" => hit.get("_id").cloned().unwrap_or(Value::Null),
        _ => hit
            .get("_source")
            .and_then(|source| sortable_source_field(source, field))
            .cloned()
            .unwrap_or(Value::Null),
    }
}

async fn search(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    if let Some(post_filter) = body.get("post_filter") {
        validate_post_filter(post_filter)?;
    }
    let (from, size) = parse_search_window(&body, state.cfg.max_page_size)?;
    let sort_specs = parse_sort_specs(body.get("sort"))?;
    validate_search_after(&body, &sort_specs, from)?;
    let track_total_hits = parse_track_total_hits(&body)?;
    // Field sorting happens after Qdrant returns candidates. Fetching only the
    // requested page makes both the first sorted page and every search_after
    // page depend on Qdrant's unrelated point order. Keep that local ordering
    // bounded by the operator's existing page-size limit.
    let candidate_limit = if sort_specs.is_empty() || size == 0 {
        (from + size).max(1)
    } else {
        (from + size).max(state.cfg.max_page_size).max(1)
    };
    let collapse_spec = parse_collapse_spec(body.get("collapse"), state.cfg.max_page_size)?;
    let (index, coll, _, vectors) = get_index(&state, &index)?;
    let mut query = body
        .get("query")
        .cloned()
        .unwrap_or_else(|| json!({"match_all":{}}));
    query = expand_more_like_this(&state, &index, query).await?;
    let mut patterns = Vec::new();
    collect_patterns(&query, &mut patterns)?;
    let (filter, _) = query_filter(&query)?;
    let text = pick_text(&query);
    let has_text = text.is_some();
    let approximate_text = contains_approx_text_query(&query);
    let exact_total = if track_total_hits != TrackTotalHits::Disabled
        && !has_text
        && patterns.is_empty()
        && body.get("post_filter").is_none()
    {
        Some(qdrant_exact_count(&state, &coll, filter.as_ref()).await?)
    } else {
        None
    };
    let mut hits: Vec<Value> = Vec::new();
    if approximate_text {
        let scan_collection = if state.cfg.document_projection {
            document_collection(&index)
        } else {
            coll.clone()
        };
        let mut offset = Value::Null;
        loop {
            let mut request = json!({"limit":state.cfg.max_page_size,"with_payload":{"include":["_es_id","_source"]}});
            if let Some(filter) = &filter {
                request["filter"] = filter.clone();
            }
            if !offset.is_null() {
                request["offset"] = offset.clone();
            }
            let response = state
                .qdrant
                .request(
                    Method::POST,
                    &format!("/collections/{scan_collection}/points/scroll"),
                    Some(request),
                )
                .await?;
            if let Some(points) = response
                .get("result")
                .and_then(|result| result.get("points"))
                .and_then(Value::as_array)
            {
                for point in points {
                    let payload = point.get("payload").cloned().unwrap_or_else(|| json!({}));
                    let source = payload.get("_source").cloned().unwrap_or_else(|| json!({}));
                    if text_query_matches(&source, &query) {
                        hits.push(json!({"_index":index,"_id":payload.get("_es_id").cloned().unwrap_or_else(|| json!("")),"_score":1.0,"_source":source}));
                    }
                }
            }
            let next = response
                .get("result")
                .and_then(|result| result.get("next_page_offset"))
                .cloned()
                .unwrap_or(Value::Null);
            if next.is_null() {
                break;
            }
            offset = next;
        }
    } else if let Some((text, fields)) = text {
        let requests = fields
            .into_iter()
            .filter_map(|(field, boost)| {
            let vecname = if field == "_all" {
                "text_all".into()
            } else {
                format!("text_{}", field.replace('.', "_"))
            };
            if !vectors.contains(&vecname) && vecname != "text_all" {
                return None;
            }
            let payload_fields = if state.cfg.document_projection && patterns.is_empty() { json!(["_es_id"]) } else { json!(["_es_id","_source"]) };
            let mut req = json!({"query":{"text":text,"model":"qdrant/bm25"},"using":vecname,"limit":candidate_limit,"with_payload":{"include":payload_fields}});
            if let Some(f) = &filter {
                req["filter"] = f.clone();
            }
            let qdrant = state.qdrant.clone();
            let path = format!("/collections/{coll}/points/query");
            Some(async move { Ok::<_, GatewayError>((qdrant.request(Method::POST, &path, Some(req)).await?, boost)) })
        })
        .collect::<Vec<_>>();
        for (r, boost) in try_join_all(requests).await? {
            if let Some(arr) = r
                .get("result")
                .and_then(|x| x.get("points"))
                .and_then(Value::as_array)
            {
                for p in arr {
                    let payload = p.get("payload").cloned().unwrap_or(json!({}));
                    hits.push(json!({"_index":index,"_id":payload.get("_es_id").cloned().unwrap_or(json!("")),"_score":p.get("score").and_then(Value::as_f64).unwrap_or(0.0)*(boost as f64),"_source":payload.get("_source").cloned().unwrap_or(json!({}))}));
                }
            }
        }
    } else {
        let payload_fields = if state.cfg.document_projection && patterns.is_empty() {
            json!(["_es_id"])
        } else {
            json!(["_es_id", "_source"])
        };
        let mut offset = Value::Null;
        loop {
            // Qdrant requires a positive scroll limit, while Elasticsearch
            // commonly uses size: 0 for aggregation-only searches. Fetch one
            // candidate for bookkeeping and keep the requested page empty.
            let mut req = json!({"limit":if patterns.is_empty() { candidate_limit } else { state.cfg.max_page_size },"with_payload":{"include":payload_fields}});
            if let Some(f) = &filter {
                req["filter"] = f.clone();
            }
            if !offset.is_null() {
                req["offset"] = offset.clone();
            }
            let r = state
                .qdrant
                .request(
                    Method::POST,
                    &format!("/collections/{coll}/points/scroll"),
                    Some(req),
                )
                .await?;
            if let Some(arr) = r
                .get("result")
                .and_then(|x| x.get("points"))
                .and_then(Value::as_array)
            {
                for p in arr {
                    let pay = p.get("payload").cloned().unwrap_or(json!({}));
                    let source = pay.get("_source").cloned().unwrap_or(json!({}));
                    if patterns
                        .iter()
                        .all(|pattern| pattern_matches(&source, pattern))
                    {
                        hits.push(json!({"_index":index,"_id":pay.get("_es_id").cloned().unwrap_or(json!("")),"_score":Value::Null,"_source":source}));
                    }
                }
                let next = r
                    .get("result")
                    .and_then(|x| x.get("next_page_offset"))
                    .cloned()
                    .unwrap_or(Value::Null);
                if next.is_null() || patterns.is_empty() {
                    break;
                }
                offset = next;
            } else {
                break;
            }
        }
    }
    let mut unique = HashMap::new();
    for h in hits {
        let id = h
            .get("_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match unique.entry(id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(h);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let new_score = h.get("_score").and_then(Value::as_f64).unwrap_or(0.0);
                let old_score = entry
                    .get()
                    .get("_score")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                if new_score > old_score {
                    entry.insert(h);
                }
            }
        }
    }
    let mut hits: Vec<_> = unique.into_values().collect();
    if state.cfg.document_projection {
        let ids = hits
            .iter()
            .filter_map(|hit| {
                hit.get("_id")
                    .and_then(Value::as_str)
                    .map(|id| point_id(&index, id))
            })
            .collect::<Vec<_>>();
        let sources = retrieve_sources(&state, &index, &ids).await?;
        hits.retain_mut(|hit| {
            let id = hit.get("_id").and_then(Value::as_str).unwrap_or("");
            if let Some(source) = sources.get(id) {
                hit["_source"] = source.clone();
                true
            } else {
                false
            }
        });
    }
    if !patterns.is_empty() {
        hits.retain(|hit| {
            let source = hit.get("_source").cloned().unwrap_or_else(|| json!({}));
            patterns
                .iter()
                .all(|pattern| pattern_matches(&source, pattern))
        });
    }
    // Elasticsearch applies post_filter, collapse, and search_after only to
    // hits. Preserve the query-matched candidate window for gateway-computed
    // aggregations before those hit-only transformations run.
    let aggregation_hits = body
        .get("aggs")
        .or_else(|| body.get("aggregations"))
        .and_then(Value::as_object)
        .filter(|aggregations| {
            aggregations
                .values()
                .any(|aggregation| aggregation.get("terms").is_none())
        })
        .map(|_| hits.clone());
    if let Some(post_filter) = body.get("post_filter") {
        hits.retain(|hit| {
            hit.get("_source")
                .is_some_and(|source| source_matches_query(source, post_filter))
        });
    }
    let total = exact_total.unwrap_or(hits.len() as u64);
    let total_is_exact = exact_total.is_some() || approximate_text || !patterns.is_empty();
    if !sort_specs.is_empty() {
        for (field, dir) in sort_specs.iter().rev() {
            hits.sort_by(|a, b| {
                let av = hit_sort_value(a, field);
                let bv = hit_sort_value(b, field);
                let ord = value_cmp(&av, &bv);
                if dir == "desc" {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }
    } else if has_text {
        hits.sort_by(|a, b| {
            let ascore = a.get("_score").and_then(Value::as_f64).unwrap_or(0.0);
            let bscore = b.get("_score").and_then(Value::as_f64).unwrap_or(0.0);
            bscore
                .partial_cmp(&ascore)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    if let Some(collapse) = &collapse_spec {
        let mut groups: Vec<(String, Vec<Value>)> = Vec::new();
        for hit in hits {
            let key = hit
                .get("_source")
                .and_then(|source| source_field(source, &collapse.field))
                .map(ToString::to_string)
                .unwrap_or_else(|| "null".into());
            if let Some((_, group)) = groups.iter_mut().find(|(group_key, _)| group_key == &key) {
                group.push(hit);
            } else {
                groups.push((key, vec![hit]));
            }
        }
        hits = groups
            .into_iter()
            .map(|(_, group)| {
                let mut primary = group[0].clone();
                if let Some(inner) = &collapse.inner_hits {
                    let mut selected = group
                        .iter()
                        .take(inner.size)
                        .cloned()
                        .collect::<Vec<_>>();
                    for hit in &mut selected {
                        apply_source_projection(hit, inner.source.as_ref());
                    }
                    primary["inner_hits"][&inner.name] = json!({"hits":{"total":{"value":group.len(),"relation":"eq"},"max_score":selected.iter().filter_map(|hit| hit.get("_score").and_then(Value::as_f64)).fold(0.0,f64::max),"hits":selected}});
                }
                primary
            })
            .collect();
    }
    if let Some(cursor) = body.get("search_after").and_then(Value::as_array) {
        let mut after = false;
        hits.retain(|hit| {
            if after {
                return true;
            }
            for ((field, direction), cursor_value) in sort_specs.iter().zip(cursor) {
                let hit_value = hit_sort_value(hit, field);
                let ordering = value_cmp(&hit_value, cursor_value);
                if ordering == std::cmp::Ordering::Equal {
                    continue;
                }
                let is_after = if direction == "desc" {
                    ordering == std::cmp::Ordering::Less
                } else {
                    ordering == std::cmp::Ordering::Greater
                };
                after = is_after;
                return is_after;
            }
            false
        });
    }
    let page = hits
        .clone()
        .into_iter()
        .skip(from as usize)
        .take(size as usize)
        .collect::<Vec<_>>();
    let source = body.get("_source");
    let page = page
        .into_iter()
        .map(|mut h| {
            if !sort_specs.is_empty() {
                let values = sort_specs
                    .iter()
                    .map(|(field, _)| hit_sort_value(&h, field))
                    .collect::<Vec<_>>();
                h["sort"] = Value::Array(values);
            }
            apply_source_projection(&mut h, source);
            h
        })
        .collect::<Vec<_>>();
    let mut response = json!({"took":started.elapsed().as_millis(),"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"max_score":page.iter().filter_map(|h|h.get("_score").and_then(Value::as_f64)).fold(0.0,f64::max),"hits":page}});
    if let Some(total) = total_hits_value(track_total_hits, total, total_is_exact) {
        response["hits"]["total"] = total;
    }
    if let Some(aggs) = body.get("aggs").or_else(|| body.get("aggregations")) {
        let output = response.as_object_mut().unwrap();
        let mut agg_result = Map::new();
        for (name, spec) in aggs
            .as_object()
            .ok_or_else(|| GatewayError::bad("aggs", "aggregations must be an object"))?
        {
            if let Some(terms) = spec.get("terms") {
                let requested = terms.get("field").and_then(Value::as_str).ok_or_else(|| {
                    GatewayError::bad(
                        format!("aggs.{name}.terms.field"),
                        "terms aggregation requires field",
                    )
                })?;
                let field = requested.strip_suffix(".keyword").unwrap_or(requested);
                let limit = terms
                    .get("size")
                    .and_then(Value::as_u64)
                    .unwrap_or(10)
                    .min(1000);
                let mut req = json!({"key":field,"limit":limit});
                if let Some(f) = query_filter(&query)?.0 {
                    req["filter"] = f;
                }
                let facet = state
                    .qdrant
                    .request(
                        Method::POST,
                        &format!("/collections/{coll}/facet"),
                        Some(req),
                    )
                    .await?;
                let buckets = facet.get("result").and_then(|r| r.get("hits")).and_then(Value::as_array).cloned().unwrap_or_default().into_iter().map(|h| json!({"key":h.get("value").cloned().unwrap_or(Value::Null),"doc_count":h.get("count").cloned().unwrap_or(json!(0))})).collect::<Vec<_>>();
                agg_result.insert(name.clone(), json!({"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":buckets}));
            } else if let Some(metric) = spec.get("min").or_else(|| spec.get("max")) {
                let aggregation_hits = aggregation_hits
                    .as_ref()
                    .expect("gateway-computed aggregations retain candidates");
                let field = metric.get("field").and_then(Value::as_str).ok_or_else(|| {
                    GatewayError::bad(
                        format!("aggs.{name}.metric.field"),
                        "metric aggregation requires field",
                    )
                })?;
                let mut values = aggregation_hits
                    .iter()
                    .filter_map(|hit| {
                        hit.get("_source")
                            .and_then(|source| source_field(source, field))
                            .and_then(Value::as_f64)
                    })
                    .collect::<Vec<_>>();
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let value = if spec.get("min").is_some() {
                    values.first().copied()
                } else {
                    values.last().copied()
                };
                agg_result.insert(name.clone(), json!({"value":value}));
            } else if let Some(filters) = spec
                .get("filters")
                .and_then(|v| v.get("filters"))
                .and_then(Value::as_object)
            {
                let aggregation_hits = aggregation_hits
                    .as_ref()
                    .expect("gateway-computed aggregations retain candidates");
                let mut buckets = Map::new();
                for (key, filter_query) in filters {
                    let count = aggregation_hits
                        .iter()
                        .filter(|hit| {
                            hit.get("_source")
                                .is_some_and(|source| source_matches_query(source, filter_query))
                        })
                        .count();
                    buckets.insert(key.clone(), json!({"doc_count":count}));
                }
                agg_result.insert(name.clone(), json!({"buckets":buckets}));
            } else if let Some(filter_query) = spec.get("filter") {
                let aggregation_hits = aggregation_hits
                    .as_ref()
                    .expect("gateway-computed aggregations retain candidates");
                let count = aggregation_hits
                    .iter()
                    .filter(|hit| {
                        hit.get("_source")
                            .is_some_and(|source| source_matches_query(source, filter_query))
                    })
                    .count();
                agg_result.insert(name.clone(), json!({"doc_count":count}));
            } else {
                return Err(GatewayError::bad(
                    format!("aggs.{name}"),
                    "supported aggregations are terms, min, max, filters, and filter",
                ));
            }
        }
        output.insert("aggregations".into(), Value::Object(agg_result));
    }
    if state.cfg.analytics {
        if let Ok(mut a) = state.analytics.lock() {
            a.requests += 1;
            a.supported += 1;
        }
    }
    Ok(es_ok(response))
}

async fn msearch(
    State(state): State<AppState>,
    default_index: Option<String>,
    body: String,
) -> Result<Response, GatewayError> {
    let lines = body
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    if lines.len() % 2 != 0 {
        return Err(GatewayError::bad(
            "_msearch",
            "NDJSON must contain alternating header and query lines",
        ));
    }
    let mut responses = Vec::with_capacity(lines.len() / 2);
    for pair in lines.chunks(2) {
        let header: Value = serde_json::from_str(pair[0])
            .map_err(|e| GatewayError::bad("_msearch.header", e.to_string()))?;
        let query: Value = serde_json::from_str(pair[1])
            .map_err(|e| GatewayError::bad("_msearch.query", e.to_string()))?;
        let index = header
            .get("index")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| default_index.clone())
            .ok_or_else(|| GatewayError::bad("_msearch.header.index", "an index is required"))?;
        let result = match search(State(state.clone()), Path(index), Json(query)).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
        let status = result.status();
        let bytes = axum::body::to_bytes(result.into_body(), state.cfg.max_body_bytes)
            .await
            .map_err(|e| GatewayError::Internal(e.to_string()))?;
        let mut value: Value =
            serde_json::from_slice(&bytes).map_err(|e| GatewayError::Internal(e.to_string()))?;
        if !status.is_success() && !value.is_object() {
            value = json!({"error":value});
        }
        responses.push(value);
    }
    Ok(es_ok(json!({"responses": responses})))
}

async fn count(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    let (_, coll, _, _) = get_index(&state, &index)?;
    let query = body
        .get("query")
        .cloned()
        .unwrap_or_else(|| json!({"match_all": {}}));
    let mut patterns = Vec::new();
    collect_patterns(&query, &mut patterns)?;
    if pick_text(&query).is_none() && patterns.is_empty() {
        let (filter, _) = query_filter(&query)?;
        let count = qdrant_exact_count(&state, &coll, filter.as_ref()).await?;
        return Ok(es_ok(
            json!({"count":count,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}),
        ));
    }
    let mut b = body;
    // Pattern and approximate-text searches compute their total before paging,
    // so count does not need to build an otherwise unused hit page.
    b["from"] = json!(0);
    b["size"] = json!(0);
    let r = search(State(state), Path(index), Json(b)).await?;
    let bytes = axum::body::to_bytes(r.into_body(), usize::MAX)
        .await
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let v: Value =
        serde_json::from_slice(&bytes).map_err(|e| GatewayError::Internal(e.to_string()))?;
    Ok(es_ok(
        json!({"count":v.get("hits").and_then(|h|h.get("total")).and_then(|t|t.get("value")).cloned().unwrap_or(json!(0)),"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}),
    ))
}

async fn qdrant_exact_count(
    state: &AppState,
    collection: &str,
    filter: Option<&Value>,
) -> Result<u64, GatewayError> {
    let mut request = json!({"exact":true});
    if let Some(filter) = filter {
        request["filter"] = filter.clone();
    }
    let response = state
        .qdrant
        .request(
            Method::POST,
            &format!("/collections/{collection}/points/count"),
            Some(request),
        )
        .await?;
    response
        .get("result")
        .and_then(|result| result.get("count"))
        .and_then(Value::as_u64)
        .ok_or_else(|| GatewayError::upstream("Qdrant count response did not contain result.count"))
}

fn bulk_response_has_errors(items: &[Value]) -> bool {
    items.iter().any(|item| {
        item.as_object()
            .is_some_and(|actions| actions.values().any(|result| result.get("error").is_some()))
    })
}

async fn bulk_item_from_response(kind: &str, response: Response) -> Result<Value, GatewayError> {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let mut result: Value =
        serde_json::from_slice(&bytes).map_err(|e| GatewayError::Internal(e.to_string()))?;
    let result = result.as_object_mut().ok_or_else(|| {
        GatewayError::Internal("bulk operation returned a non-object body".into())
    })?;
    result.insert("status".into(), json!(status.as_u16()));
    Ok(json!({kind: result}))
}

async fn bulk(
    State(state): State<AppState>,
    index: Option<Path<String>>,
    body: String,
) -> Result<Response, GatewayError> {
    if body.len() > state.cfg.max_bulk_bytes {
        return Err(GatewayError::payload_too_large(
            "MAX_BULK_BYTES",
            state.cfg.max_bulk_bytes,
        ));
    }
    let default = index.map(|p| p.0);
    let lines: Vec<&str> = body.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut actions: Vec<(String, String, String, Option<Value>)> = Vec::new();
    let mut position = 0;
    while position < lines.len() {
        let action: Value = serde_json::from_str(lines[position])
            .map_err(|e| GatewayError::bad("_bulk.action", e.to_string()))?;
        let action = action
            .as_object()
            .filter(|object| object.len() == 1)
            .ok_or_else(|| {
                GatewayError::bad(
                    "_bulk.action",
                    "each action line must contain exactly one action",
                )
            })?;
        let (kind, meta) = action.iter().next().expect("one action was validated");
        if !matches!(kind.as_str(), "index" | "create" | "update" | "delete") {
            return Err(GatewayError::bad(
                "_bulk.action",
                format!("unsupported bulk action [{kind}]"),
            ));
        }
        let obj = meta.as_object().ok_or_else(|| {
            GatewayError::bad(
                format!("_bulk.{kind}"),
                "bulk action metadata must be an object",
            )
        })?;
        let idx = match obj.get("_index") {
            Some(Value::String(index)) => index.clone(),
            Some(_) => {
                return Err(GatewayError::bad(
                    format!("_bulk.{kind}._index"),
                    "bulk action _index must be a string",
                ))
            }
            None => default
                .clone()
                .ok_or_else(|| GatewayError::bad("_bulk", "each action needs _index"))?,
        };
        let id = match obj.get("_id") {
            Some(Value::String(id)) => id.clone(),
            Some(_) => {
                return Err(GatewayError::bad(
                    format!("_bulk.{kind}._id"),
                    "bulk action _id must be a string",
                ))
            }
            None if kind == "delete" || kind == "update" => {
                return Err(GatewayError::bad(
                    format!("_bulk.{kind}._id"),
                    format!("bulk {kind} actions require an _id"),
                ));
            }
            None => uuid::Uuid::new_v4().to_string(),
        };
        let source = if kind == "delete" {
            None
        } else {
            position += 1;
            let source_line = lines.get(position).ok_or_else(|| {
                GatewayError::bad("_bulk", "index/create/update actions require a source line")
            })?;
            Some(
                serde_json::from_str(source_line)
                    .map_err(|e| GatewayError::bad("_bulk.source", e.to_string()))?,
            )
        };
        actions.push((kind.clone(), idx, id, source));
        position += 1;
    }
    let mut items = Vec::new();
    let mut cursor = 0;
    while cursor < actions.len() {
        let (kind, idx, id, source) = &actions[cursor];
        if kind == "index" {
            let batch_index = idx.clone();
            let mut batch = Vec::new();
            let mut end = cursor;
            while end < actions.len() && actions[end].0 == "index" && actions[end].1 == batch_index
            {
                batch.push((
                    actions[end].2.clone(),
                    actions[end].3.clone().unwrap_or_else(|| json!({})),
                ));
                end += 1;
            }
            let prepared = get_index(&state, &batch_index).map(|(index, coll, _, _)| {
                let ids = batch.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
                (index, coll, ids)
            });
            let result = match prepared {
                Ok((index, coll, ids)) => retrieve_existing_ids(&state, &index, &coll, &ids)
                    .await
                    .map(|existing| (index, existing)),
                Err(error) => Err(error),
            };
            let result = match result {
                Ok((index, existing)) => write_docs(State(state.clone()), index.clone(), batch)
                    .await
                    .map(|_| (index, existing)),
                Err(error) => Err(error),
            };
            let mut seen = HashSet::new();
            for action in &actions[cursor..end] {
                match &result {
                    Ok((index, existing)) => {
                        let existed = existing.contains(&action.2) || !seen.insert(action.2.clone());
                        let status = if existed { 200 } else { 201 };
                        let result = if existed { "updated" } else { "created" };
                        items.push(json!({action.0.clone():{"_index":index,"_id":action.2,"status":status,"result":result}}));
                    }
                    Err(e) => items.push(json!({action.0.clone():{"_index":action.1,"_id":action.2,"status":e.status().as_u16(),"error":e.body()["error"].clone()}})),
                }
            }
            cursor = end;
            continue;
        }
        let result = match kind.as_str() {
            "create" => create_doc(
                State(state.clone()),
                Path((idx.clone(), id.clone())),
                Json(source.clone().unwrap_or_else(|| json!({}))),
            )
            .await
            .map(|response| ("create", response)),
            "delete" => delete_doc(State(state.clone()), Path((idx.clone(), id.clone())))
                .await
                .map(|response| ("delete", response)),
            "update" => update_doc(
                State(state.clone()),
                Path((idx.clone(), id.clone())),
                Json(source.clone().unwrap_or_else(|| json!({}))),
            )
            .await
            .map(|response| ("update", response)),
            _ => Err(GatewayError::bad(
                format!("_bulk.{kind}"),
                "unsupported bulk action",
            )),
        };
        match result {
            Ok((kind, response)) => {
                items.push(bulk_item_from_response(kind, response).await?)
            }
            Err(e) => items.push(json!({kind.clone():{"_index":idx,"_id":id,"status":e.status().as_u16(),"error":e.body()["error"].clone()}})),
        }
        cursor += 1;
    }
    let errors = bulk_response_has_errors(&items);
    Ok(es_ok(json!({"took":0,"errors":errors,"items":items})))
}

async fn aliases(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    let _index_admin = state.index_admin.lock().await;
    enum AliasAction {
        Add { alias: String, index: String },
        Remove { alias: String, index: String },
    }

    let mut actions = Vec::new();
    for action in body
        .get("actions")
        .and_then(Value::as_array)
        .ok_or_else(|| GatewayError::bad("_aliases", "actions must be an array"))?
    {
        let o = action
            .as_object()
            .filter(|object| object.len() == 1)
            .ok_or_else(|| {
                GatewayError::bad(
                    "_aliases.action",
                    "each alias action must contain exactly one action",
                )
            })?;
        let (kind, metadata) = o.iter().next().expect("one alias action was validated");
        if !matches!(kind.as_str(), "add" | "remove") {
            return Err(GatewayError::bad(
                "_aliases.action",
                format!("unsupported alias action [{kind}]"),
            ));
        }
        let metadata = metadata.as_object().ok_or_else(|| {
            GatewayError::bad(
                format!("_aliases.{kind}"),
                "alias action metadata must be an object",
            )
        })?;
        let alias = metadata
            .get("alias")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                GatewayError::bad(format!("_aliases.{kind}.alias"), "missing string alias")
            })?;
        let requested_index = metadata
            .get("index")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                GatewayError::bad(format!("_aliases.{kind}.index"), "missing string index")
            })?;
        let (index, _, _, _) = get_index(&state, requested_index)?;
        if kind == "add" {
            let db = state
                .db
                .lock()
                .map_err(|e| GatewayError::Internal(e.to_string()))?;
            let index_uses_alias_name = db
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM indices WHERE name=?1)",
                    params![alias],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(|e| GatewayError::Internal(e.to_string()))?;
            if index_uses_alias_name {
                return Err(GatewayError::bad(
                    "_aliases.add.alias",
                    format!("alias [{alias}] conflicts with an existing index"),
                ));
            }
            actions.push(AliasAction::Add {
                alias: alias.to_string(),
                index,
            });
        } else {
            actions.push(AliasAction::Remove {
                alias: alias.to_string(),
                index,
            });
        }
    }

    let mut db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let transaction = db
        .transaction()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    for action in actions {
        match action {
            AliasAction::Add { alias, index } => transaction
                .execute(
                    "INSERT OR REPLACE INTO aliases(alias,index_name) VALUES (?1,?2)",
                    params![alias, index],
                )
                .map_err(|e| GatewayError::Internal(e.to_string()))?,
            AliasAction::Remove { alias, index } => transaction
                .execute(
                    "DELETE FROM aliases WHERE alias=?1 AND index_name=?2",
                    params![alias, index],
                )
                .map_err(|e| GatewayError::Internal(e.to_string()))?,
        };
    }
    transaction
        .commit()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    Ok(es_ok(json!({"acknowledged":true})))
}
async fn alias_get(
    State(state): State<AppState>,
    Path(alias): Path<String>,
) -> Result<Response, GatewayError> {
    let db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let idx: String = db
        .query_row(
            "SELECT index_name FROM aliases WHERE alias=?1",
            params![&alias],
            |r| r.get(0),
        )
        .map_err(|_| GatewayError::NotFound(format!("no such alias [{alias}]")))?;
    Ok(es_ok(json!({idx:{"aliases":{alias:{}}}})))
}

async fn alias_head(State(state): State<AppState>, Path(alias): Path<String>) -> Response {
    match alias_get(State(state), Path(alias)).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
async fn compatibility(State(state): State<AppState>) -> Response {
    let a = state.analytics.lock().unwrap();
    let pct = if a.requests == 0 {
        100.0
    } else {
        a.supported as f64 * 100.0 / a.requests as f64
    };
    Json(json!({"requests":a.requests,"fully_supported":a.supported,"compatibility_percent":pct,"unsupported":a.unsupported})).into_response()
}

async fn mapping(
    State(state): State<AppState>,
    Path(index): Path<String>,
    method: Method,
    body: Option<Json<Value>>,
) -> Result<Response, GatewayError> {
    let _index_admin = state.index_admin.lock().await;
    let (index, coll, mut m, vectors) = get_index(&state, &index)?;
    if method == Method::PUT {
        let body = body.map(|j| j.0).unwrap_or_else(|| json!({}));
        let update = body.get("mappings").unwrap_or(&body);
        let update = update.as_object().ok_or_else(|| {
            GatewayError::bad("_mapping", "mapping update body must be an object")
        })?;
        if update.keys().any(|key| key != "properties") {
            return Err(GatewayError::bad(
                "_mapping",
                "only properties are supported in mapping updates",
            ));
        }
        let properties = match update.get("properties") {
            Some(value) => value.as_object().ok_or_else(|| {
                GatewayError::bad("_mapping.properties", "properties must be an object")
            })?,
            None => return Ok(es_ok(json!({"acknowledged":true}))),
        };
        let existing_properties = m
            .as_object_mut()
            .ok_or_else(|| GatewayError::Internal("stored mapping is not an object".into()))?
            .entry("properties")
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| {
                GatewayError::Internal("stored mapping properties are not an object".into())
            })?;
        let mut payload_indexes = Vec::new();
        for (field, spec) in properties {
            let field_type = spec.get("type").and_then(Value::as_str).ok_or_else(|| {
                GatewayError::bad(
                    format!("_mapping.properties.{field}"),
                    "mapping properties require a string type",
                )
            })?;
            if let Some(existing) = existing_properties.get(field) {
                if existing != spec {
                    return Err(GatewayError::bad(
                        format!("_mapping.properties.{field}"),
                        "changing an existing field mapping is not supported",
                    ));
                }
                continue;
            }
            if field_type == "text" {
                return Err(GatewayError::bad(
                    format!("_mapping.properties.{field}"),
                    "adding text fields requires a new index because Qdrant 1.15 cannot add the required named sparse vector",
                ));
            }
            if let Some(schema) = payload_field_schema(spec) {
                payload_indexes.push((field.clone(), schema));
            }
            existing_properties.insert(field.clone(), spec.clone());
        }
        for (field, schema) in payload_indexes {
            state
                .qdrant
                .request(
                    Method::PUT,
                    &format!("/collections/{coll}/index"),
                    Some(json!({"field_name":field,"field_schema":schema,"wait":true})),
                )
                .await?;
        }
        let db = state
            .db
            .lock()
            .map_err(|e| GatewayError::Internal(e.to_string()))?;
        db.execute(
            "UPDATE indices SET mapping=?1, vectors=?2 WHERE name=?3",
            params![
                m.to_string(),
                serde_json::to_string(&vectors)
                    .map_err(|e| GatewayError::Internal(e.to_string()))?,
                index
            ],
        )
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
        return Ok(es_ok(json!({"acknowledged":true})));
    }
    Ok(es_ok(json!({index:{"mappings":m}})))
}

fn is_bulk_request(method: &Method, path: &str) -> bool {
    (method == Method::POST || method == Method::PUT)
        && (path == "/_bulk" || path.ends_with("/_bulk"))
}

fn request_body_limit(cfg: &Config, method: &Method, path: &str) -> usize {
    if is_bulk_request(method, path) {
        cfg.max_bulk_bytes
    } else {
        cfg.max_body_bytes
    }
}

fn request_body_limit_setting(method: &Method, path: &str) -> &'static str {
    if is_bulk_request(method, path) {
        "MAX_BULK_BYTES"
    } else {
        "MAX_BODY_BYTES"
    }
}

fn decode_path_segments(path: &str) -> Result<Vec<String>, GatewayError> {
    path.trim_matches('/')
        .split('/')
        .map(|segment| {
            percent_decode_str(segment)
                .decode_utf8()
                .map(|decoded| decoded.into_owned())
                .map_err(|_| GatewayError::bad("request.path", "path is not valid UTF-8"))
        })
        .collect()
}

async fn dispatch(
    state: AppState,
    method: Method,
    path: String,
    _headers: HeaderMap,
    body: String,
) -> Response {
    let body_limit = request_body_limit(&state.cfg, &method, &path);
    if body.len() > body_limit {
        return GatewayError::payload_too_large(
            request_body_limit_setting(&method, &path),
            body_limit,
        )
        .into_response();
    }
    if is_bulk_request(&method, &path) {
        let default = path
            .strip_prefix('/')
            .and_then(|p| p.strip_suffix("/_bulk"))
            .map(|p| Path(p.to_string()));
        return bulk(State(state), default, body)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if method == Method::POST && (path == "/_msearch" || path.ends_with("/_msearch")) {
        let default_index = path
            .strip_prefix('/')
            .and_then(|p| p.strip_suffix("/_msearch"))
            .filter(|p| !p.is_empty())
            .map(str::to_owned);
        return msearch(State(state), default_index, body)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    let json_body = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => return GatewayError::bad("request.body", e.to_string()).into_response(),
        }
    };
    let parts = match decode_path_segments(&path) {
        Ok(parts) => parts,
        Err(error) => return error.into_response(),
    };
    if path == "/" && method == Method::GET {
        return root(State(state)).await;
    }
    if path == "/_cluster/health" && method == Method::GET {
        return health(State(state)).await;
    }
    if path == "/healthz" && method == Method::GET {
        return healthz().await;
    }
    if path == "/readyz" && method == Method::GET {
        return readyz(State(state)).await;
    }
    if path == "/_qdrant_gateway/compatibility" {
        return compatibility(State(state)).await;
    }
    if path == "/_aliases" && method == Method::POST {
        return aliases(State(state), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[0] == "_alias" && method == Method::GET {
        return alias_get(State(state), Path(parts[1].clone()))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[0] == "_alias" && method == Method::HEAD {
        return alias_head(State(state), Path(parts[1].clone())).await;
    }
    if parts.len() == 1 && method == Method::PUT {
        return create_index(State(state), Path(parts[0].clone()), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 1 && (method == Method::DELETE) {
        return delete_index(State(state), Path(parts[0].clone()))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 1 && method == Method::HEAD {
        return head_index(State(state), Path(parts[0].clone())).await;
    }
    if parts.len() == 2 && parts[1] == "_refresh" && method == Method::POST {
        return index_control(State(state), Path(parts[0].clone()), "refresh")
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_open" && method == Method::POST {
        return index_control(State(state), Path(parts[0].clone()), "open")
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_close" && method == Method::POST {
        return index_control(State(state), Path(parts[0].clone()), "close")
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2
        && parts[1] == "_settings"
        && (method == Method::GET || method == Method::PUT)
    {
        return index_control(State(state), Path(parts[0].clone()), "settings")
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 1 && method == Method::GET {
        return get_index_info(State(state), Path(parts[0].clone()))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2
        && parts[1] == "_mapping"
        && (method == Method::GET || method == Method::PUT)
    {
        return mapping(
            State(state),
            Path(parts[0].clone()),
            method,
            Some(Json(json_body)),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::PUT {
        return write_doc(
            State(state),
            Path((parts[0].clone(), parts[2].clone())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3
        && parts[1] == "_create"
        && (method == Method::PUT || method == Method::POST)
    {
        return create_doc(
            State(state),
            Path((parts[0].clone(), parts[2].clone())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_doc" && method == Method::POST {
        return write_doc(
            State(state),
            Path((parts[0].clone(), uuid::Uuid::new_v4().to_string())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::GET {
        return get_doc(State(state), Path((parts[0].clone(), parts[2].clone())))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::HEAD {
        return head_doc(State(state), Path((parts[0].clone(), parts[2].clone()))).await;
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::DELETE {
        return delete_doc(State(state), Path((parts[0].clone(), parts[2].clone())))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_update" && method == Method::POST {
        return update_doc(
            State(state),
            Path((parts[0].clone(), parts[2].clone())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_bulk" && method == Method::POST {
        return bulk(State(state), Some(Path(parts[0].clone())), body)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2
        && parts[1] == "_search"
        && (method == Method::GET || method == Method::POST)
    {
        return search(State(state), Path(parts[0].clone()), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_count" && (method == Method::GET || method == Method::POST)
    {
        return count(State(state), Path(parts[0].clone()), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if path == "/_bulk" && method == Method::POST {
        return bulk(State(state), None, body)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    GatewayError::bad("endpoint", format!("unsupported endpoint {method} {path}")).into_response()
}

fn gateway_router(state: AppState) -> Router {
    Router::new()
        .fallback(any(
            |State(state): State<AppState>,
             method: Method,
             uri: axum::http::Uri,
             headers: HeaderMap,
             body: Body| async move {
                let body_limit = request_body_limit(&state.cfg, &method, uri.path());
                let bytes = match axum::body::to_bytes(body, body_limit).await {
                    Ok(b) => b,
                    Err(error) => {
                        let is_length_limit = std::error::Error::source(&error)
                            .is_some_and(|source| source.is::<http_body_util::LengthLimitError>());
                        if is_length_limit {
                            return GatewayError::payload_too_large(
                                request_body_limit_setting(&method, uri.path()),
                                body_limit,
                            )
                            .into_response();
                        }
                        return GatewayError::bad("request.body", error.to_string())
                            .into_response();
                    }
                };
                let body = match String::from_utf8(bytes.to_vec()) {
                    Ok(body) => body,
                    Err(_) => {
                        return GatewayError::bad(
                            "request.body",
                            "request body must be valid UTF-8",
                        )
                        .into_response()
                    }
                };
                dispatch(state, method, uri.path().to_string(), headers, body).await
            },
        ))
        .with_state(state)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into()))
        .json()
        .init();
    let cfg = Config::from_env();
    let connect_timeout_ms = cfg.qdrant_connect_timeout.as_millis();
    let request_timeout_ms = cfg.qdrant_request_timeout.as_millis();
    let async_write_queue = cfg.async_write_queue;
    let state = AppState {
        qdrant: Qdrant {
            client: Client::builder()
                .connect_timeout(cfg.qdrant_connect_timeout)
                .timeout(cfg.qdrant_request_timeout)
                .tcp_nodelay(true)
                .pool_max_idle_per_host(128)
                .build()?,
            base: cfg.qdrant_url.clone(),
            key: cfg.qdrant_api_key.clone(),
            async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
        },
        db: Arc::new(Mutex::new(init_db()?)),
        analytics: Arc::new(Mutex::new(Analytics::default())),
        index_admin: Arc::new(AsyncMutex::new(())),
        cfg,
    };
    let app = gateway_router(state.clone());
    let addr: SocketAddr = state.cfg.listen_addr.parse()?;
    info!(
        %addr,
        connect_timeout_ms,
        request_timeout_ms,
        async_write_queue,
        "starting qdrant-es-gateway"
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn ids_are_deterministic_and_uuid_shaped() {
        let a = point_id("products", "普通話/very-long-id");
        assert_eq!(a, point_id("products", "普通話/very-long-id"));
        assert_eq!(a.len(), 36);
        assert_ne!(a, point_id("products", "other"));
    }

    #[test]
    fn path_segments_decode_document_ids_without_splitting_encoded_slashes() {
        let parts =
            decode_path_segments("/products/_doc/order%2F%E6%99%AE%E9%80%9A%20%E8%A9%B1").unwrap();

        assert_eq!(
            parts,
            ["products", "_doc", "order/\u{666e}\u{901a} \u{8a71}"]
        );
    }

    #[test]
    fn path_segments_reject_non_utf8_percent_encoding() {
        let error = decode_path_segments("/products/_doc/%FF").unwrap_err();

        assert!(matches!(
            error,
            GatewayError::Bad { ref feature, .. } if feature == "request.path"
        ));
    }

    #[test]
    fn bulk_requests_use_the_dedicated_body_limit() {
        let mut cfg = Config::from_env();
        cfg.max_body_bytes = 10;
        cfg.max_bulk_bytes = 50;

        assert_eq!(request_body_limit(&cfg, &Method::POST, "/_bulk"), 50);
        assert_eq!(request_body_limit(&cfg, &Method::PUT, "/_bulk"), 50);
        assert_eq!(
            request_body_limit(&cfg, &Method::POST, "/products/_bulk"),
            50
        );
        assert_eq!(
            request_body_limit(&cfg, &Method::PUT, "/products/_bulk"),
            50
        );
        assert_eq!(
            request_body_limit(&cfg, &Method::POST, "/products/_search"),
            10
        );
        assert_eq!(request_body_limit(&cfg, &Method::GET, "/_bulk"), 10);
    }

    #[test]
    fn bulk_response_reports_nested_item_errors() {
        let successful_items = [json!({"index":{"_index":"products","_id":"1","status":201}})];
        let failed_items = [json!({"update":{
            "_index":"products",
            "_id":"missing",
            "status":404,
            "error":{"type":"document_missing_exception"}
        }})];

        assert!(!bulk_response_has_errors(&successful_items));
        assert!(bulk_response_has_errors(&failed_items));
    }

    #[tokio::test]
    async fn bulk_update_and_delete_require_document_ids() {
        let cfg = Config::from_env();
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        for (body, feature) in [
            (
                "{\"delete\":{\"_index\":\"products\"}}\n",
                "_bulk.delete._id",
            ),
            (
                "{\"update\":{\"_index\":\"products\"}}\n{\"doc\":{\"price\":42}}\n",
                "_bulk.update._id",
            ),
        ] {
            let error = bulk(State(state.clone()), None, body.into())
                .await
                .unwrap_err();

            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.body()["error"]["feature"], feature);
            assert!(error.to_string().contains("require an _id"));
        }
    }

    #[tokio::test]
    async fn malformed_bulk_actions_fail_before_any_item_is_executed() {
        let cfg = Config::from_env();
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        for (body, feature) in [
            (
                "{\"index\":{\"_index\":\"products\",\"_id\":\"valid\"}}\n{\"title\":\"would be written\"}\n{\"index\":{},\"delete\":{\"_id\":\"other\"}}\n",
                "_bulk.action",
            ),
            (
                "{\"index\":{\"_index\":\"products\",\"_id\":\"valid\"}}\n{\"title\":\"would be written\"}\n{\"rename\":{\"_index\":\"products\"}}\n{}\n",
                "_bulk.action",
            ),
            ("{\"index\":null}\n{}\n", "_bulk.index"),
            (
                "{\"index\":{\"_index\":42,\"_id\":\"one\"}}\n{}\n",
                "_bulk.index._index",
            ),
            (
                "{\"index\":{\"_index\":\"products\",\"_id\":42}}\n{}\n",
                "_bulk.index._id",
            ),
        ] {
            let error = bulk(State(state.clone()), None, body.into())
                .await
                .unwrap_err();

            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.body()["error"]["feature"], feature);
        }
    }

    #[tokio::test]
    async fn alias_actions_validate_before_an_atomic_catalogue_update() {
        let cfg = Config::from_env();
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); \
             CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); \
             INSERT INTO indices VALUES ('products-v1', '{}', '[\"text_all\"]'); \
             INSERT INTO indices VALUES ('products-v2', '{}', '[\"text_all\"]'); \
             INSERT INTO aliases VALUES ('current-products', 'products-v1');",
        )
        .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let error = aliases(
            State(state.clone()),
            Json(json!({"actions":[
                {"add":{"index":"products-v1","alias":"preview-products"}},
                {"remove":{"alias":"current-products"}}
            ]})),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.body()["error"]["feature"], "_aliases.remove.index");
        let preview_exists: bool = state
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM aliases WHERE alias='preview-products')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!preview_exists);

        aliases(
            State(state.clone()),
            Json(json!({"actions":[
                {"remove":{"index":"products-v1","alias":"current-products"}},
                {"add":{"index":"products-v2","alias":"current-products"}}
            ]})),
        )
        .await
        .unwrap();
        let target: String = state
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT index_name FROM aliases WHERE alias='current-products'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(target, "products-v2");
    }

    #[tokio::test]
    async fn failed_index_creation_is_not_published_and_cleans_up() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mock = Router::new().fallback(any(move |method: Method, uri: axum::http::Uri| {
            let observed = observed.clone();
            async move {
                observed
                    .lock()
                    .unwrap()
                    .push(format!("{method} {}", uri.path()));
                if method == Method::PUT && uri.path().ends_with("_documents") {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"status":"error"})),
                    )
                        .into_response();
                }
                Json(json!({"status":"ok"})).into_response()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        cfg.document_projection = true;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let result = create_index(
            State(state.clone()),
            Path("products".into()),
            Json(json!({"mappings":{"properties":{"title":{"type":"text"}}}})),
        )
        .await;

        assert!(matches!(result, Err(GatewayError::Upstream(_))));
        assert!(matches!(
            get_index(&state, "products"),
            Err(GatewayError::NotFound(_))
        ));
        assert_eq!(
            *requests.lock().unwrap(),
            [
                "PUT /collections/es_products",
                "PUT /collections/es_products_documents",
                "DELETE /collections/es_products"
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn index_creation_rejects_normalized_collection_namespace_collisions() {
        let requests = Arc::new(Mutex::new(0_u64));
        let observed = requests.clone();
        let mock = Router::new().fallback(any(move || {
            let observed = observed.clone();
            async move {
                *observed.lock().unwrap() += 1;
                Json(json!({"status":"ok"})).into_response()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for (existing, requested) in [
            ("catalog.v2", "catalog_v2"),
            ("catalog", "catalog_documents"),
        ] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
                .unwrap();
            db.execute(
                "INSERT INTO indices VALUES (?1, '{}', '[\"text_all\"]')",
                params![existing],
            )
            .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let error = create_index(
                State(state.clone()),
                Path(requested.into()),
                Json(json!({})),
            )
            .await
            .unwrap_err();

            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.body()["error"]["feature"], "index.name");
            assert!(matches!(
                get_index(&state, requested),
                Err(GatewayError::NotFound(_))
            ));
        }
        assert_eq!(*requests.lock().unwrap(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn aliases_route_document_and_search_requests_to_the_concrete_index() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mock = Router::new().fallback(any(
            move |method: Method, uri: axum::http::Uri| {
                let observed = observed.clone();
                async move {
                    observed
                        .lock()
                        .unwrap()
                        .push(format!("{method} {}", uri.path()));
                    if uri.path().ends_with("/points/count") {
                        Json(json!({"result":{"count":1}})).into_response()
                    } else if method == Method::GET {
                        Json(json!({"result":{"payload":{"_es_id":"1","_source":{"title":"Alias target"}}}})).into_response()
                    } else {
                        Json(json!({"result":{"points":[{"payload":{"_es_id":"1","_source":{"title":"Alias target"}}}],"next_page_offset":null}})).into_response()
                    }
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]'); INSERT INTO aliases VALUES ('current-products', 'products');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let document = get_doc(
            State(state.clone()),
            Path(("current-products".into(), "1".into())),
        )
        .await
        .unwrap();
        let document: Value = serde_json::from_slice(
            &axum::body::to_bytes(document.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(document["_index"], "products");

        let response = search(
            State(state.clone()),
            Path("current-products".into()),
            Json(json!({"query":{"match_all":{}}})),
        )
        .await
        .unwrap();
        let response: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(response["hits"]["hits"][0]["_index"], "products");

        let delete_error = delete_index(State(state.clone()), Path("current-products".into()))
            .await
            .unwrap_err();
        assert_eq!(delete_error.status(), StatusCode::BAD_REQUEST);
        assert!(delete_error
            .to_string()
            .contains("requires a concrete index name"));

        let alias = alias_get(State(state), Path("current-products".into()))
            .await
            .unwrap();
        let alias: Value = serde_json::from_slice(
            &axum::body::to_bytes(alias.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            alias,
            json!({"products":{"aliases":{"current-products":{}}}})
        );
        assert_eq!(
            *requests.lock().unwrap(),
            [
                format!(
                    "GET /collections/es_products/points/{}",
                    point_id("products", "1")
                ),
                "POST /collections/es_products/points/count".into(),
                "POST /collections/es_products/points/scroll".into()
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn oversized_requests_return_413_with_the_active_limit() {
        let mut cfg = Config::from_env();
        cfg.max_body_bytes = 10;
        cfg.max_bulk_bytes = 20;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        for (method, path, body, setting, limit) in [
            (
                Method::POST,
                "/products/_search",
                "12345678901",
                "MAX_BODY_BYTES",
                10,
            ),
            (
                Method::POST,
                "/_bulk",
                "123456789012345678901",
                "MAX_BULK_BYTES",
                20,
            ),
        ] {
            let response = gateway_router(state.clone())
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await;
            let response = response.unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let response_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(response_body["status"], 413);
            assert_eq!(response_body["error"]["type"], "content_too_long_exception");
            assert!(response_body["error"]["reason"]
                .as_str()
                .unwrap()
                .contains(&format!("{setting} limit of {limit} bytes")));
        }
    }

    #[tokio::test]
    async fn invalid_utf8_request_bodies_are_rejected_without_lossy_replacement() {
        let cfg = Config::from_env();
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        for (method, path, mut body) in [
            (Method::PUT, "/products/_doc/one", br#"{"title":""#.to_vec()),
            (
                Method::POST,
                "/_bulk",
                br#"{"index":{"_index":"products","_id":""#.to_vec(),
            ),
        ] {
            body.push(0xff);
            body.extend_from_slice(b"\"}}\n");
            let response = gateway_router(state.clone())
                .oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                response.headers().get("x-elastic-product").unwrap(),
                "Elasticsearch"
            );
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let response_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(response_body["error"]["feature"], "request.body");
            assert_eq!(
                response_body["error"]["message"],
                "request body must be valid UTF-8"
            );
        }
    }

    #[tokio::test]
    async fn missing_document_get_returns_404_in_both_storage_modes() {
        let mock = Router::new().fallback(any(|method: Method| async move {
            if method == Method::POST {
                Json(json!({"result":[]})).into_response()
            } else {
                Json(json!({"result":null})).into_response()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = get_doc(State(state), Path(("products".into(), "missing".into())))
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert_eq!(
                response.headers().get("x-elastic-product").unwrap(),
                "Elasticsearch"
            );
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let response_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                response_body,
                json!({"_index":"products","_id":"missing","found":false})
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn missing_document_update_requires_explicit_upsert_behavior() {
        let mock = Router::new().fallback(any(|method: Method| async move {
            match method {
                Method::GET => Json(json!({"result":null})).into_response(),
                Method::POST => Json(json!({"result":[]})).into_response(),
                _ => Json(json!({"status":"ok"})).into_response(),
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let script_error = update_doc(
                State(state.clone()),
                Path(("products".into(), "missing".into())),
                Json(json!({
                    "script":{"source":"ctx._source.price = 99"},
                    "doc":{"price":99}
                })),
            )
            .await
            .unwrap_err();
            assert_eq!(script_error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(script_error.body()["error"]["feature"], "body.script");

            let error = update_doc(
                State(state.clone()),
                Path(("products".into(), "missing".into())),
                Json(json!({"doc":{"price":42}})),
            )
            .await
            .unwrap_err();
            assert_eq!(error.status(), StatusCode::NOT_FOUND);
            assert_eq!(error.body()["error"]["type"], "document_missing_exception");

            let bulk_response = bulk(
                State(state.clone()),
                None,
                "{\"update\":{\"_index\":\"products\",\"_id\":\"missing\"}}\n{\"doc\":{\"price\":42}}\n".into(),
            )
            .await
            .unwrap();
            let bytes = axum::body::to_bytes(bulk_response.into_body(), usize::MAX)
                .await
                .unwrap();
            let bulk_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(bulk_body["errors"], true);
            assert_eq!(bulk_body["items"][0]["update"]["status"], 404);
            assert_eq!(
                bulk_body["items"][0]["update"]["error"]["type"],
                "document_missing_exception"
            );

            let upsert_response = update_doc(
                State(state.clone()),
                Path(("products".into(), "missing".into())),
                Json(json!({"doc":{"price":42},"doc_as_upsert":true})),
            )
            .await
            .unwrap();
            assert_eq!(upsert_response.status(), StatusCode::CREATED);

            let bulk_upsert_response = bulk(
                State(state.clone()),
                None,
                "{\"update\":{\"_index\":\"products\",\"_id\":\"missing\"}}\n{\"doc\":{\"price\":42},\"doc_as_upsert\":true}\n".into(),
            )
            .await
            .unwrap();
            let bytes = axum::body::to_bytes(bulk_upsert_response.into_body(), usize::MAX)
                .await
                .unwrap();
            let bulk_upsert_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(bulk_upsert_body["errors"], false);
            assert_eq!(bulk_upsert_body["items"][0]["update"]["status"], 201);

            let explicit_upsert_response = bulk(
                State(state),
                None,
                "{\"update\":{\"_index\":\"products\",\"_id\":\"missing\"}}\n{\"doc\":{\"price\":42},\"upsert\":{\"price\":10,\"created_by\":\"bulk\"}}\n".into(),
            )
            .await
            .unwrap();
            let bytes = axum::body::to_bytes(explicit_upsert_response.into_body(), usize::MAX)
                .await
                .unwrap();
            let explicit_upsert_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(explicit_upsert_body["errors"], false);
            assert_eq!(explicit_upsert_body["items"][0]["update"]["status"], 201);
            assert_eq!(
                explicit_upsert_body["items"][0]["update"]["result"],
                "created"
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn supported_update_upserts_write_selected_source_with_one_preflight_each() {
        for document_projection in [false, true] {
            let reads = Arc::new(Mutex::new(0_u64));
            let writes = Arc::new(Mutex::new(Vec::<Value>::new()));
            let observed_reads = reads.clone();
            let observed_writes = writes.clone();
            let mock =
                Router::new().fallback(any(move |method: Method, body: axum::body::Bytes| {
                    let observed_reads = observed_reads.clone();
                    let observed_writes = observed_writes.clone();
                    async move {
                        match method {
                            Method::GET => {
                                *observed_reads.lock().unwrap() += 1;
                                Json(json!({"result":null})).into_response()
                            }
                            Method::POST => {
                                *observed_reads.lock().unwrap() += 1;
                                Json(json!({"result":[]})).into_response()
                            }
                            Method::PUT => {
                                observed_writes
                                    .lock()
                                    .unwrap()
                                    .push(serde_json::from_slice(&body).unwrap());
                                Json(json!({"status":"ok"})).into_response()
                            }
                            _ => Json(json!({"status":"ok"})).into_response(),
                        }
                    }
                }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = update_doc(
                State(state.clone()),
                Path(("products".into(), "missing".into())),
                Json(json!({
                    "doc":{"price":42},
                    "upsert":{"title":"Initial","price":10}
                })),
            )
            .await
            .unwrap();

            assert_eq!(response.status(), StatusCode::CREATED);
            assert_eq!(*reads.lock().unwrap(), 1);

            let response = update_doc(
                State(state),
                Path(("products".into(), "another-missing".into())),
                Json(json!({
                    "doc":{"title":"Doc as upsert","price":20},
                    "doc_as_upsert":true
                })),
            )
            .await
            .unwrap();

            assert_eq!(response.status(), StatusCode::CREATED);
            assert_eq!(*reads.lock().unwrap(), 2);
            let writes = writes.lock().unwrap();
            assert!(writes.iter().any(|write| {
                write["points"][0]["payload"]["_source"] == json!({"title":"Initial","price":10})
            }));
            assert!(writes.iter().any(|write| {
                write["points"][0]["payload"]["_source"]
                    == json!({"title":"Doc as upsert","price":20})
            }));
            assert!(!writes
                .iter()
                .any(|write| { write["points"][0]["payload"]["_source"] == json!({"price":42}) }));
            server.abort();
        }
    }

    #[tokio::test]
    async fn missing_document_delete_returns_not_found_without_a_bulk_error() {
        let mock = Router::new().fallback(any(|method: Method| async move {
            match method {
                Method::GET => Json(json!({"result":null})).into_response(),
                Method::POST => Json(json!({"result":[]})).into_response(),
                _ => Json(json!({"status":"ok"})).into_response(),
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = delete_doc(
                State(state.clone()),
                Path(("products".into(), "missing".into())),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert_eq!(
                response.headers().get("x-elastic-product").unwrap(),
                "Elasticsearch"
            );
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let response_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(response_body["result"], "not_found");

            let bulk_response = bulk(
                State(state),
                None,
                "{\"delete\":{\"_index\":\"products\",\"_id\":\"missing\"}}\n".into(),
            )
            .await
            .unwrap();
            let bytes = axum::body::to_bytes(bulk_response.into_body(), usize::MAX)
                .await
                .unwrap();
            let bulk_body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(bulk_body["errors"], false);
            assert_eq!(bulk_body["items"][0]["delete"]["status"], 404);
            assert_eq!(bulk_body["items"][0]["delete"]["result"], "not_found");
            assert!(bulk_body["items"][0]["delete"].get("error").is_none());
        }
        server.abort();
    }

    #[tokio::test]
    async fn embedded_partial_update_preserves_source_and_refreshes_filter_payload() {
        let writes = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed_writes = writes.clone();
        let mock = Router::new().fallback(any(
            move |method: Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                let observed_writes = observed_writes.clone();
                async move {
                    match method {
                        Method::GET => Json(json!({
                            "result":{"payload":{"_es_id":"p-1","_es_index":"products","brand":"Acme","price":99,"_source":{"title":"Headphones","brand":"Acme","price":99,"description":"Original"}}}
                        }))
                        .into_response(),
                        Method::POST if uri.path().ends_with("/points/payload") => {
                            observed_writes
                                .lock()
                                .unwrap()
                                .push(serde_json::from_slice(&body).unwrap());
                            Json(json!({"status":"ok"})).into_response()
                        }
                        _ => Json(json!({"status":"ok"})).into_response(),
                    }
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        cfg.document_projection = false;
        cfg.async_payload_writes = false;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); \
             CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); \
             INSERT INTO indices VALUES ('products', '{\"properties\":{\"title\":{\"type\":\"text\"},\"brand\":{\"type\":\"keyword\"},\"price\":{\"type\":\"float\"}}}', '[\"text_all\",\"text_title\"]');",
        )
        .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = update_doc(
            State(state),
            Path(("products".into(), "p-1".into())),
            Json(json!({
                "doc":{"price":125},
                "upsert":{"title":"Should not replace an existing document","price":1}
            })),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let writes = writes.lock().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0]["payload"]["price"], 125);
        assert_eq!(writes[0]["payload"]["brand"], "Acme");
        assert_eq!(
            writes[0]["payload"]["_source"],
            json!({"title":"Headphones","brand":"Acme","price":125,"description":"Original"})
        );
        assert!(writes[0].get("key").is_none());
        server.abort();
    }

    #[tokio::test]
    async fn partial_updates_recursively_merge_inner_objects() {
        for document_projection in [false, true] {
            let writes = Arc::new(Mutex::new(Vec::<Value>::new()));
            let observed_writes = writes.clone();
            let mock = Router::new().fallback(any(
                move |method: Method, uri: axum::http::Uri, body: axum::body::Bytes| {
                    let observed_writes = observed_writes.clone();
                    async move {
                        if method == Method::GET {
                            return Json(json!({
                                "result":{"payload":{"_es_id":"p-1","_source":{
                                    "title":"Headphones",
                                    "details":{"manufacturer":"Acme","warranty":{"years":2,"region":"EU"}},
                                    "tags":["audio","wireless"]
                                }}}
                            }))
                            .into_response();
                        }
                        if method == Method::POST && uri.path().ends_with("/points") {
                            return Json(json!({
                                "result":[{"payload":{"_es_id":"p-1","_source":{
                                    "title":"Headphones",
                                    "details":{"manufacturer":"Acme","warranty":{"years":2,"region":"EU"}},
                                    "tags":["audio","wireless"]
                                }}}]
                            }))
                            .into_response();
                        }
                        if method == Method::PUT
                            || (method == Method::POST && uri.path().ends_with("/points/payload"))
                        {
                            observed_writes
                                .lock()
                                .unwrap()
                                .push(serde_json::from_slice(&body).unwrap());
                        }
                        Json(json!({"status":"ok"})).into_response()
                    }
                },
            ));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            cfg.async_payload_writes = false;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = update_doc(
                State(state),
                Path(("products".into(), "p-1".into())),
                Json(json!({
                    "doc":{
                        "details":{"warranty":{"years":3}},
                        "tags":["refurbished"]
                    }
                })),
            )
            .await
            .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            let writes = writes.lock().unwrap();
            assert_eq!(writes.len(), 1);
            let source = if document_projection {
                &writes[0]["points"][0]["payload"]["_source"]
            } else {
                &writes[0]["payload"]["_source"]
            };
            assert_eq!(
                source,
                &json!({
                    "title":"Headphones",
                    "details":{"manufacturer":"Acme","warranty":{"years":3,"region":"EU"}},
                    "tags":["refurbished"]
                })
            );
            server.abort();
        }
    }

    #[tokio::test]
    async fn unchanged_updates_are_noops_unless_detection_is_disabled() {
        for document_projection in [false, true] {
            let writes = Arc::new(Mutex::new(0_u64));
            let observed_writes = writes.clone();
            let mock = Router::new().fallback(any(move |method: Method, uri: axum::http::Uri| {
                let observed_writes = observed_writes.clone();
                async move {
                    if method == Method::GET {
                        return Json(json!({
                            "result":{"payload":{"_es_id":"p-1","_source":{
                                "title":"Headphones","price":99
                            }}}
                        }))
                        .into_response();
                    }
                    if method == Method::POST && uri.path().ends_with("/points") {
                        return Json(json!({
                            "result":[{"payload":{"_es_id":"p-1","_source":{
                                "title":"Headphones","price":99
                            }}}]
                        }))
                        .into_response();
                    }
                    if method == Method::PUT
                        || (method == Method::POST && uri.path().ends_with("/points/payload"))
                    {
                        *observed_writes.lock().unwrap() += 1;
                    }
                    Json(json!({"status":"ok"})).into_response()
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            cfg.async_payload_writes = false;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = update_doc(
                State(state.clone()),
                Path(("products".into(), "p-1".into())),
                Json(json!({"doc":{"price":99}})),
            )
            .await
            .unwrap();
            let body: Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap(),
            )
            .unwrap();

            assert_eq!(body["result"], "noop");
            assert_eq!(*writes.lock().unwrap(), 0);

            let response = update_doc(
                State(state),
                Path(("products".into(), "p-1".into())),
                Json(json!({"doc":{"price":99},"detect_noop":false})),
            )
            .await
            .unwrap();
            let body: Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap(),
            )
            .unwrap();

            assert_eq!(body["result"], "updated");
            assert_eq!(*writes.lock().unwrap(), 1);
            server.abort();
        }
    }

    #[tokio::test]
    async fn bulk_index_reports_created_and_updated_without_losing_batching() {
        let requests = Arc::new(Mutex::new((0_u64, 0_u64)));
        let observed_requests = requests.clone();
        let mock = Router::new().fallback(any(move |method: Method| {
            let observed_requests = observed_requests.clone();
            async move {
                match method {
                    Method::POST => {
                        observed_requests.lock().unwrap().0 += 1;
                        Json(json!({
                            "result":[{"payload":{"_es_id":"existing","_source":{"title":"Old"}}}]
                        }))
                        .into_response()
                    }
                    Method::PUT => {
                        observed_requests.lock().unwrap().1 += 1;
                        Json(json!({"status":"ok"})).into_response()
                    }
                    _ => Json(json!({"status":"ok"})).into_response(),
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]'); INSERT INTO aliases VALUES ('current-products', 'products');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = bulk(
                State(state),
                None,
                "{\"index\":{\"_index\":\"current-products\",\"_id\":\"existing\"}}\n{\"title\":\"Replacement\"}\n{\"index\":{\"_index\":\"current-products\",\"_id\":\"new\"}}\n{\"title\":\"First\"}\n{\"index\":{\"_index\":\"current-products\",\"_id\":\"new\"}}\n{\"title\":\"Second\"}\n".into(),
            )
            .await
            .unwrap();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();

            assert_eq!(body["errors"], false);
            assert_eq!(body["items"][0]["index"]["_index"], "products");
            assert_eq!(body["items"][0]["index"]["status"], 200);
            assert_eq!(body["items"][0]["index"]["result"], "updated");
            assert_eq!(body["items"][1]["index"]["status"], 201);
            assert_eq!(body["items"][1]["index"]["result"], "created");
            assert_eq!(body["items"][2]["index"]["status"], 200);
            assert_eq!(body["items"][2]["index"]["result"], "updated");
        }
        assert_eq!(*requests.lock().unwrap(), (2, 3));
        server.abort();
    }

    #[tokio::test]
    async fn zero_size_search_uses_a_positive_qdrant_scroll_limit() {
        let observed_requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = observed_requests.clone();
        let mock = Router::new().fallback(any(move |uri: axum::http::Uri, body: Body| {
            let captured_requests = captured_requests.clone();
            async move {
                let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                let request = serde_json::from_slice::<Value>(&bytes).unwrap();
                captured_requests
                    .lock()
                    .unwrap()
                    .push((uri.path().to_string(), request));
                if uri.path().ends_with("/points/count") {
                    Json(json!({"result":{"count":37}})).into_response()
                } else {
                    Json(json!({"result":{"points":[{"payload":{"_es_id":"1","_source":{"brand":"Acme"}}}],"next_page_offset":null}})).into_response()
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = search(
            State(state.clone()),
            Path("products".into()),
            Json(json!({"size":0,"query":{"match_all":{}}})),
        )
        .await
        .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(body["hits"]["hits"], json!([]));
        assert_eq!(body["hits"]["total"], json!({"value":37,"relation":"eq"}));
        {
            let requests = observed_requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0],
                (
                    "/collections/es_products/points/count".into(),
                    json!({"exact":true})
                )
            );
            assert_eq!(requests[1].1["limit"], 1);
        }

        observed_requests.lock().unwrap().clear();
        let response = search(
            State(state),
            Path("products".into()),
            Json(json!({"size":0,"track_total_hits":false,"query":{"match_all":{}}})),
        )
        .await
        .unwrap();
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert!(body["hits"].get("total").is_none());
        let requests = observed_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].1["limit"], 1);
        server.abort();
    }

    #[test]
    fn track_total_hits_shapes_exact_and_bounded_totals() {
        assert_eq!(
            parse_track_total_hits(&json!({})).unwrap(),
            TrackTotalHits::Enabled
        );
        assert_eq!(
            parse_track_total_hits(&json!({"track_total_hits":false})).unwrap(),
            TrackTotalHits::Disabled
        );
        assert_eq!(
            parse_track_total_hits(&json!({"track_total_hits":5})).unwrap(),
            TrackTotalHits::Threshold(5)
        );
        assert_eq!(
            total_hits_value(TrackTotalHits::Threshold(5), 15, true),
            Some(json!({"value":5,"relation":"gte"}))
        );
        assert_eq!(
            total_hits_value(TrackTotalHits::Threshold(20), 15, true),
            Some(json!({"value":15,"relation":"eq"}))
        );
        assert_eq!(
            total_hits_value(TrackTotalHits::Threshold(20), 15, false),
            Some(json!({"value":15,"relation":"gte"}))
        );
        assert_eq!(total_hits_value(TrackTotalHits::Disabled, 15, true), None);

        for value in [json!(null), json!("5"), json!(-1), json!(1.5), json!({})] {
            let error = parse_track_total_hits(&json!({"track_total_hits":value})).unwrap_err();
            assert_eq!(error.body()["error"]["feature"], "track_total_hits");
        }
    }

    #[tokio::test]
    async fn invalid_pagination_fails_before_qdrant_work() {
        let requests = Arc::new(Mutex::new(0_u64));
        let observed_requests = requests.clone();
        let mock = Router::new().fallback(any(move || {
            let observed_requests = observed_requests.clone();
            async move {
                *observed_requests.lock().unwrap() += 1;
                Json(json!({"result":{"points":[],"next_page_offset":null}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        cfg.max_page_size = 1000;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let cases = [
            (json!({"from":-1}), "from"),
            (json!({"from":"1"}), "from"),
            (json!({"size":-1}), "size"),
            (json!({"size":1001}), "size"),
            (json!({"from":9999,"size":2}), "from"),
            (json!({"sort":"price","search_after":"10"}), "search_after"),
            (json!({"search_after":[10]}), "search_after"),
            (
                json!({"sort":"price","search_after":[10,"extra"]}),
                "search_after",
            ),
            (
                json!({"sort":"price","search_after":[{"price":10}]}),
                "search_after",
            ),
            (
                json!({"from":1,"sort":"price","search_after":[10]}),
                "search_after",
            ),
            (json!({"track_total_hits":null}), "track_total_hits"),
            (json!({"track_total_hits":-1}), "track_total_hits"),
            (json!({"track_total_hits":"5"}), "track_total_hits"),
        ];

        for (body, feature) in cases {
            let error = search(State(state.clone()), Path("products".into()), Json(body))
                .await
                .unwrap_err();
            assert_eq!(error.body()["error"]["feature"], feature);
        }
        assert_eq!(*requests.lock().unwrap(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn retail_collapse_inner_hits_use_the_requested_name_size_and_source_filter() {
        let mock = Router::new().fallback(any(|uri: axum::http::Uri| async move {
            if uri.path().ends_with("/points/count") {
                Json(json!({"result":{"count":3}}))
            } else {
                Json(json!({"result":{"points":[
                    {"payload":{"_es_id":"shoe-1","_source":{"category":"shoes","name":"Runner"}}},
                    {"payload":{"_es_id":"shoe-2","_source":{"category":"shoes","name":"Walker"}}},
                    {"payload":{"_es_id":"watch-1","_source":{"category":"accessories","name":"Watch"}}}
                ],"next_page_offset":null}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = search(
            State(state),
            Path("products".into()),
            Json(json!({
                "size":3,
                "_source":false,
                "collapse":{
                    "field":"category.keyword",
                    "inner_hits":{
                        "name":"category_hits",
                        "size":2,
                        "fields":["_id"],
                        "_source":false
                    }
                }
            })),
        )
        .await
        .unwrap();
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(body["hits"]["total"], json!({"value":3,"relation":"eq"}));
        assert_eq!(body["hits"]["hits"].as_array().unwrap().len(), 2);
        let shoe_group = body["hits"]["hits"]
            .as_array()
            .unwrap()
            .iter()
            .find(|hit| hit["inner_hits"]["category_hits"]["hits"]["total"]["value"] == 2)
            .unwrap();
        assert!(shoe_group.get("_source").is_none());
        assert!(shoe_group["inner_hits"].get("name").is_none());
        assert!(shoe_group["inner_hits"].get("size").is_none());
        let category_hits = &shoe_group["inner_hits"]["category_hits"]["hits"];
        assert_eq!(category_hits["total"], json!({"value":2,"relation":"eq"}));
        assert_eq!(category_hits["hits"].as_array().unwrap().len(), 2);
        let mut ids = category_hits["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, ["shoe-1", "shoe-2"]);
        assert!(category_hits["hits"][0].get("_source").is_none());
        server.abort();
    }

    #[tokio::test]
    async fn invalid_collapse_fails_before_qdrant_work() {
        let requests = Arc::new(Mutex::new(0_u64));
        let observed_requests = requests.clone();
        let mock = Router::new().fallback(any(move || {
            let observed_requests = observed_requests.clone();
            async move {
                *observed_requests.lock().unwrap() += 1;
                Json(json!({"result":{"points":[],"next_page_offset":null}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        cfg.max_page_size = 100;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        for (body, feature) in [
            (json!({"collapse":"category"}), "collapse"),
            (json!({"collapse":{"field":""}}), "collapse.field"),
            (
                json!({"collapse":{"field":"category","inner_hits":[]}}),
                "collapse.inner_hits",
            ),
            (
                json!({"collapse":{"field":"category","inner_hits":{"size":101}}}),
                "collapse.inner_hits.size",
            ),
            (
                json!({"collapse":{"field":"category","inner_hits":{"sort":"_score"}}}),
                "collapse.inner_hits.sort",
            ),
        ] {
            let error = search(State(state.clone()), Path("products".into()), Json(body))
                .await
                .unwrap_err();
            assert_eq!(error.body()["error"]["feature"], feature);
        }
        assert_eq!(*requests.lock().unwrap(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn elasticsearch_sort_shapes_drive_order_and_search_after() {
        let observed_limits = Arc::new(Mutex::new(Vec::new()));
        let limits = observed_limits.clone();
        let mock = Router::new().fallback(any(move |uri: axum::http::Uri, body: Body| {
            let limits = limits.clone();
            async move {
                if uri.path().ends_with("/points/count") {
                    Json(json!({"result":{"count":3}}))
                } else {
                    let request: Value = serde_json::from_slice(
                        &axum::body::to_bytes(body, usize::MAX).await.unwrap(),
                    )
                    .unwrap();
                    let limit = request["limit"].as_u64().unwrap() as usize;
                    limits.lock().unwrap().push(limit);
                    let points = vec![
                        json!({"payload":{"_es_id":"a","_source":{"price":10}}}),
                        json!({"payload":{"_es_id":"c","_source":{"price":30}}}),
                        json!({"payload":{"_es_id":"b","_source":{"price":30}}}),
                    ];
                    Json(json!({"result":{"points":points.into_iter().take(limit).collect::<Vec<_>>(),"next_page_offset":null}}))
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        cfg.max_page_size = 3;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let first = search(
            State(state.clone()),
            Path("products".into()),
            Json(json!({
                "sort":[{"price":{"order":"desc"}},"_id"],
                "size":2
            })),
        )
        .await
        .unwrap();
        let first: Value = serde_json::from_slice(
            &axum::body::to_bytes(first.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first["hits"]["hits"][0]["_id"], "b");
        assert_eq!(first["hits"]["hits"][0]["sort"], json!([30, "b"]));
        assert_eq!(first["hits"]["hits"][1]["_id"], "c");

        let second = search(
            State(state.clone()),
            Path("products".into()),
            Json(json!({
                "sort":[{"price":{"order":"desc"}},"_id"],
                "search_after":[30,"c"],
                "size":2
            })),
        )
        .await
        .unwrap();
        let second: Value = serde_json::from_slice(
            &axum::body::to_bytes(second.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second["hits"]["hits"].as_array().unwrap().len(), 1);
        assert_eq!(second["hits"]["hits"][0]["_id"], "a");

        let ascending = search(
            State(state.clone()),
            Path("products".into()),
            Json(json!({"sort":"price","size":3})),
        )
        .await
        .unwrap();
        let ascending: Value = serde_json::from_slice(
            &axum::body::to_bytes(ascending.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(ascending["hits"]["hits"][0]["_id"], "a");
        assert_eq!(*observed_limits.lock().unwrap(), vec![3, 3, 3]);

        let error = search(
            State(state.clone()),
            Path("products".into()),
            Json(json!({"sort":[{"price":{"mode":"max"}}]})),
        )
        .await
        .unwrap_err();
        assert_eq!(error.body()["error"]["feature"], "sort");
        server.abort();
    }

    #[tokio::test]
    async fn count_applies_gateway_patterns_across_scroll_pages() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mock = Router::new().fallback(any(move |uri: axum::http::Uri, body: Body| {
            let observed = observed.clone();
            async move {
                let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                let request: Value = serde_json::from_slice(&bytes).unwrap();
                observed
                    .lock()
                    .unwrap()
                    .push((uri.path().to_string(), request.clone()));
                if request.get("offset").is_some() {
                    Json(json!({"result":{"points":[
                        {"payload":{"_es_id":"3","_source":{"sku":"ABC-200"}}}
                    ],"next_page_offset":null}}))
                    .into_response()
                } else {
                    Json(json!({"result":{"points":[
                        {"payload":{"_es_id":"1","_source":{"sku":"ABC-100"}}},
                        {"payload":{"_es_id":"2","_source":{"sku":"XYZ-100"}}}
                    ],"next_page_offset":"next"}}))
                    .into_response()
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = count(
            State(state),
            Path("products".into()),
            Json(json!({"query":{"wildcard":{"sku":"ABC-*"}}})),
        )
        .await
        .unwrap();
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(body["count"], 2);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests
            .iter()
            .all(|(path, _)| path == "/collections/es_products/points/scroll"));
        assert_eq!(requests[1].1["offset"], "next");
        server.abort();
    }

    #[tokio::test]
    async fn mapping_updates_merge_properties_and_create_payload_indexes() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let mock = Router::new().fallback(any(
            move |method: Method, uri: axum::http::Uri, body: Body| {
                let observed = observed.clone();
                async move {
                    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                    observed.lock().unwrap().push((
                        method,
                        uri.path().to_string(),
                        serde_json::from_slice::<Value>(&bytes).unwrap(),
                    ));
                    Json(json!({"status":"ok"})).into_response()
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); \
             CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); \
             INSERT INTO indices VALUES ('products', '{\"properties\":{\"title\":{\"type\":\"text\"}}}', '[\"text_all\",\"text_title\"]');",
        )
        .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = mapping(
            State(state.clone()),
            Path("products".into()),
            Method::PUT,
            Some(Json(json!({"properties":{"brand":{"type":"keyword"}}}))),
        )
        .await
        .unwrap();
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body, json!({"acknowledged":true}));

        let (_, _, stored, vectors) = get_index(&state, "products").unwrap();
        assert_eq!(stored["properties"]["title"]["type"], "text");
        assert_eq!(stored["properties"]["brand"]["type"], "keyword");
        assert_eq!(vectors, ["text_all", "text_title"]);

        mapping(
            State(state.clone()),
            Path("products".into()),
            Method::PUT,
            Some(Json(json!({"properties":{"title":{"type":"text"}}}))),
        )
        .await
        .unwrap();
        assert_eq!(
            *requests.lock().unwrap(),
            [(
                Method::PUT,
                "/collections/es_products/index".into(),
                json!({"field_name":"brand","field_schema":"keyword","wait":true})
            )]
        );

        for (update, feature) in [
            (
                json!({"properties":{"title":{"type":"keyword"}}}),
                "_mapping.properties.title",
            ),
            (
                json!({"properties":{"description":{"type":"text"}}}),
                "_mapping.properties.description",
            ),
        ] {
            let error = mapping(
                State(state.clone()),
                Path("products".into()),
                Method::PUT,
                Some(Json(update)),
            )
            .await
            .unwrap_err();
            assert_eq!(error.body()["error"]["feature"], feature);
        }
        assert_eq!(requests.lock().unwrap().len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn bulk_alias_items_preserve_concrete_operation_results() {
        let new_point = point_id("products", "new");
        let mock = Router::new().fallback(any(
            move |method: Method, uri: axum::http::Uri| {
                let new_point = new_point.clone();
                async move {
                    if method == Method::GET && uri.path().ends_with(&new_point) {
                        return Json(json!({"result":null})).into_response();
                    }
                    if method == Method::GET {
                        return Json(json!({
                            "result":{"payload":{"_es_id":"existing","_source":{"title":"Original","price":10}}}
                        }))
                        .into_response();
                    }
                    Json(json!({"status":"ok"})).into_response()
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]'); INSERT INTO aliases VALUES ('current-products', 'products');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = bulk(
            State(state),
            None,
            "{\"create\":{\"_index\":\"current-products\",\"_id\":\"new\"}}\n{\"title\":\"New\"}\n{\"update\":{\"_index\":\"current-products\",\"_id\":\"existing\"}}\n{\"doc\":{\"price\":20}}\n{\"delete\":{\"_index\":\"current-products\",\"_id\":\"existing\"}}\n"
                .into(),
        )
        .await
        .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(body["errors"], false);
        for item in body["items"].as_array().unwrap() {
            let result = item.as_object().unwrap().values().next().unwrap();
            assert_eq!(result["_index"], "products");
            assert_eq!(result["_version"], 1);
            assert_eq!(result["_shards"]["failed"], 0);
        }
        assert_eq!(body["items"][0]["create"]["status"], 201);
        assert_eq!(body["items"][0]["create"]["result"], "created");
        assert_eq!(body["items"][1]["update"]["status"], 200);
        assert_eq!(body["items"][1]["update"]["result"], "updated");
        assert_eq!(body["items"][2]["delete"]["status"], 200);
        assert_eq!(body["items"][2]["delete"]["result"], "deleted");
        server.abort();
    }

    #[tokio::test]
    async fn bulk_create_rejects_an_existing_document_without_overwriting_it() {
        let writes = Arc::new(Mutex::new(0_u64));
        let observed_writes = writes.clone();
        let mock = Router::new().fallback(any(move |method: Method, uri: axum::http::Uri| {
            let observed_writes = observed_writes.clone();
            async move {
                match method {
                    Method::GET => Json(json!({
                        "result":{"payload":{"_es_id":"existing","_source":{"title":"Original"}}}
                    }))
                    .into_response(),
                    Method::POST if uri.path().ends_with("/points") => Json(json!({
                        "result":[{"payload":{"_es_id":"existing","_source":{"title":"Original"}}}]
                    }))
                    .into_response(),
                    Method::PUT => {
                        *observed_writes.lock().unwrap() += 1;
                        Json(json!({"status":"ok"})).into_response()
                    }
                    _ => Json(json!({"status":"ok"})).into_response(),
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = bulk(
                State(state),
                None,
                "{\"create\":{\"_index\":\"products\",\"_id\":\"existing\"}}\n{\"title\":\"Replacement\"}\n".into(),
            )
            .await
            .unwrap();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();

            assert_eq!(body["errors"], true);
            assert_eq!(body["items"][0]["create"]["status"], 409);
            assert_eq!(
                body["items"][0]["create"]["error"]["type"],
                "version_conflict_engine_exception"
            );
        }
        assert_eq!(*writes.lock().unwrap(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn bulk_create_still_writes_a_missing_document() {
        let writes = Arc::new(Mutex::new(0_u64));
        let observed_writes = writes.clone();
        let mock = Router::new().fallback(any(move |method: Method| {
            let observed_writes = observed_writes.clone();
            async move {
                if method == Method::GET {
                    return Json(json!({"result":null})).into_response();
                }
                if method == Method::PUT {
                    *observed_writes.lock().unwrap() += 1;
                }
                Json(json!({"status":"ok"})).into_response()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = bulk(
            State(state),
            None,
            "{\"create\":{\"_index\":\"products\",\"_id\":\"new\"}}\n{\"title\":\"New\"}\n".into(),
        )
        .await
        .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(body["errors"], false);
        assert_eq!(body["items"][0]["create"]["status"], 201);
        assert_eq!(*writes.lock().unwrap(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn standalone_create_returns_created_for_a_missing_document() {
        let writes = Arc::new(Mutex::new(0_u64));
        let observed_writes = writes.clone();
        let mock = Router::new().fallback(any(move |method: Method| {
            let observed_writes = observed_writes.clone();
            async move {
                match method {
                    Method::GET => Json(json!({"result":null})).into_response(),
                    Method::POST => Json(json!({"result":[]})).into_response(),
                    Method::PUT => {
                        *observed_writes.lock().unwrap() += 1;
                        Json(json!({"status":"ok"})).into_response()
                    }
                    _ => Json(json!({"status":"ok"})).into_response(),
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            for method in [Method::PUT, Method::POST] {
                let mut cfg = Config::from_env();
                cfg.qdrant_url = format!("http://{address}");
                cfg.document_projection = document_projection;
                let db = Connection::open_in_memory().unwrap();
                db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                    .unwrap();
                let state = AppState {
                    qdrant: Qdrant {
                        client: Client::new(),
                        base: cfg.qdrant_url.clone(),
                        key: None,
                        async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                    },
                    db: Arc::new(Mutex::new(db)),
                    analytics: Arc::new(Mutex::new(Analytics::default())),
                    index_admin: Arc::new(AsyncMutex::new(())),
                    cfg,
                };

                let response = gateway_router(state)
                    .oneshot(
                        axum::http::Request::builder()
                            .method(method)
                            .uri("/products/_create/new")
                            .header("content-type", "application/json")
                            .body(Body::from(r#"{"title":"New"}"#))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::CREATED);
                assert_eq!(
                    response.headers().get("x-elastic-product").unwrap(),
                    "Elasticsearch"
                );
                let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["_index"], "products");
                assert_eq!(body["_id"], "new");
                assert_eq!(body["result"], "created");
            }
        }
        assert_eq!(*writes.lock().unwrap(), 6);
        server.abort();
    }

    #[tokio::test]
    async fn standalone_index_distinguishes_created_from_updated_documents() {
        for document_projection in [false, true] {
            for existed in [false, true] {
                let mock = Router::new().fallback(any(move |method: Method| async move {
                    match method {
                        Method::GET => Json(if existed {
                            json!({"result":{"payload":{"_es_id":"item","_source":{"title":"Old"}}}})
                        } else {
                            json!({"result":null})
                        })
                        .into_response(),
                        Method::POST => Json(if existed {
                            json!({"result":[{"payload":{"_es_id":"item","_source":{"title":"Old"}}}]})
                        } else {
                            json!({"result":[]})
                        })
                        .into_response(),
                        _ => Json(json!({"status":"ok"})).into_response(),
                    }
                }));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server =
                    tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

                let mut cfg = Config::from_env();
                cfg.qdrant_url = format!("http://{address}");
                cfg.document_projection = document_projection;
                let db = Connection::open_in_memory().unwrap();
                db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]'); INSERT INTO aliases VALUES ('current-products', 'products');")
                    .unwrap();
                let state = AppState {
                    qdrant: Qdrant {
                        client: Client::new(),
                        base: cfg.qdrant_url.clone(),
                        key: None,
                        async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                    },
                    db: Arc::new(Mutex::new(db)),
                    analytics: Arc::new(Mutex::new(Analytics::default())),
                    index_admin: Arc::new(AsyncMutex::new(())),
                    cfg,
                };

                let response = gateway_router(state)
                    .oneshot(
                        axum::http::Request::builder()
                            .method(Method::PUT)
                            .uri("/current-products/_doc/item")
                            .header("content-type", "application/json")
                            .body(Body::from(r#"{"title":"New"}"#))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    if existed {
                        StatusCode::OK
                    } else {
                        StatusCode::CREATED
                    }
                );
                assert_eq!(
                    response.headers().get("x-elastic-product").unwrap(),
                    "Elasticsearch"
                );
                let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["_index"], "products");
                assert_eq!(body["_id"], "item");
                assert_eq!(body["result"], if existed { "updated" } else { "created" });
                server.abort();
            }
        }
    }

    #[tokio::test]
    async fn standalone_create_rejects_an_existing_document_without_writing() {
        let writes = Arc::new(Mutex::new(0_u64));
        let observed_writes = writes.clone();
        let mock = Router::new().fallback(any(move |method: Method| {
            let observed_writes = observed_writes.clone();
            async move {
                match method {
                    Method::GET => Json(json!({
                        "result":{"payload":{"_es_id":"existing","_source":{"title":"Original"}}}
                    }))
                    .into_response(),
                    Method::POST => Json(json!({
                        "result":[{"payload":{"_es_id":"existing","_source":{"title":"Original"}}}]
                    }))
                    .into_response(),
                    Method::PUT => {
                        *observed_writes.lock().unwrap() += 1;
                        Json(json!({"status":"ok"})).into_response()
                    }
                    _ => Json(json!({"status":"ok"})).into_response(),
                }
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        for document_projection in [false, true] {
            let mut cfg = Config::from_env();
            cfg.qdrant_url = format!("http://{address}");
            cfg.document_projection = document_projection;
            let db = Connection::open_in_memory().unwrap();
            db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[\"text_all\"]');")
                .unwrap();
            let state = AppState {
                qdrant: Qdrant {
                    client: Client::new(),
                    base: cfg.qdrant_url.clone(),
                    key: None,
                    async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                index_admin: Arc::new(AsyncMutex::new(())),
                cfg,
            };

            let response = gateway_router(state)
                .oneshot(
                    axum::http::Request::builder()
                        .method(Method::PUT)
                        .uri("/products/_create/existing")
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"title":"Replacement"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CONFLICT);
            assert_eq!(
                response.headers().get("x-elastic-product").unwrap(),
                "Elasticsearch"
            );
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["type"], "version_conflict_engine_exception");
        }
        assert_eq!(*writes.lock().unwrap(), 0);
        server.abort();
    }

    #[test]
    fn mapping_creates_one_vector_per_text_field() {
        let (mapping, vectors) = mapping_vectors(
            &json!({"mappings":{"properties":{"title":{"type":"text"},"brand":{"type":"keyword"},"body":{"type":"text"}}}}),
        );
        assert_eq!(mapping["properties"]["title"]["type"], "text");
        assert_eq!(vectors, vec!["text_all", "text_body", "text_title"]);
    }

    #[test]
    fn projected_payload_keeps_only_filterable_fields() {
        let mapping = json!({"properties": {
            "title": {"type":"text"},
            "brand": {"type":"keyword"},
            "price": {"type":"float"}
        }});
        let payload = projected_search_payload(
            &json!({"title":"headphones","brand":"Acme","price":99,"description":"long text"}),
            "p-1",
            "products",
            &mapping,
        );
        assert_eq!(payload["_es_id"], "p-1");
        assert_eq!(payload["brand"], "Acme");
        assert_eq!(payload["price"], 99);
        assert!(payload.get("_source").is_none());
        assert!(payload.get("title").is_none());
        assert!(payload.get("description").is_none());
    }

    #[test]
    fn query_filter_supports_simple_should() {
        let (filter, _) = query_filter(
            &json!({"bool":{"should":[{"term":{"status":"live"}},{"term":{"status":"draft"}}]}}),
        )
        .unwrap();
        assert_eq!(filter.unwrap()["must"][0]["min_should"]["min_count"], 1);
    }

    #[test]
    fn query_filter_translates_nested_term_and_range() {
        let (filter, _) = query_filter(
            &json!({"bool":{"must":[{"term":{"status":"live"}},{"range":{"price":{"gte":10}}}]}}),
        )
        .unwrap();
        let filter = filter.unwrap();
        assert_eq!(filter["must"].as_array().unwrap().len(), 2);
        assert_eq!(filter["must"][0]["key"], "status");
        assert_eq!(filter["must"][1]["range"]["gte"], 10);
    }

    #[test]
    fn exists_query_excludes_empty_qdrant_payloads() {
        let (filter, _) = query_filter(&json!({"exists":{"field":"brand"}})).unwrap();

        assert_eq!(
            filter.unwrap(),
            json!({"must":[{"must_not":[{"is_empty":{"key":"brand"}}]}]})
        );
    }

    #[test]
    fn query_filter_accepts_java_range_and_object_terms() {
        let (filter, _) = query_filter(&json!({"bool":{"must":[
            {"term":{"brand.keyword":{"value":"Acme","boost":1.0}}},
            {"range":{"price":{"from":10,"to":50,"include_lower":true,"include_upper":false,"boost":1.0}}}
        ]}})).unwrap();
        let filter = filter.unwrap();
        assert_eq!(filter["must"][0]["key"], "brand");
        assert_eq!(filter["must"][0]["match"]["value"], "Acme");
        assert_eq!(filter["must"][1]["range"]["gte"], 10);
        assert_eq!(filter["must"][1]["range"]["lt"], 50);
    }

    #[test]
    fn bool_should_defaults_follow_required_clause_semantics() {
        let query = json!({"bool":{
            "filter":{"term":{"brand":"Acme"}},
            "should":[{"term":{"featured":true}}]
        }});
        let (filter, _) = query_filter(&query).unwrap();
        assert_eq!(
            filter.unwrap(),
            json!({"must":[{"key":"brand","match":{"value":"Acme"}}]})
        );

        let source = json!({"brand":"Acme","featured":false});
        assert!(source_matches_query(&source, &query));
        assert!(!source_matches_query(
            &source,
            &json!({"bool":{"should":[{"term":{"featured":true}}]}})
        ));
        assert!(!source_matches_query(
            &source,
            &json!({"bool":{
                "filter":{"term":{"brand":"Acme"}},
                "should":[{"term":{"featured":true}}],
                "minimum_should_match":1
            }})
        ));
    }

    #[test]
    fn bool_negative_clauses_preserve_scalar_and_nested_semantics() {
        let (filter, _) = query_filter(&json!({"bool":{"must_not":{
            "bool":{"must_not":{"term":{"status":"deleted"}}}
        }}}))
        .unwrap();
        assert_eq!(
            filter.unwrap(),
            json!({"must":[{"must_not":[{"must_not":[
                {"key":"status","match":{"value":"deleted"}}
            ]}]}]})
        );

        let (filter, _) = query_filter(&json!({"bool":{
            "should":{"bool":{"must_not":{"term":{"status":"deleted"}}}},
            "minimum_should_match":1
        }}))
        .unwrap();
        assert_eq!(
            filter.unwrap(),
            json!({"must":[{"min_should":{"conditions":[{"must_not":[
                {"key":"status","match":{"value":"deleted"}}
            ]}],"min_count":1}}]})
        );

        let (filter, _) = query_filter(&json!({"bool":{"must_not":{"bool":{"must":[
            {"term":{"status":"deleted"}},
            {"term":{"tenant":"internal"}}
        ]}}}}))
        .unwrap();
        assert_eq!(
            filter.unwrap(),
            json!({"must":[{"must_not":[{"must":[
                {"key":"status","match":{"value":"deleted"}},
                {"key":"tenant","match":{"value":"internal"}}
            ]}]}]})
        );
    }

    #[test]
    fn approximate_text_bool_should_is_optional_beside_must() {
        let source = json!({"name":"red backpack","description":"plain canvas"});
        assert!(text_query_matches(
            &source,
            &json!({"bool":{
                "must":{"match":{"name":"red"}},
                "should":{"match":{"description":"waterproof"}}
            }})
        ));
    }

    #[test]
    fn text_queries_accept_dis_max_prefix_and_fuzzy_shapes() {
        let (text, fields) = pick_text(&json!({"dis_max":{"queries":[
            {"match_bool_prefix":{"name":{"query":"back","boost":1.2}}},
            {"fuzzy":{"description":{"value":"backpack","fuzziness":"AUTO"}}}
        ]}}))
        .unwrap();
        assert_eq!(text, "back");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "name");
    }

    #[test]
    fn post_filter_predicate_handles_catalogue_ranges() {
        let source = json!({"brand":"Acme","price":25,"stock":3});
        assert!(source_matches_query(
            &source,
            &json!({"bool":{"must":[
                {"term":{"brand":{"value":"Acme"}}},
                {"range":{"price":{"lt":50}}}
            ]}})
        ));
        assert!(!source_matches_query(
            &source,
            &json!({"range":{"price":{"gt":50}}})
        ));
    }

    #[test]
    fn post_filter_validation_rejects_unsupported_and_ambiguous_clauses() {
        for (query, feature) in [
            (json!({"match":{"title":"shoe"}}), "post_filter.match"),
            (
                json!({"term":{"brand":"Acme","status":"live"}}),
                "post_filter.term",
            ),
            (
                json!({"term":{"brand":{"case_insensitive":true,"value":"Acme"}}}),
                "post_filter.term",
            ),
            (
                json!({"bool":{"filter":{"range":{"price":{"gte":"10"}}}}}),
                "post_filter.bool.filter.range.gte",
            ),
            (
                json!({"bool":{"filter":{"term":{"brand":"Acme"}},"adjust_pure_negative":true}}),
                "post_filter.bool.adjust_pure_negative",
            ),
        ] {
            let error = validate_post_filter(&query).unwrap_err();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.body()["error"]["feature"], feature);
        }

        validate_post_filter(&json!({"bool":{
            "filter":[
                {"term":{"brand.keyword":{"value":"Acme"}}},
                {"terms":{"tags":["sale","featured"]}},
                {"range":{"price":{"gte":10,"lt":50}}},
                {"exists":{"field":"sku"}}
            ],
            "should":{"term":{"featured":true}},
            "minimum_should_match":"0%"
        }}))
        .unwrap();
    }

    #[tokio::test]
    async fn invalid_post_filter_fails_before_index_or_qdrant_lookup() {
        let cfg = Config::from_env();
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let error = search(
            State(state),
            Path("missing-index".into()),
            Json(json!({"post_filter":{"wildcard":{"sku":"ABC-*"}}})),
        )
        .await
        .unwrap_err();

        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.body()["error"]["feature"], "post_filter.wildcard");
    }

    #[tokio::test]
    async fn post_filter_does_not_narrow_gateway_computed_aggregations() {
        let mock = Router::new().fallback(any(|| async {
            Json(json!({"result":{"points":[
                {"payload":{"_es_id":"acme","_source":{"brand":"Acme","price":30}}},
                {"payload":{"_es_id":"beta-low","_source":{"brand":"Beta","price":10}}},
                {"payload":{"_es_id":"beta-high","_source":{"brand":"Beta","price":20}}}
            ],"next_page_offset":null}}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });

        let mut cfg = Config::from_env();
        cfg.qdrant_url = format!("http://{address}");
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products', '{}', '[]');")
            .unwrap();
        let state = AppState {
            qdrant: Qdrant {
                client: Client::new(),
                base: cfg.qdrant_url.clone(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(cfg.async_write_queue)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };

        let response = search(
            State(state),
            Path("products".into()),
            Json(json!({
                "size":3,
                "post_filter":{"term":{"brand":"Acme"}},
                "aggs":{
                    "lowest_price":{"min":{"field":"price"}},
                    "beta":{"filter":{"term":{"brand":"Beta"}}}
                }
            })),
        )
        .await
        .unwrap();
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        assert_eq!(body["hits"]["hits"].as_array().unwrap().len(), 1);
        assert_eq!(body["hits"]["hits"][0]["_id"], "acme");
        assert_eq!(body["aggregations"]["lowest_price"]["value"], 10.0);
        assert_eq!(body["aggregations"]["beta"]["doc_count"], 2);
        server.abort();
    }

    #[test]
    fn gateway_predicates_treat_arrays_as_multi_valued_fields() {
        let source = json!({
            "tags":["clearance", "seasonal"],
            "prices":[75, 25],
            "skus":["OLD-001", "ABC-100"]
        });

        assert!(source_matches_query(
            &source,
            &json!({"term":{"tags":"seasonal"}})
        ));
        assert!(source_matches_query(
            &source,
            &json!({"terms":{"tags":["featured", "clearance"]}})
        ));
        assert!(source_matches_query(
            &source,
            &json!({"range":{"prices":{"gte":20,"lt":50}}})
        ));
        assert!(!source_matches_query(
            &source,
            &json!({"range":{"prices":{"lt":20}}})
        ));

        for pattern in [
            GatewayPattern::Prefix {
                field: "skus".into(),
                prefix: "ABC-".into(),
            },
            GatewayPattern::Wildcard {
                field: "skus".into(),
                pattern: "ABC-*00".into(),
            },
            GatewayPattern::Regex {
                field: "skus".into(),
                pattern: "ABC-[12]00".into(),
            },
        ] {
            assert!(pattern_matches(&source, &pattern));
        }
    }

    #[test]
    fn post_filter_exists_rejects_missing_null_and_empty_arrays() {
        let query = json!({"exists":{"field":"brand"}});

        assert!(source_matches_query(&json!({"brand":""}), &query));
        assert!(source_matches_query(
            &json!({"brand":[null, "Acme"]}),
            &query
        ));
        assert!(!source_matches_query(&json!({}), &query));
        assert!(!source_matches_query(&json!({"brand":null}), &query));
        assert!(!source_matches_query(&json!({"brand":[]}), &query));
        assert!(!source_matches_query(&json!({"brand":[null]}), &query));
    }

    #[test]
    fn approximate_text_queries_match_prefixes_and_typos() {
        let source = json!({"name":"Red backpack"});
        assert!(text_query_matches(
            &source,
            &json!({"match_bool_prefix":{"name":{"query":"red back"}}})
        ));
        assert!(text_query_matches(
            &source,
            &json!({"fuzzy":{"name":{"value":"backpak","fuzziness":"AUTO"}}})
        ));
        assert!(!text_query_matches(
            &source,
            &json!({"match_bool_prefix":{"name":{"query":"blue"}}})
        ));
    }

    #[test]
    fn gateway_patterns_match_keyword_values() {
        let source = json!({"sku":"ABC-100"});
        assert!(pattern_matches(
            &source,
            &GatewayPattern::Prefix {
                field: "sku".into(),
                prefix: "ABC-".into()
            }
        ));
        assert!(pattern_matches(
            &source,
            &GatewayPattern::Wildcard {
                field: "sku".into(),
                pattern: "ABC-*00".into()
            }
        ));
        assert!(pattern_matches(
            &source,
            &GatewayPattern::Regex {
                field: "sku".into(),
                pattern: "ABC-[12]00".into()
            }
        ));
        let mut patterns = Vec::new();
        collect_patterns(&json!({"prefix":{"sku.keyword":"ABC-"}}), &mut patterns).unwrap();
        assert_eq!(patterns.len(), 1);
        assert!(pattern_matches(&source, &patterns[0]));
    }

    #[test]
    fn wildcard_translation_preserves_star_positions_and_escapes_literals() {
        let source = json!({"title":"Mechanical keyboard (quiet)"});

        for pattern in [
            "*keyboard*",
            "**keyboard**",
            "Mechanical?keyboard*",
            "*(quiet)",
        ] {
            assert!(pattern_matches(
                &source,
                &GatewayPattern::Wildcard {
                    field: "title".into(),
                    pattern: pattern.into(),
                }
            ));
        }
        assert!(!pattern_matches(
            &source,
            &GatewayPattern::Wildcard {
                field: "title".into(),
                pattern: "keyboard*".into(),
            }
        ));
    }

    #[test]
    fn keyword_multifield_aliases_use_the_base_payload_field() {
        let (filter, _) = query_filter(&json!({"bool":{"filter":[
            {"term":{"brand.keyword":"Acme"}},
            {"terms":{"tags.keyword":["sale", "featured"]}},
            {"exists":{"field":"sku.keyword"}},
            {"range":{"price.keyword":{"gte":10}}}
        ]}}))
        .unwrap();
        let clauses = filter.unwrap()["must"].as_array().unwrap().clone();

        assert_eq!(clauses[0]["key"], "brand");
        assert_eq!(clauses[1]["key"], "tags");
        assert_eq!(clauses[2]["must_not"][0]["is_empty"]["key"], "sku");
        assert_eq!(clauses[3]["key"], "price");

        let source = json!({
            "brand":"Acme",
            "tags":["featured"],
            "sku":"ABC-100",
            "price":25
        });
        assert!(source_matches_query(
            &source,
            &json!({"bool":{"filter":[
                {"term":{"brand.keyword":"Acme"}},
                {"terms":{"tags.keyword":["sale", "featured"]}},
                {"exists":{"field":"sku.keyword"}},
                {"range":{"price.keyword":{"gte":10}}}
            ]}})
        ));
    }
}
