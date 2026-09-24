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
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
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
        }
    }
}

#[derive(Clone)]
struct AppState {
    cfg: Config,
    qdrant: Qdrant,
    db: Arc<Mutex<Connection>>,
    analytics: Arc<Mutex<Analytics>>,
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
            .map_err(|e| GatewayError::upstream(e.to_string()))?;
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

    fn spawn_request(&self, method: Method, path: String, body: Value) {
        let qdrant = self.clone();
        tokio::spawn(async move {
            let _ = qdrant.request(method, &path, Some(body)).await;
        });
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
        (self.status(), Json(self.body())).into_response()
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
    db.execute_batch("CREATE TABLE IF NOT EXISTS indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE IF NOT EXISTS aliases(alias TEXT PRIMARY KEY, index_name TEXT NOT NULL);")?;
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
fn get_index(state: &AppState, name: &str) -> Result<(String, Value, Vec<String>), GatewayError> {
    let db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let mut stmt = db
        .prepare("SELECT mapping, vectors FROM indices WHERE name=?1")
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let row = stmt
        .query_row(params![name], |r| {
            let m: String = r.get(0)?;
            let v: String = r.get(1)?;
            Ok((m, v))
        })
        .map_err(|_| GatewayError::NotFound(format!("no such index [{name}]")))?;
    let vectors =
        serde_json::from_str(&row.1).map_err(|e| GatewayError::Internal(e.to_string()))?;
    Ok((
        collection(name),
        serde_json::from_str(&row.0).map_err(|e| GatewayError::Internal(e.to_string()))?,
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
            let schema = match spec.get("type").and_then(Value::as_str) {
                Some("keyword") => Some("keyword"),
                Some("boolean") => Some("bool"),
                Some("byte" | "short" | "integer" | "long") => Some("integer"),
                Some("float" | "double") => Some("float"),
                Some("date") => Some("datetime"),
                _ => None,
            };
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
    get_index(&state, &index)?;
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
    let db = state
        .db
        .lock()
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    db.execute("DELETE FROM indices WHERE name=?1", params![index])
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    Ok(es_ok(json!({"acknowledged":true})))
}

async fn get_index_info(
    State(state): State<AppState>,
    Path(index): Path<String>,
) -> Result<Response, GatewayError> {
    let (_, mapping, _) = get_index(&state, &index)?;
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

async fn write_docs(
    State(state): State<AppState>,
    index: String,
    docs: Vec<(String, Value)>,
) -> Result<Response, GatewayError> {
    let (coll, mapping, vectors) = get_index(&state, &index)?;
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
    write_docs(State(state), index, vec![(id, source)]).await
}

async fn get_doc(
    State(state): State<AppState>,
    Path((index, id)): Path<(String, String)>,
) -> Result<Response, GatewayError> {
    let (coll, _, _) = get_index(&state, &index)?;
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
    let Ok((coll, _, _)) = get_index(&state, &index) else {
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
    let (coll, _, _) = get_index(&state, &index)?;
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
    let doc = body
        .get("doc")
        .ok_or_else(|| GatewayError::bad("body.doc", "_update requires a doc object"))?;
    if !doc.is_object() {
        return Err(GatewayError::bad("body.doc", "doc must be an object"));
    }
    let (coll, mapping, vectors) = get_index(&state, &index)?;
    let current = retrieve_source(&state, &index, &coll, &id).await?;
    let Some(source) = current else {
        if body
            .get("doc_as_upsert")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            write_doc(
                State(state),
                Path((index.clone(), id.clone())),
                Json(doc.clone()),
            )
            .await?;
            return Ok(es_response(
                StatusCode::CREATED,
                json!({"_index":index,"_id":id,"_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}),
            ));
        }
        return Err(GatewayError::DocumentNotFound { index, id });
    };
    if state.cfg.document_projection {
        let point = point_id(&index, &id);
        let mut obj = source.as_object().cloned().unwrap_or_default();
        for (k, v) in doc.as_object().unwrap() {
            obj.insert(k.clone(), v.clone());
        }
        let merged = Value::Object(obj);
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
            "payload": doc,
            "points": [point_id(&index, &id)],
            "key": "_source",
            "wait": !state.cfg.async_payload_writes
        });
        if state.cfg.async_payload_writes {
            state.qdrant.spawn_request(Method::POST, path, body);
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
    let mut obj = source.as_object().cloned().unwrap_or_default();
    for (k, v) in doc.as_object().unwrap() {
        obj.insert(k.clone(), v.clone());
    }
    write_doc(State(state), Path((index, id)), Json(Value::Object(obj))).await
}

fn term_condition(field: &str, v: &Value) -> Value {
    let value = v.get("value").unwrap_or(v);
    json!({"key":field,"match":{"value":value}})
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
                        field: field.clone(),
                        pattern: pattern.into(),
                    },
                    "prefix" => GatewayPattern::Prefix {
                        field: field.clone(),
                        prefix: pattern.into(),
                    },
                    _ => GatewayPattern::Wildcard {
                        field: field.clone(),
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

fn sortable_source_field<'a>(source: &'a Value, field: &str) -> Option<&'a Value> {
    source_field(source, field.strip_suffix(".keyword").unwrap_or(field))
}

fn wildcard_regex(pattern: &str) -> Result<Regex, GatewayError> {
    let mut expression = String::from("^");
    for piece in pattern.split('*') {
        if !expression.ends_with('^') {
            expression.push_str(".*");
        }
        let mut escaped = String::new();
        for c in piece.chars() {
            if c == '?' {
                escaped.push('.');
            } else {
                escaped.push_str(&regex::escape(&c.to_string()));
            }
        }
        expression.push_str(&escaped);
    }
    expression.push('$');
    Regex::new(&expression).map_err(|e| GatewayError::Internal(e.to_string()))
}

fn pattern_matches(source: &Value, pattern: &GatewayPattern) -> bool {
    match pattern {
        GatewayPattern::Regex { field, pattern } => source_field(source, field)
            .and_then(Value::as_str)
            .is_some_and(|value| Regex::new(pattern).is_ok_and(|r| r.is_match(value))),
        GatewayPattern::Prefix { field, prefix } => source_field(source, field)
            .and_then(Value::as_str)
            .is_some_and(|value| value.starts_with(prefix)),
        GatewayPattern::Wildcard { field, pattern } => source_field(source, field)
            .and_then(Value::as_str)
            .is_some_and(|value| wildcard_regex(pattern).is_ok_and(|r| r.is_match(value))),
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
            source_field(source, field.strip_suffix(".keyword").unwrap_or(field))
                .is_some_and(|actual| actual == value.get("value").unwrap_or(value))
        });
    }
    if let Some(terms) = object.get("terms").and_then(Value::as_object) {
        return terms.iter().next().is_some_and(|(field, values)| {
            let Some(values) = values.as_array() else {
                return false;
            };
            source_field(source, field.strip_suffix(".keyword").unwrap_or(field))
                .is_some_and(|actual| values.iter().any(|value| value == actual))
        });
    }
    if let Some(range) = object.get("range").and_then(Value::as_object) {
        return range.iter().next().is_some_and(|(field, bounds)| {
            let Some(actual) = source_field(source, field).and_then(Value::as_f64) else {
                return false;
            };
            bounds.as_object().is_some_and(|bounds| {
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
        });
    }
    if let Some(exists) = object.get("exists") {
        return exists
            .get("field")
            .and_then(Value::as_str)
            .is_some_and(|field| source_field(source, field).is_some());
    }
    if let Some(bool_query) = object.get("bool").and_then(Value::as_object) {
        let clauses = |key: &str| -> Vec<&Value> {
            match bool_query.get(key) {
                Some(value) => value
                    .as_array()
                    .map(|values| values.iter().collect())
                    .unwrap_or_else(|| vec![value]),
                None => Vec::new(),
            }
        };
        let must = clauses("must")
            .into_iter()
            .chain(clauses("filter"))
            .all(|clause| source_matches_query(source, clause));
        let must_not = clauses("must_not")
            .into_iter()
            .all(|clause| !source_matches_query(source, clause));
        let should = clauses("should");
        let minimum = bool_query
            .get("minimum_should_match")
            .and_then(Value::as_u64)
            .unwrap_or(if should.is_empty() { 0 } else { 1 }) as usize;
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
                    must.push(json!({"key":f,"match":{"any":arr}}));
                }
                "exists" => {
                    let f = v
                        .get("field")
                        .and_then(Value::as_str)
                        .ok_or_else(|| GatewayError::bad("exists", "exists requires field"))?;
                    must.push(json!({"is_empty":{"key":f,"strict":false}}));
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
                    must.push(json!({"key":f,"range":range}));
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
                    if let Some(a) = b.get("must_not").and_then(Value::as_array) {
                        for c in a {
                            let mut cm = Vec::new();
                            let mut ct = Vec::new();
                            walk(c, &mut cm, &mut Vec::new(), &mut ct, _sorts)?;
                            must_not.extend(cm);
                            if !ct.is_empty() {
                                return Err(GatewayError::bad(
                                    "query.bool.must_not",
                                    "text must_not is not supported safely",
                                ));
                            }
                        }
                    }
                    if let Some(clauses) = b.get("should").and_then(Value::as_array) {
                        if b.get("minimum_should_match").is_none()
                            && (b.get("must").is_some() || b.get("filter").is_some())
                        {
                            return Err(GatewayError::bad("query.bool.should", "should with must/filter requires minimum_should_match for unambiguous Qdrant translation"));
                        }
                        let minimum = b
                            .get("minimum_should_match")
                            .and_then(|value| {
                                value.as_u64().or_else(|| {
                                    value.as_str().and_then(|text| {
                                        text.strip_suffix('%').and_then(|number| {
                                            number.parse::<u64>().ok().map(|percent| {
                                                (clauses.len() as u64 * percent).div_ceil(100)
                                            })
                                        })
                                    })
                                })
                            })
                            .unwrap_or(1)
                            .min(clauses.len() as u64);
                        let mut should = Vec::new();
                        for clause in clauses {
                            let mut cm = Vec::new();
                            let mut ct = Vec::new();
                            walk(clause, &mut cm, &mut Vec::new(), &mut ct, _sorts)?;
                            if !ct.is_empty() {
                                return Err(GatewayError::bad(
                                    "query.bool.should",
                                    "text should clauses are not supported safely",
                                ));
                            }
                            if cm.len() == 1 {
                                should.push(cm.remove(0));
                            } else {
                                should.push(json!({"must":cm}));
                            }
                        }
                        must.push(json!({"min_should":{"conditions":should,"min_count":minimum}}));
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
    let (coll, _, _) = get_index(state, index)?;
    let source = if state.cfg.document_projection {
        retrieve_sources(state, index, &[point_id(index, id)])
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
                    point_id(index, id)
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

fn words(value: &str) -> Vec<String> {
    value
        .split_whitespace()
        .map(|word| word.to_lowercase())
        .collect()
}

fn field_text(source: &Value, field: &str) -> String {
    source_field(source, field.strip_suffix(".keyword").unwrap_or(field))
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
        let clauses = |key: &str| {
            bool_query
                .get(key)
                .map(|value| {
                    value
                        .as_array()
                        .cloned()
                        .unwrap_or_else(|| vec![value.clone()])
                })
                .unwrap_or_default()
        };
        let must = clauses("must")
            .into_iter()
            .chain(clauses("filter"))
            .all(|clause| text_query_matches(source, &clause));
        let must_not = clauses("must_not")
            .into_iter()
            .all(|clause| !text_query_matches(source, &clause));
        let should = clauses("should");
        let minimum = bool_query
            .get("minimum_should_match")
            .and_then(Value::as_u64)
            .unwrap_or(if should.is_empty() { 0 } else { 1 }) as usize;
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

async fn search(
    State(state): State<AppState>,
    Path(index): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    let started = Instant::now();
    let (coll, _, vectors) = get_index(&state, &index)?;
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
    let from = body.get("from").and_then(Value::as_u64).unwrap_or(0);
    let size = body
        .get("size")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .min(state.cfg.max_page_size);
    if from > 10000 {
        return Err(GatewayError::bad(
            "from",
            "deep pagination is capped at 10000; use search_after in a future release",
        ));
    }
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
            let mut req = json!({"query":{"text":text,"model":"qdrant/bm25"},"using":vecname,"limit":(from+size).max(1),"with_payload":{"include":payload_fields}});
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
            let mut req = json!({"limit":if patterns.is_empty() { from + size } else { state.cfg.max_page_size },"with_payload":{"include":payload_fields}});
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
    if let Some(post_filter) = body.get("post_filter") {
        hits.retain(|hit| {
            hit.get("_source")
                .is_some_and(|source| source_matches_query(source, post_filter))
        });
    }
    let sort_specs = body
        .get("sort")
        .and_then(Value::as_array)
        .map(|sort| {
            sort.iter()
                .map(|s| {
                    s.as_object()
                        .and_then(|o| o.iter().next())
                        .map(|(field, value)| {
                            (field.clone(), value.as_str().unwrap_or("asc").to_string())
                        })
                        .unwrap_or(("_score".into(), "desc".into()))
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(sort) = body.get("sort").and_then(Value::as_array) {
        for s in sort.iter().rev() {
            let (field, dir) = s
                .as_object()
                .and_then(|o| o.iter().next())
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("asc").to_string()))
                .unwrap_or(("_score".into(), "desc".into()));
            hits.sort_by(|a, b| {
                let av = a
                    .get("_source")
                    .and_then(|x| sortable_source_field(x, &field))
                    .cloned()
                    .unwrap_or(Value::Null);
                let bv = b
                    .get("_source")
                    .and_then(|x| sortable_source_field(x, &field))
                    .cloned()
                    .unwrap_or(Value::Null);
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
    if let Some(collapse) = body.get("collapse") {
        let requested_field = collapse
            .get("field")
            .and_then(Value::as_str)
            .ok_or_else(|| GatewayError::bad("collapse.field", "collapse requires field"))?;
        let field = requested_field
            .strip_suffix(".keyword")
            .unwrap_or(requested_field);
        let mut groups: Vec<(String, Vec<Value>)> = Vec::new();
        for hit in hits {
            let key = hit
                .get("_source")
                .and_then(|source| source_field(source, field))
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
                if let Some(inner) = collapse.get("inner_hits").and_then(Value::as_object) {
                    let mut inner_hits = Map::new();
                    for (name, spec) in inner {
                        let requested_size = spec.get("size").and_then(Value::as_u64).unwrap_or(3) as usize;
                        let selected = group.iter().take(requested_size).cloned().collect::<Vec<_>>();
                        inner_hits.insert(name.clone(), json!({"hits":{"total":{"value":group.len(),"relation":"eq"},"max_score":selected.iter().filter_map(|hit| hit.get("_score").and_then(Value::as_f64)).fold(0.0,f64::max),"hits":selected}}));
                    }
                    primary["inner_hits"] = Value::Object(inner_hits);
                }
                primary
            })
            .collect();
    }
    let total = hits.len();
    if let Some(cursor) = body.get("search_after").and_then(Value::as_array) {
        if sort_specs.is_empty() || cursor.len() != sort_specs.len() {
            return Err(GatewayError::bad(
                "search_after",
                "search_after must contain one value for every sort field",
            ));
        }
        let mut after = false;
        hits.retain(|hit| {
            if after {
                return true;
            }
            for ((field, direction), cursor_value) in sort_specs.iter().zip(cursor) {
                let hit_value = if field == "_score" {
                    hit.get("_score").cloned().unwrap_or(Value::Null)
                } else if field == "_id" {
                    hit.get("_id").cloned().unwrap_or(Value::Null)
                } else {
                    hit.get("_source")
                        .and_then(|source| sortable_source_field(source, field))
                        .cloned()
                        .unwrap_or(Value::Null)
                };
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
            let current = h.get("_source").cloned().unwrap_or_else(|| json!({}));
            if !sort_specs.is_empty() {
                let values = sort_specs
                    .iter()
                    .map(|(field, _)| {
                        if field == "_score" {
                            h.get("_score").cloned().unwrap_or(Value::Null)
                        } else if field == "_id" {
                            h.get("_id").cloned().unwrap_or(Value::Null)
                        } else {
                            sortable_source_field(&current, field)
                                .cloned()
                                .unwrap_or(Value::Null)
                        }
                    })
                    .collect::<Vec<_>>();
                h["sort"] = Value::Array(values);
            }
            match project_source(&current, source) {
                Some(projected) => {
                    h["_source"] = projected;
                }
                None => {
                    h.as_object_mut().unwrap().remove("_source");
                }
            }
            h
        })
        .collect::<Vec<_>>();
    let mut response = json!({"took":started.elapsed().as_millis(),"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":total,"relation":"eq"},"max_score":page.iter().filter_map(|h|h.get("_score").and_then(Value::as_f64)).fold(0.0,f64::max),"hits":page}});
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
                let field = metric.get("field").and_then(Value::as_str).ok_or_else(|| {
                    GatewayError::bad(
                        format!("aggs.{name}.metric.field"),
                        "metric aggregation requires field",
                    )
                })?;
                let mut values = hits
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
                let mut buckets = Map::new();
                for (key, filter_query) in filters {
                    let count = hits
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
                let count = hits
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
    let (coll, _, _) = get_index(&state, &index)?;
    let query = body
        .get("query")
        .cloned()
        .unwrap_or_else(|| json!({"match_all": {}}));
    if pick_text(&query).is_none() {
        let (filter, _) = query_filter(&query)?;
        let mut request = json!({"exact":true});
        if let Some(filter) = filter {
            request["filter"] = filter;
        }
        let result = state
            .qdrant
            .request(
                Method::POST,
                &format!("/collections/{coll}/points/count"),
                Some(request),
            )
            .await?;
        let count = result
            .get("result")
            .and_then(|r| r.get("count"))
            .cloned()
            .unwrap_or_else(|| json!(0));
        return Ok(es_ok(
            json!({"count":count,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}),
        ));
    }
    let mut b = body;
    b["size"] = json!(10000);
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

fn bulk_response_has_errors(items: &[Value]) -> bool {
    items.iter().any(|item| {
        item.as_object()
            .is_some_and(|actions| actions.values().any(|result| result.get("error").is_some()))
    })
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
        let (kind, meta) = action
            .as_object()
            .and_then(|o| o.iter().next())
            .ok_or_else(|| GatewayError::bad("_bulk.action", "invalid action"))?;
        let obj = meta.as_object().cloned().unwrap_or_default();
        let idx = obj
            .get("_index")
            .and_then(Value::as_str)
            .map(String::from)
            .or(default.clone())
            .ok_or_else(|| GatewayError::bad("_bulk", "each action needs _index"))?;
        let id = obj
            .get("_id")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
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
        if kind == "index" || kind == "create" {
            let batch_index = idx.clone();
            let mut batch = Vec::new();
            let mut end = cursor;
            while end < actions.len()
                && (actions[end].0 == "index" || actions[end].0 == "create")
                && actions[end].1 == batch_index
            {
                batch.push((
                    actions[end].2.clone(),
                    actions[end].3.clone().unwrap_or_else(|| json!({})),
                ));
                end += 1;
            }
            let result = write_docs(State(state.clone()), batch_index.clone(), batch).await;
            for action in &actions[cursor..end] {
                match &result {
                    Ok(_) => items.push(json!({action.0.clone():{"_index":action.1,"_id":action.2,"status":201}})),
                    Err(e) => items.push(json!({action.0.clone():{"_index":action.1,"_id":action.2,"status":e.status().as_u16(),"error":e.body()["error"].clone()}})),
                }
            }
            cursor = end;
            continue;
        }
        let result = match kind.as_str() {
            "delete" => delete_doc(State(state.clone()), Path((idx.clone(), id.clone())))
                .await
                .map(|_| json!({"delete":{"_index":idx,"_id":id,"status":200}})),
            "update" => update_doc(
                State(state.clone()),
                Path((idx.clone(), id.clone())),
                Json(source.clone().unwrap_or_else(|| json!({}))),
            )
            .await
            .map(|response| {
                json!({"update":{"_index":idx,"_id":id,"status":response.status().as_u16()}})
            }),
            _ => Err(GatewayError::bad(
                format!("_bulk.{kind}"),
                "unsupported bulk action",
            )),
        };
        match result { Ok(v) => items.push(v), Err(e) => items.push(json!({kind.clone():{"_index":idx,"_id":id,"status":e.status().as_u16(),"error":e.body()["error"].clone()}})) }
        cursor += 1;
    }
    let errors = bulk_response_has_errors(&items);
    Ok(es_ok(json!({"took":0,"errors":errors,"items":items})))
}

async fn aliases(
    State(state): State<AppState>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    for action in body
        .get("actions")
        .and_then(Value::as_array)
        .ok_or_else(|| GatewayError::bad("_aliases", "actions must be an array"))?
    {
        let o = action
            .as_object()
            .ok_or_else(|| GatewayError::bad("_aliases", "action must be an object"))?;
        if let Some(a) = o.get("add") {
            let alias = a
                .get("alias")
                .and_then(Value::as_str)
                .ok_or_else(|| GatewayError::bad("_aliases.add.alias", "missing alias"))?;
            let idx = a
                .get("index")
                .and_then(Value::as_str)
                .ok_or_else(|| GatewayError::bad("_aliases.add.index", "missing index"))?;
            let db = state.db.lock().unwrap();
            db.execute(
                "INSERT OR REPLACE INTO aliases(alias,index_name) VALUES (?1,?2)",
                params![alias, idx],
            )
            .map_err(|e| GatewayError::Internal(e.to_string()))?;
        } else if let Some(a) = o.get("remove") {
            let alias = a.get("alias").and_then(Value::as_str).unwrap_or("");
            let db = state.db.lock().unwrap();
            db.execute("DELETE FROM aliases WHERE alias=?1", params![alias])
                .map_err(|e| GatewayError::Internal(e.to_string()))?;
        }
    }
    Ok(es_ok(json!({"acknowledged":true})))
}
async fn alias_get(
    State(state): State<AppState>,
    Path(alias): Path<String>,
) -> Result<Response, GatewayError> {
    let db = state.db.lock().unwrap();
    let _idx: String = db
        .query_row(
            "SELECT index_name FROM aliases WHERE alias=?1",
            params![&alias],
            |r| r.get(0),
        )
        .map_err(|_| GatewayError::NotFound(format!("no such alias [{alias}]")))?;
    Ok(es_ok(json!({alias.clone():{"aliases":{alias:{}}}})))
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
    let (_, mut m, _) = get_index(&state, &index)?;
    if method == Method::PUT {
        let b = body.map(|j| j.0).unwrap_or(json!({}));
        let (nm, _) = mapping_vectors(&b);
        let db = state.db.lock().unwrap();
        db.execute(
            "UPDATE indices SET mapping=?1 WHERE name=?2",
            params![nm.to_string(), index],
        )
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
        m = nm;
    }
    Ok(es_ok(json!({index:{"mappings":m}})))
}

fn is_bulk_request(method: &Method, path: &str) -> bool {
    method == Method::POST && (path == "/_bulk" || path.ends_with("/_bulk"))
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
                dispatch(
                    state,
                    method,
                    uri.path().to_string(),
                    headers,
                    String::from_utf8_lossy(&bytes).into_owned(),
                )
                .await
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
    let state = AppState {
        qdrant: Qdrant {
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(180))
                .tcp_nodelay(true)
                .pool_max_idle_per_host(128)
                .build()?,
            base: cfg.qdrant_url.clone(),
            key: cfg.qdrant_api_key.clone(),
        },
        db: Arc::new(Mutex::new(init_db()?)),
        analytics: Arc::new(Mutex::new(Analytics::default())),
        cfg,
    };
    let app = gateway_router(state.clone());
    let addr: SocketAddr = state.cfg.listen_addr.parse()?;
    info!(%addr, "starting qdrant-es-gateway");
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
        assert_eq!(
            request_body_limit(&cfg, &Method::POST, "/products/_bulk"),
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
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
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
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
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
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
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
    async fn missing_document_update_returns_404_unless_doc_as_upsert_is_set() {
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
                },
                db: Arc::new(Mutex::new(db)),
                analytics: Arc::new(Mutex::new(Analytics::default())),
                cfg,
            };

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
        }
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
    fn query_filter_accepts_java_range_and_object_terms() {
        let (filter, _) = query_filter(&json!({"bool":{"must":[
            {"term":{"brand.keyword":{"value":"Acme","boost":1.0}}},
            {"range":{"price":{"from":10,"to":50,"include_lower":true,"include_upper":false,"boost":1.0}}}
        ]}})).unwrap();
        let filter = filter.unwrap();
        assert_eq!(filter["must"][0]["match"]["value"], "Acme");
        assert_eq!(filter["must"][1]["range"]["gte"], 10);
        assert_eq!(filter["must"][1]["range"]["lt"], 50);
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
        collect_patterns(&json!({"prefix":{"sku":"ABC-"}}), &mut patterns).unwrap();
        assert_eq!(patterns.len(), 1);
    }
}
