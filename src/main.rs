use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use futures_util::future::try_join_all;
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
use tracing::info;

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
                "Qdrant returned {}: {}",
                status, value
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
    fn status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Bad { .. } => StatusCode::BAD_REQUEST,
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

fn es_ok(body: Value) -> Response {
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        "x-elastic-product",
        HeaderValue::from_static("Elasticsearch"),
    );
    response
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
        .map_err(|_| GatewayError::NotFound(format!("no such index [{}]", name)))?;
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

fn source_props(source: &Value) -> Map<String, Value> {
    source.as_object().cloned().unwrap_or_default()
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
    {
        let db = state
            .db
            .lock()
            .map_err(|e| GatewayError::Internal(e.to_string()))?;
        db.execute(
            "INSERT OR REPLACE INTO indices(name,mapping,vectors) VALUES (?1,?2,?3)",
            params![
                index,
                mapping.to_string(),
                serde_json::to_string(&vectors).unwrap()
            ],
        )
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    }
    let mut sparse = Map::new();
    for v in &vectors {
        sparse.insert(v.clone(), json!({}));
    }
    state
        .qdrant
        .request(
            Method::PUT,
            &format!("/collections/{}", collection(&index)),
            Some(json!({"sparse_vectors": sparse})),
        )
        .await?;
    if state.cfg.document_projection {
        state
            .qdrant
            .request(
                Method::PUT,
                &format!("/collections/{}", document_collection(&index)),
                // Qdrant 1.15 requires a vector field on each point. A
                // one-dimensional on-disk vector with m=0 keeps this
                // collection payload-first and disables HNSW construction.
                Some(json!({"vectors":{"size":1,"distance":"Dot","on_disk":true},"hnsw_config":{"m":0},"optimizers_config":{"indexing_threshold":0}})),
            )
            .await?;
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
                state
                    .qdrant
                    .request(
                        Method::PUT,
                        &format!("/collections/{}/index", collection(&index)),
                        Some(json!({"field_name":field,"field_schema":schema,"wait":true})),
                    )
                    .await?;
            }
        }
    }
    Ok(es_ok(
        json!({"acknowledged":true,"shards_acknowledged":true,"index":index}),
    ))
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

fn qdrant_payload(source: &Value, id: &str, index: &str) -> Value {
    let mut p = source_props(source);
    p.insert("_es_id".into(), json!(id));
    p.insert("_es_index".into(), json!(index));
    p.insert("_source".into(), source.clone());
    Value::Object(p)
}

fn source_point(source: &Value, id: &str, index: &str) -> Value {
    json!({"id":point_id(index,id),"vector":[0.0],"payload":qdrant_payload(source,id,index)})
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

async fn write_docs(
    State(state): State<AppState>,
    index: String,
    docs: Vec<(String, Value)>,
) -> Result<Response, GatewayError> {
    let (coll, _, vectors) = get_index(&state, &index)?;
    let points = docs.iter().map(|(id, source)| {
        let texts = flatten_text(source, &vectors);
        let mut vector = Map::new();
        for (name, text) in texts { vector.insert(name, json!({"text":text,"model":"qdrant/bm25"})); }
        json!({"id":point_id(&index,id),"vector":vector,"payload":qdrant_payload(source,id,&index)})
    }).collect::<Vec<_>>();
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
            &format!("/collections/{}/points", coll),
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
        return Ok(es_ok(match sources.get(&id) {
            Some(source) => {
                json!({"_index":index,"_id":id,"found":true,"_source":source,"_version":1})
            }
            None => json!({"_index":index,"_id":id,"found":false}),
        }));
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
        return Ok(es_ok(json!({"_index":index,"_id":id,"found":false})));
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
    let path = format!("/collections/{}/points/delete", coll);
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
    if state.cfg.document_projection {
        let point = point_id(&index, &id);
        let current = retrieve_sources(&state, &index, std::slice::from_ref(&point)).await?;
        let source = current.get(&id).cloned().unwrap_or_else(|| json!({}));
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
            let mut payload = doc.as_object().cloned().unwrap_or_default();
            payload.insert("_source".into(), merged);
            state
                .qdrant
                .request(
                    Method::POST,
                    &format!("/collections/{}/points/payload", coll),
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
        let path = format!("/collections/{}/points/payload", coll);
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
    let source = p
        .get("result")
        .and_then(|r| r.get("payload"))
        .and_then(|p| p.get("_source"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let mut obj = source.as_object().cloned().unwrap_or_default();
    for (k, v) in doc.as_object().unwrap() {
        obj.insert(k.clone(), v.clone());
    }
    write_doc(State(state), Path((index, id)), Json(Value::Object(obj))).await
}

fn term_condition(field: &str, v: &Value) -> Value {
    json!({"key":field,"match":{"value":v}})
}
type FilterResult = (Option<Value>, Vec<(String, String)>);

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
                "match" | "match_phrase" => text.push(v.clone()),
                "multi_match" => text.push(v.clone()),
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
                            _ => {
                                return Err(GatewayError::bad(
                                    format!("range.{}", k),
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
                            .and_then(Value::as_u64)
                            .unwrap_or(1);
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
                        must.push(json!({"should":should,"min_should":minimum}));
                    }
                }
                "prefix" => {
                    return Err(GatewayError::bad(
                        "query.prefix",
                        "prefix queries are not supported in the safe MVP",
                    ))
                }
                other => {
                    return Err(GatewayError::bad(
                        format!("query.{}", other),
                        format!("{} queries are not supported", other),
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
        return Some((v.as_str()?.into(), vec![(f.clone(), 1.0)]));
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
    let query = body
        .get("query")
        .cloned()
        .unwrap_or_else(|| json!({"match_all":{}}));
    if body.get("search_after").is_some() {
        return Err(GatewayError::bad(
            "search_after",
            "search_after is not yet implemented; use from/size within the configured page limit",
        ));
    }
    let (filter, _) = query_filter(&query)?;
    let text = pick_text(&query);
    let has_text = text.is_some();
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
    if let Some((text, fields)) = text {
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
            let payload_fields = if state.cfg.document_projection { json!(["_es_id"]) } else { json!(["_es_id","_source"]) };
            let mut req = json!({"query":{"text":text,"model":"qdrant/bm25"},"using":vecname,"limit":(from+size).max(1),"with_payload":{"include":payload_fields}});
            if let Some(f) = &filter {
                req["filter"] = f.clone();
            }
            let qdrant = state.qdrant.clone();
            let path = format!("/collections/{}/points/query", coll);
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
        let payload_fields = if state.cfg.document_projection {
            json!(["_es_id"])
        } else {
            json!(["_es_id", "_source"])
        };
        let mut req = json!({"limit":from+size,"with_payload":{"include":payload_fields}});
        if let Some(f) = filter {
            req["filter"] = f;
        }
        let r = state
            .qdrant
            .request(
                Method::POST,
                &format!("/collections/{}/points/scroll", coll),
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
                hits.push(json!({"_index":index,"_id":pay.get("_es_id").cloned().unwrap_or(json!("")),"_score":Value::Null,"_source":pay.get("_source").cloned().unwrap_or(json!({}))}));
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
                    .and_then(|x| x.get(&field))
                    .cloned()
                    .unwrap_or(Value::Null);
                let bv = b
                    .get("_source")
                    .and_then(|x| x.get(&field))
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
    let total = hits.len();
    let page = hits
        .into_iter()
        .skip(from as usize)
        .take(size as usize)
        .collect::<Vec<_>>();
    let source = body.get("_source");
    let page = page
        .into_iter()
        .map(|mut h| {
            let current = h.get("_source").cloned().unwrap_or_else(|| json!({}));
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
            let terms = spec.get("terms").ok_or_else(|| {
                GatewayError::bad(
                    format!("aggs.{}.terms", name),
                    "only terms aggregations are supported",
                )
            })?;
            let requested = terms.get("field").and_then(Value::as_str).ok_or_else(|| {
                GatewayError::bad(
                    format!("aggs.{}.terms.field", name),
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
                    &format!("/collections/{}/facet", coll),
                    Some(req),
                )
                .await?;
            let buckets = facet.get("result").and_then(|r| r.get("hits")).and_then(Value::as_array).cloned().unwrap_or_default().into_iter().map(|h| json!({"key":h.get("value").cloned().unwrap_or(Value::Null),"doc_count":h.get("count").cloned().unwrap_or(json!(0))})).collect::<Vec<_>>();
            agg_result.insert(
                name.clone(),
                json!({"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":buckets}),
            );
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
                &format!("/collections/{}/points/count", coll),
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

async fn bulk(
    State(state): State<AppState>,
    index: Option<Path<String>>,
    body: String,
) -> Result<Response, GatewayError> {
    if body.len() > state.cfg.max_bulk_bytes {
        return Err(GatewayError::bad(
            "_bulk",
            "bulk request exceeds MAX_BULK_BYTES",
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
            .map(|_| json!({"update":{"_index":idx,"_id":id,"status":200}})),
            _ => Err(GatewayError::bad(
                format!("_bulk.{}", kind),
                "unsupported bulk action",
            )),
        };
        match result { Ok(v) => items.push(v), Err(e) => items.push(json!({kind.clone():{"_index":idx,"_id":id,"status":e.status().as_u16(),"error":e.body()["error"].clone()}})) }
        cursor += 1;
    }
    Ok(es_ok(
        json!({"took":0,"errors":items.iter().any(|x|x.get("error").is_some()),"items":items}),
    ))
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
        .map_err(|_| GatewayError::NotFound(format!("no such alias [{}]", alias)))?;
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

async fn dispatch(
    state: AppState,
    method: Method,
    path: String,
    _headers: HeaderMap,
    body: String,
) -> Response {
    if body.len() > state.cfg.max_body_bytes {
        return GatewayError::bad("request", "request body exceeds MAX_BODY_BYTES").into_response();
    }
    if method == Method::POST && (path == "/_bulk" || path.ends_with("/_bulk")) {
        let default = path
            .strip_prefix('/')
            .and_then(|p| p.strip_suffix("/_bulk"))
            .map(|p| Path(p.to_string()));
        return bulk(State(state), default, body)
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
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
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
        return alias_get(State(state), Path(parts[1].into()))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[0] == "_alias" && method == Method::HEAD {
        return alias_head(State(state), Path(parts[1].into())).await;
    }
    if parts.len() == 1 && method == Method::PUT {
        return create_index(State(state), Path(parts[0].into()), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 1 && (method == Method::DELETE) {
        return delete_index(State(state), Path(parts[0].into()))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 1 && method == Method::HEAD {
        return head_index(State(state), Path(parts[0].into())).await;
    }
    if parts.len() == 1 && method == Method::GET {
        return get_index_info(State(state), Path(parts[0].into()))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2
        && parts[1] == "_mapping"
        && (method == Method::GET || method == Method::PUT)
    {
        return mapping(
            State(state),
            Path(parts[0].into()),
            method,
            Some(Json(json_body)),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::PUT {
        return write_doc(
            State(state),
            Path((parts[0].into(), parts[2].into())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_doc" && method == Method::POST {
        return write_doc(
            State(state),
            Path((parts[0].into(), uuid::Uuid::new_v4().to_string())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::GET {
        return get_doc(State(state), Path((parts[0].into(), parts[2].into())))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::HEAD {
        return head_doc(State(state), Path((parts[0].into(), parts[2].into()))).await;
    }
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::DELETE {
        return delete_doc(State(state), Path((parts[0].into(), parts[2].into())))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 3 && parts[1] == "_update" && method == Method::POST {
        return update_doc(
            State(state),
            Path((parts[0].into(), parts[2].into())),
            Json(json_body),
        )
        .await
        .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_bulk" && method == Method::POST {
        return bulk(State(state), Some(Path(parts[0].into())), body)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2
        && parts[1] == "_search"
        && (method == Method::GET || method == Method::POST)
    {
        return search(State(state), Path(parts[0].into()), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if parts.len() == 2 && parts[1] == "_count" && (method == Method::GET || method == Method::POST)
    {
        return count(State(state), Path(parts[0].into()), Json(json_body))
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    if path == "/_bulk" && method == Method::POST {
        return bulk(State(state), None, body)
            .await
            .unwrap_or_else(IntoResponse::into_response);
    }
    GatewayError::bad(
        "endpoint",
        format!("unsupported endpoint {} {}", method, path),
    )
    .into_response()
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
    let app = Router::new()
        .fallback(any(
            |State(state): State<AppState>,
             method: Method,
             uri: axum::http::Uri,
             headers: HeaderMap,
             body: Body| async move {
                let bytes = match axum::body::to_bytes(body, state.cfg.max_body_bytes).await {
                    Ok(b) => b,
                    Err(e) => {
                        return GatewayError::bad("request.body", e.to_string()).into_response()
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
        .with_state(state.clone());
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

    #[test]
    fn ids_are_deterministic_and_uuid_shaped() {
        let a = point_id("products", "普通話/very-long-id");
        assert_eq!(a, point_id("products", "普通話/very-long-id"));
        assert_eq!(a.len(), 36);
        assert_ne!(a, point_id("products", "other"));
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
    fn query_filter_supports_simple_should() {
        let (filter, _) = query_filter(
            &json!({"bool":{"should":[{"term":{"status":"live"}},{"term":{"status":"draft"}}]}}),
        )
        .unwrap();
        assert_eq!(filter.unwrap()["must"][0]["min_should"], 1);
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
}
