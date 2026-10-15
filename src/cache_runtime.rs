//! Runtime cache boundary. Tickets precede catalog snapshots, and persistent
//! writer tokens outlive cancellation or uncertainty in either backing system.
use super::*;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub(crate) struct CacheRuntime {
    pub(crate) cache: read_cache::ReadCache,
    pub(crate) serve_reads: bool,
    pub(crate) max_entry_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    status: u16,
    headers: Vec<(String, Vec<u8>)>,
    body: Value,
}

// Everything that may mutate is fenced, including unsupported/future write
// endpoints. POST exceptions are the existing read-only gateway handlers.
fn mutating(method: &Method, path: &str) -> bool {
    if *method == Method::GET || *method == Method::HEAD || *method == Method::OPTIONS {
        return false;
    }
    if *method != Method::POST {
        return true;
    }
    let Ok(parts) = decode_path_segments(path) else {
        return true;
    };
    !matches!(parts.as_slice(), [name] if name == "_msearch")
        && !matches!(parts.as_slice(), [_, name] if matches!(name.as_str(), "_search" | "_msearch" | "_count"))
}

fn search_route(path: &str) -> bool {
    decode_path_segments(path).is_ok_and(|parts| parts.len() == 2 && parts[1] == "_search")
}

impl CacheRuntime {
    pub(crate) async fn dispatch(
        &self,
        mut state: AppState,
        method: Method,
        path: String,
        query: Option<String>,
        headers: HeaderMap,
        body: String,
    ) -> Response {
        let started = Instant::now();
        if mutating(&method, &path) {
            let token = match self.cache.admit_writer().await {
                Ok(token) => token,
                Err(_) => {
                    return GatewayError::upstream(
                        "cache writer coordination unavailable or requires recovery",
                    )
                    .into_response()
                }
            };
            let completion = mutation_completion::MutationCompletion::default();
            state.qdrant.mutation_completion = Some(completion.clone());
            let outcome = dispatch_catalog(state, method, path, query, headers, body).await;
            if outcome.publication_complete && completion.is_complete() {
                if self.cache.release_writer(&token).await.is_err() {
                    return GatewayError::upstream("cache writer completion could not be recorded")
                        .into_response();
                }
            } else {
                warn!(
                    writer_token = token.id(),
                    "cache writer requires explicit reconciliation"
                );
            }
            // No destructor clears the token: cancellation or an uncertain
            // catalog/backend write leaves cached reads and fills fenced.
            return outcome.response;
        }
        let eligible = self.serve_reads
            && cache_request::eligible(&state.cfg, &method, &path, query.as_deref(), &body);
        if !eligible {
            return dispatch_catalog(state, method, path, query, headers, body)
                .await
                .response;
        }
        let uri = match &query {
            Some(query) => format!("{path}?{query}"),
            None => path.clone(),
        };
        let key = self
            .cache
            .request_key(method.as_str(), &uri, body.as_bytes());
        // This is before dispatch_catalog loads aliases/mappings. Never move the
        // ticket acquisition after that snapshot boundary.
        let lookup = self.cache.lookup(&key).await.ok();
        if let Some(bytes) = lookup.as_ref().and_then(|v| v.response.as_deref()) {
            if let Some(response) = decode(bytes, self.max_entry_bytes, &path, started) {
                if search_route(&path) && state.cfg.analytics {
                    if let Ok(mut analytics) = state.analytics.lock() {
                        analytics.requests += 1;
                        analytics.supported += 1;
                    }
                }
                return response;
            }
        }
        let response =
            if let Some(snapshot) = lookup.as_ref().and_then(|v| v.catalog_snapshot.as_deref()) {
                // The Lua miss returned this snapshot with its quiet epoch. Do not
                // issue a second catalog read, or acquire a replacement epoch later.
                let db = match shared_catalog::snapshot_db(snapshot) {
                    Ok(db) => db,
                    Err(error) => return error.into_response(),
                };
                state.db = Arc::new(Mutex::new(db));
                dispatch_inner(state, method, path, query, headers, body).await
            } else {
                // Busy/unavailable lookup or an invalid hit envelope retains the
                // ordinary authoritative-catalog path and any original read ticket.
                dispatch_catalog(state, method, path, query, headers, body)
                    .await
                    .response
            };
        let Some(ticket) = lookup.and_then(|v| v.ticket) else {
            return response;
        };
        // Current handlers create finite JSON bodies. Unknown-size or oversized
        // streams are returned intact; the cache never truncates their output.
        let exact = axum::body::HttpBody::size_hint(response.body()).exact();
        if response.status() != StatusCode::OK
            || exact.is_none_or(|n| n > self.max_entry_bytes as u64)
        {
            return response;
        }
        let (parts, body) = response.into_parts();
        let bytes = match body.collect().await {
            Ok(body) => body.to_bytes(),
            Err(_) => return GatewayError::upstream("response body unavailable").into_response(),
        };
        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
            let envelope = Envelope {
                version: 1,
                status: parts.status.as_u16(),
                headers: parts
                    .headers
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.as_bytes().to_vec()))
                    .collect(),
                body: value,
            };
            if let Ok(encoded) = serde_json::to_vec(&envelope) {
                if encoded.len() <= self.max_entry_bytes {
                    // Failed/colliding publication is only a missed cache fill.
                    let _ = self.cache.publish(&ticket, &key, &encoded).await;
                }
            }
        }
        Response::from_parts(parts, Body::from(bytes))
    }
}

fn decode(bytes: &[u8], limit: usize, path: &str, started: Instant) -> Option<Response> {
    if bytes.len() > limit {
        return None;
    }
    let mut envelope: Envelope = serde_json::from_slice(bytes).ok()?;
    if envelope.version != 1 || envelope.status != 200 || !envelope.body.is_object() {
        return None;
    }
    if search_route(path) {
        if !envelope.body.get("took")?.is_number()
            || !envelope.body.pointer("/hits/hits")?.is_array()
        {
            return None;
        }
        envelope.body["took"] = json!(started.elapsed().as_millis());
    } else if envelope.body.get("found")?.as_bool() != Some(true)
        || !envelope.body.get("_id")?.is_string()
    {
        return None;
    }
    let mut response = Response::new(Body::from(serde_json::to_vec(&envelope.body).ok()?));
    for (name, value) in envelope.headers {
        let name = axum::http::HeaderName::try_from(name).ok()?;
        // Recomputed JSON length/took must not retain the original length.
        if name == axum::http::header::CONTENT_LENGTH {
            continue;
        }
        if matches!(
            name.as_str(),
            "transfer-encoding" | "connection" | "upgrade"
        ) {
            return None;
        }
        response
            .headers_mut()
            .append(name, axum::http::HeaderValue::from_bytes(&value).ok()?);
    }
    if response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)?
        .to_str()
        .ok()?
        != "application/json"
    {
        return None;
    }
    Some(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_write_route_families_are_fenced() {
        assert!(!search_route("/idx/_doc/_search"));
        assert!(search_route("/idx/%5Fsearch"));
        for method in [Method::PUT, Method::DELETE, Method::PATCH, Method::POST] {
            for path in [
                "/idx",
                "/idx/_doc/a",
                "/idx/_create/a",
                "/idx/_update/a",
                "/_bulk",
                "/idx/_bulk",
                "/idx/_mapping",
                "/_aliases",
                "/new_future_route",
            ] {
                assert!(mutating(&method, path), "{method} {path}");
            }
        }
        for path in ["/idx/_search", "/idx/_count", "/idx/_msearch", "/_msearch"] {
            assert!(!mutating(&Method::POST, path));
        }
    }
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn atomic_miss_uses_snapshot_without_second_catalog_transport() {
        let (mut state, mut redis, key) = fixture().await;
        state.cfg.document_projection = false;
        redis::cmd("SET")
            .arg(&key)
            .arg(json!({"indices":[["items","{}","[]"]],"aliases":[]}).to_string())
            .query_async::<()>(&mut redis)
            .await
            .unwrap();
        // Any second catalog begin fails; the cache owns a separate live connection.
        state.shared_catalog = Some(shared_catalog::SharedCatalog::test_handle(
            redis::Client::open("redis://127.0.0.1:1").unwrap(),
            key.clone(),
        ));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        state.qdrant.base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(any(move || {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async {
                        Json(json!({"status":"ok","result":[{"payload":{"_source":{"n":1}}}]}))
                    }
                })),
            )
            .await
            .unwrap();
        });
        let response = dispatch(
            state.clone(),
            Method::GET,
            "/items/_doc/a".into(),
            None,
            HeaderMap::new(),
            String::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let cache = &state.cache_runtime.as_ref().unwrap().cache;
        let cachekey = cache.request_key("GET", "/items/_doc/b", b"");
        let lookup = cache.lookup(&cachekey).await.unwrap();
        assert!(lookup.response.is_none() && lookup.catalog_snapshot.is_some());
        cache
            .publish(&lookup.ticket.unwrap(), &cachekey, b"invalid envelope")
            .await
            .unwrap();
        let corrupt = dispatch(
            state.clone(),
            Method::GET,
            "/items/_doc/b".into(),
            None,
            HeaderMap::new(),
            String::new(),
        )
        .await;
        assert_eq!(
            corrupt.status(),
            StatusCode::BAD_GATEWAY,
            "invalid hit must use ordinary catalog path"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        server.abort();
        cleanup(&mut redis, &key).await;
    }

    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn atomic_miss_rejects_malformed_oversized_and_duplicate_catalogs_before_backend() {
        let (mut state, mut redis, key) = fixture().await;
        state.cfg.document_projection = false;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        state.qdrant.base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(any(move || {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async { Json(json!({"result":[]})) }
                })),
            )
            .await
            .unwrap();
        });
        let snapshots = [
            "invalid json".to_owned(),
            json!({"indices":[["items","{}","[]"],["items","{}","[]"]],"aliases":[]}).to_string(),
            json!({"indices":[["items","invalid mapping","[]"]],"aliases":[]}).to_string(),
            json!({"indices":[["items","{}","{}"]],"aliases":[]}).to_string(),
            json!({"indices":[["items","{}","[]"]],"aliases":[["alias","items"],["alias","items"]]}).to_string(),
            " ".repeat(shared_catalog::MAX_BYTES+1),
        ];
        for (i, snapshot) in snapshots.into_iter().enumerate() {
            redis::cmd("SET")
                .arg(&key)
                .arg(snapshot)
                .query_async::<()>(&mut redis)
                .await
                .unwrap();
            let response = dispatch(
                state.clone(),
                Method::GET,
                format!("/items/_doc/{i}"),
                None,
                HeaderMap::new(),
                String::new(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        server.abort();
        cleanup(&mut redis, &key).await;
    }

    #[tokio::test]
    async fn envelopes_preserve_big_integers_headers_and_reject_corruption() {
        let body = json!({"found":true,"_id":"one","_source":{"large":u64::MAX,"small":i64::MIN}});
        let envelope = Envelope {
            version: 1,
            status: 200,
            headers: vec![
                ("content-type".into(), b"application/json".to_vec()),
                ("x-elastic-product".into(), b"Elasticsearch".to_vec()),
            ],
            body: body.clone(),
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        let response = decode(&encoded, 4096, "/idx/_doc/one", Instant::now()).unwrap();
        assert_eq!(response.headers()["x-elastic-product"], "Elasticsearch");
        let decoded: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, body);
        assert!(decode(&encoded, 1, "/idx/_doc/one", Instant::now()).is_none());
        assert!(decode(b"{}", 4096, "/idx/_doc/one", Instant::now()).is_none());
        assert!(decode(&encoded, 4096, "/idx/_search", Instant::now()).is_none());
    }

    async fn fixture() -> (AppState, redis::aio::MultiplexedConnection, String) {
        let url = env::var("CATALOG_TEST_REDIS_URL").unwrap();
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut redis = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!("qeg:{{runtime-{}}}:v1", uuid::Uuid::new_v4().simple());
        redis::cmd("SET")
            .arg(&key)
            .arg(r#"{"indices":[],"aliases":[]}"#)
            .query_async::<()>(&mut redis)
            .await
            .unwrap();
        redis::cmd("HSET")
            .arg(format!("{key}:read-cache:control"))
            .arg("schema")
            .arg("1")
            .arg("deployment")
            .arg("runtime-test")
            .arg("epoch")
            .arg("0")
            .arg("state")
            .arg("active")
            .query_async::<()>(&mut redis)
            .await
            .unwrap();
        let cache = read_cache::ReadCache::connect(
            &url,
            &key,
            "runtime-test",
            b"runtime-tests",
            60,
            1024 * 1024,
        )
        .await
        .unwrap();
        let state = AppState {
            cfg: Config::from_env(),
            qdrant: Qdrant {
                client: Client::new(),
                base: "http://127.0.0.1:1".into(),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
                admin_mutation_started: None,
                mutation_completion: None,
            },
            db: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            shared_catalog: Some(shared_catalog::SharedCatalog::test_handle(
                client,
                key.clone(),
            )),
            cache_runtime: Some(CacheRuntime {
                cache,
                serve_reads: true,
                max_entry_bytes: 1024 * 1024,
            }),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
        };
        (state, redis, key)
    }
    async fn cleanup(redis: &mut redis::aio::MultiplexedConnection, key: &str) {
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{key}*"))
            .query_async(redis)
            .await
            .unwrap();
        if !keys.is_empty() {
            redis::cmd("DEL")
                .arg(keys)
                .query_async::<()>(redis)
                .await
                .unwrap();
        }
    }
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn malformed_requests_and_catalog_rejections_do_not_strand_writers() {
        let (state, mut redis, key) = fixture().await;
        let cache = state.cache_runtime.as_ref().unwrap().cache.clone();
        let response = dispatch(
            state.clone(),
            Method::PUT,
            "/%FF".into(),
            None,
            HeaderMap::new(),
            "{}".into(),
        )
        .await;
        assert!(!response.status().is_success());
        assert!(cache.status().await.unwrap().writer_tokens.is_empty());
        // Admission linearizes before another request obtains the admin claim.
        for path in ["/idx/_doc/a", "/idx"] {
            let token = cache.admit_writer().await.unwrap();
            let (_, claim) = state
                .shared_catalog
                .as_ref()
                .unwrap()
                .begin(true)
                .await
                .unwrap();
            let outcome = dispatch_catalog(
                state.clone(),
                Method::PUT,
                path.into(),
                None,
                HeaderMap::new(),
                "{}".into(),
            )
            .await;
            assert!(outcome.publication_complete);
            assert!(!outcome.response.status().is_success());
            cache.release_writer(&token).await.unwrap();
            state
                .shared_catalog
                .as_ref()
                .unwrap()
                .release(&claim.unwrap())
                .await
                .unwrap();
        }
        assert!(cache.status().await.unwrap().writer_tokens.is_empty());
        cleanup(&mut redis, &key).await;
    }
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn valid_hits_skip_catalog_but_bypass_and_disabled_serving_do_not() {
        let (mut state, mut redis, key) = fixture().await;
        let runtime = state.cache_runtime.clone().unwrap();
        let path = "/idx/_search";
        let cachekey = runtime.cache.request_key("POST", path, b"{}");
        let ticket = runtime
            .cache
            .lookup(&cachekey)
            .await
            .unwrap()
            .ticket
            .unwrap();
        let envelope = Envelope {
            version: 1,
            status: 200,
            headers: vec![("content-type".into(), b"application/json".to_vec())],
            body: json!({"took":99999,"hits":{"hits":[]}}),
        };
        assert!(runtime
            .cache
            .publish(&ticket, &cachekey, &serde_json::to_vec(&envelope).unwrap())
            .await
            .unwrap());
        let response = dispatch(
            state.clone(),
            Method::POST,
            path.into(),
            None,
            HeaderMap::new(),
            "{}".into(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(value["took"].as_u64().unwrap() < 99999);
        assert_eq!(state.analytics.lock().unwrap().supported, 1);
        let response = dispatch(
            state.clone(),
            Method::POST,
            path.into(),
            Some("request_cache=false".into()),
            HeaderMap::new(),
            "{}".into(),
        )
        .await;
        assert_ne!(response.status(), StatusCode::OK); // index absent in authoritative snapshot
        state.cache_runtime.as_mut().unwrap().serve_reads = false;
        let response = dispatch(
            state.clone(),
            Method::POST,
            path.into(),
            None,
            HeaderMap::new(),
            "{}".into(),
        )
        .await;
        assert_ne!(response.status(), StatusCode::OK);
        // Serving disabled still fences validation-only writes and advances epoch.
        let before = runtime.cache.status().await.unwrap().epoch;
        dispatch(
            state,
            Method::PUT,
            "/%FF".into(),
            None,
            HeaderMap::new(),
            "{}".into(),
        )
        .await;
        assert_ne!(runtime.cache.status().await.unwrap().epoch, before);
        cleanup(&mut redis, &key).await;
    }
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cancellation_and_acknowledgement_keep_cross_replica_fence() {
        for cancel in [true, false] {
            let (mut state, mut redis, key) = fixture().await;
            state.cfg.document_projection = false;
            state.cfg.async_search_projection = false;
            state.cfg.async_payload_writes = false;
            redis::cmd("SET")
                .arg(&key)
                .arg(json!({"indices":[["idx","{}","[]"]],"aliases":[]}).to_string())
                .query_async::<()>(&mut redis)
                .await
                .unwrap();
            let accepted = Arc::new(tokio::sync::Notify::new());
            let signal = accepted.clone();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            state.qdrant.base = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                axum::serve(
                    listener,
                    Router::new().fallback(any(move |method: Method| {
                        let signal = signal.clone();
                        async move {
                            if method == Method::PUT {
                                signal.notify_one();
                                if cancel {
                                    std::future::pending::<()>().await;
                                }
                                return Json(
                                    json!({"status":"ok","result":{"status":"acknowledged"}}),
                                );
                            }
                            Json(json!({"status":"ok","result":null}))
                        }
                    })),
                )
                .await
                .unwrap();
            });
            let runtime = state.cache_runtime.clone().unwrap();
            let cachekey = runtime.cache.request_key("GET", "/idx/_doc/a", b"");
            let ticket = runtime
                .cache
                .lookup(&cachekey)
                .await
                .unwrap()
                .ticket
                .unwrap();
            runtime
                .cache
                .publish(&ticket, &cachekey, b"old response")
                .await
                .unwrap();
            let task = tokio::spawn(async move {
                dispatch(
                    state,
                    Method::PUT,
                    "/idx/_doc/a".into(),
                    None,
                    HeaderMap::new(),
                    r#"{"value":1}"#.into(),
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(5), accepted.notified())
                .await
                .unwrap();
            if cancel {
                task.abort();
                let _ = task.await;
            } else {
                assert!(!task.await.unwrap().status().is_success());
            }
            assert_eq!(runtime.cache.status().await.unwrap().writer_tokens.len(), 1);
            // Independent connection represents another gateway/process.
            let url = env::var("CATALOG_TEST_REDIS_URL").unwrap();
            let other = read_cache::ReadCache::connect(
                &url,
                &key,
                "runtime-test",
                b"runtime-tests",
                60,
                1024 * 1024,
            )
            .await
            .unwrap();
            let lookup = other.lookup(&cachekey).await.unwrap();
            assert!(lookup.response.is_none() && lookup.ticket.is_none());
            assert!(!other
                .publish(&ticket, &cachekey, b"stale publication")
                .await
                .unwrap());
            server.abort();
            cleanup(&mut redis, &key).await;
        }
    }
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn cold_read_cannot_publish_after_alias_snapshot_changes() {
        let (mut state, mut redis, key) = fixture().await;
        state.cfg.document_projection = false;
        redis::cmd("SET").arg(&key).arg(json!({"indices":[["old","{}","[]"],["new","{}","[]"]],"aliases":[["alias","old"]]}).to_string()).query_async::<()>(&mut redis).await.unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let signal = reached.clone();
        let release = resume.clone();
        let old_collection = collection("old");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        state.qdrant.base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener,Router::new().fallback(any(move |uri:axum::http::Uri| {
                let signal=signal.clone();let release=release.clone();let old_collection=old_collection.clone();
                async move {
                    let old=uri.path().contains(&old_collection);
                    if old {signal.notify_one();release.notified().await;}
                    Json(json!({"status":"ok","result":[{"payload":{"_source":{"generation":if old {"old"} else {"new"}}}}]}))
                }
            }))).await.unwrap();
        });
        let reader = state.clone();
        let pending = tokio::spawn(async move {
            dispatch(
                reader,
                Method::GET,
                "/alias/_doc/a".into(),
                None,
                HeaderMap::new(),
                String::new(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), reached.notified())
            .await
            .unwrap();
        let changed=dispatch(state.clone(),Method::POST,"/_aliases".into(),None,HeaderMap::new(),json!({"actions":[{"remove":{"index":"old","alias":"alias"}},{"add":{"index":"new","alias":"alias"}}]}).to_string()).await;
        assert_eq!(changed.status(), StatusCode::OK);
        resume.notify_one();
        let old: Value = serde_json::from_slice(
            &axum::body::to_bytes(pending.await.unwrap().into_body(), 4096)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(old["_source"]["generation"], "old");
        let cache = &state.cache_runtime.as_ref().unwrap().cache;
        let cachekey = cache.request_key("GET", "/alias/_doc/a", b"");
        assert!(cache.lookup(&cachekey).await.unwrap().response.is_none());
        let fresh = dispatch(
            state.clone(),
            Method::GET,
            "/alias/_doc/a".into(),
            None,
            HeaderMap::new(),
            String::new(),
        )
        .await;
        let fresh: Value =
            serde_json::from_slice(&axum::body::to_bytes(fresh.into_body(), 4096).await.unwrap())
                .unwrap();
        assert_eq!(fresh["_source"]["generation"], "new");
        assert!(cache.lookup(&cachekey).await.unwrap().response.is_some());
        assert!(cache.status().await.unwrap().writer_tokens.is_empty());
        server.abort();
        cleanup(&mut redis, &key).await;
    }
    #[tokio::test]
    #[ignore = "requires CATALOG_TEST_REDIS_URL"]
    async fn bulk_http_success_with_uncertain_item_retains_writer() {
        let (mut state, mut redis, key) = fixture().await;
        state.cfg.document_projection = false;
        state.cfg.async_search_projection = false;
        redis::cmd("SET")
            .arg(&key)
            .arg(json!({"indices":[["idx","{}","[]"]],"aliases":[]}).to_string())
            .query_async::<()>(&mut redis)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        state.qdrant.base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(any(|method: Method| async move {
                    Json(if method == Method::PUT {
                        json!({"status":"ok","result":{"status":"acknowledged"}})
                    } else {
                        json!({"status":"ok","result":[]})
                    })
                })),
            )
            .await
            .unwrap();
        });
        let cache = state.cache_runtime.as_ref().unwrap().cache.clone();
        let body = "{\"index\":{\"_index\":\"idx\",\"_id\":\"a\"}}\n{\"value\":1}\n";
        let response = dispatch(
            state,
            Method::POST,
            "/_bulk".into(),
            None,
            HeaderMap::new(),
            body.into(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 4096)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["errors"], true);
        assert_eq!(cache.status().await.unwrap().writer_tokens.len(), 1);
        server.abort();
        cleanup(&mut redis, &key).await;
    }
}
