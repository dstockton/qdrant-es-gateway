//! Opt-in read transport. Unsupported requests and lossy payloads retain REST.
use crate::{Config, GatewayError};
use qdrant_client::qdrant as p;
use serde_json::{json, Map, Value};
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request,
};

#[derive(Clone)]
pub(crate) struct GrpcReads {
    pub(crate) timeout: std::time::Duration,
    channel: Channel,
    key: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl GrpcReads {
    pub(crate) fn from_env(cfg: &Config) -> anyhow::Result<Option<Self>> {
        let Ok(url) = std::env::var("QDRANT_GRPC_URL") else {
            return Ok(None);
        };
        let mut endpoint = Endpoint::from_shared(url.clone())?
            .connect_timeout(cfg.qdrant_connect_timeout)
            .timeout(cfg.qdrant_request_timeout)
            .tcp_nodelay(true);
        if url.starts_with("https://") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
        }
        Ok(Some(Self {
            timeout: cfg.qdrant_request_timeout,
            channel: endpoint.connect_lazy(),
            key: cfg.qdrant_api_key.as_deref().map(str::parse).transpose()?,
        }))
    }

    fn request<T>(&self, body: T) -> Request<T> {
        let mut request = Request::new(body);
        request.set_timeout(self.timeout);
        if let Some(key) = &self.key {
            request.metadata_mut().insert("api-key", key.clone());
        }
        request
    }

    pub(crate) async fn read(
        &self,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Option<Value>, GatewayError> {
        let Some(call) = parse_call(path, body) else {
            return Ok(None);
        };
        let mut client = p::points_client::PointsClient::new(self.channel.clone())
            .max_decoding_message_size(usize::MAX);
        let result = match call {
            Call::Count(request) => {
                let response = client
                    .count(self.request(request))
                    .await
                    .map_err(upstream)?
                    .into_inner();
                let count = response
                    .result
                    .ok_or_else(|| GatewayError::upstream("Qdrant gRPC count omitted result"))?;
                Some(json!({"result":{"count":count.count},"status":"ok"}))
            }
            Call::Facet(request) => {
                let response = client
                    .facet(self.request(request))
                    .await
                    .map_err(upstream)?
                    .into_inner();
                let hits: Option<Vec<_>> = response
                    .hits
                    .into_iter()
                    .map(|hit| {
                        let value = match hit.value?.variant? {
                            p::facet_value::Variant::StringValue(v) => json!(v),
                            p::facet_value::Variant::IntegerValue(v) => json!(v),
                            p::facet_value::Variant::BoolValue(v) => json!(v),
                        };
                        Some(json!({"value":value,"count":hit.count}))
                    })
                    .collect();
                hits.map(|hits| json!({"result":{"hits":hits},"status":"ok"}))
            }
            Call::Scroll(request) => {
                let response = client
                    .scroll(self.request(request))
                    .await
                    .map_err(upstream)?
                    .into_inner();
                let offset = match response.next_page_offset {
                    Some(id) => point_id_json(id),
                    None => Some(Value::Null),
                };
                points_json(response.result).zip(offset).map(|(points, offset)| json!({"result":{"points":points,"next_page_offset":offset},"status":"ok"}))
            }
            Call::Get(request) => {
                let response = client
                    .get(self.request(request))
                    .await
                    .map_err(upstream)?
                    .into_inner();
                points_json(response.result).map(|points| json!({"result":points,"status":"ok"}))
            }
        };
        Ok(result)
    }
}

fn upstream(error: tonic::Status) -> GatewayError {
    GatewayError::upstream(format!(
        "Qdrant gRPC returned {}: {}",
        error.code(),
        error.message()
    ))
}

enum Call {
    Count(p::CountPoints),
    Facet(p::FacetCounts),
    Scroll(p::ScrollPoints),
    Get(p::GetPoints),
}
fn keys(value: &Value, allowed: &[&str]) -> Option<()> {
    value
        .as_object()?
        .keys()
        .all(|k| allowed.contains(&k.as_str()))
        .then_some(())
}
fn optional<T>(
    value: Option<&Value>,
    convert: impl FnOnce(&Value) -> Option<T>,
) -> Option<Option<T>> {
    match value {
        None => Some(None),
        Some(v) => convert(v).map(Some),
    }
}
fn parse_call(path: &str, body: Option<&Value>) -> Option<Call> {
    let parts: Vec<_> = path.split('/').collect();
    if parts.len() < 4 || !parts[0].is_empty() || parts[1] != "collections" || parts[2].is_empty() {
        return None;
    }
    let collection_name = parts[2].to_owned();
    let body = body?;
    let filter = optional(body.get("filter"), filter)?;
    match parts.as_slice() {
        [_, _, _, "points", "count"] => {
            keys(body, &["filter", "exact"])?;
            Some(Call::Count(p::CountPoints {
                collection_name,
                filter,
                exact: optional(body.get("exact"), Value::as_bool)?,
                ..Default::default()
            }))
        }
        [_, _, _, "facet"] => {
            keys(body, &["filter", "key", "limit", "exact"])?;
            Some(Call::Facet(p::FacetCounts {
                collection_name,
                filter,
                key: body.get("key")?.as_str()?.to_owned(),
                limit: optional(body.get("limit"), Value::as_u64)?,
                exact: optional(body.get("exact"), Value::as_bool)?,
                ..Default::default()
            }))
        }
        [_, _, _, "points", "scroll"] => {
            keys(
                body,
                &[
                    "filter",
                    "limit",
                    "offset",
                    "with_payload",
                    "with_vector",
                    "order_by",
                ],
            )?;
            if body.get("with_vector").is_some_and(|v| v != &json!(false)) {
                return None;
            }
            let order_by = optional(body.get("order_by"), |v| {
                keys(v, &["key", "direction"])?;
                Some(p::OrderBy {
                    key: v.get("key")?.as_str()?.to_owned(),
                    direction: optional(v.get("direction"), |v| match v.as_str()? {
                        "asc" => Some(0),
                        "desc" => Some(1),
                        _ => None,
                    })?,
                    ..Default::default()
                })
            })?;
            Some(Call::Scroll(p::ScrollPoints {
                collection_name,
                filter,
                limit: optional(body.get("limit"), |v| v.as_u64()?.try_into().ok())?,
                offset: optional(body.get("offset"), point_id)?,
                with_payload: Some(selector(body.get("with_payload")?)?),
                with_vectors: Some(false.into()),
                order_by,
                ..Default::default()
            }))
        }
        [_, _, _, "points"] => {
            keys(body, &["ids", "with_payload", "with_vector"])?;
            if body.get("with_vector").is_some_and(|v| v != &json!(false)) {
                return None;
            }
            let ids = body
                .get("ids")?
                .as_array()?
                .iter()
                .map(point_id)
                .collect::<Option<_>>()?;
            Some(Call::Get(p::GetPoints {
                collection_name,
                ids,
                with_payload: Some(selector(body.get("with_payload")?)?),
                with_vectors: Some(false.into()),
                ..Default::default()
            }))
        }
        _ => None,
    }
}
fn point_id(v: &Value) -> Option<p::PointId> {
    let point_id_options = if let Some(n) = v.as_u64() {
        p::point_id::PointIdOptions::Num(n)
    } else {
        p::point_id::PointIdOptions::Uuid(v.as_str()?.to_owned())
    };
    Some(p::PointId {
        point_id_options: Some(point_id_options),
    })
}
fn point_id_json(id: p::PointId) -> Option<Value> {
    Some(match id.point_id_options? {
        p::point_id::PointIdOptions::Num(n) => json!(n),
        p::point_id::PointIdOptions::Uuid(s) => json!(s),
    })
}
fn strings(v: &Value) -> Option<Vec<String>> {
    v.as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_owned))
        .collect()
}
fn selector(v: &Value) -> Option<p::WithPayloadSelector> {
    use p::with_payload_selector::SelectorOptions;
    let option = if let Some(b) = v.as_bool() {
        SelectorOptions::Enable(b)
    } else if v.is_array() {
        SelectorOptions::Include(p::PayloadIncludeSelector {
            fields: strings(v)?,
        })
    } else {
        keys(v, &["include"])?;
        SelectorOptions::Include(p::PayloadIncludeSelector {
            fields: strings(v.get("include")?)?,
        })
    };
    Some(p::WithPayloadSelector {
        selector_options: Some(option),
    })
}
fn filter(v: &Value) -> Option<p::Filter> {
    keys(v, &["must", "must_not", "should", "min_should"])?;
    // REST distinguishes Some(empty should) from absent; protobuf does not.
    if v.get("should")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        return None;
    }
    let clauses = |name| -> Option<Vec<p::Condition>> {
        match v.get(name) {
            None => Some(vec![]),
            Some(a) => a.as_array()?.iter().map(condition).collect(),
        }
    };
    let min_should = optional(v.get("min_should"), |m| {
        keys(m, &["conditions", "min_count"])?;
        Some(p::MinShould {
            conditions: m
                .get("conditions")?
                .as_array()?
                .iter()
                .map(condition)
                .collect::<Option<_>>()?,
            min_count: m.get("min_count")?.as_u64()?,
        })
    })?;
    Some(p::Filter {
        must: clauses("must")?,
        must_not: clauses("must_not")?,
        should: clauses("should")?,
        min_should,
    })
}
fn condition(v: &Value) -> Option<p::Condition> {
    use p::condition::ConditionOneOf as C;
    let c = if v.get("key").is_some() {
        keys(v, &["key", "match", "range"])?;
        let r#match = optional(v.get("match"), |m| {
            keys(m, &["value"])?;
            let value = m.get("value")?;
            let match_value = if let Some(s) = value.as_str() {
                p::r#match::MatchValue::Keyword(s.to_owned())
            } else if let Some(i) = value.as_i64() {
                p::r#match::MatchValue::Integer(i)
            } else {
                p::r#match::MatchValue::Boolean(value.as_bool()?)
            };
            Some(p::Match {
                match_value: Some(match_value),
            })
        })?;
        let range = optional(v.get("range"), |r| {
            keys(r, &["lt", "gt", "gte", "lte"])?;
            Some(p::Range {
                lt: optional(r.get("lt"), safe_f64)?,
                gt: optional(r.get("gt"), safe_f64)?,
                gte: optional(r.get("gte"), safe_f64)?,
                lte: optional(r.get("lte"), safe_f64)?,
            })
        })?;
        if r#match.is_none() && range.is_none() {
            return None;
        }
        C::Field(p::FieldCondition {
            key: v.get("key")?.as_str()?.to_owned(),
            r#match,
            range,
            ..Default::default()
        })
    } else if let Some(e) = v.get("is_empty") {
        keys(v, &["is_empty"])?;
        keys(e, &["key"])?;
        C::IsEmpty(p::IsEmptyCondition {
            key: e.get("key")?.as_str()?.to_owned(),
        })
    } else {
        C::Filter(filter(v)?)
    };
    Some(p::Condition {
        condition_one_of: Some(c),
    })
}
fn safe_f64(v: &Value) -> Option<f64> {
    if let Some(i) = v.as_i64() {
        if !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&i) {
            return None;
        }
    } else if v.as_u64().is_some() {
        return None;
    }
    v.as_f64().filter(|f| f.is_finite())
}
fn value_json(v: p::Value) -> Option<Value> {
    use p::value::Kind;
    Some(match v.kind? {
        Kind::NullValue(0) => Value::Null,
        Kind::NullValue(_) => return None,
        Kind::IntegerValue(n) => json!(n),
        // The server coerces unsigned integers outside i64 into doubles. A
        // successful but ambiguous response must be re-read through REST.
        Kind::DoubleValue(n) => {
            if n >= 9_223_372_036_854_775_808.0 {
                return None;
            }
            Value::Number(serde_json::Number::from_f64(n)?)
        }
        Kind::BoolValue(b) => json!(b),
        Kind::StringValue(s) => json!(s),
        Kind::ListValue(a) => Value::Array(
            a.values
                .into_iter()
                .map(value_json)
                .collect::<Option<_>>()?,
        ),
        Kind::StructValue(o) => Value::Object(
            o.fields
                .into_iter()
                .map(|(k, v)| Some((k, value_json(v)?)))
                .collect::<Option<Map<_, _>>>()?,
        ),
    })
}
fn points_json(points: Vec<p::RetrievedPoint>) -> Option<Vec<Value>> {
    points
        .into_iter()
        .map(|point| {
            if point.vectors.is_some() || point.shard_key.is_some() {
                return None;
            }
            let payload = point
                .payload
                .into_iter()
                .map(|(k, v)| Some((k, value_json(v)?)))
                .collect::<Option<Map<_, _>>>()?;
            let mut out = json!({"id":point_id_json(point.id?)?,"payload":payload});
            if let Some(order) = point.order_value {
                out["order_value"] = match order.variant? {
                    p::order_value::Variant::Int(i) => json!(i),
                    p::order_value::Variant::Float(f) => value_json(p::Value {
                        kind: Some(p::value::Kind::DoubleValue(f)),
                    })?,
                };
            }
            Some(out)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn deadline_includes_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let server_received = received.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let service = hyper::service::service_fn(move |_| {
                server_received.store(true, std::sync::atomic::Ordering::SeqCst);
                async {
                    let body = axum::body::Body::from_stream(futures_util::stream::pending::<
                        Result<axum::body::Bytes, std::convert::Infallible>,
                    >());
                    Ok::<_, std::convert::Infallible>(
                        hyper::Response::builder()
                            .header("content-type", "application/grpc")
                            .body(body)
                            .unwrap(),
                    )
                }
            });
            let _ = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                .await;
        });
        let qdrant = crate::Qdrant {
            grpc: Some(GrpcReads {
                channel: Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect_lazy(),
                key: None,
                timeout: std::time::Duration::from_millis(50),
            }),
            client: reqwest::Client::new(),
            base: "http://127.0.0.1:1".into(),
            key: None,
            async_write_queue: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            admin_mutation_started: None,
        };
        let started = std::time::Instant::now();
        let result = qdrant
            .request(
                axum::http::Method::POST,
                "/collections/example/points/count",
                Some(json!({"exact":true})),
            )
            .await;
        assert!(received.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            result.unwrap_err().to_string(),
            "upstream error: Qdrant request timed out"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        server.abort();
    }

    #[tokio::test]
    async fn api_key_and_non_ok_status_are_preserved_without_rest_retry() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server_received = received.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    assert_eq!(request.headers().get("api-key").unwrap(), "test-key");
                    assert!(request.headers().contains_key("grpc-timeout"));
                    server_received.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async {
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "7")
                                .header("grpc-message", "deliberate-test-denial")
                                .body(axum::body::Body::empty())
                                .unwrap(),
                        )
                    }
                },
            );
            let _ = hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                .await;
        });
        let qdrant = crate::Qdrant {
            grpc: Some(GrpcReads {
                channel: Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect_lazy(),
                key: Some("test-key".parse().unwrap()),
                timeout: std::time::Duration::from_secs(1),
            }),
            client: reqwest::Client::new(),
            base: "http://127.0.0.1:1".into(),
            key: None,
            async_write_queue: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            admin_mutation_started: None,
        };
        let error = qdrant
            .request(
                axum::http::Method::POST,
                "/collections/example/points/count",
                Some(json!({"exact":true})),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("deliberate-test-denial"));
        assert!(matches!(error, crate::GatewayError::Upstream(_)));
        assert_eq!(received.load(std::sync::atomic::Ordering::SeqCst), 1);
        server.abort();
    }

    #[test]
    fn precision_and_nested_payload_guard() {
        assert_eq!(
            value_json(p::Value {
                kind: Some(p::value::Kind::IntegerValue(i64::MAX))
            }),
            Some(json!(i64::MAX))
        );
        assert!(value_json(p::Value {
            kind: Some(p::value::Kind::ListValue(p::ListValue {
                values: vec![p::Value {
                    kind: Some(p::value::Kind::DoubleValue(2_f64.powi(63)))
                }]
            }))
        })
        .is_none());
        assert_eq!(
            value_json(p::Value {
                kind: Some(p::value::Kind::DoubleValue(1.5))
            }),
            Some(json!(1.5))
        );
    }
    #[test]
    fn strict_request_allowlist() {
        assert!(parse_call(
            "/collections/es_x/points/count",
            Some(&json!({"exact":true,"filter":{"must":[{"key":"x","match":{"value":i64::MAX}}]}}))
        )
        .is_some());
        for body in [
            json!({"exact":true,"unexpected":1}),
            json!({"filter":{"must":[{"key":"x","match":{"value":"a"},"unknown":true}]}}),
            json!({"filter":{"must":[{"key":"x","range":{"gte":i64::MAX}}]}}),
        ] {
            assert!(parse_call("/collections/es_x/points/count", Some(&body)).is_none());
        }
        assert!(parse_call("/collections/es_x/points", Some(&json!({"points":[]}))).is_none());
        assert!(parse_call(
            "/collections/es_x/points/scroll",
            Some(&json!({"limit":u64::MAX}))
        )
        .is_none());
    }
    #[test]
    fn empty_should_and_ambiguous_order_fall_back() {
        assert!(filter(&json!({"should":[]})).is_none());
        assert!(filter(&json!({"must":[{"should":[]}]})).is_none());
        assert!(filter(&json!({"must":[],"must_not":[]})).is_some());
        assert!(points_json(vec![p::RetrievedPoint {
            id: point_id(&json!(1)),
            order_value: Some(p::OrderValue {
                variant: Some(p::order_value::Variant::Float(2_f64.powi(63)))
            }),
            ..Default::default()
        }])
        .is_none());
    }

    #[test]
    fn ordered_scroll_and_payload_selection() {
        let Some(Call::Scroll(request)) = parse_call(
            "/collections/es_x/points/scroll",
            Some(
                &json!({"limit":21,"order_by":{"key":"rank","direction":"asc"},"with_payload":{"include":["_source"]},"with_vector":false}),
            ),
        ) else {
            panic!("supported request")
        };
        assert_eq!(request.limit, Some(21));
        assert_eq!(request.order_by.unwrap().key, "rank");
        assert!(request.with_payload.is_some());
    }
}
