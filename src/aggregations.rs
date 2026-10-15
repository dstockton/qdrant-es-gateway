//! Streaming reductions: query membership is independent of the hit window.
use super::*;
use chrono::{DateTime, Datelike, Duration, LocalResult, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use std::collections::BTreeMap;

const MAX_BUCKETS: usize = 10_000;

enum Reduction {
    Metric {
        field: String,
        minimum: bool,
        value: Option<f64>,
    },
    Filter {
        query: Value,
        count: u64,
    },
    Filters(BTreeMap<String, (Value, u64)>),
    Terms {
        field: String,
        size: usize,
        counts: BTreeMap<String, (Value, u64)>,
    },
    Histogram {
        field: String,
        interval: String,
        timezone: Tz,
        counts: BTreeMap<i64, u64>,
    },
}

pub(super) struct Plan(BTreeMap<String, Reduction>);

fn bad(message: impl Into<String>) -> GatewayError {
    GatewayError::bad("aggs", message)
}
fn only_options(value: &Value, allowed: &[&str]) -> Result<(), GatewayError> {
    let object = value
        .as_object()
        .ok_or_else(|| bad("aggregation options must be an object"))?;
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(bad(format!("unsupported aggregation option: {key}")));
    }
    Ok(())
}
fn field(value: &Value) -> Result<String, GatewayError> {
    value
        .get("field")
        .and_then(Value::as_str)
        .filter(|field| !field.is_empty())
        .map(|field| compatibility_field(field).to_owned())
        .ok_or_else(|| bad("aggregation requires a field"))
}
fn values(value: Option<&Value>) -> Vec<&Value> {
    match value {
        Some(Value::Array(values)) => values.iter().collect(),
        Some(value) => vec![value],
        None => vec![],
    }
}

impl Plan {
    pub(super) fn parse(value: Option<&Value>) -> Result<Option<Self>, GatewayError> {
        let Some(value) = value else { return Ok(None) };
        let specs = value
            .as_object()
            .ok_or_else(|| bad("aggregations must be an object"))?;
        if specs.len() > 100 {
            return Err(bad("at most 100 aggregations are supported"));
        }
        let mut plan = BTreeMap::new();
        for (name, spec) in specs {
            let spec = spec
                .as_object()
                .ok_or_else(|| bad("aggregation must be an object"))?;
            if spec.len() != 1 {
                return Err(bad("each aggregation must contain one supported reducer; nested aggregations are unsupported"));
            }
            let (kind, options) = spec.iter().next().unwrap();
            let reducer = match kind.as_str() {
                "min" | "max" => {
                    only_options(options, &["field"])?;
                    Reduction::Metric {
                        field: field(options)?,
                        minimum: kind == "min",
                        value: None,
                    }
                }
                "filter" => {
                    validate_post_filter(options)?;
                    Reduction::Filter {
                        query: options.clone(),
                        count: 0,
                    }
                }
                "filters" => {
                    only_options(options, &["filters"])?;
                    let filters = options
                        .get("filters")
                        .and_then(Value::as_object)
                        .ok_or_else(|| bad("filters requires a keyed filters object"))?;
                    if filters.len() > MAX_BUCKETS {
                        return Err(bad("too many keyed filters"));
                    }
                    let mut buckets = BTreeMap::new();
                    for (name, query) in filters {
                        validate_post_filter(query)?;
                        buckets.insert(name.clone(), (query.clone(), 0));
                    }
                    Reduction::Filters(buckets)
                }
                "terms" => {
                    only_options(options, &["field", "size"])?;
                    let size = match options.get("size") {
                        None => 10,
                        Some(size) => size
                            .as_u64()
                            .filter(|size| (1..=1000).contains(size))
                            .ok_or_else(|| bad("terms size must be between 1 and 1000"))?
                            as usize,
                    };
                    Reduction::Terms {
                        field: field(options)?,
                        size,
                        counts: BTreeMap::new(),
                    }
                }
                "date_histogram" => {
                    only_options(
                        options,
                        &["field", "calendar_interval", "time_zone", "min_doc_count"],
                    )?;
                    // Explicitly require nonempty buckets: synthesizing empty buckets also requires
                    // bounds and offset semantics, which this small reducer does not implement.
                    if options.get("min_doc_count").and_then(Value::as_u64) != Some(1) {
                        return Err(bad("date_histogram requires min_doc_count: 1; empty bucket generation is unsupported"));
                    }
                    let interval = options
                        .get("calendar_interval")
                        .and_then(Value::as_str)
                        .ok_or_else(|| bad("date_histogram requires calendar_interval"))?;
                    let interval = match interval {
                        "day" | "1d" => "day",
                        "week" | "1w" => "week",
                        "month" | "1M" => "month",
                        "year" | "1y" => "year",
                        _ => {
                            return Err(bad(
                                "supported calendar intervals are day, week, month, and year",
                            ))
                        }
                    }
                    .to_owned();
                    let zone = options
                        .get("time_zone")
                        .map(|value| {
                            value
                                .as_str()
                                .ok_or_else(|| bad("time_zone must be an IANA name"))
                        })
                        .transpose()?
                        .unwrap_or("UTC");
                    let timezone = zone
                        .parse::<Tz>()
                        .map_err(|_| bad("time_zone must be a known IANA name"))?;
                    Reduction::Histogram {
                        field: field(options)?,
                        interval,
                        timezone,
                        counts: BTreeMap::new(),
                    }
                }
                _ => return Err(bad(format!("unsupported aggregation: {kind}"))),
            };
            plan.insert(name.clone(), reducer);
        }
        Ok(Some(Self(plan)))
    }

    pub(super) fn validate_projection(&self, mapping: &Value) -> Result<(), GatewayError> {
        fn query_fields(query: &Value, fields: &mut Vec<String>) {
            if let Some(object) = query.as_object() {
                for (kind, options) in object {
                    match kind.as_str() {
                        "term" | "terms" | "range" => {
                            if let Some(options) = options.as_object() {
                                fields.extend(
                                    options
                                        .keys()
                                        .map(|field| compatibility_field(field).to_owned()),
                                );
                            }
                        }
                        "exists" => {
                            if let Some(field) = options.get("field").and_then(Value::as_str) {
                                fields.push(compatibility_field(field).to_owned());
                            }
                        }
                        _ => query_fields(options, fields),
                    }
                }
            } else if let Some(array) = query.as_array() {
                for query in array {
                    query_fields(query, fields);
                }
            }
        }
        let mut fields = Vec::new();
        for reducer in self.0.values() {
            match reducer {
                Reduction::Metric { field, .. }
                | Reduction::Terms { field, .. }
                | Reduction::Histogram { field, .. } => fields.push(field.clone()),
                Reduction::Filter { query, .. } => query_fields(query, &mut fields),
                Reduction::Filters(filters) => {
                    for (query, _) in filters.values() {
                        query_fields(query, &mut fields);
                    }
                }
            }
        }
        for field in fields {
            let kind = mapping
                .get("properties")
                .and_then(|properties| properties.get(&field))
                .and_then(|spec| spec.get("type"))
                .and_then(Value::as_str);
            if kind.is_none() || kind == Some("text") {
                return Err(bad(format!(
                    "document projection aggregations require a mapped non-text field: {field}"
                )));
            }
        }
        Ok(())
    }

    pub(super) async fn execute(
        mut self,
        state: &AppState,
        collection: &str,
        filter: Option<&Value>,
    ) -> Result<Value, GatewayError> {
        if self.0.is_empty() {
            return Ok(json!({}));
        }
        let mut offset = Value::Null;
        loop {
            // Search payload is the membership authority. Do not hydrate from a newer
            // document projection and accidentally mix differently aged field values.
            let mut request = json!({"limit":state.cfg.max_page_size.max(1), "with_payload":true, "with_vector":false});
            if let Some(filter) = filter {
                request["filter"] = filter.clone();
            }
            if !offset.is_null() {
                request["offset"] = offset.clone();
            }
            let response = state
                .qdrant
                .request(
                    Method::POST,
                    &format!("/collections/{collection}/points/scroll"),
                    Some(request),
                )
                .await?;
            let result = response
                .get("result")
                .ok_or_else(|| bad("invalid Qdrant scroll response"))?;
            let points = result
                .get("points")
                .and_then(Value::as_array)
                .ok_or_else(|| bad("invalid Qdrant scroll points"))?;
            for point in points {
                let payload = point
                    .get("payload")
                    .ok_or_else(|| bad("aggregation payload is missing"))?;
                let source = payload.get("_source").unwrap_or(payload);
                for reducer in self.0.values_mut() {
                    reducer.observe(source)?;
                }
            }
            let next = result
                .get("next_page_offset")
                .cloned()
                .unwrap_or(Value::Null);
            if next.is_null() {
                break;
            }
            if next == offset || points.is_empty() {
                return Err(bad("Qdrant scroll did not advance"));
            }
            offset = next;
        }
        Ok(Value::Object(
            self.0
                .into_iter()
                .map(|(name, reducer)| (name, reducer.finish()))
                .collect(),
        ))
    }
}

fn bucket_key(value: &Value, interval: &str, zone: Tz) -> Result<i64, GatewayError> {
    let instant = if let Some(millis) = value.as_i64() {
        DateTime::<Utc>::from_timestamp_millis(millis)
            .ok_or_else(|| bad("date value is outside the supported timestamp range"))?
    } else if let Some(text) = value.as_str() {
        if let Ok(instant) = DateTime::parse_from_rfc3339(text) {
            instant.with_timezone(&Utc)
        } else if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
            date.and_hms_opt(0, 0, 0).unwrap().and_utc()
        } else {
            return Err(bad(
                "date_histogram accepts epoch milliseconds, RFC3339 timestamps, or YYYY-MM-DD",
            ));
        }
    } else {
        return Err(bad("date_histogram field contains an unsupported value"));
    };
    let local = instant.with_timezone(&zone);
    let date = match interval {
        "week" => local.date_naive().checked_sub_signed(Duration::days(
            local.weekday().num_days_from_monday().into(),
        )),
        "month" => NaiveDate::from_ymd_opt(local.year(), local.month(), 1),
        "year" => NaiveDate::from_ymd_opt(local.year(), 1, 1),
        _ => Some(local.date_naive()),
    }
    .ok_or_else(|| bad("calendar bucket is outside the supported date range"))?;
    let midnight = date.and_hms_opt(0, 0, 0).unwrap();
    match zone.from_local_datetime(&midnight) {
        LocalResult::Single(value) => Ok(value.timestamp_millis()),
        LocalResult::Ambiguous(first, second) => Ok(first.timestamp_millis().min(second.timestamp_millis())),
        LocalResult::None => Err(bad("calendar bucket starts at a nonexistent local midnight; this timezone transition is unsupported")),
    }
}

impl Reduction {
    fn observe(&mut self, source: &Value) -> Result<(), GatewayError> {
        match self {
            Self::Metric {
                field,
                minimum,
                value,
            } => {
                for candidate in values(source_field(source, field))
                    .into_iter()
                    .filter(|value| !value.is_null())
                {
                    let candidate = candidate
                        .as_f64()
                        .ok_or_else(|| bad("numeric metric field contains a nonnumeric value"))?;
                    *value = Some(value.map_or(candidate, |previous| {
                        if *minimum {
                            previous.min(candidate)
                        } else {
                            previous.max(candidate)
                        }
                    }));
                }
            }
            Self::Filter { query, count } => {
                *count += u64::from(source_matches_query(source, query));
            }
            Self::Filters(filters) => {
                for (query, count) in filters.values_mut() {
                    *count += u64::from(source_matches_query(source, query));
                }
            }
            Self::Terms { field, counts, .. } => {
                let mut seen = HashSet::new();
                for value in values(source_field(source, field))
                    .into_iter()
                    .filter(|value| !value.is_null())
                {
                    if !value.is_string() && !value.is_number() && !value.is_boolean() {
                        return Err(bad("terms field must contain scalar values"));
                    }
                    let key = value.to_string();
                    if seen.insert(key.clone()) {
                        counts.entry(key).or_insert_with(|| (value.clone(), 0)).1 += 1;
                    }
                }
                if counts.len() > MAX_BUCKETS {
                    return Err(bad(
                        "aggregation exceeds 10000 distinct buckets; narrow the query",
                    ));
                }
            }
            Self::Histogram {
                field,
                interval,
                timezone,
                counts,
            } => {
                let mut seen = HashSet::new();
                for value in values(source_field(source, field))
                    .into_iter()
                    .filter(|value| !value.is_null())
                {
                    let key = bucket_key(value, interval, *timezone)?;
                    if seen.insert(key) {
                        *counts.entry(key).or_default() += 1;
                    }
                }
                if counts.len() > MAX_BUCKETS {
                    return Err(bad(
                        "aggregation exceeds 10000 distinct buckets; narrow the query",
                    ));
                }
            }
        }
        Ok(())
    }
    fn finish(self) -> Value {
        match self {
            Self::Metric { value, .. } => json!({"value":value}),
            Self::Filter { count, .. } => json!({"doc_count":count}),
            Self::Filters(filters) => {
                json!({"buckets":filters.into_iter().map(|(name,(_,count))| (name,json!({"doc_count":count}))).collect::<Map<_,_>>()})
            }
            Self::Terms { counts, size, .. } => {
                let mut buckets = counts.into_values().collect::<Vec<_>>();
                buckets.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| value_cmp(&a.0, &b.0)));
                let others = buckets
                    .iter()
                    .skip(size)
                    .map(|(_, count)| count)
                    .sum::<u64>();
                json!({"doc_count_error_upper_bound":0,"sum_other_doc_count":others,"buckets":buckets.into_iter().take(size).map(|(key,count)|json!({"key":key,"doc_count":count})).collect::<Vec<_>>()})
            }
            Self::Histogram {
                timezone, counts, ..
            } => {
                json!({"buckets":counts.into_iter().map(|(key,count)| json!({"key":key,"key_as_string":Utc.timestamp_millis_opt(key).unwrap().with_timezone(&timezone).to_rfc3339_opts(chrono::SecondsFormat::Millis,true),"doc_count":count})).collect::<Vec<_>>()})
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_days_follow_dst_and_weeks_start_monday() {
        let zone = "Europe/London".parse().unwrap();
        let key = |date: &str| bucket_key(&json!(date), "day", zone).unwrap();
        assert_eq!(
            key("2026-03-30T12:00:00Z") - key("2026-03-29T12:00:00Z"),
            23 * 3_600_000
        );
        assert_eq!(
            key("2026-10-26T12:00:00Z") - key("2026-10-25T12:00:00Z"),
            25 * 3_600_000
        );
        assert_eq!(
            bucket_key(&json!("2026-10-15T12:00:00Z"), "week", zone).unwrap(),
            key("2026-10-12T12:00:00Z")
        );
    }

    #[test]
    fn reducers_handle_arrays_duplicates_missing_and_other_buckets() {
        let mut plan = Plan::parse(Some(&json!({"low":{"min":{"field":"price"}},"high":{"max":{"field":"price"}},"brands":{"terms":{"field":"brands","size":1}}}))).unwrap().unwrap();
        for source in [
            json!({"price":[20,2],"brands":["B","B","A"]}),
            json!({"price":null,"brands":["A"]}),
            json!({}),
        ] {
            for reducer in plan.0.values_mut() {
                reducer.observe(&source).unwrap();
            }
        }
        assert_eq!(plan.0.remove("low").unwrap().finish(), json!({"value":2.0}));
        assert_eq!(
            plan.0.remove("high").unwrap().finish(),
            json!({"value":20.0})
        );
        assert_eq!(
            plan.0.remove("brands").unwrap().finish(),
            json!({"doc_count_error_upper_bound":0,"sum_other_doc_count":1,"buckets":[{"key":"A","doc_count":2}]})
        );
    }

    #[test]
    fn options_fail_closed_instead_of_silently_changing_semantics() {
        for spec in [
            json!({"a":{"min":{"field":"price","missing":5}}}),
            json!({"a":{"filter":{"match":{"title":"x"}}}}),
            json!({"a":{"date_histogram":{"field":"date","calendar_interval":"day"}}}),
            json!({"a":{"terms":{"field":"brand"},"aggs":{"x":{"min":{"field":"price"}}}}}),
        ] {
            assert!(Plan::parse(Some(&spec)).is_err());
        }
    }

    #[tokio::test]
    async fn aggregation_scroll_visits_every_page_and_uses_search_projection_values() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let requests = observed.clone();
        let mock = Router::new().fallback(any(move |uri: axum::http::Uri, Json(request): Json<Value>| {
            let requests = requests.clone();
            async move {
                if uri.path().ends_with("/count") { return Json(json!({"result":{"count":2}})); }
                requests.lock().unwrap().push(request.clone());
                let (payload, next) = if request.get("offset").is_none() {
                    (json!({"_es_id":"a","_source":{"price":50,"brand":"A"},"price":50,"brand":"A"}), json!("second"))
                } else {
                    (json!({"_es_id":"b","_source":{"price":2,"brand":"B"},"price":2,"brand":"B"}), Value::Null)
                };
                Json(json!({"result":{"points":[{"payload":payload}],"next_page_offset":next}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let mut cfg = Config::from_env();
        cfg.max_page_size = 1;
        cfg.document_projection = true;
        let state = AppState {
            shared_catalog: None,
            qdrant: Qdrant {
                admin_mutation_started: None,
                client: Client::new(),
                base: format!("http://{address}"),
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
            },
            db: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        };
        let plan = Plan::parse(Some(
            &json!({"low":{"min":{"field":"price"}},"b":{"filter":{"term":{"brand":"B"}}}}),
        ))
        .unwrap()
        .unwrap();
        let filter = json!({"must":[{"key":"in_stock","match":{"value":true}}]});
        let result = plan
            .execute(&state, "es_products", Some(&filter))
            .await
            .unwrap();
        assert_eq!(result, json!({"low":{"value":2.0},"b":{"doc_count":1}}));
        {
            let requests = observed.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1]["offset"], "second");
            assert_eq!(requests[1]["filter"], filter);
            assert!(requests.iter().all(|request| request["limit"] == 1));
        }
        state.db.lock().unwrap().execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY, mapping TEXT NOT NULL, vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY,index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products','{}','[]');").unwrap();
        let mut state = state;
        state.cfg.document_projection = false;
        for (query, expected_id, expected_total) in [
            (json!({"size":1,"sort":[{"price":"asc"}]}), "b", 2),
            (
                json!({"size":1,"sort":[{"price":"asc"}],"search_after":[2]}),
                "a",
                2,
            ),
            (
                json!({"size":1,"post_filter":{"term":{"brand":"B"}}}),
                "b",
                1,
            ),
        ] {
            let response = search(State(state.clone()), Path("products".into()), Json(query))
                .await
                .unwrap();
            let result: Value = serde_json::from_slice(
                &axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(result["hits"]["hits"][0]["_id"], expected_id);
            assert_eq!(
                result["hits"]["total"],
                json!({"value":expected_total,"relation":"eq"})
            );
        }
        server.abort();
    }
}
