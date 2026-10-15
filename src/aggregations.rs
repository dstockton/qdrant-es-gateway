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

// Preserve integer precision while treating integral JSON floats as the same
// numeric value. Converting every key to f64 would merge adjacent large longs.
fn terms_key(value: &Value) -> String {
    if let Some(number) = value.as_number() {
        if let Some(integer) = number.as_i64() {
            return format!("number:{integer}");
        }
        if let Some(integer) = number.as_u64() {
            return format!("number:{integer}");
        }
        if let Some(float) = number.as_f64() {
            if float.fract() == 0.0 {
                if float >= i64::MIN as f64 && float < i64::MAX as f64 {
                    return format!("number:{}", float as i64);
                }
                if float >= 0.0 && float < u64::MAX as f64 {
                    return format!("number:{}", float as u64);
                }
            }
            return format!("number:{float}");
        }
    }
    value.to_string()
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

    pub(super) fn validate_mapping(
        &self,
        mapping: &Value,
        document_projection: bool,
    ) -> Result<(), GatewayError> {
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
            if matches!(
                kind,
                Some("text" | "sparse_vector" | "dense_vector" | "knn_vector")
            ) {
                return Err(bad(format!(
                    "aggregations do not support text or vector field: {field}"
                )));
            }
            if document_projection && kind.is_none() {
                return Err(bad(format!(
                    "document projection aggregations require a mapped scalar field: {field}"
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
        mapping: &Value,
    ) -> Result<Value, GatewayError> {
        if self.0.is_empty() {
            return Ok(json!({}));
        }
        let fast_paths_allowed = validate_fast_field_namespace(mapping).is_ok();
        let mut completed = Map::new();
        let filter_results = try_join_all(self.0.iter().filter_map(|(name, reducer)| {
            if !fast_paths_allowed {
                return None;
            }
            let Reduction::Filter { query, .. } = reducer else {
                return None;
            };
            let (marker, condition) = count_condition(query, mapping)?;
            Some(async move {
                let mut conditions = vec![condition];
                if let Some(filter) = filter {
                    conditions.push(filter.clone());
                }
                let path = format!("/collections/{collection}/points/count");
                let (covered, response) = tokio::join!(
                    marker_covered(state, collection, filter, &marker),
                    state.qdrant.request(
                        Method::POST,
                        &path,
                        Some(json!({"exact":true,"filter":{"must":conditions}}))
                    )
                );
                // The speculative read is never authoritative without coverage;
                // even an index error is discarded for legacy/malformed payloads.
                if !covered? {
                    return Ok::<_, GatewayError>(None);
                }
                let response = response?;
                let count = response
                    .pointer("/result/count")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| bad("invalid Qdrant count response"))?;
                Ok(Some((name.clone(), json!({"doc_count":count}))))
            })
        }))
        .await?;
        for (name, result) in filter_results.into_iter().flatten() {
            self.0.remove(&name);
            completed.insert(name, result);
        }
        let names = self.0.keys().cloned().collect::<Vec<_>>();
        for name in names {
            if !fast_paths_allowed {
                break;
            }
            let Reduction::Terms { field, .. } = &self.0[&name] else {
                continue;
            };
            if field.starts_with('_')
                || field.contains(['.', '[', ']'])
                || mapping
                    .get("properties")
                    .and_then(|p| p.get(field))
                    .and_then(|p| p.get("type"))
                    .and_then(Value::as_str)
                    != Some("keyword")
            {
                continue;
            }
            // Old or malformed documents never carry this versioned proof. Check
            // coverage in the same query scope before trusting the indexed facet.
            let mut request = json!({"key":field,"exact":true,"limit":MAX_BUCKETS + 1});
            if let Some(filter) = filter {
                request["filter"] = filter.clone();
            }
            let marker = format!("v1:keyword:{field}");
            let path = format!("/collections/{collection}/facet");
            let (covered, response) = tokio::join!(
                marker_covered(state, collection, filter, &marker),
                state.qdrant.request(Method::POST, &path, Some(request))
            );
            if !covered? {
                continue;
            }
            let response = response?;
            let counts = facet_counts(&response)?;
            let Reduction::Terms { counts: target, .. } = self.0.get_mut(&name).unwrap() else {
                unreachable!()
            };
            *target = counts;
            completed.insert(name.clone(), self.0.remove(&name).unwrap().finish());
        }
        if self.0.is_empty() {
            return Ok(Value::Object(completed));
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
        completed.extend(
            self.0
                .into_iter()
                .map(|(name, reducer)| (name, reducer.finish())),
        );
        Ok(Value::Object(completed))
    }
}

// Only these predicates have the same source and indexed-payload semantics
// under the writer's marker. Other queries retain the streaming evaluator.
fn count_condition(query: &Value, mapping: &Value) -> Option<(String, Value)> {
    let object = query.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let (kind, options) = object.iter().next()?;
    let options = options.as_object()?;
    if options.len() != 1 {
        return None;
    }
    let (raw_field, value) = options.iter().next()?;
    let field = compatibility_field(raw_field);
    if field.starts_with('_') || field.contains(['.', '[', ']']) {
        return None;
    }
    let mapped = mapping
        .get("properties")?
        .get(field)?
        .get("type")?
        .as_str()?;
    match kind.as_str() {
        "term" if mapped == "keyword" => {
            let value = value.get("value").unwrap_or(value).as_str()?;
            Some((
                format!("v1:keyword:{field}"),
                json!({"key":field,"match":{"value":value}}),
            ))
        }
        "range"
            if matches!(
                mapped,
                "integer"
                    | "long"
                    | "short"
                    | "byte"
                    | "float"
                    | "double"
                    | "half_float"
                    | "scaled_float"
            ) =>
        {
            let bounds = value.as_object()?;
            if bounds.is_empty()
                || !bounds.iter().all(|(op, value)| {
                    matches!(op.as_str(), "gt" | "gte" | "lt" | "lte")
                        && value
                            .as_f64()
                            .is_some_and(|n| n.is_finite() && n.abs() <= 9_007_199_254_740_991.0)
                })
            {
                return None;
            }
            Some((
                format!("v1:number:{field}"),
                json!({"key":field,"range":bounds}),
            ))
        }
        _ => None,
    }
}

async fn marker_covered(
    state: &AppState,
    collection: &str,
    filter: Option<&Value>,
    marker: &str,
) -> Result<bool, GatewayError> {
    let mut coverage = json!({"must_not":[{"key":"_qeg_fast_fields","match":{"value":marker}}]});
    if let Some(filter) = filter {
        coverage["must"] = json!([filter]);
    }
    let response = state
        .qdrant
        .request(
            Method::POST,
            &format!("/collections/{collection}/points/scroll"),
            Some(json!({"limit":1,"with_payload":false,"with_vector":false,"filter":coverage})),
        )
        .await?;
    let points = response
        .pointer("/result/points")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("invalid Qdrant coverage response"))?;
    Ok(points.is_empty())
}

fn facet_counts(response: &Value) -> Result<BTreeMap<String, (Value, u64)>, GatewayError> {
    let hits = response
        .pointer("/result/hits")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("invalid Qdrant facet response"))?;
    if hits.len() > MAX_BUCKETS {
        return Err(bad(
            "aggregation exceeds 10000 distinct buckets; narrow the query",
        ));
    }
    let mut counts = BTreeMap::new();
    let mut total = 0u64;
    for hit in hits {
        let value = hit
            .get("value")
            .filter(|value| value.is_string())
            .ok_or_else(|| bad("invalid Qdrant keyword facet value"))?;
        let count = hit
            .get("count")
            .and_then(Value::as_u64)
            .filter(|count| *count > 0)
            .ok_or_else(|| bad("invalid Qdrant facet count"))?;
        total = total
            .checked_add(count)
            .ok_or_else(|| bad("Qdrant facet count overflow"))?;
        if counts
            .insert(value.to_string(), (value.clone(), count))
            .is_some()
        {
            return Err(bad("duplicate Qdrant facet value"));
        }
    }
    Ok(counts)
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
                    let key = terms_key(value);
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
                json!({"doc_count_error_upper_bound":0,"sum_other_doc_count":others,"buckets":buckets.into_iter().take(size).map(|(key,count)| if let Some(value) = key.as_bool() { json!({"key":u8::from(value),"key_as_string":value.to_string(),"doc_count":count}) } else { json!({"key":key,"doc_count":count}) }).collect::<Vec<_>>()})
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

    #[tokio::test]
    async fn speculative_aggregation_reads_overlap_and_obey_the_coverage_result() {
        for facet in [false, true] {
            for (covered, guard_error, read_error) in [
                (true, false, false),
                (false, false, true),
                (true, true, false),
                (true, false, true),
            ] {
                let barrier = Arc::new(tokio::sync::Barrier::new(2));
                let mock = Router::new().fallback(any(move |uri: axum::http::Uri, Json(request): Json<Value>| {
                    let barrier = barrier.clone();
                    async move {
                        if request["with_payload"] == true {
                            return Json(json!({"result":{"points":[{"payload":{"_source":{"brand":"A"}}}],"next_page_offset":null}})).into_response();
                        }
                        // Neither request can finish until both have started.
                        barrier.wait().await;
                        if uri.path().ends_with("/scroll") {
                            if guard_error { return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"error":"coverage failed"}))).into_response(); }
                            return Json(json!({"result":{"points":if covered { json!([]) } else { json!([{"id":1}]) }}})).into_response();
                        }
                        if read_error { return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"error":"index unavailable"}))).into_response(); }
                        Json(if facet { json!({"result":{"hits":[{"value":"A","count":1}]}}) }
                            else { json!({"result":{"count":1}}) }).into_response()
                    }
                }));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server =
                    tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
                let state = search_test_state(format!("http://{address}"), false);
                let spec = if facet {
                    json!({"a":{"terms":{"field":"brand"}}})
                } else {
                    json!({"a":{"filter":{"term":{"brand":"A"}}}})
                };
                let plan = Plan::parse(Some(&spec)).unwrap().unwrap();
                let mapping = json!({"properties":{"brand":{"type":"keyword"}}});
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    plan.execute(&state, "es_products", None, &mapping),
                )
                .await
                .expect("coverage and speculative read must overlap");
                if guard_error || (covered && read_error) {
                    assert!(result.is_err());
                } else {
                    let result = result.unwrap();
                    if facet {
                        assert_eq!(result["a"]["buckets"], json!([{"key":"A","doc_count":1}]));
                    } else {
                        assert_eq!(result["a"]["doc_count"], 1);
                    }
                }
                server.abort();
            }
        }
    }

    #[tokio::test]
    async fn simple_filter_counts_push_down_exactly_with_common_scope() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let requests = observed.clone();
        let mock = Router::new().fallback(any(
            move |uri: axum::http::Uri, Json(request): Json<Value>| {
                let requests = requests.clone();
                async move {
                    requests
                        .lock()
                        .unwrap()
                        .push((uri.path().to_owned(), request.clone()));
                    if uri.path().ends_with("/count") {
                        return Json(json!({"result":{"count":7}}));
                    }
                    Json(json!({"result":{"points":[]}}))
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let state = search_test_state(format!("http://{address}"), false);
        let plan = Plan::parse(Some(&json!({"cheap":{"filter":{"range":{"price":{"lt":250}}}},"group":{"filter":{"term":{"group":"g03"}}}}))).unwrap().unwrap();
        let filter = json!({"must_not":[{"key":"deleted","match":{"value":true}}]});
        let mapping = json!({"properties":{"price":{"type":"integer"},"group":{"type":"keyword"}}});
        let result = plan
            .execute(&state, "es_products", Some(&filter), &mapping)
            .await
            .unwrap();
        assert_eq!(
            result,
            json!({"cheap":{"doc_count":7},"group":{"doc_count":7}})
        );
        let requests = observed.lock().unwrap();
        assert_eq!(requests.len(), 4);
        for (path, request) in requests.iter() {
            if path.ends_with("/count") {
                assert_eq!(request["exact"], true);
                assert_eq!(request["filter"]["must"][1], filter);
            } else {
                assert_eq!(request["filter"]["must"], json!([filter]));
            }
        }
        server.abort();
    }

    #[tokio::test]
    async fn unmarked_filter_values_fall_back_to_source_predicate() {
        let mock = Router::new().fallback(any(|uri: axum::http::Uri, Json(request): Json<Value>| async move {
            if uri.path().ends_with("/count") { return Json(json!({"error":"unsupported legacy index"})); }
            let points = if request["with_payload"] == false { json!([{"id":1}]) }
                else { json!([{"payload":{"_source":{"price":["1",2,null]}}},{"payload":{"_source":{"price":"3"}}}]) };
            Json(json!({"result":{"points":points,"next_page_offset":null}}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let state = search_test_state(format!("http://{address}"), false);
        let plan = Plan::parse(Some(
            &json!({"cheap":{"filter":{"range":{"price":{"lt":10}}}}}),
        ))
        .unwrap()
        .unwrap();
        let result = plan
            .execute(
                &state,
                "es_products",
                None,
                &json!({"properties":{"price":{"type":"integer"}}}),
            )
            .await
            .unwrap();
        assert_eq!(result, json!({"cheap":{"doc_count":1}}));
        server.abort();
    }

    #[test]
    fn count_optimization_rejects_predicates_without_equivalence_proof() {
        let mapping = json!({"properties":{"price":{"type":"long"},"group":{"type":"keyword"},"a.b":{"type":"keyword"}}});
        for query in [
            json!({"term":{"price":1}}),
            json!({"range":{"price":{"gte":9007199254740992u64}}}),
            json!({"exists":{"field":"group"}}),
            json!({"term":{"group":1}}),
            json!({"term":{"a.b":"x"}}),
            json!({"range":{"price":{"gte":1,"format":"x"}}}),
        ] {
            assert!(count_condition(&query, &mapping).is_none());
        }
    }

    #[tokio::test]
    async fn keyword_facets_preserve_scope_ties_other_counts_and_legacy_fallback() {
        for legacy in [false, true] {
            let observed = Arc::new(Mutex::new(Vec::new()));
            let requests = observed.clone();
            let mock = Router::new().fallback(any(move |uri: axum::http::Uri, Json(request): Json<Value>| {
                let requests = requests.clone();
                async move {
                    requests.lock().unwrap().push((uri.path().to_owned(),request.clone()));
                    if uri.path().ends_with("/facet") {
                        return Json(json!({"result":{"hits":[{"value":"B","count":1},{"value":"A","count":1}]}}));
                    }
                    if request["with_payload"] == false {
                        return Json(json!({"result":{"points":if legacy { json!([{"id":1}]) } else { json!([]) }}}));
                    }
                    // An older payload has no marker, but the full reducer still
                    // counts each term once per document and ignores null values.
                    Json(json!({"result":{"points":[{"payload":{"_source":{"brand":["B","A","A",null]}}}],"next_page_offset":null}}))
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
            let state = search_test_state(format!("http://{address}"), false);
            let plan = Plan::parse(Some(
                &json!({"brands":{"terms":{"field":"brand","size":1}}}),
            ))
            .unwrap()
            .unwrap();
            let filter = json!({"should":[{"key":"group","match":{"value":"g1"}}]});
            let result = plan
                .execute(
                    &state,
                    "es_products",
                    Some(&filter),
                    &json!({"properties":{"brand":{"type":"keyword"}}}),
                )
                .await
                .unwrap();
            assert_eq!(
                result["brands"],
                json!({"doc_count_error_upper_bound":0,"sum_other_doc_count":1,"buckets":[{"key":"A","doc_count":1}]})
            );
            let requests = observed.lock().unwrap();
            assert_eq!(requests.len(), if legacy { 3 } else { 2 });
            let coverage = &requests
                .iter()
                .find(|(_, req)| req["with_payload"] == false)
                .unwrap()
                .1;
            let facet = &requests
                .iter()
                .find(|(path, _)| path.ends_with("/facet"))
                .unwrap()
                .1;
            assert_eq!(coverage["filter"]["must"], json!([filter]));
            assert_eq!(
                coverage["filter"]["must_not"][0]["match"]["value"],
                "v1:keyword:brand"
            );
            assert_eq!(facet["filter"], filter);
            assert_eq!(facet["exact"], true);
            assert_eq!(facet["limit"], MAX_BUCKETS + 1);
            if legacy {
                assert_eq!(requests[2].1["with_payload"], true);
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn legacy_reserved_fields_are_preserved_and_never_trusted() {
        let mapping = json!({"properties":{"brand":{"type":"keyword"},"price":{"type":"integer"},"_qeg_fast_fields":{"type":"keyword"}}});
        let source = json!({"brand":17,"price":null,"_qeg_fast_fields":["v1:keyword:brand","v1:number:price"]});
        assert_eq!(
            projected_search_payload(&source, "a", "items", &mapping)["_qeg_fast_fields"],
            source["_qeg_fast_fields"]
        );
        let mock = Router::new().fallback(any(move |uri: axum::http::Uri, Json(request): Json<Value>| {
            let source = source.clone();
            async move {
                if uri.path().ends_with("/index") {
                    assert_eq!(request["field_name"],"newfield");
                    return Json(json!({"status":"ok"}));
                }
                assert!(uri.path().ends_with("/scroll"));
                assert_eq!(request["with_payload"],true);
                Json(json!({"result":{"points":[{"payload":{"_source":source}}],"next_page_offset":null}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let state = search_test_state(format!("http://{address}"), false);
        let plan = Plan::parse(Some(&json!({"brands":{"terms":{"field":"brand"}},"cheap":{"filter":{"range":{"price":{"lt":20}}}}}))).unwrap().unwrap();
        let result = plan
            .execute(&state, "es_products", None, &mapping)
            .await
            .unwrap();
        assert_eq!(
            result["brands"]["buckets"],
            json!([{"key":17,"doc_count":1}])
        );
        assert_eq!(result["cheap"]["doc_count"], 0);
        assert!(ordered_numeric_hits(
            &state,
            ("products", "es_products", &mapping),
            &json!({}),
            &[("price".into(), "asc".into())],
            None,
            1
        )
        .await
        .unwrap()
        .is_none());
        state
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE indices SET mapping=?1 WHERE name='products'",
                params![mapping.to_string()],
            )
            .unwrap();
        super::mapping(
            State(state),
            Path("products".into()),
            Method::PUT,
            Some(Json(json!({"properties":{"newfield":{"type":"keyword"}}}))),
        )
        .await
        .unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn malformed_unmarked_payload_still_errors() {
        let mock = Router::new().fallback(any(|Json(request): Json<Value>| async move {
            let point = if request["with_payload"] == false {
                json!({"id":1})
            } else {
                json!({"payload":{"_source":{"brand":["A",{"nested":"B"}]}}})
            };
            Json(json!({"result":{"points":[point],"next_page_offset":null}}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let state = search_test_state(format!("http://{address}"), false);
        let plan = Plan::parse(Some(&json!({"brands":{"terms":{"field":"brand"}}})))
            .unwrap()
            .unwrap();
        assert!(plan
            .execute(
                &state,
                "es_products",
                None,
                &json!({"properties":{"brand":{"type":"keyword"}}})
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("scalar"));
        server.abort();
    }

    #[test]
    fn facet_response_rejects_truncation_duplicate_keys_and_malformed_counts() {
        for hits in [
            json!([{"value":"a","count":0}]),
            json!([{"value":1,"count":1}]),
            json!([{"value":"a","count":1},{"value":"a","count":2}]),
            json!([{"value":"a","count":u64::MAX},{"value":"b","count":1}]),
            Value::Array(
                (0..=MAX_BUCKETS)
                    .map(|n| json!({"value":n.to_string(),"count":1}))
                    .collect(),
            ),
        ] {
            assert!(facet_counts(&json!({"result":{"hits":hits}})).is_err());
        }
        assert!(facet_counts(&json!({"result":{}})).is_err());
    }

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
    fn numeric_terms_merge_equivalent_json_values_without_losing_integer_precision() {
        let mut plan = Plan::parse(Some(&json!({"numbers":{"terms":{"field":"n"}}})))
            .unwrap()
            .unwrap();
        let reducer = plan.0.get_mut("numbers").unwrap();
        for source in [
            json!({"n":[1,1.0]}),
            json!({"n":1.0}),
            json!({"n":[0,-0.0]}),
            json!({"n":[9007199254740992_u64,9007199254740993_u64]}),
        ] {
            reducer.observe(&source).unwrap();
        }
        let result = plan.0.remove("numbers").unwrap().finish();
        let buckets = result["buckets"].as_array().unwrap();
        assert_eq!(buckets.len(), 4);
        assert_eq!(buckets[0]["doc_count"], 2);
        assert_eq!(buckets[0]["key"], 1);
        assert_eq!(terms_key(&json!(-0.0)), terms_key(&json!(0)));
        assert_ne!(terms_key(&json!(u64::MAX)), terms_key(&json!(u64::MAX - 1)));
    }

    #[test]
    fn tied_integer_terms_keep_exact_order_beyond_float_precision() {
        let mut plan = Plan::parse(Some(&json!({"numbers":{"terms":{"field":"n","size":1}}})))
            .unwrap()
            .unwrap();
        let reducer = plan.0.get_mut("numbers").unwrap();
        reducer
            .observe(&json!({"n":-9007199254740992_i64}))
            .unwrap();
        reducer
            .observe(&json!({"n":-9007199254740993_i64}))
            .unwrap();
        assert_eq!(
            plan.0.remove("numbers").unwrap().finish()["buckets"],
            json!([{"key":-9007199254740993_i64,"doc_count":1}])
        );
        assert!(value_cmp(&json!(-1), &json!(u64::MAX)).is_lt());
        assert!(value_cmp(&json!(-9007199254740993_i64), &json!(-9007199254740992.0)).is_lt());
        assert!(value_cmp(&json!(-9007199254740992.0), &json!(-9007199254740993_i64)).is_gt());
        assert!(value_cmp(&json!(1), &json!(1.5)).is_lt());
        assert!(value_cmp(&json!(-1), &json!(-1.5)).is_gt());
        assert!(value_cmp(&json!(u64::MAX), &json!(1e99)).is_lt());
        assert!(value_cmp(&json!(i64::MIN), &json!(-1e99)).is_gt());
        assert!(value_cmp(&json!(u64::MAX), &json!(i64::MAX)).is_gt());
    }

    #[tokio::test]
    async fn ranked_aggregation_queries_are_rejected_before_backend_access() {
        for query in [
            json!({"multi_match":{"query":"shoes"}}),
            json!({"bool":{"should":[{"match":{"title":"shoes"}}]}}),
            json!({"bool":{"must_not":{"match":{"title":"shoes"}}}}),
            json!({"bool":{"filter":{"multi_match":{"query":"shoes"}}}}),
        ] {
            let state = search_test_state("http://127.0.0.1:1".into(), false);
            let error = search(
                State(state),
                Path("products".into()),
                Json(json!({"size":0,"query":query,"aggs":{"brands":{"terms":{"field":"brand"}}}})),
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("filter-only query"), "{error}");
        }
    }

    #[test]
    fn mappings_reject_text_and_vectors_in_every_storage_mode() {
        for kind in ["text", "sparse_vector", "dense_vector", "knn_vector"] {
            let mapping = json!({"properties":{"value":{"type":kind}}});
            for projection in [false, true] {
                for spec in [
                    json!({"values":{"terms":{"field":"value"}}}),
                    json!({"values":{"filter":{"exists":{"field":"value"}}}}),
                ] {
                    let plan = Plan::parse(Some(&spec)).unwrap().unwrap();
                    assert!(plan.validate_mapping(&mapping, projection).is_err());
                }
            }
        }
        let plan = Plan::parse(Some(&json!({"values":{"terms":{"field":"value"}}})))
            .unwrap()
            .unwrap();
        assert!(plan.validate_mapping(&json!({}), false).is_ok());
        assert!(plan.validate_mapping(&json!({}), true).is_err());
    }

    #[test]
    fn boolean_terms_use_elasticsearch_numeric_keys_and_string_labels() {
        let mut plan = Plan::parse(Some(&json!({"flags":{"terms":{"field":"flag"}}})))
            .unwrap()
            .unwrap();
        let reducer = plan.0.get_mut("flags").unwrap();
        reducer.observe(&json!({"flag":[true,true,false]})).unwrap();
        reducer.observe(&json!({"flag":true})).unwrap();
        let result = plan.0.remove("flags").unwrap().finish();
        assert_eq!(
            result["buckets"],
            json!([{"key":1,"key_as_string":"true","doc_count":2},{"key":0,"key_as_string":"false","doc_count":1}])
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

    fn search_test_state(base: String, projection: bool) -> AppState {
        let mut cfg = Config::from_env();
        cfg.max_page_size = 1;
        cfg.document_projection = projection;
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE indices(name TEXT PRIMARY KEY,mapping TEXT NOT NULL,vectors TEXT NOT NULL); CREATE TABLE aliases(alias TEXT PRIMARY KEY,index_name TEXT NOT NULL); INSERT INTO indices VALUES ('products','{}','[]');").unwrap();
        AppState {
            shared_catalog: None,
            qdrant: Qdrant {
                admin_mutation_started: None,
                client: Client::new(),
                base,
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
            },
            db: Arc::new(Mutex::new(db)),
            analytics: Arc::new(Mutex::new(Analytics::default())),
            index_admin: Arc::new(AsyncMutex::new(())),
            cfg,
        }
    }

    #[tokio::test]
    async fn projected_sort_skips_missing_sources_and_hydrates_each_page_once() {
        let reads = Arc::new(Mutex::new(0));
        let observed_reads = reads.clone();
        let mock = Router::new().fallback(any(move |uri: axum::http::Uri, Json(request): Json<Value>| {
            let reads = reads.clone();
            async move {
                if uri.path().ends_with("/count") { return Json(json!({"result":{"count":3}})); }
                if uri.path().ends_with("_documents/points") {
                    *reads.lock().unwrap() += 1;
                    let id = &request["ids"][0];
                    if id == &json!(point_id("products","missing")) { return Json(json!({"result":[]})); }
                    let (id,price) = if id == &json!(point_id("products","a")) { ("a",50) } else { ("b",2) };
                    return Json(json!({"result":[{"payload":{"_es_id":id,"_source":{"price":price}}}]}));
                }
                let (id,next) = match request.get("offset").and_then(Value::as_str) {
                    None => ("missing",json!("a")), Some("a") => ("a",json!("b")), _ => ("b",Value::Null),
                };
                Json(json!({"result":{"points":[{"payload":{"_es_id":id}}],"next_page_offset":next}}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let state = search_test_state(format!("http://{address}"), true);
        let response = search(
            State(state.clone()),
            Path("products".into()),
            Json(json!({"size":1,"sort":[{"price":"asc"}]})),
        )
        .await
        .unwrap();
        let result: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["hits"]["hits"][0]["_id"], "b");
        assert_eq!(result["hits"]["total"], json!({"value":2,"relation":"eq"}));
        assert_eq!(*observed_reads.lock().unwrap(), 3);
        let response = search(
            State(state),
            Path("products".into()),
            Json(json!({"size":0,"sort":[{"price":"asc"}]})),
        )
        .await
        .unwrap();
        let result: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["hits"]["total"], json!({"value":2,"relation":"eq"}));
        assert_eq!(result["hits"]["hits"], json!([]));
        assert_eq!(*observed_reads.lock().unwrap(), 6);

        server.abort();
    }

    #[tokio::test]
    async fn sorted_scan_rejects_a_nonadvancing_cursor() {
        let mock = Router::new().fallback(any(|uri: axum::http::Uri| async move {
            if uri.path().ends_with("/count") { return Json(json!({"result":{"count":1}})); }
            Json(json!({"result":{"points":[{"payload":{"_es_id":"a","_source":{"price":1}}}],"next_page_offset":"repeated"}}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        let state = search_test_state(format!("http://{address}"), false);
        let error = search(
            State(state),
            Path("products".into()),
            Json(json!({"size":1,"sort":[{"price":"asc"}]})),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("did not advance"));
        server.abort();
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
            .execute(&state, "es_products", Some(&filter), &json!({}))
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
