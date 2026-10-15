//! Conservative admission for the shared response cache. This does not replace
//! endpoint validation: only responses from successful ordinary dispatch may be
//! published, and the semantic configuration is part of every entry key.
use super::*;

pub(crate) fn eligible(
    cfg: &Config,
    method: &Method,
    path: &str,
    query: Option<&str>,
    body: &str,
) -> bool {
    if body.len() > request_body_limit(cfg, method, path)
        || validate_request_parameters(query).is_err()
    {
        return false;
    }
    // Unknown parameters stay uncached, including future time-dependent or
    // authorization options. Different encodings remain distinct cache keys.
    for pair in query
        .unwrap_or_default()
        .split('&')
        .filter(|p| !p.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let Ok(name) = percent_decode_str(name).decode_utf8() else {
            return false;
        };
        let Ok(value) = percent_decode_str(value).decode_utf8() else {
            return false;
        };
        match name.as_ref() {
            "request_cache" if value == "true" => {}
            "rest_total_hits_as_int" if value == "true" || value == "false" => {}
            _ => return false,
        }
    }
    let Ok(parts) = decode_path_segments(path) else {
        return false;
    };
    let value: Value = if body.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(body) {
            Ok(value) => value,
            Err(_) => return false,
        }
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if parts.len() == 3 && parts[1] == "_doc" && method == Method::GET {
        return object.is_empty();
    }
    if parts.len() != 2
        || parts[1] != "_search"
        || (method != Method::GET && method != Method::POST)
    {
        return false;
    }
    const OPTIONS: &[&str] = &[
        "query",
        "post_filter",
        "from",
        "size",
        "sort",
        "search_after",
        "track_total_hits",
        "_source",
        "collapse",
        "aggs",
        "aggregations",
    ];
    if object.keys().any(|key| !OPTIONS.contains(&key.as_str())) {
        return false;
    }
    if value
        .get("query")
        .is_some_and(|q| validate_post_filter(q).is_err())
        || value
            .get("post_filter")
            .is_some_and(|q| validate_post_filter(q).is_err())
    {
        return false;
    }
    let Ok((from, _)) = parse_search_window(&value, cfg.max_page_size) else {
        return false;
    };
    let Ok(sorts) = parse_sort_specs(value.get("sort")) else {
        return false;
    };
    validate_search_after(&value, &sorts, from).is_ok()
        && parse_track_total_hits(&value).is_ok()
        && parse_rest_total_hits_as_int(query).is_ok()
        && parse_source_filter(value.get("_source"), "_source").is_ok()
        && parse_collapse_spec(value.get("collapse"), cfg.max_page_size).is_ok()
        && aggregations::Plan::parse(value.get("aggs").or_else(|| value.get("aggregations")))
            .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn search(body: Value, query: Option<&str>) -> bool {
        eligible(
            &Config::from_env(),
            &Method::POST,
            "/products/_search",
            query,
            &body.to_string(),
        )
    }
    #[test]
    fn admits_pages_totals_projection_and_supported_aggregations() {
        assert!(search(
            json!({"query":{"bool":{"filter":[{"term":{"brand":"a"}},{"range":{"price":{"lt":250}}}]}},"sort":[{"price":"asc"}],"size":10,"track_total_hits":false,"_source":["price"]}),
            None
        ));
        assert!(search(
            json!({"size":0,"aggs":{"brands":{"terms":{"field":"brand"}}}}),
            Some("request_cache=true")
        ));
        assert!(eligible(
            &Config::from_env(),
            &Method::GET,
            "/alias/_doc/a%2Fb",
            None,
            ""
        ));
    }
    #[test]
    fn bypasses_explicit_disable_unknown_parameters_and_nondeterministic_queries() {
        for query in [
            "request_cache=false",
            "request%5Fcache=false",
            "request_cache=true&request_cache=false",
            "routing=x",
            "future_option=true",
        ] {
            assert!(!search(json!({}), Some(query)), "{query}");
        }
        for body in [
            json!({"query":{"match":{"title":"hello"}}}),
            json!({"query":{"bool":{"filter":{"prefix":{"title":"a"}}}}}),
            json!({"pit":{"id":"x"}}),
            json!({"knn":{}}),
            json!({"future_option":true}),
            json!({"query":{"range":{"created":{"gte":"now-1h"}}}}),
        ] {
            assert!(!search(body.clone(), None), "{body}");
        }
    }
    #[test]
    fn malformed_or_mutating_requests_never_hit() {
        let cfg = Config::from_env();
        for method in [Method::PUT, Method::DELETE, Method::PATCH] {
            assert!(!eligible(&cfg, &method, "/products/_search", None, "{}"));
        }
        assert!(!eligible(&cfg, &Method::POST, "/_msearch", None, "{}"));
        assert!(!eligible(
            &cfg,
            &Method::POST,
            "/products/_search",
            None,
            "{"
        ));
        assert!(!search(json!({"size":-1}), None));
        assert!(!search(
            json!({"aggs":{"random":{"scripted_metric":{}}}}),
            None
        ));
    }
}
