//! Explicit token-weight vectors. No inference, token hashing or narrowing.
use crate::GatewayError;
use serde_json::{json, Map, Value};

pub fn name(field: &str) -> String {
    format!(
        "sparse_{}",
        field
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

pub fn validate_mapping(mapping: &Value) -> Result<(), GatewayError> {
    if let Some(properties) = mapping.get("properties").and_then(Value::as_object) {
        for (field, spec) in properties {
            if spec.get("type").and_then(Value::as_str) != Some("sparse_vector") {
                continue;
            }
            let valid_keys = spec
                .as_object()
                .is_some_and(|s| s.keys().all(|k| k == "type" || k == "method"));
            let valid_method = spec.get("method").is_none_or(|method| {
                method.as_object().is_some_and(|m| {
                    m.keys().all(|k| k == "name" || k == "engine")
                        && m.get("name").and_then(Value::as_str) == Some("seismic")
                        && m.get("engine")
                            .is_none_or(|engine| engine.as_str() == Some("lucene"))
                })
            });
            if !valid_keys || !valid_method {
                return Err(GatewayError::bad(format!("mappings.properties.{field}"), "sparse_vector accepts type and optional method:{name:seismic,engine:lucene}; engine tuning, native engine and pruning options are unsupported"));
            }
        }
    }
    Ok(())
}

pub fn vector(tokens: &Value) -> Result<Value, GatewayError> {
    let tokens = tokens.as_object().ok_or_else(|| {
        GatewayError::bad(
            "sparse_vector",
            "expected an object of numeric token IDs and positive weights",
        )
    })?;
    let mut pairs = Vec::with_capacity(tokens.len());
    for (token, weight) in tokens {
        let id = token.parse::<u32>().map_err(|_| {
            GatewayError::bad(
                "sparse_vector",
                "token IDs must be canonical unsigned 32-bit decimal integers",
            )
        })?;
        if id.to_string() != *token {
            return Err(GatewayError::bad(
                "sparse_vector",
                "token IDs must be canonical unsigned 32-bit decimal integers",
            ));
        }
        let weight = weight.as_f64().ok_or_else(|| {
            GatewayError::bad(
                "sparse_vector",
                "weights must be positive finite float32 numbers",
            )
        })?;
        let converted = weight as f32;
        if !converted.is_finite() || converted <= 0.0 {
            return Err(GatewayError::bad(
                "sparse_vector",
                "weights must be positive finite float32 numbers",
            ));
        }
        pairs.push((id, converted));
    }
    pairs.sort_unstable_by_key(|(id, _)| *id);
    Ok(
        json!({"indices":pairs.iter().map(|(i,_)| *i).collect::<Vec<_>>(), "values":pairs.iter().map(|(_,v)| *v).collect::<Vec<_>>()}),
    )
}

pub fn document(source: &Value, mapping: &Value) -> Result<Map<String, Value>, GatewayError> {
    let mut result = Map::new();
    if let Some(properties) = mapping.get("properties").and_then(Value::as_object) {
        for (field, spec) in properties {
            if spec.get("type").and_then(Value::as_str) == Some("sparse_vector") {
                if let Some(tokens) = source.get(field).filter(|v| !v.is_null()) {
                    result.insert(name(field), vector(tokens)?);
                }
            }
        }
    }
    Ok(result)
}

fn check_scalar_field(field: &str, mapping: &Value) -> Result<(), GatewayError> {
    let field = field.split('^').next().unwrap_or(field);
    if mapping
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| {
            properties.iter().any(|(name, spec)| {
                spec.get("type").and_then(Value::as_str) == Some("sparse_vector")
                    && (field == name
                        || field
                            .strip_prefix(name)
                            .is_some_and(|suffix| suffix.starts_with('.')))
            })
        })
    {
        return Err(GatewayError::bad("sparse_vector.scalar_query", "scalar predicates and aggregations on sparse_vector fields are unsupported; use neural_sparse query_tokens"));
    }
    Ok(())
}

/// Sparse vectors are deliberately absent from scalar payload indexes.
/// Walk query syntax, never user scalar values, to reject unsupported uses.
pub fn validate_scalar_queries(query: &Value, mapping: &Value) -> Result<(), GatewayError> {
    if let Some(clauses) = query.as_array() {
        for clause in clauses {
            validate_scalar_queries(clause, mapping)?;
        }
    }
    if let Some(object) = query.as_object() {
        for (kind, value) in object {
            match kind.as_str() {
                "term" | "terms" | "range" | "prefix" | "wildcard" | "regexp" | "match"
                | "match_phrase" | "match_bool_prefix" | "fuzzy" => {
                    if let Some(fields) = value.as_object() {
                        for field in fields.keys() {
                            check_scalar_field(field, mapping)?;
                        }
                    }
                }
                "exists" => {
                    if let Some(field) = value.get("field").and_then(Value::as_str) {
                        check_scalar_field(field, mapping)?;
                    }
                }
                "multi_match" => {
                    if let Some(fields) = value.get("fields").and_then(Value::as_array) {
                        for field in fields.iter().filter_map(Value::as_str) {
                            check_scalar_field(field, mapping)?;
                        }
                    }
                }
                "bool" => {
                    for key in ["must", "must_not", "should", "filter"] {
                        if let Some(clauses) = value.get(key) {
                            validate_scalar_queries(clauses, mapping)?;
                        }
                    }
                }
                "dis_max" => {
                    if let Some(clauses) = value.get("queries") {
                        validate_scalar_queries(clauses, mapping)?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub fn validate_request(body: &Value, mapping: &Value) -> Result<(), GatewayError> {
    fn aggregations(value: &Value, mapping: &Value) -> Result<(), GatewayError> {
        if let Some(aggs) = value.as_object() {
            for spec in aggs.values() {
                if let Some(spec) = spec.as_object() {
                    for (kind, options) in spec {
                        match kind.as_str() {
                            "aggs" | "aggregations" => aggregations(options, mapping)?,
                            "filter" => validate_scalar_queries(options, mapping)?,
                            "filters" => {
                                if let Some(filters) = options.get("filters") {
                                    if let Some(filters) = filters.as_object() {
                                        for filter in filters.values() {
                                            validate_scalar_queries(filter, mapping)?;
                                        }
                                    } else {
                                        validate_scalar_queries(filters, mapping)?;
                                    }
                                }
                            }
                            _ => {
                                if let Some(field) = options.get("field").and_then(Value::as_str) {
                                    check_scalar_field(field, mapping)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    for key in ["query", "post_filter"] {
        if let Some(query) = body.get(key) {
            validate_scalar_queries(query, mapping)?;
        }
    }
    for key in ["aggs", "aggregations"] {
        if let Some(value) = body.get(key) {
            aggregations(value, mapping)?;
        }
    }
    Ok(())
}

pub fn query(query: &Value, mapping: &Value) -> Result<Option<(String, Value)>, GatewayError> {
    let Some(sparse) = query.get("neural_sparse") else {
        return Ok(None);
    };
    if query.as_object().is_none_or(|q| q.len() != 1) {
        return Err(GatewayError::bad(
            "neural_sparse",
            "must be the sole top-level query",
        ));
    }
    let fields = sparse
        .as_object()
        .filter(|f| f.len() == 1)
        .ok_or_else(|| GatewayError::bad("neural_sparse", "exactly one field is required"))?;
    let (field, options) = fields.iter().next().unwrap();
    if mapping
        .get("properties")
        .and_then(|p| p.get(field))
        .and_then(|s| s.get("type"))
        .and_then(Value::as_str)
        != Some("sparse_vector")
    {
        return Err(GatewayError::bad(
            "neural_sparse",
            "field must be mapped as sparse_vector",
        ));
    }
    let options = options.as_object().filter(|o| o.len() == 1 && o.contains_key("query_tokens")).ok_or_else(|| GatewayError::bad("neural_sparse", "only explicit query_tokens is supported; inference and method_parameters are unsupported"))?;
    let vector = vector(&options["query_tokens"])?;
    if vector["indices"].as_array().is_none_or(|v| v.is_empty()) {
        return Err(GatewayError::bad(
            "neural_sparse",
            "query_tokens must not be empty",
        ));
    }
    Ok(Some((name(field), vector)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_full_ids_and_pairing() {
        assert_eq!(
            vector(&json!({"0":0.5,"32768":1.0,"65535":2.0,"65536":3.0,"4294967295":4.0})).unwrap(),
            json!({"indices":[0,32768,65535,65536,4294967295u32],"values":[0.5,1.0,2.0,3.0,4.0]})
        );
        for bad in [
            json!({"01":1}),
            json!({"-1":1}),
            json!({"4294967296":1}),
            json!({"word":1}),
            json!({"1":0}),
            json!({"1":-1}),
            json!({"1":1e99}),
            json!({"1":1e-99}),
            json!({"1":"1"}),
            json!([]),
        ] {
            assert!(vector(&bad).is_err(), "{bad}");
        }
    }
    #[test]
    fn scalar_queries_on_sparse_fields_fail_explicitly() {
        let mapping =
            json!({"properties":{"v":{"type":"sparse_vector"},"category":{"type":"keyword"}}});
        for query in [
            json!({"exists":{"field":"v"}}),
            json!({"term":{"v.65536":1}}),
            json!({"range":{"v":{"gte":0}}}),
            json!({"bool":{"must_not":[{"terms":{"v":[1]}}]}}),
            json!({"multi_match":{"query":"x","fields":["v^2"]}}),
        ] {
            assert!(
                (if query.get("aggs").is_some() {
                    validate_request(&query, &mapping)
                } else {
                    validate_scalar_queries(&query, &mapping)
                })
                .is_err(),
                "{query}"
            );
        }
        assert!(validate_scalar_queries(&json!({"term":{"category":"v"}}), &mapping).is_ok());
        assert!(validate_scalar_queries(
            &json!({"neural_sparse":{"v":{"query_tokens":{"65536":1}}}}),
            &mapping
        )
        .is_ok());
    }

    #[test]
    fn scalar_values_are_not_mistaken_for_field_references() {
        let mapping = json!({"properties":{"v":{"type":"sparse_vector"}}});
        for query in [
            json!({"term":{"field":"v"}}),
            json!({"terms":{"fields":["v"]}}),
        ] {
            assert!(validate_scalar_queries(&query, &mapping).is_ok());
        }
        assert!(
            validate_request(&json!({"aggs":{"term":{"min":{"field":"v"}}}}), &mapping).is_err()
        );
    }
    #[test]
    fn names_are_collision_free() {
        assert_ne!(name("a.b"), name("a_b"));
    }
    #[test]
    fn rejects_inference_and_tuning() {
        let mapping = json!({"properties":{"v":{"type":"sparse_vector"}}});
        assert!(query(
            &json!({"neural_sparse":{"v":{"query_tokens":{"65536":1}}}}),
            &mapping
        )
        .unwrap()
        .is_some());
        for options in [
            json!({"query_text":"hello"}),
            json!({"query_tokens":{"0":1},"method_parameters":{"k":10}}),
            json!({"query_tokens":{}}),
        ] {
            assert!(query(&json!({"neural_sparse":{"v":options}}), &mapping).is_err());
        }
        assert!(query(
            &json!({"neural_sparse":{"unknown":{"query_tokens":{"0":1}}}}),
            &mapping
        )
        .is_err());
        assert!(validate_mapping(
            &json!({"properties":{"v":{"type":"sparse_vector","method":{"name":"seismic","parameters":{"approximate_threshold":50}}}}})
        )
        .is_err());
    }
}
