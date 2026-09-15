use reqwest::Client;
use serde_json::json;

#[tokio::test]
#[ignore = "requires docker compose up"]
async fn live_qdrant_smoke() {
    let client = Client::new();
    let index = format!("integration_{}", uuid::Uuid::new_v4().simple());
    let base = std::env::var("GATEWAY_URL").unwrap_or_else(|_| "http://localhost:9200".into());
    let response = client.put(format!("{base}/{index}")).json(&json!({"mappings":{"properties":{"title":{"type":"text"},"brand":{"type":"keyword"},"price":{"type":"float"}}}})).send().await.unwrap();
    assert!(response.status().is_success());
    let bulk = "{\"index\":{\"_id\":\"one\"}}\n{\"title\":\"wireless headphones\",\"brand\":\"Acme\",\"price\":99}\n{\"index\":{\"_id\":\"two\"}}\n{\"title\":\"charger\",\"brand\":\"Acme\",\"price\":29}\n".to_string();
    let response = client
        .post(format!("{base}/{index}/_bulk"))
        .header("content-type", "application/x-ndjson")
        .body(bulk)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let search = client.post(format!("{base}/{index}/_search")).json(&json!({"query":{"bool":{"must":{"match":{"title":"headphones"}},"filter":[{"term":{"brand":"Acme"}}]}}})).send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    assert_eq!(search["hits"]["hits"][0]["_id"], "one");
    let _ = client.delete(format!("{base}/{index}")).send().await;
}
