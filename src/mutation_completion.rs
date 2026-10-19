//! Request-local completion evidence for future cache writer fencing.
//! There is no drop/reset recovery: only the owner of an acknowledged completed
//! attempt can clear that attempt. Clones share uncertainty across cleanup paths.
use reqwest::Method;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Default)]
pub(crate) struct MutationCompletion(Arc<AtomicUsize>);
impl MutationCompletion {
    pub(crate) fn begin(&self) -> Attempt {
        self.0.fetch_add(1, Ordering::SeqCst);
        Attempt(self.clone())
    }
    // Consumed by the future outer cache dispatcher, after catalog publication.
    #[allow(dead_code)]
    pub(crate) fn is_complete(&self) -> bool {
        self.0.load(Ordering::SeqCst) == 0
    }
}
pub(crate) struct Attempt(MutationCompletion);
impl Attempt {
    pub(crate) fn complete(self) {
        self.0 .0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Proof {
    Completed,
    CollectionBoolean,
}
impl Proof {
    pub(crate) fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Completed => {
                value.pointer("/result/status").and_then(Value::as_str) == Some("completed")
            }
            Self::CollectionBoolean => value.get("result").and_then(Value::as_bool) == Some(true),
        }
    }
}

pub(crate) fn classify(method: &Method, path: &str) -> Option<Proof> {
    // Helpers own the wait query. Reject caller-supplied query/fragment controls.
    if path.contains(['?', '#', '%']) {
        return None;
    }
    let parts: Vec<_> = path.split('/').collect();
    match parts.as_slice() {
        ["", "collections", name]
            if !name.is_empty() && (*method == Method::PUT || *method == Method::DELETE) =>
        {
            Some(Proof::CollectionBoolean)
        }
        ["", "collections", name, "points" | "index"]
            if !name.is_empty() && *method == Method::PUT =>
        {
            Some(Proof::Completed)
        }
        ["", "collections", name, "points", "delete" | "payload"]
            if !name.is_empty() && *method == Method::POST =>
        {
            Some(Proof::Completed)
        }
        _ => None,
    }
}
pub(crate) fn is_read(method: &Method, path: &str) -> bool {
    if *method == Method::GET || *method == Method::HEAD {
        return true;
    }
    if *method != Method::POST || path.contains(['#', '%']) {
        return false;
    }
    let clean = path.split('?').next().unwrap_or(path);
    let parts: Vec<_> = clean.split('/').collect();
    matches!(parts.as_slice(), ["", "collections", name, "points" | "facet"] if !name.is_empty())
        || matches!(parts.as_slice(), ["", "collections", name, "points", "count" | "scroll" | "query"] if !name.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Qdrant;
    use axum::{routing::any, Json, Router};
    use serde_json::json;
    use tokio::sync::{Notify, Semaphore};

    async fn peer(
        value: Value,
        status: axum::http::StatusCode,
    ) -> (Qdrant, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(any(move || {
            let value = value.clone();
            async move { (status, Json(value)) }
        }));
        serve(app).await
    }
    async fn serve(app: Router) -> (Qdrant, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            Qdrant {
                client: reqwest::Client::new(),
                base,
                key: None,
                async_write_queue: Arc::new(Semaphore::new(1)),
                admin_mutation_started: None,
                mutation_completion: Some(MutationCompletion::default()),
            },
            server,
        )
    }
    #[test]
    fn method_path_classification_is_closed() {
        for (method, path, proof) in [
            (Method::PUT, "/collections/x", Proof::CollectionBoolean),
            (Method::DELETE, "/collections/x", Proof::CollectionBoolean),
            (Method::PUT, "/collections/x/points", Proof::Completed),
            (Method::PUT, "/collections/x/index", Proof::Completed),
            (
                Method::POST,
                "/collections/x/points/delete",
                Proof::Completed,
            ),
            (
                Method::POST,
                "/collections/x/points/payload",
                Proof::Completed,
            ),
        ] {
            assert!(classify(&method, path) == Some(proof));
            assert!(!is_read(&method, path));
            for suffix in ["?wait=true", "?wait=false", "#fragment", "/", "%2f"] {
                assert!(classify(&method, &format!("{path}{suffix}")).is_none());
            }
        }
        for (method, path) in [
            (Method::PATCH, "/collections/x"),
            (Method::POST, "/collections/x/points/batch"),
            (Method::PUT, "/collections//points"),
            (Method::POST, "/future/write"),
        ] {
            assert!(classify(&method, path).is_none());
            assert!(!is_read(&method, path));
        }
    }

    #[tokio::test]
    async fn completion_proof_is_strict_and_later_success_cannot_clear_uncertainty() {
        for (response, status, path, method, body, complete) in [
            (
                json!({"status":"ok","result":{"status":"completed"}}),
                200,
                "/collections/x/points",
                Method::PUT,
                Some(json!({"wait":true})),
                true,
            ),
            (
                json!({"status":"ok","result":{"status":"acknowledged"}}),
                200,
                "/collections/x/points",
                Method::PUT,
                Some(json!({"wait":true})),
                false,
            ),
            (
                json!({"status":"ok","result":{}}),
                200,
                "/collections/x/points",
                Method::PUT,
                Some(json!({"wait":true})),
                false,
            ),
            (
                json!({"status":"ok","result":true}),
                200,
                "/collections/x",
                Method::PUT,
                None,
                true,
            ),
            (
                json!({"status":"ok","result":false}),
                200,
                "/collections/x",
                Method::DELETE,
                None,
                false,
            ),
            (
                json!({"status":"ok"}),
                200,
                "/collections/x",
                Method::DELETE,
                None,
                false,
            ),
            (
                json!({"status":"ok","result":true}),
                500,
                "/collections/x",
                Method::DELETE,
                None,
                false,
            ),
        ] {
            let (q, server) =
                peer(response, axum::http::StatusCode::from_u16(status).unwrap()).await;
            assert_eq!(q.mutation(method, path, body).await.is_ok(), complete);
            let tracker = q.mutation_completion.as_ref().unwrap();
            assert_eq!(tracker.is_complete(), complete);
            let (mut cleanup, cleanup_server) = peer(
                json!({"status":"ok","result":true}),
                axum::http::StatusCode::OK,
            )
            .await;
            cleanup.mutation_completion = Some(tracker.clone());
            cleanup
                .mutation(Method::DELETE, "/collections/x", None)
                .await
                .unwrap();
            assert_eq!(tracker.is_complete(), complete);
            server.abort();
            cleanup_server.abort();
        }
    }
    #[tokio::test]
    async fn cancellation_after_send_retains_attempt() {
        let received = Arc::new(Notify::new());
        let signal = received.clone();
        let app = Router::new().fallback(any(move || {
            let signal = signal.clone();
            async move {
                signal.notify_one();
                std::future::pending::<Json<Value>>().await
            }
        }));
        let (q, server) = serve(app).await;
        let tracker = q.mutation_completion.clone().unwrap();
        let task = tokio::spawn(async move {
            q.mutation(
                Method::PUT,
                "/collections/x/points",
                Some(json!({"wait":true})),
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), received.notified())
            .await
            .unwrap();
        assert!(!tracker.is_complete());
        task.abort();
        let _ = task.await;
        assert!(!tracker.is_complete());
        server.abort();
    }
    #[tokio::test]
    async fn partial_projection_error_stays_pending() {
        let (q, server) = peer(
            json!({"status":"ok","result":{"status":"completed"}}),
            axum::http::StatusCode::OK,
        )
        .await;
        q.mutation(
            Method::PUT,
            "/collections/source/points",
            Some(json!({"wait":true})),
        )
        .await
        .unwrap();
        let (mut failed, failed_server) = peer(
            json!({"status":"error"}),
            axum::http::StatusCode::BAD_REQUEST,
        )
        .await;
        failed.mutation_completion = q.mutation_completion.clone();
        assert!(failed
            .mutation(
                Method::PUT,
                "/collections/search/points",
                Some(json!({"wait":true}))
            )
            .await
            .is_err());
        q.mutation(
            Method::PUT,
            "/collections/source/points",
            Some(json!({"wait":true})),
        )
        .await
        .unwrap();
        assert!(!q.mutation_completion.unwrap().is_complete());
        server.abort();
        failed_server.abort();
    }
    #[tokio::test]
    async fn reads_are_allowed_and_unclassified_or_async_writes_never_send() {
        let (q, server) = peer(
            json!({"status":"ok","result":{}}),
            axum::http::StatusCode::OK,
        )
        .await;
        for suffix in [
            "points",
            "points/count",
            "points/scroll",
            "points/query",
            "facet",
        ] {
            q.request(
                Method::POST,
                &format!("/collections/x/{suffix}"),
                Some(json!({})),
            )
            .await
            .unwrap();
        }
        // A non-listening address makes an accidental send distinguishable from guard errors.
        let mut blocked = q.clone();
        blocked.base = "http://127.0.0.1:1".into();
        for path in ["/collections/x", "/collections/x/points", "/future/write"] {
            let error = blocked.request(Method::PUT, path, None).await.unwrap_err();
            assert!(error.to_string().contains("unclassified"));
        }
        assert!(blocked
            .mutation(Method::POST, "/future/write", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported"));
        for body in [
            None,
            Some(json!({"wait":false})),
            Some(json!({"wait":"true"})),
        ] {
            assert!(blocked
                .mutation(Method::PUT, "/collections/x/points", body)
                .await
                .unwrap_err()
                .to_string()
                .contains("wait=true"));
        }
        assert!(blocked
            .spawn_request(
                Method::POST,
                "/collections/x/points/payload".into(),
                json!({})
            )
            .unwrap_err()
            .to_string()
            .contains("asynchronously"));
        assert!(q.mutation_completion.unwrap().is_complete());
        server.abort();
    }
    #[tokio::test]
    async fn wait_moves_to_url_before_completion_and_default_mode_keeps_acknowledgements() {
        let app = Router::new().fallback(any(
            |uri: axum::http::Uri, Json(body): Json<Value>| async move {
                assert_eq!(uri.query(), Some("wait=true"));
                assert!(body.get("wait").is_none());
                assert_eq!(body.pointer("/payload/wait"), Some(&json!(false)));
                Json(json!({"status":"ok","result":{"status":"completed"}}))
            },
        ));
        let (q, server) = serve(app).await;
        q.mutation(
            Method::POST,
            "/collections/x/points/payload",
            Some(json!({"wait":true,"payload":{"wait":false}})),
        )
        .await
        .unwrap();
        assert!(q.mutation_completion.unwrap().is_complete());
        server.abort();
        let (mut q, server) = peer(
            json!({"status":"ok","result":{"status":"acknowledged"}}),
            axum::http::StatusCode::OK,
        )
        .await;
        q.mutation_completion = None;
        q.mutation(
            Method::PUT,
            "/collections/x/points",
            Some(json!({"wait":false})),
        )
        .await
        .unwrap();
        server.abort();
    }
}
