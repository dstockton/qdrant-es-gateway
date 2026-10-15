//! Compile-time-only diagnostic timeline. Never includes request data or URLs.
//! The header is added after dispatch, so cache envelopes never contain it.
use axum::response::Response;
use std::future::Future;

#[cfg(feature = "diagnostic-timing")]
mod enabled {
    use super::*;
    use std::{cell::RefCell, time::Instant};

    const LIMIT: usize = 48;
    tokio::task_local! { static TRACE: RefCell<Trace>; }
    #[derive(serde::Serialize)]
    struct Event {
        id: usize,
        parent: Option<usize>,
        name: &'static str,
        start_us: u64,
        duration_us: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        bytes: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_us: Option<u64>,
    }
    struct Trace {
        started: Instant,
        events: Vec<Event>,
        dropped: usize,
    }
    pub(crate) struct Span(Option<(usize, Instant)>);
    impl Span {
        pub(crate) fn id(&self) -> Option<usize> {
            self.0.map(|(id, _)| id)
        }
        pub(crate) fn metadata(&self, bytes: usize, backend_seconds: Option<f64>) {
            if let Some(id) = self.id() {
                let _ = TRACE.try_with(|trace| {
                    let mut trace = trace.borrow_mut();
                    let event = &mut trace.events[id];
                    event.bytes = Some(bytes);
                    event.backend_us = backend_seconds
                        .filter(|v| v.is_finite() && *v >= 0.0)
                        .map(|v| (v * 1_000_000.0) as u64);
                });
            }
        }
    }
    impl Drop for Span {
        fn drop(&mut self) {
            if let Some((id, started)) = self.0 {
                let _ = TRACE.try_with(|trace| {
                    trace.borrow_mut().events[id].duration_us =
                        started.elapsed().as_micros() as u64;
                });
            }
        }
    }
    pub(crate) fn child(name: &'static str, parent: Option<usize>) -> Span {
        Span(
            TRACE
                .try_with(|trace| {
                    let mut trace = trace.borrow_mut();
                    if trace.events.len() == LIMIT {
                        trace.dropped += 1;
                        return None;
                    }
                    let now = Instant::now();
                    let id = trace.events.len();
                    let start_us = now.duration_since(trace.started).as_micros() as u64;
                    trace.events.push(Event {
                        id,
                        parent,
                        name,
                        start_us,
                        duration_us: 0,
                        bytes: None,
                        backend_us: None,
                    });
                    Some((id, now))
                })
                .ok()
                .flatten(),
        )
    }
    pub(crate) async fn run(future: impl Future<Output = Response>) -> Response {
        TRACE.scope(RefCell::new(Trace { started: Instant::now(), events: Vec::with_capacity(LIMIT), dropped: 0 }), async {
            let mut response = future.await;
            // Only this outer boundary writes the header, after cache fill/hit handling.
            let encoded = TRACE.with(|trace| {
                let trace = trace.borrow();
                serde_json::to_string(&serde_json::json!({"version":1,"total_us":trace.started.elapsed().as_micros() as u64,"dropped":trace.dropped,"events":trace.events}))
            });
            if let Some(value) = encoded.ok().filter(|value| value.len() <= 8192) {
                if let Ok(value) = axum::http::HeaderValue::from_str(&value) {
                    response.headers_mut().insert("x-qeg-diagnostic-timing", value);
                }
            }
            response
        }).await
    }
}

#[cfg(feature = "diagnostic-timing")]
pub(crate) use enabled::{child, run, Span};

#[cfg(not(feature = "diagnostic-timing"))]
pub(crate) struct Span;
// Match enabled guard lifetime semantics without any diagnostic work.
#[cfg(not(feature = "diagnostic-timing"))]
impl Drop for Span {
    fn drop(&mut self) {}
}
#[cfg(not(feature = "diagnostic-timing"))]
impl Span {
    pub(crate) fn id(&self) -> Option<usize> {
        None
    }
}
#[cfg(not(feature = "diagnostic-timing"))]
pub(crate) fn child(_name: &'static str, _parent: Option<usize>) -> Span {
    Span
}
#[cfg(not(feature = "diagnostic-timing"))]
pub(crate) async fn run(future: impl Future<Output = Response>) -> Response {
    future.await
}
pub(crate) fn span(name: &'static str) -> Span {
    child(name, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn header_is_only_attached_after_inner_response() {
        let response = run(async {
            let response = Response::new(axum::body::Body::empty());
            assert!(!response.headers().contains_key("x-qeg-diagnostic-timing"));
            let _span = span("test.fixed");
            response
        })
        .await;
        assert_eq!(
            response.headers().contains_key("x-qeg-diagnostic-timing"),
            cfg!(feature = "diagnostic-timing")
        );
    }
    #[cfg(feature = "diagnostic-timing")]
    #[tokio::test]
    async fn timelines_are_bounded_and_request_local() {
        let first = run(async {
            for _ in 0..60 {
                drop(span("test.fill"));
            }
            Response::new(axum::body::Body::empty())
        })
        .await;
        let first: serde_json::Value =
            serde_json::from_str(first.headers()["x-qeg-diagnostic-timing"].to_str().unwrap())
                .unwrap();
        assert_eq!(first["events"].as_array().unwrap().len(), 48);
        assert_eq!(first["dropped"], 12);
        let second = run(async {
            let _hit = span("cache.hit");
            Response::new(axum::body::Body::empty())
        })
        .await;
        let second: serde_json::Value = serde_json::from_str(
            second.headers()["x-qeg-diagnostic-timing"]
                .to_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second["events"].as_array().unwrap().len(), 1);
        assert_eq!(second["events"][0]["name"], "cache.hit");
    }
}
