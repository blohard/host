use super::*;
use axum::{Router, routing::post};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn http_throttle_pauses_all_classes_at_only_that_endpoint() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let app = Router::new().route(
        "/",
        post(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::Relaxed);
                (
                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "60")],
                    "not JSON",
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let upstream = Upstream::unlimited(reqwest::Client::new());
    let error = call_as(Class::Client, &upstream, &url, "eth_chainId", json!([]), 1)
        .await
        .unwrap_err();
    assert!(matches!(error, RpcError::RateLimited(wait) if wait == Duration::from_secs(60)));
    for class in [Class::Client, Class::Names, Class::Ingestion] {
        assert!(
            call_as(
                class,
                &upstream.clone(),
                &url,
                "eth_chainId",
                json!([]),
                MAX_RESPONSE
            )
            .await
            .is_err()
        );
    }
    assert_eq!(
        calls.load(Ordering::Relaxed),
        1,
        "cooldown must suppress network requests"
    );
    let healthy = crate::mock_node::spawn(crate::mock_node::Node::new(
        1,
        "0x0000000000000000000000000000000000000001",
        Vec::new(),
    ))
    .await;
    assert!(
        call(&upstream, &healthy.url, "eth_chainId", json!([]))
            .await
            .is_ok()
    );
    server.abort();
    healthy.stop().await;
}

#[tokio::test]
async fn rpc_throttle_is_not_a_log_size_refusal() {
    let node = crate::mock_node::spawn(crate::mock_node::Node::new(
        1,
        "0x0000000000000000000000000000000000000001",
        Vec::new(),
    ))
    .await;
    node.node.response_overrides.lock().unwrap().insert(
        "eth_getLogs".into(),
        json!({"error":{"code":-32005,"message":"rate limit exceeded"}}),
    );
    let upstream = Upstream::unlimited(reqwest::Client::new());
    assert!(
        call(&upstream, &node.url, "eth_getLogs", json!([]))
            .await
            .is_err()
    );
    assert!(
        call(&upstream, &node.url, "eth_chainId", json!([]))
            .await
            .is_err()
    );
    assert_eq!(node.node.count("eth_chainId"), 0);
    // Expire the clock, not the shared state, to exercise recovery without a sleep.
    upstream
        .cooldowns
        .lock()
        .unwrap()
        .insert(node.url.clone(), Instant::now());
    node.node.response_overrides.lock().unwrap().clear();
    assert!(
        call(&upstream, &node.url, "eth_chainId", json!([]))
            .await
            .is_ok()
    );
    assert_eq!(node.node.count("eth_chainId"), 1);
    node.stop().await;
}

#[test]
fn throttle_retry_after_is_bounded_and_accepts_http_dates() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let parse = |value: &str| {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, value.parse().unwrap());
        retry_after(&headers, now)
    };
    for (value, seconds) in [("60", 60), ("0", 1), ("999999999999999999999999999", 300)] {
        assert_eq!(parse(value), Some(Duration::from_secs(seconds)));
    }
    assert_eq!(
        parse(&httpdate::fmt_http_date(now + Duration::from_secs(42))),
        Some(Duration::from_secs(42))
    );
    assert_eq!(
        parse(&httpdate::fmt_http_date(now - Duration::from_secs(42))),
        Some(Duration::from_secs(1))
    );
    for value in ["", "later", "-1", "1.5"] {
        assert_eq!(parse(value), None);
    }
    assert_eq!(retry_after(&reqwest::header::HeaderMap::new(), now), None);
}

#[test]
fn throttle_classification_preserves_query_limits_and_reverts() {
    for message in [
        "rate limited",
        "Rate Limit Exceeded",
        "rate-limit exceeded",
        "too many requests",
        "requests per second exceeded",
        "project ID request rate exceeded",
    ] {
        assert!(rate_throttle(&json!({"code": -32005, "message": message})));
    }
    assert!(rate_throttle(&json!({"code": 429})));
    for message in [
        "query limit exceeded",
        "too many logs",
        "request limit exceeded",
        "response size exceeded",
        "unknown",
    ] {
        assert!(!rate_throttle(&json!({"code": -32005, "message": message})));
    }
    assert!(!rate_throttle(
        &json!({"code": 3, "message": "execution reverted: rate limited"})
    ));
}

#[tokio::test]
async fn throttle_waiters_recheck_after_acquiring_a_slot() {
    let upstream = Upstream::new(reqwest::Client::new(), Budget::new(10, 0));
    let permits = upstream
        .client_limiter
        .slots
        .acquire_many(28)
        .await
        .unwrap();
    let mut request = Box::pin(call(
        &upstream,
        "http://127.0.0.1:1",
        "eth_chainId",
        json!([]),
    ));
    // Poll into the semaphore wait before another response establishes the pause.
    assert!(futures::poll!(&mut request).is_pending());
    upstream.pause("http://127.0.0.1:1", Duration::from_secs(60));
    drop(permits);
    assert!(matches!(request.await, Err(RpcError::RateLimited(_))));
    assert_eq!(upstream.counts().client.calls, 0);
    assert_eq!(upstream.client_limiter.slots.available_permits(), 28);
}

#[tokio::test]
async fn throttle_pause_does_not_spend_tokens_or_shorten_an_existing_pause() {
    let upstream = Upstream::new(reqwest::Client::new(), Budget::new(1, 0));
    upstream.pause("http://127.0.0.1:1", Duration::from_secs(60));
    let until = upstream.cooldowns.lock().unwrap()["http://127.0.0.1:1"];
    upstream.pause("http://127.0.0.1:1", Duration::from_secs(2));
    assert_eq!(
        upstream.cooldowns.lock().unwrap()["http://127.0.0.1:1"],
        until
    );
    assert!(matches!(
        call(&upstream, "http://127.0.0.1:1", "eth_chainId", json!([])).await,
        Err(RpcError::RateLimited(_))
    ));
    assert!(
        upstream
            .budget
            .take_at(Instant::now(), Class::Client)
            .is_ok()
    );
}

#[tokio::test]
async fn throttle_http_and_rpc_answers_keep_size_failures_distinct() {
    let answer = Arc::new(Mutex::new((200u16, None::<String>, String::new())));
    let served = answer.clone();
    let app = Router::new().route(
        "/",
        post(move || {
            let served = served.clone();
            async move {
                let (status, retry, body) = served.lock().unwrap().clone();
                let mut headers = axum::http::HeaderMap::new();
                if let Some(retry) = retry {
                    headers.insert("retry-after", retry.parse().unwrap());
                }
                (
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    headers,
                    body,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cases = [
        (429, None, "not JSON", Some(2)),
        (429, Some("invalid"), "not JSON", Some(2)),
        (503, Some("40"), "not JSON", Some(40)),
        (503, None, "not JSON", None),
        (
            200,
            None,
            r#"{"error":{"code":429,"message":"busy"}}"#,
            Some(2),
        ),
        (
            200,
            Some("90"),
            r#"{"error":{"code":-32005,"message":"too many requests"}}"#,
            Some(90),
        ),
        (
            200,
            None,
            r#"{"error":{"code":-32005,"message":"query limit exceeded"}}"#,
            None,
        ),
        (
            200,
            None,
            r#"{"error":{"code":3,"message":"execution reverted: rate limited"}}"#,
            None,
        ),
    ];
    for (status, retry, body, delay) in cases {
        *answer.lock().unwrap() = (status, retry.map(String::from), body.into());
        let upstream = Upstream::unlimited(reqwest::Client::new());
        let result = call(&upstream, &url, "eth_getLogs", json!([])).await;
        match delay {
            Some(seconds) => assert!(
                matches!(result, Err(RpcError::RateLimited(wait)) if wait == Duration::from_secs(seconds)),
                "{status}: {result:?}"
            ),
            None => {
                assert!(upstream.check_cooldown(&url).is_ok());
                if status == 200 {
                    assert_eq!(
                        result.unwrap(),
                        serde_json::from_str::<Value>(body).unwrap()
                    );
                } else {
                    assert!(matches!(result, Err(RpcError::NotJson(_))));
                }
            }
        }
    }
    server.abort();
}
