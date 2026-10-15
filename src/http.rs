//! Shared HTTP extraction, client identity and response helpers.
//! Refusals have one envelope, `{"error", "code"}`: `code` is always a stable
//! snake_case reason for scripts, and `error` is the code itself on domain
//! routes or an explanatory message on content routes. Internal transport detail
//! belongs in logs, and simulation revert data is carried separately.
use crate::{rpc::RpcError, verifier::VerifyError};
use axum::{
    Json,
    extract::{FromRequestParts, Query},
    http::{HeaderMap, StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub revert: Option<String>,
    retry_after: Option<Duration>,
}

fn is_code(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
}

impl ApiError {
    /// A refusal whose message is its code, as domain routes answer.
    pub fn new(status: StatusCode, code: &'static str) -> Self {
        debug_assert!(is_code(code), "{code:?} is not a code");
        Self::said(status, code, code)
    }

    /// A refusal explained in words, with the code a script acts on.
    pub fn said(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        debug_assert!(is_code(code), "{code:?} is not a code");
        Self {
            status,
            code,
            message: message.into(),
            revert: None,
            retry_after: None,
        }
    }

    pub fn with_revert(mut self, revert: String) -> Self {
        self.revert = Some(revert);
        self
    }

    /// A content store operation failed. Store errors carry internal state
    /// and paths, so the detail is logged and the client gets a fixed phrase.
    pub fn store_failed(error: impl std::fmt::Debug) -> Self {
        tracing::error!(?error, "content store operation failed");
        Self::said(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_failed",
            "the content store failed; try again shortly",
        )
    }

    /// An upload over its cap: 413 `too_large`, saying what was too large.
    pub fn too_large(what: &str, max: usize) -> Self {
        Self::said(
            StatusCode::PAYLOAD_TOO_LARGE,
            "too_large",
            format!("{what} exceeds {max} bytes"),
        )
    }

    /// Over the body cap: a refusal of the bytes as a body, which a peer pull
    /// holds back in that role only (peers.rs). Only `too_large` refuses the
    /// bytes outright, over the largest size this host takes for anything.
    pub fn body_too_large(max: usize) -> Self {
        Self::said(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            format!("a message body exceeds {max} bytes"),
        )
    }

    /// Over the attachment cap: the same, as an attachment.
    pub fn attachment_too_large(max: usize) -> Self {
        Self::said(
            StatusCode::PAYLOAD_TOO_LARGE,
            "attachment_too_large",
            format!("an attachment exceeds {max} bytes"),
        )
    }

    /// No room on this host for the request just now: 503 `host_busy`.
    pub fn busy() -> Self {
        Self::said(
            StatusCode::SERVICE_UNAVAILABLE,
            "host_busy",
            "the host is busy; try again shortly",
        )
    }

    /// The provider asked us to wait; keep its bounded wait for the client.
    pub fn rate_limited(wait: Duration) -> Self {
        let mut error = Self::said(
            StatusCode::SERVICE_UNAVAILABLE,
            "chain_read_unavailable",
            "the chain provider is busy; try again shortly",
        );
        error.retry_after = Some(wait);
        error
    }

    pub fn unavailable(error: impl std::fmt::Debug) -> Self {
        tracing::warn!(?error, "application chain read failed");
        Self::new(StatusCode::BAD_GATEWAY, "chain_read_unavailable")
    }
}

// Rendering does not log: the caller may already have logged chain/request
// context. Transport conversions log the detail that cannot be sent publicly.
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({ "error": self.message, "code": self.code });
        if let Some(revert) = self.revert {
            body["revert"] = json!(revert);
        }
        let mut response = (self.status, Json(body)).into_response();
        if let Some(wait) = self.retry_after {
            // Round up so a fractional second cannot prompt an early retry.
            let seconds = wait
                .as_secs()
                .saturating_add(u64::from(wait.subsec_nanos() != 0))
                .max(1);
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                seconds.to_string().parse().expect("numeric retry delay"),
            );
        }
        response
    }
}

impl From<RpcError> for ApiError {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::Busy => Self::new(StatusCode::SERVICE_UNAVAILABLE, "host_busy"),
            RpcError::RateLimited(wait) => Self::rate_limited(wait),
            other => Self::unavailable(other),
        }
    }
}

// Upload verification has an established human-readable error contract.
impl From<VerifyError> for ApiError {
    fn from(error: VerifyError) -> Self {
        match error {
            VerifyError::UnsupportedChain(_) => Self::said(
                StatusCode::BAD_REQUEST,
                "chain_not_served",
                error.to_string(),
            ),
            VerifyError::NotAnchored(_) => Self::said(
                StatusCode::FORBIDDEN,
                "not_anchored",
                format!("not anchored on-chain: {error}"),
            ),
            VerifyError::Rpc(detail) => {
                tracing::warn!(?detail, "content anchoring read failed");
                Self::said(
                    StatusCode::BAD_GATEWAY,
                    "chain_read_unavailable",
                    "could not verify anchoring; try again shortly",
                )
            }
            VerifyError::Busy => Self::busy(),
            VerifyError::RateLimited(wait) => Self::rate_limited(wait),
            VerifyError::IndexUnavailable => Self::said(
                StatusCode::SERVICE_UNAVAILABLE,
                "index_unavailable",
                error.to_string(),
            ),
        }
    }
}

pub fn json_ok(value: Value) -> Response {
    (StatusCode::OK, axum::Json(value)).into_response()
}

/// A refusal whose message is its code, logged as [`ApiError::respond`] does.
pub fn json_error(status: StatusCode, code: &'static str) -> Response {
    ApiError::new(status, code).respond()
}

/// A refusal explained in words, logged as [`ApiError::respond`] does.
pub fn json_said(status: StatusCode, code: &'static str, message: &str) -> Response {
    ApiError::said(status, code, message).respond()
}

impl ApiError {
    /// The response, logged with its message: every 5xx as an error, since
    /// the client sees only the status in the access line, and every 4xx at
    /// debug for when a client's refusals need tracing.
    pub fn respond(self) -> Response {
        let status = self.status;
        if status == StatusCode::SERVICE_UNAVAILABLE {
            // Busy, warming or paused: the host shedding load, not failing.
            tracing::warn!(
                status = status.as_u16(),
                code = self.code,
                "{}",
                self.message
            );
        } else if status.is_server_error() {
            tracing::error!(
                status = status.as_u16(),
                code = self.code,
                "{}",
                self.message
            );
        } else {
            tracing::debug!(
                status = status.as_u16(),
                code = self.code,
                "{}",
                self.message
            );
        }
        self.into_response()
    }
}

/// Path parameters that fail to parse answer in the JSON envelope, as queries
/// do, instead of the framework's plain-text rejection.
pub struct JsonPath<T>(pub T);

impl<S, T> FromRequestParts<S> for JsonPath<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned + Send,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Path::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Path(v)) => Ok(Self(v)),
            Err(e) => Err(json_said(
                StatusCode::BAD_REQUEST,
                "malformed_request",
                &e.body_text(),
            )),
        }
    }
}

/// `Query<T>` whose rejection is our JSON error shape rather than axum's
/// plain text, so `?chain_id=abc` is refused like every other bad request.
pub struct JsonQuery<T>(pub T);

impl<S, T> FromRequestParts<S> for JsonQuery<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(v)) => Ok(Self(v)),
            Err(e) => Err(json_said(
                StatusCode::BAD_REQUEST,
                "malformed_request",
                &e.body_text(),
            )),
        }
    }
}

/// Only a local reverse proxy may supply the client address, as the last
/// entry of the last `X-Forwarded-For` line: the proxy appends or overwrites
/// that one, and whatever came before it is the client's own doing. A last
/// entry that is not an address gives the peer's address, never an earlier
/// entry. Direct remote peers cannot spoof it. The peer's address is read in
/// its canonical form, so a proxy reaching a dual-stack listener as
/// `::ffff:127.0.0.1` is local too.
pub fn client(peer: Option<SocketAddr>, headers: &HeaderMap) -> IpAddr {
    let Some(peer) = peer else {
        return IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    };
    let peer = peer.ip().to_canonical();
    if peer.is_loopback()
        && let Some(ip) = headers
            .get_all("x-forwarded-for")
            .iter()
            .next_back()
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.rsplit(',').next())
            .and_then(|h| h.trim().parse().ok())
    {
        return ip;
    }
    peer
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A script reads `code`; `error` is the code itself or an explanation.
    #[tokio::test]
    async fn every_refusal_carries_a_code() {
        for (error, body) in [
            (
                ApiError::new(StatusCode::CONFLICT, "reset_required"),
                json!({"error": "reset_required", "code": "reset_required"}),
            ),
            (
                ApiError::said(StatusCode::BAD_REQUEST, "empty_content", "empty content"),
                json!({"error": "empty content", "code": "empty_content"}),
            ),
        ] {
            let response = error.into_response();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), body);
        }
    }
    #[test]
    fn only_loopback_peers_can_supply_forwarded_addresses() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "192.0.2.1, 192.0.2.2".parse().unwrap());
        assert_eq!(
            client(Some("127.0.0.1:80".parse().unwrap()), &headers),
            "192.0.2.2".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            client(Some("192.0.2.3:80".parse().unwrap()), &headers),
            "192.0.2.3".parse::<IpAddr>().unwrap()
        );
        assert_eq!(client(None, &headers), IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    }

    /// The proxy's entry is the last one of the last line. A client's own
    /// lines and entries come before it and count for nothing, and a last
    /// entry that is not an address falls back to the peer, never to one of
    /// the client's. A proxy reaching a dual-stack listener is local too.
    #[test]
    fn the_proxys_entry_is_the_last_of_the_last_line_and_nothing_else_is_trusted() {
        let local: SocketAddr = "127.0.0.1:80".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-for", "192.0.2.1".parse().unwrap());
        headers.append("x-forwarded-for", "192.0.2.2, 192.0.2.3".parse().unwrap());
        assert_eq!(
            client(Some(local), &headers),
            "192.0.2.3".parse::<IpAddr>().unwrap()
        );
        let mut malformed = HeaderMap::new();
        malformed.append(
            "x-forwarded-for",
            "192.0.2.1, not-an-address".parse().unwrap(),
        );
        assert_eq!(
            client(Some(local), &malformed),
            "127.0.0.1".parse::<IpAddr>().unwrap()
        );
        let mapped: SocketAddr = "[::ffff:127.0.0.1]:80".parse().unwrap();
        assert_eq!(
            client(Some(mapped), &headers),
            "192.0.2.3".parse::<IpAddr>().unwrap()
        );
        let mapped_remote: SocketAddr = "[::ffff:192.0.2.9]:80".parse().unwrap();
        assert_eq!(
            client(Some(mapped_remote), &headers),
            "192.0.2.9".parse::<IpAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn refusals_preserve_busy_and_revert_without_exposing_rpc_detail() {
        let cases = [
            (
                ApiError::from(RpcError::Busy),
                StatusCode::SERVICE_UNAVAILABLE,
                "host_busy",
            ),
            (
                ApiError::from(VerifyError::Busy),
                StatusCode::SERVICE_UNAVAILABLE,
                "the host is busy; try again shortly",
            ),
            (
                ApiError::from(RpcError::Unreachable("secret endpoint".into())),
                StatusCode::BAD_GATEWAY,
                "chain_read_unavailable",
            ),
            (
                ApiError::from(VerifyError::Rpc("secret endpoint".into())),
                StatusCode::BAD_GATEWAY,
                "could not verify anchoring; try again shortly",
            ),
            (
                ApiError::store_failed(anyhow::anyhow!("put failed: secret path")),
                StatusCode::INTERNAL_SERVER_ERROR,
                "the content store failed; try again shortly",
            ),
            (
                ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "simulation_refused")
                    .with_revert("0x1234".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
                "simulation_refused",
            ),
        ];
        for (error, status, message) in cases {
            let revert = error.revert.clone();
            let response = error.into_response();
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"], message);
            assert_eq!(
                body.get("revert").and_then(|v| v.as_str()),
                revert.as_deref()
            );
            assert!(!String::from_utf8_lossy(&bytes).contains("secret"));
        }
    }

    #[tokio::test]
    async fn throttle_survives_verification_and_http_conversion() {
        for error in [
            ApiError::from(RpcError::RateLimited(Duration::from_millis(1501))),
            ApiError::from(VerifyError::from(RpcError::RateLimited(
                Duration::from_millis(1501),
            ))),
        ] {
            let response = error.into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()["retry-after"], "2");
            let bytes = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["code"], "chain_read_unavailable");
        }
    }
}
