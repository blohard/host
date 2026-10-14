//! Preview request authorization, cache shortcuts and HTTP responses.
//! The service owns detached fetching and its permits; handlers only authorize
//! a post (or draft) and pass the visitor permit into that work.
use super::{Entry, PreviewSource, Previews, link_url};
use crate::{chain::Hash, http::ApiError, server::AppState};
use anyhow::Result;
use axum::{
    Json,
    extract::{Extension, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use std::{sync::Arc, time::Instant};
use tokio::sync::OwnedSemaphorePermit;

impl Previews {
    /// Cache hits spend no outbound capacity or visitor allowance.
    pub fn cached(&self, uri: &axum::http::Uri) -> Option<Response> {
        let link = if uri.path() == "/api/link-preview/draft" {
            Query::<DraftLookup>::try_from_uri(uri).ok()?.0.link
        } else {
            Query::<Lookup>::try_from_uri(uri).ok()?.0.link
        };
        let url = link_url(&link).ok()?;
        Some(self.cache.get(&url.to_string())?.into_response())
    }

    // A custom header excludes browser no-cors requests. Origin and Fetch
    // Metadata checks reject cross-site calls before cache lookup or admission.
    pub fn allows_request(&self, headers: &HeaderMap) -> bool {
        headers.get("x-blohard-preview").is_some_and(|h| h == "1")
            && headers
                .get("sec-fetch-site")
                .is_none_or(|h| h == "same-origin")
            && headers.get("origin").is_none_or(|h| {
                h.to_str()
                    .ok()
                    .is_some_and(|origin| self.fetcher.allows_origin(origin))
            })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lookup {
    chain: u64,
    id: u64,
    // Original link text, not a separately supplied destination. This supports
    // bare domains and URL normalization without a second linkifier on the host.
    link: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DraftLookup {
    link: String,
}

/// The post asking for a preview, from the index alone: it must exist, be
/// neither deleted nor removed, not be by a blocked author, and not have its
/// body taken down. Cached previews are shown only to such a post; refusing
/// one post leaves the link's preview to the others.
fn asking_post(state: &AppState, chain: u64, id: u64) -> Result<Hash, ApiError> {
    match state.read_index(chain, |model| {
        model
            .messages
            .get(&id)
            .filter(|m| {
                m.stands_alone()
                    && !state.policy.blocked(&m.author.0)
                    && !state.policy.taken_down(&m.content_hash.0)
            })
            .map(|m| m.content_hash)
    }) {
        Ok(Some(hash)) => Ok(hash),
        Ok(None) | Err(crate::verifier::VerifyError::UnsupportedChain(_)) => {
            Err(ApiError::new(StatusCode::NOT_FOUND, "message_not_found"))
        }
        Err(_) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "preview_unavailable",
        )),
    }
}

/// Only the post's locally stored plain text containing the link authorizes
/// a cache miss. Neither peers nor chain RPCs may be contacted to authorize
/// preview fetching.
async fn authorize(state: &AppState, hash: Hash, link: &str) -> Result<(), ApiError> {
    let Some(store) = state.store.as_ref() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "preview_unavailable",
        ));
    };
    let bytes = match store.bodies.get(&crate::keccak::Digest(hash.0)).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "preview_unavailable",
            ));
        }
        Err(error) => return Err(ApiError::store_failed(error)),
    };
    if !contains_link(&bytes, link) {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "link_not_in_post"));
    }
    Ok(())
}

/// A refusal of the post or its link, as the handler answers it: the host
/// being unable to say is "unavailable", anything else is its own answer.
fn refused(error: ApiError) -> Response {
    if error.status == StatusCode::SERVICE_UNAVAILABLE {
        unavailable()
    } else {
        error.into_response()
    }
}
pub(super) fn contains_link(bytes: &[u8], link: &str) -> bool {
    let Ok(header) = crate::envelope::parse(bytes) else {
        return false;
    };
    header.media_type == "text/plain"
        && !link.is_empty()
        && std::str::from_utf8(&bytes[header.body_start..]).is_ok_and(|text| {
            // A URL embedded in another URL must not authorize a different
            // destination. Permit surrounding prose punctuation and emphasis,
            // but not a prefix of a longer hostname, path or query.
            let boundary = |c: char| {
                c.is_whitespace() || "<>\"'`()[]{}“”‘’«»‹›「」『』【】（）［］｛｝—–".contains(c)
            };
            text.match_indices(link).any(|(start, _)| {
                let before = &text[..start];
                let after = &text[start + link.len()..];
                let left = before.chars().next_back().is_none_or(boundary)
                    || before
                        .strip_suffix(['*', '_'])
                        .is_some_and(|s| s.chars().next_back().is_none_or(boundary));
                let tail = after.trim_start_matches(['.', ',', ';', ':', '!', '?', '*', '_']);
                left && (after.chars().next().is_none_or(boundary)
                    || tail.chars().next().is_none_or(boundary))
            })
        })
}
/// Reject cross-site readers before revealing cached URLs or spending capacity.
pub fn authorize_or_cached_response(
    request: &axum::extract::Request,
    state: &AppState,
) -> Option<Response> {
    let service = state.gateway.as_ref()?.link_previews.as_ref()?;
    if !service.allows_request(request.headers()) {
        return Some(crate::http::json_error(
            StatusCode::FORBIDDEN,
            "preview_origin_forbidden",
        ));
    }
    // A post's cached preview still needs the post to be one that may ask;
    // when it is not, the handler answers why.
    if request.uri().path() != "/api/link-preview/draft" {
        let query = Query::<Lookup>::try_from_uri(request.uri()).ok()?.0;
        asking_post(state, query.chain, query.id).ok()?;
    }
    service.cached(request.uri())
}

pub async fn lookup(
    State(state): State<Arc<AppState>>,
    Query(query): Query<Lookup>,
    permit: Option<Extension<Arc<OwnedSemaphorePermit>>>,
) -> Response {
    let Some(service) = state
        .gateway
        .as_ref()
        .and_then(|g| g.link_previews.as_ref())
    else {
        return unavailable();
    };
    let Ok(url) = link_url(&query.link) else {
        return crate::http::json_error(StatusCode::BAD_REQUEST, "invalid_preview_url");
    };
    let hash = match asking_post(&state, query.chain, query.id) {
        Ok(hash) => hash,
        Err(error) => return refused(error),
    };
    if let Some(entry) = service.cache.get(&url.to_string()) {
        return entry.into_response();
    }
    if let Err(error) = authorize(&state, hash, &query.link).await {
        return refused(error);
    }
    match service
        .read_admitted(
            url,
            PreviewSource::Post {
                chain: query.chain,
                id: query.id,
            },
            permit.map(|Extension(p)| p),
        )
        .await
    {
        Ok(entry) => entry.into_response(),
        Err(seconds) => unavailable_for(seconds),
    }
}

/// Composer-only discovery, without a post or draft upload. Visitor admission
/// plus worker-owned capacity and rate limits bound unpublished fetches.
pub async fn draft(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DraftLookup>,
    permit: Option<Extension<Arc<OwnedSemaphorePermit>>>,
) -> Response {
    let Some(service) = state
        .gateway
        .as_ref()
        .and_then(|g| g.link_previews.as_ref())
    else {
        return unavailable();
    };
    if state.store.is_none() {
        return unavailable();
    }
    let Ok(url) = link_url(&query.link) else {
        return crate::http::json_error(StatusCode::BAD_REQUEST, "invalid_preview_url");
    };
    if let Some(entry) = service.cache.get(&url.to_string()) {
        return entry.into_response();
    }
    match service
        .read_admitted(url, PreviewSource::Draft, permit.map(|Extension(p)| p))
        .await
    {
        Ok(entry) => {
            let mut response = entry.into_response();
            response
                .headers_mut()
                .insert("cache-control", "no-store".parse().unwrap());
            response
        }
        Err(seconds) => unavailable_for(seconds),
    }
}

fn unavailable() -> Response {
    unavailable_for(60)
}
pub(super) fn unavailable_for(seconds: u64) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            ("cache-control", "no-store".to_string()),
            ("retry-after", seconds.to_string()),
        ],
        Json(serde_json::json!({ "error": "preview_unavailable", "code": "preview_unavailable" })),
    )
        .into_response()
}

impl IntoResponse for Entry {
    fn into_response(self) -> Response {
        match self.value {
            Ok(preview) => (
                [(
                    "cache-control",
                    format!(
                        "public, max-age={}",
                        self.until
                            .saturating_duration_since(Instant::now())
                            .as_secs()
                    ),
                )],
                Json(serde_json::json!({ "preview": preview })),
            )
                .into_response(),
            Err(()) => {
                let remaining = self.until.saturating_duration_since(Instant::now());
                unavailable_for(remaining.as_secs() + u64::from(remaining.subsec_nanos() != 0))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{link_preview::Config, server::GatewayState, verifier::Verifier};
    use axum::{body::Body, http::Request};
    use std::{collections::HashMap, time::Duration};

    #[tokio::test]
    async fn an_unavailable_preview_answers_in_the_shared_refusal_shape() {
        let response = unavailable_for(5);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "5");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({ "error": "preview_unavailable", "code": "preview_unavailable" })
        );
    }

    #[tokio::test]
    async fn origin_checks_precede_warm_cache_hits_even_when_fetch_capacity_is_full() {
        let service = Arc::new(
            Previews::new(Config {
                public_names: vec!["operator.invalid".into()],
            })
            .unwrap(),
        );
        service.cache.put(
            "https://article.example/".into(),
            Entry {
                value: Ok(None),
                until: Instant::now() + Duration::from_secs(60),
            },
            Duration::from_secs(60),
        );
        let _shared = service.slots.acquire_many(8).await.unwrap();
        let _drafts = service.drafts.acquire_many(2).await.unwrap();
        let state = AppState {
            policy: Default::default(),
            gate_checks: Default::default(),
            keyrings: Default::default(),
            verifier: Verifier::AllowAll,
            notice: None,
            store: None,
            gateway: Some(GatewayState {
                indexes: HashMap::new(),
                names: None,
                link_previews: Some(service.clone()),
            }),
        };
        for uri in [
            "/api/link-preview?chain=1&id=1&link=https://article.example/",
            "/api/link-preview/draft?link=https://article.example/",
        ] {
            let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            assert_eq!(
                authorize_or_cached_response(&request, &state)
                    .unwrap()
                    .status(),
                403
            );
            for (header, value) in [
                ("origin", "https://attacker.example"),
                ("origin", "null"),
                ("sec-fetch-site", "cross-site"),
            ] {
                let request = Request::builder()
                    .uri(uri)
                    .header("x-blohard-preview", "1")
                    .header(header, value)
                    .body(Body::empty())
                    .unwrap();
                assert_eq!(
                    authorize_or_cached_response(&request, &state)
                        .unwrap()
                        .status(),
                    403
                );
            }
            let request = Request::builder()
                .uri(uri)
                .header("x-blohard-preview", "1")
                .header("origin", "https://operator.invalid")
                .body(Body::empty())
                .unwrap();
            let answer = authorize_or_cached_response(&request, &state);
            if uri.contains("/draft") {
                assert_eq!(answer.unwrap().status(), 200);
            } else {
                // A post's cached preview needs the post, which this host
                // has no index for: the handler answers why.
                assert!(answer.is_none());
            }
        }
        assert_eq!(service.slots.available_permits(), 0);
        assert_eq!(service.drafts.available_permits(), 0);
    }
    #[test]
    fn posted_link_text_derives_the_destination_without_accepting_an_unposted_url() {
        let bytes =
            "version/1 text/plain\nsee *example.org/café#intro* or https://example.net/a?b=1&c=2."
                .as_bytes();
        for link in ["example.org/café#intro", "https://example.net/a?b=1&c=2"] {
            let query = Lookup {
                chain: 1,
                id: 1,
                link: link.into(),
            };
            assert!(contains_link(bytes, link));
            let url = link_url(&query.link).unwrap();
            assert!(url.scheme() == "https" && url.fragment().is_none());
        }
        let query = Lookup {
            chain: 1,
            id: 1,
            link: "example.org/?next=https://other.example/".into(),
        };
        assert_eq!(
            link_url(&query.link).unwrap().as_str(),
            "https://example.org/?next=https://other.example/"
        );
        for (text, link) in [
            ("https://example.org/path", "example.org/path"),
            ("https://example.org.evil/path", "https://example.org"),
            ("https://example.org/path", "https://example.org"),
            ("https://example.org/?target=evil.example", "evil.example"),
            ("word_example.org", "example.org"),
        ] {
            assert!(!contains_link(
                format!("version/1 text/plain\n{text}").as_bytes(),
                link
            ));
        }
        for text in [
            "example.org.",
            "(example.org)",
            "*example.org*",
            "_example.org_",
            "read example.org, next",
            "“example.org”",
            "‘example.org’",
            "—example.org—",
            "«example.org»",
            "「example.org」",
            "example.org: read this",
        ] {
            assert!(
                contains_link(
                    format!("version/1 text/plain\n{text}").as_bytes(),
                    "example.org"
                ),
                "{text}"
            );
        }
        for link in ["example.org/other", "https://example.net/a?b=1&c=3", ""] {
            assert!(!contains_link(bytes, link));
        }
        assert!(!contains_link(
            b"version/1 image/png\nexample.org/",
            "example.org/"
        ));
        assert!(!contains_link(
            b"not an envelope example.org/",
            "example.org/"
        ));
    }
}
