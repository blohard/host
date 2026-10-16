//! The read side: what the host is (`/api/status`); stored bytes by hash
//! (`/api/content/{hash}`), pulled from a peer when a reader names the
//! anchoring message and the store lacks them. Indexed queries, transaction
//! services and verified names have their own modules.

use crate::abi::hex0x;
use crate::envelope;
use crate::gate_validation::Kind;
use crate::http::ApiError;
use crate::keccak::Digest;
use crate::server::StoreState;
use crate::{
    http::{JsonPath, JsonQuery, json_ok, json_said},
    server::{AppState, NO_STORE},
};
use axum::{
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub async fn status(State(state): State<Arc<AppState>>) -> Response {
    // Chains are reported by id, name, contract, deployment block, and the
    // operator's explicitly public RPC URL (if any) — never the verifying
    // RPC URL, which may embed an API key.
    let chains: Vec<_> = state
        .verifier
        .chains()
        .into_iter()
        .map(|(id, c)| {
            let published = |kind| state.gate_checks.published(id, c, kind);
            let blocking = c
                .blocking
                .as_ref()
                .filter(|_| published(Kind::Blocklist).is_some());
            json!({
                "id": id,
                "name": c.name,
                "contract": hex0x(&c.contract),
                "public_rpc_url": c.public_rpc_url,
                "native_currency": c.native_currency,
                "start_block": c.start_block,
                "confirmations": c.confirmations,
                "index": state.gateway.as_ref().and_then(|g| g.indexes.get(&id).map(|index| {
                    serde_json::to_value(index.status(g.indexes.keys())).expect("index status")
                })),
                "curated": state.policy.enabled(),
                "closed_gate": published(Kind::Closed),
                // Configured gates still being validated are omitted above.
                "gates_pending": state.gate_checks.pending(id, c),
                "key_gates": c.key_gates.as_ref().map(|_| json!({
                    "holders": published(Kind::Keys),
                    "holders_blocklist": published(Kind::KeysBlocklist),
                })),
                // Display policy is available even while the suggested gate is unverified.
                "blocklist": c.blocking.as_ref().map(|b| json!({
                    "registry": hex0x(&b.registry),
                    "list": b.list,
                })),
                "blocking": blocking.map(|b| json!({
                    "gate": hex0x(&b.gate),
                    "registry": hex0x(&b.registry),
                    "list": b.list,
                })),
            })
        })
        .collect();
    // Each role reports its own facts, or null when this host does not play it.
    let store = state.store.as_ref().map(|s| {
        let (held_entries, held_bytes) = s.holding.stats();
        json!({
            "max_content_size": s.bodies.max_size(),
            "envelope": { "version": envelope::VERSION },
            "attachments": s.attachments.as_ref().map(|(_, policy)| json!({
                "max_bytes": policy.max_bytes,
                "max_per_message": policy.max_per_message,
                "media_types": policy.media_types,
            })),
            "peers": s.peers.urls(),
            "holding": {
                "entries": held_entries,
                "bytes": held_bytes,
                "max_bytes": s.holding.config().max_bytes,
                "max_per_signer": s.holding.config().max_per_signer,
                "max_bytes_per_signer": s.holding.config().max_bytes_per_signer,
                "max_entries": s.holding.config().max_entries,
                "ttl_secs": s.holding.config().ttl.as_secs(),
            },
        })
    });
    let gateway = if let Some(g) = &state.gateway {
        let previews = g.link_previews.as_ref().filter(|_| state.store.is_some());
        let ready = match previews {
            Some(previews) => previews.ready().await,
            None => false,
        };
        Some(json!({
            "names": g.names.is_some(),
            "link_previews": previews.is_some(),
            "link_previews_ready": ready,
        }))
    } else {
        None
    };
    let mut response = json_ok(json!({
        "verifier": state.verifier.mode(),
        "upstream": state.verifier.upstream().map(|u| u.counts()),
        "notice": state.notice,
        "host_policy": state.policy.public(),
        "chains": chains,
        "store": store,
        "gateway": gateway,
        "keyrings": state.keyrings.values().map(|k| k.status()).collect::<Vec<_>>(),
    }));
    if state
        .gateway
        .as_ref()
        .is_some_and(|g| g.indexes.values().any(|index| index.state().is_stopped()))
    {
        *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    }
    // Public, read-only: any origin may ask what this host is.
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

/// Optional on `GET /api/content/{hash}`: the message that anchors the record,
/// so a host that lacks it may ask its peers
/// (https://github.com/blohard/host/blob/main/STORE_API.md).
#[derive(Deserialize)]
pub struct ContentQuery {
    chain_id: Option<u64>,
    message_id: Option<u64>,
}

/// How long a cache may reuse a record before asking again. Bytes behind a
/// hash never change, but this host may stop serving them: a takedown reaches
/// caches that follow the rules within this time.
const CONTENT_CACHE: &str = "public, max-age=300, must-revalidate";

/// Whether a content request must be admitted as a live read before the
/// handler runs: on a store-only host with peers, it names a message for a
/// record this host lacks, so the handler would ask the chain about that
/// message and then peers. Everything else is free, since none of it leads
/// to that work: a host with an index vouches for the record from it, one
/// without peers fetches nothing, and a record this host has, a request
/// naming no message, a taken-down hash or a malformed request asks nobody.
/// A request admitted here reads the store again in the handler: a local
/// read, next to the chain and peers it guards.
pub async fn asks_the_chain(request: &mut axum::extract::Request, state: &AppState) -> bool {
    use axum::RequestExt as _;
    if state.gateway.is_some() || !crate::index::Host::has_peers(state) {
        return false;
    }
    let Ok(JsonPath(hash)) = request.extract_parts::<JsonPath<String>>().await else {
        return false;
    };
    let Ok(JsonQuery(anchor)) = request.extract_parts::<JsonQuery<ContentQuery>>().await else {
        return false;
    };
    let (Some(digest), Some(store)) = (Digest::from_hex(&hash), state.store.as_ref()) else {
        return false;
    };
    if anchor.chain_id.is_none()
        || anchor.message_id.is_none()
        || state.policy.taken_down(&digest.0)
    {
        return false;
    }
    // A store read failing is the handler's to report, free.
    matches!(from_store(store, &digest).await, Ok(None))
}

/// The record `digest` from this host's store: bodies first, then
/// attachments. A hash can be in both, as when an attachment repeats a body's
/// bytes, and names the same bytes in either.
async fn from_store(store: &StoreState, digest: &Digest) -> anyhow::Result<Option<Vec<u8>>> {
    if let Some(bytes) = store.bodies.get(digest).await? {
        return Ok(Some(bytes));
    }
    match &store.attachments {
        Some((attachments, _)) => attachments.get(digest).await,
        None => Ok(None),
    }
}

pub async fn content(
    State(state): State<Arc<AppState>>,
    JsonPath(hash): JsonPath<String>,
    JsonQuery(anchor): JsonQuery<ContentQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Some(digest) = Digest::from_hex(&hash) else {
        return json_said(
            StatusCode::BAD_REQUEST,
            "malformed_request",
            "malformed hash",
        );
    };
    // Before anything is read or fetched, and with or without an index.
    if state.policy.taken_down(&digest.0) {
        return crate::host_policy::takedowns::refused().respond();
    }
    let Some(store) = state.store.as_ref() else {
        return json_said(StatusCode::NOT_FOUND, "no_store", NO_STORE);
    };
    let mut found = match from_store(store, &digest).await {
        Ok(v) => v,
        Err(e) => return ApiError::store_failed(e).into_response(),
    };
    // Not here. A reader who names the anchoring message lets us ask peers.
    if found.is_none()
        && let (Some(chain_id), Some(message_id)) = (anchor.chain_id, anchor.message_id)
    {
        found = match crate::peers::pull(&state, store, chain_id, message_id, digest).await {
            Ok(pulled) => pulled.stored().map(|b| b.to_vec()),
            Err(error) => return error.respond(),
        };
    }
    let Some(bytes) = found else {
        return json_said(
            StatusCode::NOT_FOUND,
            "content_not_found",
            "unknown content hash",
        );
    };
    // Again, now the bytes are here: a takedown may have landed while the
    // store or a peer was being read.
    if state.policy.taken_down(&digest.0) {
        return crate::host_policy::takedowns::refused().respond();
    }
    // The hash names the bytes, so it is their ETag. A cache asking again
    // gets 304 only for a record this host still serves.
    let etag = format!("\"{}\"", digest.to_hex());
    let shared = [
        (header::ETAG, HeaderValue::from_str(&etag).expect("hex")),
        (
            header::CACHE_CONTROL,
            HeaderValue::from_static(CONTENT_CACHE),
        ),
        // Public bytes, verified by whoever fetches them: any origin may.
        (
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        ),
    ];
    let revalidated = headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|tag| {
            let tag = tag.trim();
            tag == "*" || tag.trim_start_matches("W/") == etag
        });
    if revalidated {
        return (StatusCode::NOT_MODIFIED, shared).into_response();
    }

    // Serve the type the record declares. Records are untrusted bytes, so
    // whatever the type, the browser must neither sniff nor run them.
    let content_type = match envelope::parse(&bytes) {
        Ok(h) if h.media_type.starts_with("text/") => format!("{}; charset=utf-8", h.media_type),
        Ok(h) => h.media_type.to_string(),
        Err(_) => "application/octet-stream".to_string(),
    };
    (
        StatusCode::OK,
        shared,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
                    .unwrap_or(HeaderValue::from_static("application/octet-stream")),
            ),
            (
                header::X_CONTENT_TYPE_OPTIONS,
                HeaderValue::from_static("nosniff"),
            ),
            (
                header::CONTENT_SECURITY_POLICY,
                HeaderValue::from_static("default-src 'none'; sandbox"),
            ),
        ],
        bytes,
    )
        .into_response()
}
