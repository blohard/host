//! The key index over HTTP: requests, limits, gate validation and error codes.
//! The index answers through its read methods (crate::keyring); nothing here
//! touches its model or locks.

use crate::{
    chain::{Address, Hex, Position},
    http::{JsonPath, JsonQuery, json_error, json_ok},
    index::types::ListKey,
    keyring::BalanceView,
    server::AppState,
};
use axum::{extract::State, http::StatusCode, response::Response};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldingsQuery {
    pub(crate) gate: Address,
    pub(crate) pairs: Vec<HoldingPair>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HoldingPair {
    pub(crate) creator: Address,
    pub(crate) holder: Address,
}

fn syncing() -> Response {
    json_error(StatusCode::SERVICE_UNAVAILABLE, "keyring_syncing")
}

/// Bounded point lookups, independent of the truncated keyring feed lists.
pub async fn holdings(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    axum::Json(query): axum::Json<HoldingsQuery>,
) -> Response {
    if query.pairs.is_empty() || query.pairs.len() > 50 {
        return json_error(StatusCode::BAD_REQUEST, "invalid_holdings_count");
    }
    if !state
        .gate_checks
        .contains(chain, query.gate, crate::gate_validation::Kind::Keys)
        && !state.gate_checks.contains(
            chain,
            query.gate,
            crate::gate_validation::Kind::KeysBlocklist,
        )
    {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "key_gate_unverified");
    }
    let Some(index) = state.keyrings.get(&chain) else {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "keyring_not_configured");
    };
    let pairs: Vec<_> = query.pairs.iter().map(|p| (p.creator, p.holder)).collect();
    index.held(&pairs).map(json_ok).unwrap_or_else(syncing)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BalanceQuery {
    #[serde(default)]
    pub(crate) view: BalanceView,
    pub(crate) after: Option<Address>,
    #[serde(default = "balance_page_size")]
    pub(crate) limit: usize,
}
fn balance_page_size() -> usize {
    64
}

/// Address cursors seek directly into the index; no RPC or full-list scan.
pub async fn balances(
    State(state): State<Arc<AppState>>,
    JsonPath((chain, account)): JsonPath<(u64, Address)>,
    JsonQuery(query): JsonQuery<BalanceQuery>,
) -> Response {
    if !(1..=256).contains(&query.limit) {
        return json_error(StatusCode::BAD_REQUEST, "invalid_keyring_limit");
    }
    let Some(index) = state.keyrings.get(&chain) else {
        return json_error(StatusCode::NOT_FOUND, "keyring_not_configured");
    };
    index
        .balances(account, query.view, query.after, query.limit)
        .map(json_ok)
        .unwrap_or_else(syncing)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradesQuery {
    pub(crate) before_block: Option<u64>,
    pub(crate) before_log: Option<u32>,
    #[serde(default = "trade_page_size")]
    pub(crate) limit: usize,
}
fn trade_page_size() -> usize {
    20
}

/// Creator notifications read retained events and the host's blocklist index only.
pub async fn trades(
    State(state): State<Arc<AppState>>,
    JsonPath((chain, creator)): JsonPath<(u64, Address)>,
    JsonQuery(query): JsonQuery<TradesQuery>,
) -> Response {
    if !(1..=100).contains(&query.limit)
        || query.before_block.is_some() != query.before_log.is_some()
    {
        return json_error(StatusCode::BAD_REQUEST, "invalid_trade_query");
    }
    let Some(index) = state.keyrings.get(&chain) else {
        return json_error(StatusCode::NOT_FOUND, "keyring_not_configured");
    };
    let before = query
        .before_block
        .zip(query.before_log)
        .map(|(block, log)| Position { block, log });
    let blocking = state
        .verifier
        .config(chain)
        .and_then(|c| c.blocking.as_ref())
        .map(|b| ListKey {
            registry: Hex(b.registry),
            owner: creator,
            list: Hex(crate::keccak::hash(&[b.list.as_bytes()]).0),
        });
    // The blocklist filters the page before it is cut, read from the Board index.
    let answer = index.trades(creator, before, query.limit, |page| match &blocking {
        Some(key) => state.read_index(chain, |board| page(board.lists.get(key))),
        None => Ok(page(None)),
    });
    match answer {
        None => syncing(),
        Some(Ok(page)) => json_ok(page),
        Some(Err(_)) => json_error(StatusCode::SERVICE_UNAVAILABLE, "index_unavailable"),
    }
}

pub async fn lookup(
    State(state): State<Arc<AppState>>,
    JsonPath((chain, holder)): JsonPath<(u64, Address)>,
) -> Response {
    let Some(index) = state.keyrings.get(&chain) else {
        return json_error(StatusCode::NOT_FOUND, "keyring_not_configured");
    };
    index.lookup(holder).map(json_ok).unwrap_or_else(syncing)
}

/// Public market activity is served from the accepted index, never from RPC.
pub async fn activity(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    JsonQuery(query): JsonQuery<TradesQuery>,
) -> Response {
    if !(1..=100).contains(&query.limit)
        || query.before_block.is_some() != query.before_log.is_some()
    {
        return json_error(StatusCode::BAD_REQUEST, "invalid_trade_query");
    }
    let Some(index) = state.keyrings.get(&chain) else {
        return json_error(StatusCode::NOT_FOUND, "keyring_not_configured");
    };
    let before = query
        .before_block
        .zip(query.before_log)
        .map(|(block, log)| Position { block, log });
    index
        .activity(before, query.limit, |address| {
            state.policy.blocked(&address.0)
        })
        .map(json_ok)
        .unwrap_or_else(syncing)
}
