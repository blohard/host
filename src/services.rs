//! Live reads needed to submit or recover a transaction. These are explicit
//! application operations, not a browser-selectable RPC method or call target.

use crate::chain::{Address, Hash, Hex};
use crate::http::JsonPath;
use crate::{
    http::ApiError,
    http::{json_error, json_ok},
    index::types::*,
    rpc,
    server::AppState,
    verifier::Chain,
};
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::FutureExt as _;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};

pub const POST_GAS_LIMIT: u64 = 500_000;

fn chain(state: &AppState, id: u64) -> Result<(&Chain, &rpc::Upstream), ApiError> {
    state
        .verifier
        .chain(id)
        .map(|(client, chain)| (chain, client))
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "chain_not_served"))
}

// Simulations may run during backfill, once the endpoint is verified.
fn simulation_chain(state: &AppState, id: u64) -> Result<(&Chain, &rpc::Upstream), ApiError> {
    let (chain, client) = chain(state, id)?;
    let Some(index) = state.index(id) else {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "chain_not_served"));
    };
    if !index.state().is_established() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "index_unavailable",
        ));
    }
    Ok((chain, client))
}

async fn simulation(
    client: &rpc::Upstream,
    chain: &Chain,
    author: Address,
    data: &[u8],
    estimate: bool,
) -> Result<Value, ApiError> {
    let response = rpc::call(client, &chain.url, if estimate { "eth_estimateGas" } else { "eth_call" }, json!([
        {"from": author, "to": crate::abi::hex0x(&chain.contract), "data": crate::abi::hex0x(data), "gas": format!("0x{POST_GAS_LIMIT:x}")}, "latest"
    ])).await.map_err(ApiError::from)?;
    if response.get("error").is_some() {
        if let Some(revert) = rpc::error_data(&response["error"]).filter(|s| {
            s.starts_with("0x") && s.len() <= 4098 && crate::abi::decode_hex(s).is_some()
        }) {
            return Err(
                ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "simulation_refused")
                    .with_revert(revert.to_owned()),
            );
        }
        return Err(ApiError::unavailable(&response["error"]));
    }
    response
        .get("result")
        .cloned()
        .ok_or_else(|| ApiError::unavailable("missing result"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    parent: Id,
    replier: Address,
    gate_data: Option<String>,
}

pub async fn admission(
    State(state): State<Arc<AppState>>,
    JsonPath(id): JsonPath<u64>,
    Json(req): Json<Admission>,
) -> Response {
    let (chain, client) = match simulation_chain(&state, id) {
        Ok(pair) => pair,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = state.policy.allow(&req.replier.0) {
        return error.into_response();
    }
    let blob = req.gate_data.as_deref().unwrap_or("0x");
    let Some(blob) = (blob.starts_with("0x") && blob.len() <= 32770)
        .then(|| crate::abi::decode_hex(blob))
        .flatten()
    else {
        return json_error(StatusCode::BAD_REQUEST, "invalid_gate_data");
    };
    if req.parent.0 == 0 {
        return json_ok(json!({"gate": Address::ZERO}));
    }
    let data = crate::abi::check_reply_calldata(req.parent.0, req.replier, &blob);
    match simulation(client, chain, req.replier, &data, false).await {
        Ok(value) => {
            let bytes = value.as_str().and_then(crate::abi::decode_hex);
            match bytes {
                Some(bytes) if bytes.len() == 32 && bytes[..12].iter().all(|&b| b == 0) => {
                    json_ok(json!({"gate": crate::abi::hex0x(&bytes[12..])}))
                }
                _ => ApiError::unavailable("invalid admission result").into_response(),
            }
        }
        Err(response) => response.into_response(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostEstimate {
    author: Address,
    content_hash: Hash,
    parent: Id,
    reply_gate: Option<Address>,
}

pub async fn post_estimate(
    State(state): State<Arc<AppState>>,
    JsonPath(id): JsonPath<u64>,
    Json(req): Json<PostEstimate>,
) -> Response {
    let (chain, client) = match simulation_chain(&state, id) {
        Ok(pair) => pair,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = state.policy.allow(&req.author.0) {
        return error.into_response();
    }
    let data = crate::abi::post_calldata(
        req.content_hash,
        req.parent.0,
        req.reply_gate.unwrap_or(Address::ZERO),
    );
    match simulation(client, chain, req.author, &data, true).await {
        Ok(value) => match value
            .as_str()
            .and_then(|s| crate::abi::parse_quantity(s).ok())
        {
            Some(gas) if gas > 0 && gas <= POST_GAS_LIMIT => {
                json_ok(json!({"gas": (gas + gas / 4).min(POST_GAS_LIMIT).to_string()}))
            }
            Some(_) => json_error(StatusCode::UNPROCESSABLE_ENTITY, "post_exceeds_gas_limit"),
            None => ApiError::unavailable("invalid gas estimate").into_response(),
        },
        Err(response) => response.into_response(),
    }
}

type Check = futures::future::BoxFuture<'static, Result<Value, ApiError>>;
/// Above this many entries, checks nobody waits for are pruned.
const RECEIPT_ENTRIES: usize = 1024;

/// Receipt checks in flight on one chain, by transaction hash. Identical
/// checks at the same moment (two tabs, or a composer and its recovery) share
/// one read of the chain, and only while someone waits: a check whose readers
/// all left is dropped with its chain calls, never left running, so a reader
/// that disconnects frees what it used. Nothing is kept once answered:
/// pending must be asked again, and a mined receipt is cheap to read again.
#[derive(Default)]
pub struct Receipts(Arc<std::sync::Mutex<HashMap<Hash, futures::future::WeakShared<Check>>>>);

impl Receipts {
    async fn check(&self, hash: Hash, work: impl FnOnce() -> Check) -> Result<Value, ApiError> {
        let lock = || {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        };
        let shared = {
            let mut checks = lock();
            match checks.get(&hash).and_then(|check| check.upgrade()) {
                Some(running) => running,
                None => {
                    if checks.len() >= RECEIPT_ENTRIES {
                        checks.retain(|_, check| check.upgrade().is_some());
                    }
                    let map = self.0.clone();
                    let read = work();
                    // The entry goes before the answer is handed out, so a
                    // reader arriving after it starts a read of its own.
                    let running = async move {
                        let answer = read.await;
                        map.lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&hash);
                        answer
                    }
                    .boxed()
                    .shared();
                    checks.insert(hash, running.downgrade().expect("not yet polled"));
                    running
                }
            }
        };
        shared.await
    }
}

pub async fn transaction(
    State(state): State<Arc<AppState>>,
    JsonPath((id, hash)): JsonPath<(u64, Hash)>,
) -> Response {
    let (chain, _) = match chain(&state, id) {
        Ok(pair) => pair,
        Err(error) => return error.into_response(),
    };
    let work = state.clone();
    match chain
        .receipts
        .check(hash, || {
            async move { receipt(&work, id, hash).await }.boxed()
        })
        .await
    {
        Ok(value) => json_ok(value),
        Err(error) => error.into_response(),
    }
}

async fn receipt(state: &AppState, id: u64, hash: Hash) -> Result<Value, ApiError> {
    let (chain, client) = chain(state, id)?;
    // A running index already verified this endpoint. Without one, verify
    // directly so receipt recovery still works before backfill or after a stop.
    let verified = state
        .index(id)
        .is_some_and(|index| index.state().is_established());
    if !verified && let Err(error) = state.verifier.check_chain(id, rpc::Class::Client).await {
        return Err(match error.downcast::<rpc::RpcError>() {
            Ok(error) => ApiError::from(error),
            Err(error) => ApiError::unavailable(error),
        });
    }
    let response = rpc::call(
        client,
        &chain.url,
        "eth_getTransactionReceipt",
        json!([hash]),
    )
    .await?;
    let receipt = match rpc::optional_result(&response)? {
        None => return Ok(json!({"hash": hash, "status": "pending"})),
        Some(value) => value,
    };
    let decoded = async {
        let receipt = crate::abi::Receipt::parse(receipt, hash)?;
        let header = crate::chain::header(client, chain, receipt.block, rpc::Class::Client).await?;
        anyhow::ensure!(receipt.block_hash == header.checkpoint.hash, "receipt no longer canonical");
        let registries: Vec<_> = chain.blocking.iter().map(|b| Hex(b.registry)).collect();
        let events = receipt.events(Hex(chain.contract), &registries, header.timestamp)?;
        Ok::<_, anyhow::Error>(json!({"hash": hash, "status": if receipt.succeeded { "success" } else { "reverted" }, "block": receipt.block.to_string(), "block_hash": receipt.block_hash, "events": events}))
    }.await;
    // The header lookup inside is metered too: out of budget there is the
    // same retryable busy as on the first call.
    decoded.map_err(|error| match error.downcast::<rpc::RpcError>() {
        Ok(error) => ApiError::from(error),
        Err(error) => ApiError::unavailable(error),
    })
}
