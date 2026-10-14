//! Fixtures shared by the index-follower tests: a chain followed from a
//! given block, the Posted logs it carries, and what the index made of them.

use super::*;
use crate::chain::Checkpoint;
use crate::index::{Index, State};

/// The body every message in these tests commits to.
pub(super) const ANCHORED: &[u8] = b"version/1 text/plain\nhello";

/// A Posted log for message `id` in `block` at `log_index`, shaped as the
/// mock's own: author 0x…0id, no parent, committing to `anchored`.
pub(super) fn posted(id: u64, block: u64, log_index: u64, anchored: &[u8]) -> Value {
    let word = |n: u64| format!("0x{n:064x}");
    let mut data = [0u8; 32].to_vec();
    data[24..32].copy_from_slice(&id.to_be_bytes());
    data.extend_from_slice(&keccak::hash(&[anchored]).0);
    data.extend_from_slice(&[0u8; 32]);
    json!({
        "address": CONTRACT.to_lowercase(),
        "topics": [hex0x(&keccak::hash(&[crate::abi::POSTED_SIGNATURE]).0), word(id), word(0), word(0)],
        "data": hex0x(&data),
        "blockNumber": format!("0x{block:x}"), "logIndex": format!("0x{log_index:x}"),
        "transactionHash": word(1000 + id), "transactionIndex": format!("0x{log_index:x}"),
        "blockHash": format!("0x{:064x}", 2000 + block), "removed": false,
    })
}

/// A host that follows chain 31337 from block 10, accepting blocks two deep.
pub(super) fn setup(url: Option<&str>) -> Setup {
    setup_from(10)(url)
}

/// `setup`, following from `start_block`.
pub(super) fn setup_from(start_block: u64) -> impl Fn(Option<&str>) -> Setup {
    move |url| Setup {
        verifier: chain_verifier(
            url.unwrap(),
            ChainConfig {
                start_block,
                confirmations: 2,
                ..ChainConfig::for_test(31337, "", CONTRACT)
            },
        ),
        ..Setup::default()
    }
}

pub(super) fn index(state: &AppState) -> &Index {
    &state.gateway.as_ref().unwrap().indexes[&31337]
}

pub(super) fn index_state(state: &AppState) -> State {
    index(state).state()
}

/// Why the index stopped; the test fails if it has not.
pub(super) fn stopped_reason(state: &AppState) -> String {
    match index_state(state) {
        State::Stopped(reason) => reason,
        other => panic!("expected a stop, got {other:?}"),
    }
}

/// Messages committed so far, read straight from the model.
pub(super) fn committed(state: &AppState) -> usize {
    index(state).view.read().unwrap().model.messages.len()
}

/// The committed checkpoint, read straight from the model.
pub(super) fn checkpoint(state: &AppState) -> Option<Checkpoint> {
    index(state).view.read().unwrap().model.at.checkpoint
}

/// The feed's status and its ids, newest first.
pub(super) async fn feed_ids(client: &reqwest::Client, base: &str) -> (u16, Vec<String>) {
    let response = client
        .post(format!("{base}/api/chains/31337/feed"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap();
    let ids = body["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|r| r["id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default();
    (status, ids)
}
