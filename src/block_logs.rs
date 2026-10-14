//! Reading a contract's logs from a provider, shared by the Board's index and
//! the keyring: a range of blocks as `eth_getLogs` answers it, and one block
//! the provider will not answer that way, picked out of its receipts.
//!
//! A provider limits log queries by result count or size, so a dense block
//! can be refused however narrow the range. It serves a block's receipts in
//! full, every transaction's logs included, under a bound of their own.

use crate::abi::{quantity, text};
use crate::chain::{Address, Hash};
use crate::{rpc, verifier::Chain};
use anyhow::{Result, anyhow, bail, ensure};
use serde_json::{Value, json};

/// Why a log query brought no logs.
pub enum Fetch {
    /// The round trip failed for reasons that say nothing about the range:
    /// the endpoint unreachable or answering with something other than JSON.
    Transient(anyhow::Error),
    /// This host had no upstream slot free.
    Busy,
    /// The endpoint refused the query as posed, or its answer exceeded this
    /// host's bound. A narrower range may fare better.
    Refused,
}

/// The logs of `addresses` with any of `topics` first in `from..=to`, as
/// `eth_getLogs` answers them.
pub async fn range_logs(
    client: &rpc::Upstream,
    chain: &Chain,
    addresses: &[Address],
    topics: &[Hash],
    from: u64,
    to: u64,
    bound: usize,
) -> Result<Vec<Value>, Fetch> {
    let query = json!([{
        "address": addresses, "fromBlock": format!("0x{from:x}"), "toBlock": format!("0x{to:x}"),
        "topics": [topics],
    }]);
    let mut answer = match rpc::call_as(
        rpc::Class::Ingestion,
        client,
        &chain.url,
        "eth_getLogs",
        query,
        bound,
    )
    .await
    {
        Ok(answer) => answer,
        Err(rpc::RpcError::Busy) => return Err(Fetch::Busy),
        Err(error) if error.is_transient() => return Err(Fetch::Transient(error.into())),
        Err(_) => return Err(Fetch::Refused),
    };
    if answer.get("error").is_some() {
        return Err(Fetch::Refused);
    }
    match answer["result"].take() {
        Value::Array(logs) => Ok(logs),
        _ => Err(Fetch::Refused),
    }
}

/// Why a block's receipts brought no logs.
#[derive(Debug)]
pub enum Receipts {
    /// The receipts exceed `bound`; the block will not shrink.
    TooLarge(usize),
    /// The endpoint does not serve `eth_getBlockReceipts`.
    Unsupported,
    /// Anything else, retryable: a transport failure, a refused call, or
    /// receipts that do not describe the block asked for.
    Failed(anyhow::Error),
}

impl std::fmt::Display for Receipts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge(bound) => {
                write!(f, "its receipts exceed the response bound of {bound} bytes")
            }
            Self::Unsupported => f.write_str("the endpoint does not serve eth_getBlockReceipts"),
            Self::Failed(error) => write!(f, "block receipts unavailable: {error:#}"),
        }
    }
}
impl std::error::Error for Receipts {}

impl Receipts {
    /// This failure as the follower's error for `block`: receipts over the
    /// bound make the block unreadable, no receipts count one more refusal
    /// of it in `refused`, and anything else retries next poll.
    pub fn reported(self, block: u64, refused: &mut Option<(u64, u32)>) -> anyhow::Error {
        match self {
            error @ Self::TooLarge(_) => Unreadable {
                block,
                reason: error.to_string(),
            }
            .into(),
            Self::Unsupported => refused_again(refused, block),
            Self::Failed(error) => error,
        }
    }
}

/// The logs of `addresses` with any of `topics` first in block `block`, whose
/// hash the caller has read as `hash`, picked out of its receipts, for a
/// block `eth_getLogs` will not answer whole. Only successful transactions'
/// logs count. Every receipt must name this block and hash: one that does not
/// is a replaced block or a confused provider, and the whole read fails.
pub async fn receipt_logs(
    client: &rpc::Upstream,
    chain: &Chain,
    addresses: &[Address],
    topics: &[Hash],
    block: u64,
    hash: Hash,
    bound: usize,
) -> Result<Vec<Value>, Receipts> {
    let mut answer = match rpc::call_as(
        rpc::Class::Ingestion,
        client,
        &chain.url,
        "eth_getBlockReceipts",
        json!([format!("0x{block:x}")]),
        bound,
    )
    .await
    {
        Ok(answer) => answer,
        Err(rpc::RpcError::TooLarge(bound)) => return Err(Receipts::TooLarge(bound)),
        Err(error) => return Err(Receipts::Failed(error.into())),
    };
    if let Some(error) = answer.get("error") {
        if error["code"] == json!(-32601) {
            return Err(Receipts::Unsupported);
        }
        return Err(Receipts::Failed(anyhow!("block receipts refused: {error}")));
    }
    pick(answer["result"].take(), addresses, topics, block, hash).map_err(Receipts::Failed)
}

fn pick(
    result: Value,
    addresses: &[Address],
    topics: &[Hash],
    block: u64,
    hash: Hash,
) -> Result<Vec<Value>> {
    let Value::Array(receipts) = result else {
        bail!("block receipts returned no list");
    };
    let mut logs = Vec::new();
    for mut receipt in receipts {
        ensure!(
            quantity(&receipt["blockNumber"])? == block,
            "receipt outside requested block"
        );
        ensure!(
            text(&receipt["blockHash"])?.parse::<Hash>()? == hash,
            "receipt disagrees with header"
        );
        if quantity(&receipt["status"])? != 1 {
            continue;
        }
        let Value::Array(entries) = receipt["logs"].take() else {
            bail!("receipt without logs");
        };
        for log in entries {
            let ours = log["address"]
                .as_str()
                .and_then(|a| a.parse::<Address>().ok())
                .is_some_and(|a| addresses.contains(&a))
                && log["topics"][0]
                    .as_str()
                    .and_then(|t| t.parse::<Hash>().ok())
                    .is_some_and(|t| topics.contains(&t));
            if ours {
                logs.push(log);
            }
        }
    }
    Ok(logs)
}

/// Response bound for one block's receipts, which carry every transaction's
/// logs: well over the ordinary RPC bound.
pub const RECEIPT_RESPONSE: usize = 128 << 20;

/// Consecutive polls a block refused as logs, with no receipts to fall back
/// on, is retried before it stops a follower: a few minutes at the
/// follower's longest backoff, long enough for a provider's rate limit to
/// pass.
pub const PERSISTENT_REFUSALS: u32 = 16;

/// A block this host cannot read from this provider as it stands. Retrying
/// cannot help, so the follower stops with this as its reason. The history
/// taken so far is intact: the operator changes the endpoint or the limit
/// and restarts, keeping the journal.
#[derive(Debug)]
pub struct Unreadable {
    pub block: u64,
    pub reason: String,
}

impl std::fmt::Display for Unreadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block {} cannot be read: {}", self.block, self.reason)
    }
}
impl std::error::Error for Unreadable {}

/// One more poll on which `block` was refused as logs with no receipts to
/// fall back on, counted in `slot`. The sixteenth in a row on the same
/// block is an [`Unreadable`]. Each follower keeps its own slot and clears
/// it when a range is taken in; transport failures neither count nor clear.
pub fn refused_again(slot: &mut Option<(u64, u32)>, block: u64) -> anyhow::Error {
    let count = match *slot {
        Some((refused, count)) if refused == block => count + 1,
        _ => 1,
    };
    *slot = Some((block, count));
    if count >= PERSISTENT_REFUSALS {
        return Unreadable {
            block,
            reason: format!(
                "eth_getLogs refused it {count} polls in a row and the endpoint does not serve eth_getBlockReceipts"
            ),
        }
        .into();
    }
    anyhow!(
        "block {block} refused as logs, poll {count} of {PERSISTENT_REFUSALS}, and the endpoint does not serve eth_getBlockReceipts"
    )
}
