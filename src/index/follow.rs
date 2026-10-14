//! RPC belongs to ingestion, not to indexed queries. Only complete accepted
//! block ranges are submitted to the writer. Failed fetches advance nothing.

use super::Host;
use super::{
    HistoryChanged, Index,
    model::{Batch, Event, MAX_EVENTS},
};
use crate::abi::{decode_event, quantity, text, topics};
use crate::block_logs::{Fetch, Unreadable, range_logs, receipt_logs};
use crate::chain::{Hash, Hex, header};
use crate::{rpc, verifier::Chain};
use anyhow::{Result, anyhow, ensure};
use std::collections::BTreeMap;
use std::{sync::Arc, time::Duration};

const BLOCK_SPAN: u64 = 2_000;
/// Consecutive polls on which a range was assembled but rejected as invalid
/// history before the chain stops naming it. A reorg or a provider answering
/// from a lagging replica can look the same for a while.
pub(crate) const PERSISTENT_REJECTIONS: u32 = 16;

/// What one poll may take in. Production uses [`Limits::DEFAULT`]; tests
/// shrink these to reach the limits with a handful of events.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Events one block may carry into the index.
    pub max_events: usize,
    /// Response bound for a log query, over a range or a single block.
    pub log_response: usize,
    /// Response bound for one block's receipts: `max_events` of our logs as a
    /// provider spells them, plus unrelated receipts. This is a hard ceiling,
    /// not a guarantee that every served chain's block will fit.
    pub receipt_response: usize,
}

impl Limits {
    pub const DEFAULT: Self = Self {
        max_events: MAX_EVENTS,
        log_response: rpc::MAX_RESPONSE,
        receipt_response: crate::block_logs::RECEIPT_RESPONSE,
    };
}

/// One chain's follower state across polls.
pub struct Follower {
    pub limits: Limits,
    /// A block refused as logs while its receipts were unavailable, and for
    /// how many polls in a row. A refusal can be a provider's rate limit as
    /// easily as a dense block, so only persistence stops the chain. Only
    /// progress clears it: an answer that then fails its checks does not
    /// start the count over.
    refused: Option<(u64, u32)>,
    /// A range start whose assembled batch was rejected as invalid history,
    /// by event decoding or by validation, and for how many polls in a row.
    /// Transport failures neither count nor clear it; progress clears it.
    rejected: Option<(u64, u32)>,
}

impl Follower {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            refused: None,
            rejected: None,
        }
    }

    /// One more poll on which the range starting at `from` was answered by
    /// the provider but could not be taken in as valid history.
    fn rejected(&mut self, from: u64, reason: &str) -> anyhow::Error {
        let count = match self.rejected {
            Some((start, count)) if start == from => count + 1,
            _ => 1,
        };
        self.rejected = Some((from, count));
        if count >= PERSISTENT_REJECTIONS {
            return InvalidRange {
                from,
                reason: format!("{reason}; rejected {count} polls in a row"),
            }
            .into();
        }
        anyhow!(
            "range from block {from} rejected, poll {count} of {PERSISTENT_REJECTIONS}: {reason}"
        )
    }
}

/// A range the provider keeps answering the same way that the index cannot
/// take in as valid history: an event that does not decode, a post out of
/// sequence, a registry event disagreeing with history. Waiting cannot help,
/// so the chain stops with this as its reason instead of sitting at a stale
/// checkpoint labelled ready; the operator checks endpoint and configuration,
/// then rebuilds.
#[derive(Debug)]
pub struct InvalidRange {
    pub from: u64,
    pub reason: String,
}

impl std::fmt::Display for InvalidRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "range from block {} is not valid history: {}",
            self.from, self.reason
        )
    }
}
impl std::error::Error for InvalidRange {}

fn too_dense(block: u64, events: usize, max_events: usize) -> anyhow::Error {
    Unreadable {
        block,
        reason: format!(
            "it holds {events} board and registry events and the index takes at most {max_events} per block"
        ),
    }
    .into()
}

/// Read the committed checkpoint's block again. A provider that now answers
/// another hash has changed accepted history, and this returns
/// `HistoryChanged`. A transport failure is an ordinary error and must never
/// be taken for a reorg.
async fn verify_checkpoint(client: &rpc::Upstream, chain: &Chain, index: &Index) -> Result<()> {
    let checkpoint = index.view.read().expect("index lock").model.at.checkpoint;
    if let Some(c) = checkpoint {
        let answered = header(client, chain, c.block, rpc::Class::Ingestion).await?;
        if answered.checkpoint != c {
            return Err(HistoryChanged.into());
        }
    }
    Ok(())
}

/// Read one bounded range; return whether it reached the sampled accepted head.
pub async fn advance(
    client: &rpc::Upstream,
    chain: &Chain,
    index: &Index,
    follower: &mut Follower,
) -> Result<bool> {
    let Limits {
        max_events,
        log_response,
        receipt_response,
    } = follower.limits;
    let (from, registries) = {
        let view = index.view.read().expect("index lock");
        ensure!(!view.state().is_stopped(), "index stopped");
        ensure!(view.state().is_established(), "index not established");
        (
            view.model.next_block()?,
            view.model.deployment.registries.clone(),
        )
    };
    let head = rpc::head(client, &chain.url, rpc::Class::Ingestion).await?;
    let Some(accepted) = head.checked_sub(chain.confirmations) else {
        // Nothing is accepted yet, but a checkpoint that exists is verified.
        verify_checkpoint(client, chain, index).await?;
        return Ok(true);
    };
    index.observe_head(accepted);
    if from > accepted {
        // An idle poll still reads the checkpoint header, so a replaced
        // block is caught without waiting for new blocks.
        verify_checkpoint(client, chain, index).await?;
        index.mark_ready();
        return Ok(true);
    }
    let mut to = accepted.min(from.saturating_add(BLOCK_SPAN - 1));
    let addresses: Vec<_> = std::iter::once(Hex(chain.contract))
        .chain(registries.iter().copied())
        .collect();
    let topics = topics();
    // A range that will not come back whole is halved down to one block. A
    // single block the provider still refuses is read from its receipts.
    // Transport failures may narrow a multi-block range, but a failed single
    // block retries next poll; transport failure never triggers receipts or a stop.
    //
    // The end header is read before each attempt at a range and compared
    // again after assembly. Reading it afterwards would miss a block replaced
    // while the logs were in flight when the old block held none of our
    // logs: the per-log hash checks have nothing to check, and both reads
    // would agree on the replacement.
    let (logs, through) = loop {
        let through = header(client, chain, to, rpc::Class::Ingestion).await?;
        match range_logs(client, chain, &addresses, &topics, from, to, log_response).await {
            Ok(logs) if logs.len() <= max_events => break (logs, through),
            Ok(logs) if to == from => return Err(too_dense(from, logs.len(), max_events)),
            Err(Fetch::Busy) => return Err(anyhow!("no upstream slot free")),
            Err(Fetch::Transient(error)) if to == from => return Err(error),
            Err(Fetch::Refused) if to == from => {
                let logs = receipt_logs(
                    client,
                    chain,
                    &addresses,
                    &topics,
                    from,
                    through.checkpoint.hash,
                    receipt_response,
                )
                .await
                .map_err(|error| error.reported(from, &mut follower.refused))?;
                if logs.len() > max_events {
                    return Err(too_dense(from, logs.len(), max_events));
                }
                break (logs, through);
            }
            Ok(_) | Err(_) => to = from + (to - from) / 2,
        }
    };
    let mut headers = BTreeMap::from([(to, through)]);
    let mut events = Vec::with_capacity(logs.len());
    // A log the provider keeps answering with a bad block number, a block
    // outside the range, a hash its header denies or an event that does not
    // decode is a rejection of this range; only the header reads themselves
    // are transport and stay retryable.
    for log in logs {
        let block = quantity(&log["blockNumber"])
            .map_err(|error| follower.rejected(from, &error.to_string()))?;
        if block < from || block > to {
            return Err(follower.rejected(from, "log outside requested range"));
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = headers.entry(block) {
            entry.insert(header(client, chain, block, rpc::Class::Ingestion).await?);
        }
        let h = &headers[&block];
        let hash = text(&log["blockHash"])
            .and_then(|hash| hash.parse::<Hash>())
            .map_err(|error| follower.rejected(from, &error.to_string()))?;
        if hash != h.checkpoint.hash {
            return Err(follower.rejected(from, "log disagrees with header"));
        }
        events.push(
            decode_event(&log, Hex(chain.contract), &registries, h.timestamp)
                .map_err(|error| follower.rejected(from, &error.to_string()))?,
        );
    }
    events.sort_by_key(Event::position);
    // Recheck the end against the header read before the logs: never publish
    // a range that changed while it was being assembled.
    let through = headers[&to].checkpoint;
    ensure!(
        header(client, chain, to, rpc::Class::Ingestion)
            .await?
            .checkpoint
            == through,
        "range changed during read"
    );
    // One checkpoint read per poll: here, as late as possible, when the
    // poll advances; above when it is idle.
    verify_checkpoint(client, chain, index).await?;
    // Validation rejects a batch without advancing; a rejection that repeats
    // at the same start is data the chain will keep answering, not weather.
    if let Err(error) = index
        .append(Batch {
            from,
            through,
            events,
        })
        .await
    {
        if index.state().is_stopped() {
            return Err(error);
        }
        return Err(follower.rejected(from, &error.to_string()));
    }
    follower.refused = None;
    follower.rejected = None;
    if to == accepted {
        index.mark_ready();
    }
    Ok(to == accepted)
}

/// The beta stops all indexed serving on contradictory history. Rebuilding all
/// journals also reconstructs cross-chain references; there is no online rewind.
async fn step(
    host: &impl Host,
    client: &rpc::Upstream,
    id: u64,
    chain: &Chain,
    follower: &mut Follower,
) -> Result<bool> {
    let Some(index) = host.indexes().and_then(|all| all.get(&id)) else {
        return Ok(true);
    };
    if index.state().is_stopped() {
        return Ok(true);
    }
    let result = async {
        // The journal opened without the chain. The first successful read
        // confirms the endpoint serves this chain before the index accepts
        // any chain update; a transport failure here is retried like any other.
        if matches!(index.state(), super::State::Starting) {
            host.verifier().check_chain(id, rpc::Class::Ingestion).await?;
            index.establish()?;
            let fresh = index
                .view
                .read()
                .expect("index lock")
                .model
                .at
                .checkpoint
                .is_none();
            if fresh && chain.index_start_block == 0 {
                tracing::warn!(
                    chain = id,
                    "FULL-HISTORY SCAN FROM GENESIS: this index is new and starts at block zero, which can be slow and cost many RPC calls. \
                     Set index_start_block to the earliest deployment block needed by the Board and every configured registry; \
                     the first post's block can miss gate or membership history."
                );
            }
        }
        match advance(client, chain, index, follower).await {
            Ok(caught_up) => Ok(caught_up),
            Err(error) if error.is::<HistoryChanged>() => Err(error),
            Err(error) => {
                // A poll that failed before its own checkpoint read still
                // verifies the checkpoint, so a replaced block cannot hide
                // behind a range that keeps failing. A failed read here keeps
                // the original error; the next poll checks again.
                match verify_checkpoint(client, chain, index).await {
                    Err(changed) if changed.is::<HistoryChanged>() => Err(changed),
                    _ => Err(error),
                }
            }
        }
    }
    .await;
    if let Err(error) = &result {
        if error.is::<HistoryChanged>() {
            tracing::error!(
                chain = id,
                "accepted history changed; rebuild all indexes before serving"
            );
            for index in host.indexes().into_iter().flat_map(|all| all.values()) {
                index.stop("accepted history changed; rebuild required");
            }
        } else if error.is::<crate::verifier::ChainMismatch>() {
            tracing::error!(
                chain = id,
                ?error,
                "chain endpoint misconfigured; serving stopped"
            );
            index.stop("chain id mismatch; correct configuration and restart");
        } else if error.is::<Unreadable>() {
            tracing::error!(chain = id, %error, "block cannot be indexed; serving stopped");
            index.stop(&error.to_string());
        } else if error.is::<InvalidRange>() {
            tracing::error!(chain = id, %error, "range cannot be indexed; serving stopped");
            index.stop(&error.to_string());
        } else {
            tracing::warn!(chain = id, ?error, "index range not advanced");
        }
    }
    result
}

/// Slow or unavailable RPC on one chain must not hold up the other chains.
pub fn start<H: Host>(host: Arc<H>) {
    let Some(client) = host.verifier().upstream().cloned() else {
        return;
    };
    for (id, _) in host.verifier().chains() {
        let host = host.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let chain = host.verifier().config(id).expect("configured chain");
            let Some(index) = host.indexes().and_then(|all| all.get(&id)) else {
                return;
            };
            let mut follower = Follower::new(Limits::DEFAULT);
            let mut delay = 1;
            loop {
                if index.state().is_stopped() {
                    break;
                }
                match step(&*host, &client, id, chain, &mut follower).await {
                    Ok(caught_up) => {
                        delay = 1;
                        if caught_up {
                            tokio::time::sleep(chain.poll_interval).await;
                        } else {
                            // Continue catch-up even if this index was ready before falling behind.
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                        delay = (delay * 2).min(30);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
pub async fn once(host: &impl Host) {
    once_with(host, Limits::DEFAULT).await;
}

/// One pass within a test's own limits, remembering nothing between passes.
#[cfg(test)]
pub async fn once_with(host: &impl Host, limits: Limits) {
    once_on(host, &mut Follower::new(limits)).await;
}

/// One pass of every chain with the same follower, so a test can watch what
/// persists between polls.
#[cfg(test)]
pub async fn once_on(host: &impl Host, follower: &mut Follower) {
    let Some(client) = host.verifier().upstream() else {
        return;
    };
    for (id, chain) in host.verifier().chains() {
        let _ = step(host, client, id, chain, follower).await;
    }
}
