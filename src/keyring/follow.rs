//! The follower: reads the market's logs in bounded ranges at a safe depth,
//! decodes them and hands each range to the writer. A range the provider
//! refuses is halved down to one block, and a block it still refuses is read
//! from its receipts, as the Board's index does; see [`crate::block_logs`].
use super::{
    Command, Config, Fatal, KeyState, Keyring,
    model::{Event, Logged, Range, Trade, Transfer},
    stop,
};
use crate::{
    abi::{quantity, text},
    block_logs::{Fetch, Unreadable, range_logs, receipt_logs},
    chain::{Address, Checkpoint, Hash, Position},
    rpc,
    verifier::{Chain, ChainMismatch, Verifier},
};
use anyhow::{Result, ensure};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};

/// Keep key ownership behind shallow reorganisations without slowing the Board.
/// This is a risk reduction, not Ethereum finality.
pub(super) const MIN_CONFIRMATIONS: u64 = 12;

fn topics() -> [Hash; 3] {
    [topic(), activation_topic(), transfer_topic()].map(|t| t.parse().expect("a topic hash"))
}

pub(super) fn topic() -> String {
    crate::keccak::hash(&[b"Trade(address,address,bool,uint256,uint256,uint256,uint256,uint256)"])
        .to_hex()
}

pub(super) fn activation_topic() -> String {
    crate::keccak::hash(&[b"Activated(address)"]).to_hex()
}

pub(super) fn transfer_topic() -> String {
    crate::keccak::hash(&[
        b"KeysTransferred(address,address,address,uint256,uint256,uint256,uint256)",
    ])
    .to_hex()
}

pub(super) fn decode_event(log: &Value, config: &Config) -> Result<Event> {
    if log["topics"][0].as_str() == Some(activation_topic().as_str()) {
        ensure!(
            text(&log["address"])?.parse::<Address>()? == config.market,
            "wrong activation market"
        );
        ensure!(
            log["removed"].as_bool() == Some(false)
                && log["topics"].as_array().is_some_and(|t| t.len() == 2)
                && log["data"] == "0x",
            "invalid activation"
        );
        Ok(Event::Activated {
            creator: word_address(text(&log["topics"][1])?)?,
        })
    } else if log["topics"][0].as_str() == Some(transfer_topic().as_str()) {
        Ok(Event::Transfer(decode_transfer(log, config)?))
    } else {
        Ok(Event::Trade(decode(log, config)?))
    }
}

fn word_address(s: &str) -> Result<Address> {
    let word: Hash = s.parse()?;
    ensure!(word.0[..12] == [0; 12], "noncanonical address");
    Ok(crate::chain::Hex(word.0[12..].try_into()?))
}

pub(super) fn decode(log: &Value, config: &Config) -> Result<Trade> {
    ensure!(
        text(&log["address"])?.parse::<Address>()? == config.market,
        "wrong market"
    );
    ensure!(
        log.get("removed").and_then(Value::as_bool) == Some(false),
        "removed or incomplete log"
    );
    let topics = log["topics"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing topics"))?;
    ensure!(
        topics.len() == 3 && text(&topics[0])? == topic(),
        "wrong trade topics"
    );
    let data = crate::abi::decode_hex(text(&log["data"])?)
        .ok_or_else(|| anyhow::anyhow!("invalid data"))?;
    ensure!(data.len() == 192, "wrong trade size");
    let number = |i: usize| -> Result<u64> {
        let word = &data[i * 32..(i + 1) * 32];
        ensure!(word[..24] == [0; 24], "trade integer overflow");
        Ok(u64::from_be_bytes(word[24..].try_into()?))
    };
    let buy = number(0)?;
    let amount = number(1)?;
    let supply = number(5)?;
    ensure!(
        buy <= 1 && (1..=u32::MAX as u64).contains(&amount) && supply <= u32::MAX as u64,
        "invalid trade"
    );
    Ok(Trade {
        creator: word_address(text(&topics[1])?)?,
        holder: word_address(text(&topics[2])?)?,
        buy: buy == 1,
        amount,
        supply,
    })
}

pub(super) fn decode_transfer(log: &Value, config: &Config) -> Result<Transfer> {
    ensure!(
        text(&log["address"])?.parse::<Address>()? == config.market,
        "wrong transfer market"
    );
    ensure!(
        log["removed"].as_bool() == Some(false),
        "removed or incomplete transfer"
    );
    let topics = log["topics"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing topics"))?;
    ensure!(
        topics.len() == 4 && text(&topics[0])? == transfer_topic(),
        "wrong transfer topics"
    );
    let data = crate::abi::decode_hex(text(&log["data"])?)
        .ok_or_else(|| anyhow::anyhow!("invalid data"))?;
    ensure!(
        data.len() == 128 && data[..28] == [0; 28],
        "wrong transfer data"
    );
    let event = Transfer {
        creator: word_address(text(&topics[1])?)?,
        from: word_address(text(&topics[2])?)?,
        to: word_address(text(&topics[3])?)?,
        amount: u32::from_be_bytes(data[28..32].try_into()?) as u64,
    };
    ensure!(
        event.amount > 0
            && event.from != Address::ZERO
            && event.to != Address::ZERO
            && event.from != event.to
            && event.to != config.market,
        "invalid transfer"
    );
    Ok(event)
}

impl Keyring {
    /// Follow the market until a clean stop or a fatal error. The caller spawns
    /// it and owns what the verifier lives in.
    pub async fn run(&self, verifier: &Verifier) {
        // What persists between polls: the range size, and a block refused
        // as logs with no receipts to fall back on, with its count.
        let mut span = 2_000;
        let mut refused = None;
        loop {
            if self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            match self.poll(verifier, span, &mut refused).await {
                Ok(caught_up) => {
                    self.set_ready(caught_up);
                    // Regain throughput after transient errors during backfill.
                    span = (span * 2).min(2_000);
                    if caught_up {
                        span = 2_000;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
                Err(error) => {
                    // A transport hiccup does not invalidate a verified snapshot.
                    // read_ready/status expire it after sixty seconds regardless
                    // of whether a later RPC request is still waiting.
                    let advice = if error.is::<Fatal>() {
                        Some("move its journal aside and restart to rebuild")
                    } else if error.is::<Unreadable>() {
                        Some(
                            "its journal is intact: change the endpoint or raise the limit, then restart",
                        )
                    } else {
                        None
                    };
                    if let Some(advice) = advice {
                        tracing::error!("keyring stopped; {advice}: {error:#}");
                        stop(&self.view, &error.to_string());
                        break;
                    }
                    // Smaller log ranges accommodate provider response limits.
                    if !matches!(
                        error.downcast_ref::<rpc::RpcError>(),
                        Some(rpc::RpcError::RateLimited(_))
                    ) {
                        span = (span / 2).max(1);
                    }
                    tracing::warn!("keyring catch-up retry: {error:#}");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }
    }

    /// One bounded range; return whether it reached the accepted head.
    /// `refused` is the block refused as logs with no receipts to fall back
    /// on, and for how many polls in a row: the caller keeps it between
    /// polls.
    pub(super) async fn poll(
        &self,
        verifier: &Verifier,
        span: u64,
        refused: &mut Option<(u64, u32)>,
    ) -> Result<bool> {
        let (client, chain) = verifier
            .chain(self.config.chain)
            .map_err(|_| Fatal("keyring chain not served"))?;
        let (starting, previous, from) = {
            let view = self.view.read().expect("keyring lock");
            ensure!(
                !matches!(view.state, KeyState::Stopped(_)),
                Fatal("keyring stopped")
            );
            (
                view.state == KeyState::Starting,
                view.model.checkpoint,
                view.model.next_block(&self.config)?,
            )
        };
        if starting {
            // A wrong chain stops the keyring; an unreachable one is retried.
            verifier
                .check_chain(self.config.chain, rpc::Class::Ingestion)
                .await
                .map_err(|error| {
                    if error.is::<ChainMismatch>() {
                        anyhow::Error::new(Fatal("chain id changed"))
                    } else {
                        error
                    }
                })?;
            let mut view = self.view.write().expect("keyring lock");
            ensure!(
                !matches!(view.state, KeyState::Stopped(_)),
                Fatal("keyring stopped")
            );
            view.state = KeyState::Syncing;
        }
        let accepted = rpc::head(client, &chain.url, rpc::Class::Ingestion)
            .await?
            .saturating_sub(chain.confirmations.max(MIN_CONFIRMATIONS));
        if from > accepted {
            // Even an idle poll must verify history before renewing readiness.
            if let Some(checkpoint) = previous {
                let header =
                    crate::chain::header(client, chain, checkpoint.block, rpc::Class::Ingestion)
                        .await?;
                ensure!(
                    header.checkpoint == checkpoint,
                    Fatal("accepted keyring history changed")
                );
            }
            return Ok(previous.is_some());
        }
        let through = accepted.min(from.saturating_add(span - 1));
        let (logs, boundary) = self
            .read_range(client, chain, from, through, refused)
            .await?;
        let through = boundary.block;
        let mut ordered = BTreeMap::new();
        let mut block_hashes = BTreeMap::new();
        for log in &logs {
            let block = quantity(&log["blockNumber"])?;
            let ordinal = u32::try_from(quantity(&log["logIndex"])?)?;
            let transaction: Hash = text(&log["transactionHash"])?.parse()?;
            ensure!(
                (from..=through).contains(&block),
                "trade outside requested range"
            );
            let hash: Hash = text(&log["blockHash"])?.parse()?;
            if let Some(old) = block_hashes.insert(block, hash) {
                ensure!(old == hash, "mixed block hashes");
            }
            ensure!(
                ordered
                    .insert(
                        (block, ordinal),
                        (decode_event(log, &self.config)?, transaction)
                    )
                    .is_none(),
                "duplicate trade position"
            );
        }
        let mut timestamps = BTreeMap::new();
        for (block, hash) in block_hashes {
            let header = crate::chain::header(client, chain, block, rpc::Class::Ingestion).await?;
            ensure!(header.checkpoint.hash == hash, "trade on replaced block");
            timestamps.insert(block, header.timestamp);
        }
        ensure!(
            crate::chain::header(client, chain, through, rpc::Class::Ingestion)
                .await?
                .checkpoint
                == boundary,
            "range changed during fetch"
        );
        if let Some(checkpoint) = previous {
            ensure!(
                crate::chain::header(client, chain, checkpoint.block, rpc::Class::Ingestion)
                    .await?
                    .checkpoint
                    == checkpoint,
                Fatal("accepted keyring history changed")
            );
        }
        let range = Range {
            from,
            through: boundary,
            events: ordered
                .into_iter()
                .map(|((block, log), (event, transaction))| Logged {
                    position: Position { block, log },
                    transaction,
                    timestamp: timestamps[&block],
                    event,
                })
                .collect(),
        };
        // Stable, validated events must agree with the accepted market state.
        // Contradictions need operator repair, not a smaller RPC range.
        self.command(|answer| Command::Range { range, answer })
            .await?;
        *refused = None;
        Ok(through == accepted)
    }

    /// The market's logs in `from..=through`, and the end block's header,
    /// read before the logs. A range the provider refuses is halved down to
    /// one block, and a block it still refuses is read from its receipts. A
    /// transport failure on a single block retries next poll: it never
    /// reaches receipts or a stop. The end header is read before each
    /// attempt and compared again after assembly, so a block replaced while
    /// the logs were in flight is caught even when neither answer held any
    /// of our logs.
    async fn read_range(
        &self,
        client: &rpc::Upstream,
        chain: &Chain,
        from: u64,
        mut through: u64,
        refused: &mut Option<(u64, u32)>,
    ) -> Result<(Vec<Value>, Checkpoint)> {
        let addresses = [self.config.market];
        let topics = topics();
        loop {
            let boundary = crate::chain::header(client, chain, through, rpc::Class::Ingestion)
                .await?
                .checkpoint;
            match range_logs(
                client,
                chain,
                &addresses,
                &topics,
                from,
                through,
                rpc::MAX_RESPONSE,
            )
            .await
            {
                Ok(logs) => return Ok((logs, boundary)),
                Err(Fetch::Busy) => anyhow::bail!("no upstream slot free"),
                Err(Fetch::Throttled(error)) => return Err(error.into()),
                Err(Fetch::Transient(error)) if through == from => return Err(error),
                Err(Fetch::Refused) if through == from => {
                    let logs = self
                        .block_from_receipts(client, chain, from, boundary.hash, refused)
                        .await?;
                    return Ok((logs, boundary));
                }
                Err(_) => through = from + (through - from) / 2,
            }
        }
    }

    /// One block's market logs from its receipts, for a block the provider
    /// will not answer as logs. Receipts over the bound stop the keyring; no
    /// receipts at all count a refusal; anything else retries next poll.
    async fn block_from_receipts(
        &self,
        client: &rpc::Upstream,
        chain: &Chain,
        block: u64,
        hash: Hash,
        refused: &mut Option<(u64, u32)>,
    ) -> Result<Vec<Value>> {
        let bound = self
            .receipt_response
            .load(std::sync::atomic::Ordering::Relaxed);
        receipt_logs(
            client,
            chain,
            &[self.config.market],
            &topics(),
            block,
            hash,
            bound,
        )
        .await
        .map_err(|error| error.reported(block, refused))
    }
}
