//! Optional account-key holdings, independent of the core Board index.
//!
//! Accepted market events live in an append-only journal of block ranges,
//! replayed into RAM on startup. A range is validated against the current
//! model, made durable, and only then applied, so readers only ever see whole,
//! durable ranges. Empty ranges advance the checkpoint in RAM and reach the
//! journal once per `CHECKPOINT_BLOCKS`, like the Board index.
//!
//! `model.rs` holds the events and the rules every range is checked against,
//! `journal.rs` opening, replay and the writer, and `follow.rs` the follower
//! that reads the market's logs. This file is the handle readers use.
mod follow;
mod journal;
mod model;

use crate::chain::{Address, Position};
use anyhow::{Result, anyhow};
use follow::MIN_CONFIRMATIONS;
use model::{Ledger as _, Model, Range, TradeHistory};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub chain: u64,
    pub market: Address,
    pub start_block: u64,
}

/// Starting until the first successful poll establishes the journal against
/// the chain; ready once caught up. A stop is final for this process.
#[derive(Clone, Debug, PartialEq, Eq)]
enum KeyState {
    Starting,
    Syncing,
    Ready,
    Stopped(String),
}

/// The model and its state share one lock, so a reader checks readiness in
/// the same guard it reads balances with.
struct View {
    model: Model,
    state: KeyState,
    ready_at: std::time::Instant,
}

enum Command {
    Range {
        range: Range,
        answer: oneshot::Sender<Result<()>>,
    },
    /// Journal the empty progress held only in RAM, as on a clean stop.
    Flush { answer: oneshot::Sender<Result<()>> },
}

pub struct Keyring {
    config: Config,
    view: Arc<RwLock<View>>,
    commands: mpsc::Sender<Command>,
    /// Set on a clean stop; the follower starts no further polls.
    stopping: std::sync::atomic::AtomicBool,
    /// Response bound for one block's receipts; tests shrink it.
    receipt_response: std::sync::atomic::AtomicUsize,
}

/// Mark the keyring stopped. Nothing it holds is served again in this process.
fn stop(view: &RwLock<View>, reason: &str) {
    let mut view = view.write().expect("keyring lock");
    if !matches!(view.state, KeyState::Stopped(_)) {
        view.state = KeyState::Stopped(reason.into());
    }
}

impl Keyring {
    /// Retain configured identity and an explicit failure when replay cannot open.
    pub fn unavailable(config: Config) -> Arc<Self> {
        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        Arc::new(Self {
            config,
            view: Arc::new(RwLock::new(View {
                model: Model::default(),
                state: KeyState::Stopped(
                    "journal could not open; repair and restart required".into(),
                ),
                ready_at: std::time::Instant::now(),
            })),
            commands,
            stopping: std::sync::atomic::AtomicBool::new(true),
            receipt_response: std::sync::atomic::AtomicUsize::new(
                crate::block_logs::RECEIPT_RESPONSE,
            ),
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Read state and progress together, including when no holdings may be served.
    pub fn status(&self) -> Value {
        let view = self.view.read().expect("keyring lock");
        let (state, reason) = match &view.state {
            KeyState::Starting => ("starting", None),
            KeyState::Syncing => ("syncing", None),
            KeyState::Ready if view.ready_at.elapsed() < Duration::from_secs(60) => ("ready", None),
            KeyState::Ready => ("syncing", None),
            KeyState::Stopped(reason) => ("stopped", Some(reason)),
        };
        json!({
            "chain": self.config.chain,
            "market": self.config.market,
            "start_block": self.config.start_block,
            "state": state,
            "reason": reason,
            "checkpoint": view.model.checkpoint,
            "minimum_confirmations": MIN_CONFIRMATIONS,
        })
    }

    /// A keyring with this configuration, an empty model and no journal or
    /// writer: every write fails. For tests that need only its configuration
    /// or a hand-built model.
    #[cfg(test)]
    fn detached_with(config: Config, model: Model, state: KeyState) -> Arc<Self> {
        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        Arc::new(Self {
            config,
            view: Arc::new(RwLock::new(View {
                model,
                state,
                ready_at: std::time::Instant::now(),
            })),
            commands,
            stopping: std::sync::atomic::AtomicBool::new(false),
            receipt_response: std::sync::atomic::AtomicUsize::new(
                crate::block_logs::RECEIPT_RESPONSE,
            ),
        })
    }

    #[cfg(test)]
    pub fn detached(config: Config) -> Arc<Self> {
        Self::detached_with(
            config,
            Model::default(),
            KeyState::Stopped("detached".into()),
        )
    }

    async fn command(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<()>>) -> Command,
    ) -> Result<()> {
        let (answer, result) = oneshot::channel();
        self.commands
            .send(command(answer))
            .await
            .map_err(|_| anyhow!(Fatal("keyring writer stopped")))?;
        result
            .await
            .map_err(|_| anyhow!(Fatal("keyring writer stopped")))?
    }

    /// Journal empty progress RAM holds beyond the last journaled block.
    pub async fn flush(&self) -> Result<()> {
        self.command(|answer| Command::Flush { answer }).await
    }

    /// A clean stop: start no further polls, then flush. Commands reach the
    /// writer in order, so the flush is written after any range a poll
    /// already submitted, and a range still being fetched is simply read
    /// again after the restart.
    pub async fn shutdown(&self) -> Result<()> {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.flush().await
    }

    fn set_ready(&self, ready: bool) {
        let mut view = self.view.write().expect("keyring lock");
        if ready {
            view.ready_at = std::time::Instant::now();
        }
        match (&view.state, ready) {
            (KeyState::Syncing, true) => view.state = KeyState::Ready,
            (KeyState::Ready, false) => view.state = KeyState::Syncing,
            _ => {}
        }
    }

    // The reads below answer at one ready checkpoint, or None while the index
    // is syncing, starting or stopped. Shaping stays here; HTTP stays out.

    /// Whether each holder holds any of its creator's keys: bounded point lookups.
    pub fn held(&self, pairs: &[(Address, Address)]) -> Option<Value> {
        self.read_ready(|model| {
            let held: Vec<bool> = pairs
                .iter()
                .map(|&(creator, holder)| model.balance(creator, holder) > 0)
                .collect();
            json!({"market":self.config.market,"checkpoint":model.checkpoint,"held":held})
        })
    }

    /// One page of an account's balances, seeking by address; no full-list scan.
    pub fn balances(
        &self,
        account: Address,
        view: BalanceView,
        after: Option<Address>,
        limit: usize,
    ) -> Option<Value> {
        self.read_ready(|model| model.balance_page(self.config.market, account, view, after, limit))
    }

    /// One page of a creator's retained trade notices. `blocklist` is handed
    /// the page builder and supplies the creator's blocklist, read from the
    /// Board index while this index's read lock is held, as before.
    pub fn trades<E>(
        &self,
        creator: Address,
        before: Option<Position>,
        limit: usize,
        blocklist: impl FnOnce(&dyn Fn(Option<&BTreeSet<Address>>) -> Value) -> Result<Value, E>,
    ) -> Option<Result<Value, E>> {
        self.read_ready(|model| {
            let page = |blocked: Option<&BTreeSet<Address>>| {
                let empty = TradeHistory::default();
                let mut page = model
                    .activity
                    .get(&creator)
                    .unwrap_or(&empty)
                    .page(before, limit, blocked);
                page["market"] = json!(self.config.market);
                page["active"] = json!(model.supplies.contains_key(&creator));
                page["checkpoint"] = json!(model.checkpoint);
                page
            };
            blocklist(&page)
        })
    }

    /// Public purchases and transfers, newest first. A cursor seeks directly;
    /// filtering scans at most 2,000 events, including hidden accounts.
    pub fn activity(
        &self,
        before: Option<Position>,
        limit: usize,
        blocked: impl Fn(&Address) -> bool,
    ) -> Option<Value> {
        self.read_ready(|model| {
            let events = &model.public_activity;
            let end = before.map_or(events.len(), |p| events.partition_point(|e| e.position < p));
            let mut at = end;
            let mut items = Vec::new();
            while at > 0 && end - at < 2_000 && items.len() < limit {
                at -= 1;
                let e = &events[at];
                let hidden = match &e.event {
                    model::Event::Trade(t) => blocked(&t.creator) || blocked(&t.holder),
                    model::Event::Transfer(t) => blocked(&t.creator) || blocked(&t.from) || blocked(&t.to),
                    model::Event::Activated { .. } => true,
                };
                if !hidden { items.push(e); }
            }
            let next = (at > 0 && at < end).then(|| events[at].position);
            json!({"market":self.config.market,"checkpoint":model.checkpoint,"items":items,"next":next})
        })
    }

    /// An account's holders and holdings, each capped at 256 and marked when cut.
    pub fn lookup(&self, holder: Address) -> Option<Value> {
        self.read_ready(|model| {
            let (mut holders, mut holding) = model.authors(holder);
            let holders_truncated = holders.len() > 256;
            let holding_truncated = holding.len() > 256;
            holders.truncate(256);
            holding.truncate(256);
            json!({"market":self.config.market,"checkpoint":model.checkpoint,
            "active":model.supplies.contains_key(&holder),"holders":holders,"holding":holding,
            "truncated":{"holders":holders_truncated,"holding":holding_truncated}})
        })
    }

    /// Answer only from a caught-up keyring, inside the same guard as its state.
    fn read_ready<T>(&self, read: impl FnOnce(&Model) -> T) -> Option<T> {
        let view = self.view.read().expect("keyring lock");
        (view.state == KeyState::Ready
            && view.ready_at.elapsed() < Duration::from_secs(60)
            && view.model.checkpoint.is_some())
        .then(|| read(&view.model))
    }
}

#[derive(Debug)]
/// History this keyring cannot take as valid, or storage failing: the
/// journal must be rebuilt.
struct Fatal(&'static str);
impl std::fmt::Display for Fatal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Fatal {}

/// Which side of an account's key balances a page lists.
#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BalanceView {
    Holders,
    #[default]
    Holding,
}

#[cfg(test)]
mod tests;
