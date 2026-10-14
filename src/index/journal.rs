//! The writer owns its journal for its entire lifetime. Dropping an HTTP
//! request can drop an acknowledgement, never the append or durability barrier.
//!
//! Opening needs no chain access: a fresh journal writes its identity then.
//! The index is starting, not stopped, until the follower's first successful
//! read confirms the endpoint serves this chain: a chain that is slow to
//! answer at boot is waited for, not given up. A journal built on other
//! history is caught by the checkpoint check on every poll (follow.rs).

use super::{
    model::{At, Batch, Model, Update},
    types::*,
};
use crate::chain::Hash;
use crate::journal::{CHECKPOINT_BLOCKS, REPLAY_BUFFER, append_and_sync, journal_config};
use anyhow::{Context as _, Result, anyhow, ensure};
use commonware_runtime::{ReadOptions, Spawner};
use commonware_storage::{
    Context,
    journal::contiguous::{Contiguous as _, variable::Journal},
};
use commonware_utils::NZUsize;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, RwLock},
};
use tokio::sync::{mpsc, oneshot};

const FORMAT: u32 = 1;
/// Decode bound for one journal record. A batch of `MAX_EVENTS` posted
/// messages serializes to under half of this.
const MAX_BATCH_BYTES: usize = 128 << 20;
/// An invariant failure can leave the model partly changed. The caller must
/// discard it on replay or stop serving it before releasing the write lock.
/// Keep that lock outside this unwind boundary so it is never poisoned.
fn apply(model: &mut Model, update: Update) -> Result<()> {
    catch_unwind(AssertUnwindSafe(|| model.apply_update(update))).map_err(|panic| {
        let detail = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-text panic");
        anyhow!("index apply failed: {detail}")
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    format: u32,
    deployment: Deployment,
    generation: Hash,
}

/// Only established histories may accept updates; only caught-up histories
/// may answer indexed queries. A stop is final for this process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Starting,
    Backfilling,
    Ready,
    Stopped(String),
}
impl State {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped(_))
    }
    pub fn is_established(&self) -> bool {
        matches!(self, Self::Backfilling | Self::Ready)
    }
    fn label(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Backfilling => "backfilling",
            Self::Ready => "ready",
            Self::Stopped(_) => "stopped",
        }
    }
}

pub struct View {
    pub model: Model,
    accepted_head: Option<u64>,
    state: State,
}
impl View {
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Stop serving, keeping the first reason given. A stop is final for
    /// this process.
    fn stop(&mut self, reason: &str) {
        if !self.state.is_stopped() {
            self.state = State::Stopped(reason.into());
        }
    }
}

#[derive(Serialize)]
pub struct Status {
    at: At,
    state: &'static str,
    messages: usize,
    pending_bodies: usize,
    pending_quotes: usize,
    accepted_head: Option<String>,
}

enum Command {
    Write {
        update: Update,
        answer: oneshot::Sender<Result<At>>,
    },
    /// Journal the empty progress held only in RAM, as on a clean stop.
    Flush { answer: oneshot::Sender<Result<()>> },
}

#[derive(Clone)]
pub struct Index {
    pub(crate) view: Arc<RwLock<View>>,
    commands: mpsc::Sender<Command>,
    /// Bodies just stored, for the sweep to take first; never journaled.
    pub(crate) arrivals: Arc<std::sync::Mutex<super::bodies::Arrivals>>,
}

/// After a failed journal write: the index stops, and the caller hears that
/// storage failed, not the detail.
fn write_failed(view: &RwLock<View>, error: anyhow::Error) -> anyhow::Error {
    tracing::error!(?error, "index journal failed; serving stopped");
    view.write()
        .expect("index lock")
        .stop("journal write failed; restart required");
    anyhow!("index storage failed")
}

/// The record a flush journals: the empty progress RAM holds beyond the
/// last journaled block, if there is any.
fn pending_progress(view: &RwLock<View>, durable_next: u64) -> Result<Option<Batch>> {
    let view = view.read().expect("index lock");
    ensure!(!view.state.is_stopped(), "index stopped");
    Ok(view
        .model
        .at
        .checkpoint
        .filter(|through| through.block >= durable_next)
        .map(|through| Batch {
            from: durable_next,
            through,
            events: Vec::new(),
        }))
}

/// What a write puts in the journal, once the update is valid: nothing for
/// an empty range short of the interval, which RAM serves alone, otherwise
/// the encoded update. A journaled chain batch starts just after the last
/// journaled block, covering any empty ranges RAM applied since: they hold
/// no events, so the wider batch means the same thing. A refusal changes
/// nothing.
fn record_for(
    view: &RwLock<View>,
    update: &mut Update,
    durable_next: u64,
) -> Result<Option<Vec<u8>>> {
    {
        let view = view.read().expect("index lock");
        ensure!(!view.state.is_stopped(), "index stopped");
        ensure!(view.state.is_established(), "index not established");
        view.model.validate_update(update)?;
    }
    if let Update::Chain(batch) = update {
        if batch.events.is_empty() && batch.through.block - durable_next + 1 < CHECKPOINT_BLOCKS {
            return Ok(None);
        }
        // Applying ignores `from`; replay checks it against the last
        // journaled checkpoint.
        batch.from = durable_next;
    }
    let bytes = serde_json::to_vec(update)?;
    ensure!(bytes.len() <= MAX_BATCH_BYTES, "index batch is too large");
    Ok(Some(bytes))
}

/// Apply an update readers will see. A failure can leave the model half
/// changed, so the index stops before the write lock is released.
fn apply_or_stop(view: &RwLock<View>, update: Update) -> Result<At> {
    let mut view = view.write().expect("index lock");
    match apply(&mut view.model, update) {
        Ok(()) => Ok(view.model.at.clone()),
        Err(error) => {
            tracing::error!(
                chain = view.model.deployment.chain,
                ?error,
                "index apply failed; serving stopped"
            );
            view.stop("index apply failed; repair and restart required");
            Err(error)
        }
    }
}

impl Index {
    /// Status-only entry for a journal that could not open. No writer or disk
    /// state is created, and the closed queue refuses every attempted update.
    pub fn unavailable(deployment: Deployment, generation: Hash) -> Self {
        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        Self {
            view: Arc::new(RwLock::new(View {
                model: Model::new(deployment, generation),
                accepted_head: None,
                state: State::Stopped("index could not open; repair and restart required".into()),
            })),
            commands,
            arrivals: Arc::default(),
        }
    }

    /// Open the journal for `deployment` and replay it into RAM, or create it
    /// and write its identity. An existing journal must name this deployment.
    pub async fn open<E: Context + Spawner>(
        context: E,
        deployment: Deployment,
        generation: Hash,
    ) -> Result<Self> {
        let cfg = journal_config(
            &context,
            format!("index-{}-{}", deployment.chain, deployment.board),
            MAX_BATCH_BYTES,
        );
        let mut journal = Journal::<E, Vec<u8>>::init(context.child("journal"), cfg).await?;
        let mut model = if journal.size() == 0 {
            let identity = Identity {
                format: FORMAT,
                deployment,
                generation,
            };
            journal = append_and_sync(journal, serde_json::to_vec(&identity)?).await?;
            Model::new(identity.deployment, identity.generation)
        } else {
            let identity: Identity = serde_json::from_slice(&journal.read(0).await?)
                .context("index identity is in another format; rebuild required")?;
            ensure!(
                identity.format == FORMAT,
                "unsupported index format; rebuild required"
            );
            ensure!(
                identity.deployment == deployment,
                "index belongs to another deployment; rebuild required"
            );
            Model::new(identity.deployment, identity.generation)
        };
        // One sequential pass through the records after the identity, rather
        // than a positioned read for each.
        if journal.size() > 1 {
            let records = journal
                .replay(1, NZUsize!(REPLAY_BUFFER), ReadOptions::default())
                .await?;
            let mut records = std::pin::pin!(records);
            while let Some(record) = records.next().await {
                let (i, bytes) = record?;
                let update: Update = serde_json::from_slice(&bytes)?;
                model.validate_update(&update)?;
                apply(&mut model, update)
                    .with_context(|| format!("index replay failed at journal record {i}"))?;
            }
        }
        let view = Arc::new(RwLock::new(View {
            model,
            accepted_head: None,
            state: State::Starting,
        }));
        // The block after the last journaled checkpoint. RAM may be ahead of
        // it by empty ranges; the next journaled chain batch starts here.
        let mut durable_next = view.read().expect("index lock").model.next_block()?;
        let (commands, mut receiver) = mpsc::channel::<Command>(8);
        let state = view.clone();
        context.child("writer").spawn(move |_| async move {
            let mut journal = journal;
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Flush { answer } => {
                        let batch = match pending_progress(&state, durable_next) {
                            Ok(Some(batch)) => batch,
                            Ok(None) => {
                                let _ = answer.send(Ok(()));
                                continue;
                            }
                            Err(refused) => {
                                let _ = answer.send(Err(refused));
                                continue;
                            }
                        };
                        let next = batch.through.block + 1;
                        let record = Update::Chain(batch);
                        let stored =
                            async { append_and_sync(journal, serde_json::to_vec(&record)?).await }
                                .await;
                        journal = match stored {
                            Ok(journal) => journal,
                            Err(error) => {
                                let _ = answer.send(Err(write_failed(&state, error)));
                                break;
                            }
                        };
                        durable_next = next;
                        let _ = answer.send(Ok(()));
                    }
                    Command::Write { mut update, answer } => {
                        let record = match record_for(&state, &mut update, durable_next) {
                            Ok(record) => record,
                            Err(refused) => {
                                let _ = answer.send(Err(refused));
                                continue;
                            }
                        };
                        if let Some(bytes) = record {
                            journal = match append_and_sync(journal, bytes).await {
                                Ok(journal) => journal,
                                Err(error) => {
                                    let _ = answer.send(Err(write_failed(&state, error)));
                                    break;
                                }
                            };
                            if let Update::Chain(batch) = &update {
                                durable_next = batch.through.block + 1;
                            }
                        }
                        // Only now that the record is safe do readers see it.
                        let applied = apply_or_stop(&state, update);
                        let failed = applied.is_err();
                        let _ = answer.send(applied);
                        if failed {
                            break;
                        }
                    }
                }
            }
        });
        Ok(Self {
            view,
            commands,
            arrivals: Arc::default(),
        })
    }

    /// The follower has confirmed the endpoint serves this chain: the index
    /// takes updates from now on. A stopped index stays stopped.
    pub fn establish(&self) -> Result<()> {
        let mut view = self.view.write().expect("index lock");
        ensure!(!view.state.is_stopped(), "index stopped");
        if matches!(view.state, State::Starting) {
            view.state = State::Backfilling;
        }
        Ok(())
    }

    /// Journal empty progress RAM holds beyond the last journaled checkpoint.
    /// A clean stop calls this so a restart resumes where it served from.
    pub async fn flush(&self) -> Result<()> {
        self.ask(|answer| Command::Flush { answer }).await
    }

    pub async fn append(&self, batch: Batch) -> Result<At> {
        self.update(Update::Chain(batch)).await
    }

    pub async fn update(&self, update: Update) -> Result<At> {
        self.ask(|answer| Command::Write { update, answer }).await
    }

    /// Hand the writer a command and wait for its answer.
    async fn ask<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> Command,
    ) -> Result<T> {
        let (answer, result) = oneshot::channel();
        self.commands
            .send(command(answer))
            .await
            .map_err(|_| anyhow!("index writer stopped"))?;
        result.await.map_err(|_| anyhow!("index writer stopped"))?
    }

    /// Indexed reads fail closed, including during startup and backfill.
    pub fn read_ready<T>(&self, read: impl FnOnce(&Model) -> T) -> Option<T> {
        let view = self.view.read().expect("index lock");
        view.state.is_ready().then(|| read(&view.model))
    }

    pub fn state(&self) -> State {
        self.view.read().expect("index lock").state.clone()
    }

    /// `served` lists the chains this host indexes, so quotes of other
    /// chains are not reported as a backlog.
    pub fn status<'a>(&self, served: impl IntoIterator<Item = &'a u64>) -> Status {
        let view = self.view.read().expect("index lock");
        Status {
            at: view.model.at.clone(),
            accepted_head: view.accepted_head.map(|n| n.to_string()),
            state: view.state.label(),
            messages: view.model.messages.len(),
            pending_bodies: view.model.references.pending_bodies.len(),
            pending_quotes: view.model.references.pending_quotes_on(served),
        }
    }

    pub fn observe_head(&self, block: u64) {
        self.view.write().expect("index lock").accepted_head = Some(block);
    }

    pub fn mark_ready(&self) {
        let mut view = self.view.write().expect("index lock");
        if matches!(view.state, State::Backfilling) {
            view.state = State::Ready;
            // One line per chain so an operator can read time-to-ready from
            // the log instead of polling the status endpoint.
            tracing::info!(
                chain = view.model.deployment.chain,
                messages = view.model.messages.len(),
                block = view.model.at.checkpoint.map_or(0, |c| c.block),
                sequence = view.model.at.sequence,
                "index ready"
            );
        }
    }

    pub fn stop(&self, reason: &str) {
        self.view.write().expect("index lock").stop(reason);
    }
}
