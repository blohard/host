//! The keyring's journal: opening and replaying it, and the writer that
//! makes each range durable before readers see it.
use super::{
    Command, Config, Fatal, KeyState, Keyring, View,
    model::{Model, Range},
    stop,
};
use crate::chain::Address;
use crate::journal::{CHECKPOINT_BLOCKS, REPLAY_BUFFER, append_and_sync, journal_config};
use anyhow::{Context as _, Result, anyhow, ensure};
use commonware_runtime::ReadOptions;
use commonware_storage::{
    Context as StorageContext,
    journal::contiguous::{Contiguous as _, variable::Journal},
};
use commonware_utils::NZUsize;
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Formats are not versioned before release; a change resets the index.
const FORMAT: u32 = 1;
/// Decode bound for one journal record.
const MAX_RECORD_BYTES: usize = 128 << 20;

/// Journal record zero: which market, on which chain, this history belongs to.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    format: u32,
    config: Config,
}

/// Encode, bound, append and sync one record. Any failure takes the journal
/// with it, so the writer must stop without publishing the record.
async fn write_record<E: StorageContext>(
    journal: Journal<E, Vec<u8>>,
    record: &impl Serialize,
) -> Result<Journal<E, Vec<u8>>> {
    let bytes = serde_json::to_vec(record)?;
    ensure!(
        bytes.len() <= MAX_RECORD_BYTES,
        "keyring record is too large"
    );
    append_and_sync(journal, bytes).await
}

/// After a failed journal write: the keyring stops, and the caller hears a
/// fatal storage failure, not the detail.
fn write_failed(view: &RwLock<View>, error: anyhow::Error) -> anyhow::Error {
    tracing::error!(?error, "keyring journal failed; keyring stopped");
    stop(view, "journal write failed; restart required");
    anyhow!(Fatal("keyring journal write failed"))
}

/// The record a flush journals: the empty progress RAM holds beyond the
/// last journaled block, if there is any.
fn pending_progress(view: &RwLock<View>, durable_next: u64) -> Result<Option<Range>> {
    let view = view.read().expect("keyring lock");
    ensure!(
        !matches!(view.state, KeyState::Stopped(_)),
        "keyring stopped"
    );
    Ok(view
        .model
        .checkpoint
        .filter(|through| through.block >= durable_next)
        .map(|through| Range {
            from: durable_next,
            through,
            events: Vec::new(),
        }))
}

impl Keyring {
    /// Open, or create, the market's journal and replay it into RAM. An
    /// identity that names another market or chain, a record that does not
    /// decode, or corruption inside synced records is an error: the host then
    /// runs without a keyring rather than serving a different or shorter history.
    pub async fn open<E: StorageContext + commonware_runtime::Spawner>(
        context: E,
        config: Config,
    ) -> Result<Arc<Self>> {
        ensure!(
            config.chain > 0 && config.market != Address::ZERO && config.start_block > 0,
            "keyring requires a chain, market and its deployment block"
        );
        let cfg = journal_config(
            &context,
            format!("keyring-{}-{}", config.chain, config.market),
            MAX_RECORD_BYTES,
        );
        let mut journal = Journal::<E, Vec<u8>>::init(context.child("journal"), cfg).await?;
        let mut model = Model::default();
        if journal.size() > 0 {
            let identity: Identity = serde_json::from_slice(&journal.read(0).await?)?;
            ensure!(
                identity.format == FORMAT && identity.config == config,
                "keyring journal belongs to another market or format; rebuild required"
            );
        } else {
            journal = write_record(
                journal,
                &Identity {
                    format: FORMAT,
                    config: config.clone(),
                },
            )
            .await?;
        }
        if journal.size() > 1 {
            let records = journal
                .replay(1, NZUsize!(REPLAY_BUFFER), ReadOptions::default())
                .await?;
            let mut records = std::pin::pin!(records);
            while let Some(record) = records.next().await {
                let (i, bytes) = record?;
                let range: Range = serde_json::from_slice(&bytes)?;
                model
                    .check(&config, &range)
                    .with_context(|| format!("keyring replay failed at journal record {i}"))?;
                model.apply(range);
            }
        }
        // The block after the last journaled checkpoint. RAM may be ahead of
        // it by empty ranges; the next journaled range starts here.
        let mut durable_next = model.next_block(&config)?;
        let view = Arc::new(RwLock::new(View {
            model,
            state: KeyState::Starting,
            ready_at: std::time::Instant::now(),
        }));
        let (commands, mut receiver) = mpsc::channel::<Command>(8);
        let state = view.clone();
        let identity_config = config.clone();
        context.child("writer").spawn(move |_| async move {
            let mut journal = journal;
            while let Some(command) = receiver.recv().await {
                match command {
                    Command::Flush { answer } => {
                        let range = match pending_progress(&state, durable_next) {
                            Ok(Some(range)) => range,
                            Ok(None) => {
                                let _ = answer.send(Ok(()));
                                continue;
                            }
                            Err(refused) => {
                                let _ = answer.send(Err(refused));
                                continue;
                            }
                        };
                        let next = range.through.block + 1;
                        journal = match write_record(journal, &range).await {
                            Ok(journal) => journal,
                            Err(error) => {
                                let _ = answer.send(Err(write_failed(&state, error)));
                                break;
                            }
                        };
                        durable_next = next;
                        let _ = answer.send(Ok(()));
                    }
                    Command::Range { mut range, answer } => {
                        let checked = {
                            let view = state.read().expect("keyring lock");
                            match &view.state {
                                KeyState::Stopped(_) => Err(anyhow!("keyring stopped")),
                                KeyState::Starting => Err(anyhow!("keyring not established")),
                                _ => view.model.check(&identity_config, &range),
                            }
                        };
                        if let Err(error) = checked {
                            let _ = answer.send(Err(error));
                            continue;
                        }
                        // An empty range short of the interval is served from
                        // RAM only. A journaled range starts just after the
                        // last journaled block: the empty ranges it now covers
                        // hold no events, so the wider range means the same thing.
                        let journaled = !range.events.is_empty()
                            || range.through.block - durable_next + 1 >= CHECKPOINT_BLOCKS;
                        if journaled {
                            range.from = durable_next;
                            // Nothing from this range is served if it fails.
                            journal = match write_record(journal, &range).await {
                                Ok(journal) => journal,
                                Err(error) => {
                                    let _ = answer.send(Err(write_failed(&state, error)));
                                    break;
                                }
                            };
                            durable_next = range.through.block + 1;
                        }
                        state.write().expect("keyring lock").model.apply(range);
                        let _ = answer.send(Ok(()));
                    }
                }
            }
        });
        Ok(Arc::new(Self {
            config,
            view,
            commands,
            stopping: std::sync::atomic::AtomicBool::new(false),
            receipt_response: std::sync::atomic::AtomicUsize::new(
                crate::block_logs::RECEIPT_RESPONSE,
            ),
        }))
    }
}
