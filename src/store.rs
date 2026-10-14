//! Content-addressed store: keccak256(bytes) -> bytes, on the commonware
//! [`Freezer`].
//!
//! Every record is written once and never changed, which is what the Freezer
//! is built for: an on-disk hash table over a key index and a value journal,
//! no in-memory index and no rewriting of stored data, so memory stays flat
//! however many records a host holds. Nothing here is authenticated: the chain
//! commits to every content hash and whoever fetches a record checks it against
//! that hash, so the store has nothing to prove.
//!
//! The Freezer persists only what a checkpoint covers; opened without one it
//! deletes whatever is on disk and starts empty. So [`ContentStore::commit`]
//! syncs the Freezer and writes the checkpoint it returns to a small
//! [`Metadata`] record, which is what the next start hands back to the
//! Freezer, which discards anything written after it. The host commits every
//! few seconds and at shutdown rather than on every write: a crash loses the
//! records taken since the last commit, and no more. The chain still points
//! at them and anyone who still has the bytes can upload them again, but a
//! browser drops its copy once the host has answered, so the loss is real for
//! a record nobody else holds.
//!
//! The store is generic over the runtime context so the same code runs on the
//! production tokio runtime and, in tests, on the deterministic in-memory one.
//!
//! Both storage APIs pass ownership through a write, so the state is taken
//! out of its lock while a write is in flight. A write therefore runs in a
//! task of its own and the caller only waits for its answer: an upload whose
//! request is dropped or timed out stops waiting, and the task still finishes
//! the write and puts the state back. Only a failed write poisons the store.

use crate::keccak::{self, Digest};
use anyhow::{Context as _, Result, anyhow, ensure};
use commonware_runtime::{Spawner, buffer::paged::CacheRef};
use commonware_storage::{
    Context,
    freezer::{Checkpoint, Config as FreezerConfig, Freezer, Identifier},
    metadata::{Config as MetadataConfig, Metadata},
};
use commonware_utils::{NZU16, NZUsize, sequence::prefixed_u64::U64};
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};

/// Maximum size of a message body (1 MiB). Attachments have their own cap,
/// set in the host's config.
pub const MAX_CONTENT_SIZE: usize = 1 << 20;

/// Hard ceiling on any stored value, whatever the config says (16 MiB): the
/// value codec is configured with it.
pub const MAX_VALUE_SIZE: usize = 16 << 20;

/// Which content a store holds. The two are kept apart so that attachments
/// (anchored by a body, potentially large) never mix with bodies (anchored by
/// the chain directly), and each can be reset alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Bodies,
    Attachments,
}

impl Kind {
    /// The word for one item of this kind, as logs and the publish response
    /// use it.
    pub fn label(self) -> &'static str {
        match self {
            Self::Bodies => "body",
            Self::Attachments => "attachment",
        }
    }

    fn prefix(self) -> &'static str {
        // These prefixes are the on-disk layout; renaming orphans existing
        // data. They keep the program's earlier name, blohard-cdn. The partitions
        // are `<prefix>-freezer-{keys,values,table,commits}`.
        match self {
            Self::Bodies => "blohard-cdn",
            Self::Attachments => "blohard-cdn-attachments",
        }
    }
}

/// The one key in the commit record: the Freezer's latest checkpoint.
fn checkpoint_key() -> U64 {
    U64::new(0, 0)
}

/// The store's two pieces of storage. Owned through the lock as one value
/// because both APIs pass ownership through put and sync.
struct Inner<E: Context> {
    freezer: Freezer<E, Digest, Vec<u8>>,
    commits: Metadata<E, U64, Checkpoint>,
    /// Records taken since the last commit.
    dirty: bool,
}

/// A content-addressed store: keccak256(bytes) -> bytes.
pub struct ContentStore<E: Context + Spawner> {
    /// Spawns the task that owns each write for its duration.
    context: E,
    /// `None` means an earlier write failed mid-flight and the store is
    /// poisoned: better to refuse than to resume on unknown state. Reads share
    /// the lock; writes and periodic commits hold it alone.
    inner: Arc<RwLock<Option<Inner<E>>>>,
    /// Largest value this store accepts.
    max_size: usize,
}

impl<E: Context + Spawner> ContentStore<E> {
    /// Open (or create) the store of the given kind using the runtime
    /// context's storage. `max_size` caps what [`ContentStore::put`] accepts
    /// and may not exceed [`MAX_VALUE_SIZE`].
    #[cfg(test)]
    pub async fn init(context: E, kind: Kind, max_size: usize) -> Result<Self> {
        Self::init_with(context, kind, max_size, 64 << 20).await
    }

    /// `init` with the memory for the key index's page cache in bytes: how
    /// much of the index stays in RAM between reads, least-recently-used.
    pub async fn init_with(
        context: E,
        kind: Kind,
        max_size: usize,
        page_cache_bytes: usize,
    ) -> Result<Self> {
        let pages = std::num::NonZeroUsize::new((page_cache_bytes / 4096).max(1))
            .expect("at least one page");
        ensure!(
            max_size <= MAX_VALUE_SIZE,
            "store size cap {max_size} exceeds the hard ceiling {MAX_VALUE_SIZE}"
        );
        let p = kind.prefix();
        // The commit record first: the checkpoint it holds is what makes the
        // Freezer's existing data count.
        let commits = Metadata::<E, U64, Checkpoint>::init(
            context.child("commits"),
            MetadataConfig {
                partition: format!("{p}-freezer-commits"),
                codec_config: (),
            },
        )
        .await
        .map_err(|e| anyhow!("failed to open the {kind:?} commit record: {e:?}"))?;
        let checkpoint = commits.get(&checkpoint_key()).copied();
        let cfg = FreezerConfig {
            key_partition: format!("{p}-freezer-keys"),
            key_write_buffer: NZUsize!(1 << 20),
            key_page_cache: CacheRef::from_pooler(&context, NZU16!(4096), pages),
            value_partition: format!("{p}-freezer-values"),
            value_compression: None,
            value_write_buffer: NZUsize!(1 << 20),
            value_target_size: 64 << 20,
            table_partition: format!("{p}-freezer-table"),
            table_initial_size: 65_536,
            table_resize_frequency: 4,
            table_resize_chunk_size: 16_384,
            table_replay_buffer: NZUsize!(1 << 20),
            codec_config: ((0..=MAX_VALUE_SIZE).into(), ()),
        };
        let freezer = Freezer::init(context.child("freezer"), cfg, checkpoint)
            .await
            .map_err(|e| anyhow!("failed to open the {kind:?} store: {e:?}"))?;
        Ok(Self {
            context,
            inner: Arc::new(RwLock::new(Some(Inner {
                freezer,
                commits,
                dirty: false,
            }))),
            max_size,
        })
    }

    /// Let concurrency tests hold a write at the store boundary without
    /// sleeping or changing production I/O.
    #[cfg(test)]
    pub async fn hold_writes_for_test(&self) -> impl Drop {
        self.inner.clone().read_owned().await
    }

    #[cfg(test)]
    pub fn writer_waiting_for_test(&self) -> bool {
        self.inner.try_read().is_err()
    }

    /// Run `op` on the state in a task of its own and wait for its answer.
    /// The state leaves the lock only here, and the task always puts back
    /// what `op` returns, so a caller that stops waiting cannot leave the
    /// store empty-handed. `op` returns `None` to poison the store after a
    /// write whose outcome is unknown.
    async fn owned<T, F, Fut>(&self, op: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Inner<E>) -> Fut + Send + 'static,
        Fut: Future<Output = (Option<Inner<E>>, Result<T>)> + Send + 'static,
    {
        let mut guard = self.inner.clone().write_owned().await;
        let Some(inner) = guard.take() else {
            return Err(anyhow!("store poisoned by earlier failure"));
        };
        let (answer, waiting) = oneshot::channel();
        self.context.child("write").spawn(move |_| async move {
            let (inner, result) = op(inner).await;
            *guard = inner;
            drop(guard);
            let _ = answer.send(result);
        });
        waiting
            .await
            .map_err(|_| anyhow!("store write ended without an answer"))?
    }

    /// The largest value this store accepts.
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Fetch content by its keccak-256 digest.
    pub async fn get(&self, digest: &Digest) -> Result<Option<Vec<u8>>> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .context("store poisoned by earlier failure")?;
        inner
            .freezer
            .get(Identifier::Key(digest))
            .await
            .map_err(|e| anyhow!("get failed: {e:?}"))
    }

    /// Store content under its keccak-256 digest, which the store computes
    /// itself so the key/preimage invariant can never be violated by a caller.
    /// Returns the digest and whether the bytes were newly stored (`false` =
    /// already present, which by content addressing means identical bytes).
    pub async fn put(&self, content: Vec<u8>) -> Result<(Digest, bool)> {
        ensure!(!content.is_empty(), "empty content");
        ensure!(
            content.len() <= self.max_size,
            "content exceeds {} bytes",
            self.max_size
        );
        let digest = keccak::hash(&[content.as_slice()]);
        self.owned(move |inner| async move {
            // The Freezer would take a second value for a key. Content
            // addressing makes a duplicate the same bytes, so it is a no-op
            // here, and the store never holds a key twice.
            match inner.freezer.has(&digest).await {
                Ok(true) => return (Some(inner), Ok((digest, false))),
                Ok(false) => {}
                Err(e) => return (Some(inner), Err(anyhow!("pre-put lookup failed: {e:?}"))),
            }
            // Written now, durable at the next commit. If the write fails the
            // store is left poisoned rather than resumed on unknown state.
            let Inner {
                freezer, commits, ..
            } = inner;
            match freezer.put(digest, content).await {
                Ok((freezer, _)) => (
                    Some(Inner {
                        freezer,
                        commits,
                        dirty: true,
                    }),
                    Ok((digest, true)),
                ),
                Err(e) => (None, Err(anyhow!("put failed: {e:?}"))),
            }
        })
        .await
    }

    /// Make every record taken so far durable: sync the Freezer and write the
    /// checkpoint to the commit record. A no-op when nothing was written since
    /// the last commit. If either step fails the store is left poisoned.
    pub async fn commit(&self) -> Result<()> {
        self.owned(|inner| async move {
            if !inner.dirty {
                return (Some(inner), Ok(()));
            }
            let Inner {
                freezer, commits, ..
            } = inner;
            let (freezer, checkpoint) = match freezer.sync().await {
                Ok(synced) => synced,
                Err(e) => return (None, Err(anyhow!("sync failed: {e:?}"))),
            };
            match commits.put_sync(checkpoint_key(), checkpoint).await {
                Ok(commits) => (
                    Some(Inner {
                        freezer,
                        commits,
                        dirty: false,
                    }),
                    Ok(()),
                ),
                Err(e) => (None, Err(anyhow!("commit failed: {e:?}"))),
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_runtime::{Runner as _, Supervisor as _, deterministic};

    fn body(i: usize) -> Vec<u8> {
        format!("# post {i}\n\n{}\n", "lorem ".repeat(i * 3)).into_bytes()
    }

    #[test]
    fn put_get_roundtrip_and_dedup() {
        deterministic::Runner::default().start(|ctx| async move {
            let store = ContentStore::init(ctx, Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();

            let content = b"# hello blohard\n".to_vec();
            let (digest, stored) = store.put(content.clone()).await.unwrap();
            assert!(stored);
            // The key is exactly the EVM keccak of the bytes.
            assert_eq!(digest, keccak::hash(&[content.as_slice()]));
            assert_eq!(store.get(&digest).await.unwrap(), Some(content.clone()));

            // A duplicate publish is a no-op: same digest, nothing stored.
            let (again, stored) = store.put(content.clone()).await.unwrap();
            assert_eq!(again, digest);
            assert!(!stored);
            assert_eq!(store.get(&digest).await.unwrap(), Some(content));
        });
    }

    /// An upload whose request is dropped mid-write must neither lose the
    /// write nor leave the store refusing every later one.
    #[test]
    fn a_cancelled_write_still_lands_and_the_store_stays_usable() {
        deterministic::Runner::default().start(|ctx| async move {
            let store = ContentStore::init(ctx, Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();
            let first = b"written by a request that went away".to_vec();
            let digest = keccak::hash(&[first.as_slice()]);
            {
                let mut put = Box::pin(store.put(first.clone()));
                // The write is handed to its own task; the request stops waiting.
                assert!(futures::poll!(&mut put).is_pending());
            }
            let (second, fresh) = store.put(b"the next upload".to_vec()).await.unwrap();
            assert!(fresh);
            store.commit().await.unwrap();
            assert_eq!(store.get(&digest).await.unwrap(), Some(first));
            assert_eq!(
                store.get(&second).await.unwrap(),
                Some(b"the next upload".to_vec())
            );
        });
    }

    #[test]
    fn enforces_size_bounds() {
        deterministic::Runner::default().start(|ctx| async move {
            let store = ContentStore::init(ctx, Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();
            assert!(store.put(Vec::new()).await.is_err(), "empty rejected");

            let too_big = vec![b'x'; MAX_CONTENT_SIZE + 1];
            assert!(store.put(too_big).await.is_err(), "oversize rejected");

            // Exactly the cap must round-trip.
            let max = vec![b'y'; MAX_CONTENT_SIZE];
            let (digest, stored) = store.put(max.clone()).await.unwrap();
            assert!(stored);
            assert_eq!(store.get(&digest).await.unwrap(), Some(max));
        });
    }

    #[test]
    fn distinct_content_gets_distinct_keys() {
        deterministic::Runner::default().start(|ctx| async move {
            let store = ContentStore::init(ctx, Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();
            let mut digests = Vec::new();
            for i in 1..=50 {
                let (d, stored) = store.put(body(i)).await.unwrap();
                assert!(stored);
                digests.push(d);
            }
            let unique: std::collections::HashSet<_> = digests.iter().collect();
            assert_eq!(unique.len(), 50);
            for (i, d) in digests.iter().enumerate() {
                assert_eq!(store.get(d).await.unwrap(), Some(body(i + 1)));
            }
        });
    }

    #[test]
    fn stores_are_independent_and_capped_separately() {
        deterministic::Runner::default().start(|ctx| async move {
            let bodies = ContentStore::init(ctx.child("b"), Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();
            let blobs = ContentStore::init(ctx.child("a"), Kind::Attachments, 2 << 20)
                .await
                .unwrap();
            assert_eq!(bodies.max_size(), MAX_CONTENT_SIZE);
            assert_eq!(blobs.max_size(), 2 << 20);

            // A 1.5 MiB value fits the attachments store but not the bodies store.
            let big = vec![b'z'; (3 << 20) / 2];
            assert!(bodies.put(big.clone()).await.is_err());
            let (d, stored) = blobs.put(big.clone()).await.unwrap();
            assert!(stored);
            // It lives in one store only.
            assert_eq!(blobs.get(&d).await.unwrap(), Some(big));
            assert_eq!(bodies.get(&d).await.unwrap(), None);

            // A cap above the hard ceiling is refused at init.
            assert!(
                ContentStore::init(ctx.child("x"), Kind::Attachments, MAX_VALUE_SIZE + 1)
                    .await
                    .is_err()
            );
        });
    }

    /// What a crash costs: the records taken since the last commit, which
    /// the store then takes again as new.
    #[test]
    fn uncommitted_writes_do_not_survive_reopen() {
        deterministic::Runner::default().start(|ctx| async move {
            let (kept, lost) = {
                let store = ContentStore::init(ctx.child("s"), Kind::Bodies, MAX_CONTENT_SIZE)
                    .await
                    .unwrap();
                let (kept, _) = store.put(b"committed".to_vec()).await.unwrap();
                store.commit().await.unwrap();
                let (lost, _) = store.put(b"not yet committed".to_vec()).await.unwrap();
                // Readable right away, before any commit.
                assert_eq!(
                    store.get(&lost).await.unwrap(),
                    Some(b"not yet committed".to_vec())
                );
                (kept, lost)
            };
            let store = ContentStore::init(ctx.child("s"), Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();
            assert_eq!(store.get(&kept).await.unwrap(), Some(b"committed".to_vec()));
            assert_eq!(store.get(&lost).await.unwrap(), None);
            let (again, stored) = store.put(b"not yet committed".to_vec()).await.unwrap();
            assert_eq!(again, lost);
            assert!(stored, "taken again as new");
        });
    }

    #[test]
    fn persists_across_reopen() {
        deterministic::Runner::default().start(|ctx| async move {
            let bodies: Vec<Vec<u8>> = (1..=25).map(body).collect();

            // First lifetime: write everything, commit, then drop the store
            // without any explicit close. Committed is committed.
            let digests = {
                let store = ContentStore::init(ctx.child("first"), Kind::Bodies, MAX_CONTENT_SIZE)
                    .await
                    .unwrap();
                let mut digests = Vec::new();
                for b in &bodies {
                    digests.push(store.put(b.clone()).await.unwrap().0);
                }
                store.commit().await.unwrap();
                store.commit().await.unwrap(); // nothing new: a no-op
                digests
            };

            // Second lifetime over the same partitions: every item readable,
            // duplicates still detected, and new writes still land.
            let store = ContentStore::init(ctx.child("second"), Kind::Bodies, MAX_CONTENT_SIZE)
                .await
                .unwrap();
            for (d, b) in digests.iter().zip(&bodies) {
                assert_eq!(store.get(d).await.unwrap().as_deref(), Some(b.as_slice()));
            }
            let (_, stored) = store.put(bodies[0].clone()).await.unwrap();
            assert!(!stored);
            let (d, stored) = store.put(b"after reopen".to_vec()).await.unwrap();
            assert!(stored);
            assert_eq!(store.get(&d).await.unwrap(), Some(b"after reopen".to_vec()));
        });
    }
}
