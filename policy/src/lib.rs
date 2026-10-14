//! The operator's local lists, shared by the host and its relayer.
//! The host is the only writer. Readers open the atomically replaced file;
//! unreadable policy is an error, never permission to spend or accept a post.

use alloy_primitives::Address;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    io::Read,
    path::{Path, PathBuf},
};

pub const MAX_MEMBERS: usize = 4096;
/// Three full lists, as the host writes them, come to about 600 KiB.
pub const MAX_BYTES: u64 = 1 << 20;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lists {
    pub revision: u64,
    pub curated: BTreeSet<Address>,
    pub blocked: BTreeSet<Address>,
    /// Accounts the host labels as bots, whether or not they declared it.
    pub bots: BTreeSet<Address>,
}

impl Lists {
    pub fn validate(&self) -> Result<()> {
        for list in [&self.curated, &self.blocked, &self.bots] {
            ensure!(list.len() <= MAX_MEMBERS, "host list is full");
            ensure!(
                !list.contains(&Address::ZERO),
                "zero address is not an account"
            );
        }
        Ok(())
    }
    pub fn blocked(&self, address: &[u8; 20]) -> bool {
        self.blocked.contains(&Address::from(*address))
    }
    pub fn curated(&self, address: &[u8; 20]) -> bool {
        !self.blocked(address) && self.curated.contains(&Address::from(*address))
    }
    pub fn read(path: &Path) -> Result<Self> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .with_context(|| format!("opening host policy {}", path.display()))?
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() as u64 <= MAX_BYTES, "host policy is too large");
        let lists: Self = serde_json::from_slice(&bytes).context("reading host policy")?;
        lists.validate()?;
        Ok(lists)
    }
    /// The file's bytes as the host writes them. An edit that would not fit
    /// the reader's limit is refused before anything is written.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(self)?;
        ensure!(bytes.len() as u64 <= MAX_BYTES, "host policy is too large");
        Ok(bytes)
    }
}

/// No configured file means no curated or blocked accounts. A configured
/// file must exist; removing or corrupting it cannot switch blocking off.
#[derive(Clone)]
pub struct Reader {
    pub path: Option<PathBuf>,
    reads: std::sync::Arc<tokio::sync::Semaphore>,
}
impl Default for Reader {
    fn default() -> Self {
        Self::new(None)
    }
}
impl Reader {
    pub fn new(path: Option<PathBuf>) -> Self {
        Self {
            path,
            reads: std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
        }
    }
    pub async fn read(&self) -> Result<Lists> {
        match self.path.clone() {
            Some(path) => {
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let permit = self
                        .reads
                        .clone()
                        .acquire_owned()
                        .await
                        .context("host policy reader closed")?;
                    // The same deadline covers queuing and disk work. A timed
                    // out reader leaves its worker holding the slot until done.
                    tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        Lists::read(&path)
                    })
                    .await?
                })
                .await
                .context("host policy read timed out")?
            }
            None => Ok(Lists::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocking_wins_over_curation_and_lists_are_bounded() {
        let author = Address::repeat_byte(1);
        let mut lists = Lists::default();
        lists.curated.insert(author);
        assert!(lists.curated(author.as_ref()));
        lists.blocked.insert(author);
        assert!(!lists.curated(author.as_ref()));
        lists.curated.insert(Address::ZERO);
        assert!(lists.validate().is_err());
        lists.curated = (1..=MAX_MEMBERS + 1)
            .map(|n| {
                let mut a = [0; 20];
                a[12..].copy_from_slice(&(n as u64).to_be_bytes());
                Address::from(a)
            })
            .collect();
        assert!(lists.validate().is_err());
    }
    #[test]
    fn three_full_lists_are_written_and_read_back() {
        let full = |seed: u8| -> BTreeSet<Address> {
            (1..=MAX_MEMBERS as u64)
                .map(|n| {
                    let mut a = [seed; 20];
                    a[12..].copy_from_slice(&n.to_be_bytes());
                    Address::from(a)
                })
                .collect()
        };
        let lists = Lists {
            revision: u64::MAX,
            curated: full(1),
            blocked: full(2),
            bots: full(3),
        };
        let path =
            std::env::temp_dir().join(format!("host-policy-full-{}.json", std::process::id()));
        std::fs::write(&path, lists.encode().unwrap()).unwrap();
        let read = Lists::read(&path).unwrap();
        assert_eq!(read.bots.len(), MAX_MEMBERS);
        assert_eq!(read.revision, u64::MAX);
        std::fs::remove_file(path).unwrap();
        // Past the limit the writer refuses rather than saving an unreadable file.
        let mut padded = lists.clone();
        for seed in 4..=6 {
            padded.curated.extend(full(seed));
        }
        assert!(padded.encode().is_err());
    }
    #[test]
    fn a_file_without_the_bots_list_is_refused() {
        assert!(
            serde_json::from_str::<Lists>(r#"{"revision":0,"curated":[],"blocked":[]}"#).is_err()
        );
    }
    #[tokio::test]
    async fn bursts_wait_for_a_read_slot_instead_of_being_refused() {
        let path =
            std::env::temp_dir().join(format!("host-policy-burst-{}.json", std::process::id()));
        std::fs::write(
            &path,
            br#"{"revision":7,"curated":[],"blocked":[],"bots":[]}"#,
        )
        .unwrap();
        let reader = Reader::new(Some(path.clone()));
        let slots = reader.reads.clone().acquire_many_owned(4).await.unwrap();
        let mut reads: Vec<_> = (0..16)
            .map(|_| {
                let reader = reader.clone();
                Box::pin(async move { reader.read().await })
            })
            .collect();
        // Poll every request while all workers are occupied. None may turn
        // a brief overlap into a refusal, or bypass the worker limit.
        std::future::poll_fn(|cx| {
            for read in &mut reads {
                assert!(read.as_mut().poll(cx).is_pending());
            }
            std::task::Poll::Ready(())
        })
        .await;
        drop(slots);
        let mut tasks = tokio::task::JoinSet::new();
        for read in reads {
            tasks.spawn(read);
        }
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap().unwrap().revision, 7);
        }
        assert_eq!(reader.reads.available_permits(), 4);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn configured_missing_file_is_not_an_empty_policy() {
        assert!(
            Reader::new(Some(PathBuf::from("/nonexistent/host-policy.json")))
                .read()
                .await
                .is_err()
        );
        assert!(Reader::default().read().await.unwrap().blocked.is_empty());
    }
}
