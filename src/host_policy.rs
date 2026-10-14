//! Durable host lists, bot-declaration and takedown snapshots. Each has its
//! own writer and counter; accepted writes finish even after the caller
//! disconnects.
//! [`http`] authenticates requests and renders policy answers. This module
//! rechecks edits under the writer and saves before publishing a new snapshot.
//! Declaration rules live in [`crate::bot_declarations`].

pub(crate) mod http;
pub(crate) mod takedowns;

use crate::{
    bot_declarations::{Declarations, Refusal, Statement},
    chain::Address,
    http::ApiError,
};
use axum::http::StatusCode;
use blohard_policy::Lists;
use serde::Deserialize;
use std::{
    io::Write,
    path::PathBuf,
    sync::{Arc, PoisonError, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub file: PathBuf,
    pub admin: Address,
    /// The configured chain on which a contract admin verifies signatures.
    pub admin_chain: u64,
    /// Canonical browser origin; signatures cannot be moved to another host.
    pub origin: String,
}

#[derive(Clone)]
pub struct Policy(Arc<Inner>);
struct Inner {
    config: Option<Config>,
    lists: Durable<Lists>,
    declarations: Durable<Declarations>,
    takedowns: Durable<takedowns::Takedowns>,
}
impl Default for Policy {
    fn default() -> Self {
        Self(Arc::new(Inner {
            config: None,
            lists: Durable::new(Lists::default()),
            declarations: Durable::new(Declarations::default()),
            takedowns: Durable::new(takedowns::Takedowns::default()),
        }))
    }
}

/// One policy file as this host holds it: the published snapshot, and the
/// one writer that saves the next snapshot to disk before publishing it.
/// The writer closes for good once a save cannot be trusted.
struct Durable<T> {
    current: RwLock<Arc<T>>,
    writer: Arc<tokio::sync::Semaphore>,
}

/// The refusals a file's writer answers with, in its own words.
struct Refusals {
    busy: &'static str,
    closed: &'static str,
    pending: &'static str,
}

impl<T: Send + Sync + 'static> Durable<T> {
    fn new(value: T) -> Self {
        Self {
            current: RwLock::new(Arc::new(value)),
            writer: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    fn snapshot(&self) -> Arc<T> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Run `work` under this file's writer, in a blocking task that finishes
    /// even after the caller disconnects, waiting `wait` for the writer and
    /// ten seconds for the work. Once the writer has closed, the file is not
    /// written again until a restart.
    async fn write(
        &self,
        refusals: Refusals,
        wait: Duration,
        work: impl FnOnce() -> Result<(), ApiError> + Send + 'static,
    ) -> Result<(), ApiError> {
        let permit = tokio::time::timeout(wait, self.writer.clone().acquire_owned())
            .await
            .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, refusals.busy))?
            .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, refusals.closed))?;
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        });
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .map_err(|_| ApiError::new(StatusCode::SERVICE_UNAVAILABLE, refusals.pending))?
            .map_err(ApiError::store_failed)?
    }

    /// Save `bytes` to `path`, then publish `next`. Once the rename has
    /// happened the save stands, so `next` is published even if syncing the
    /// directory then fails; that failure closes the writer, since the next
    /// save could not be trusted either.
    fn save_and_publish(
        &self,
        path: &std::path::Path,
        bytes: &[u8],
        next: T,
    ) -> Result<(), ApiError> {
        let synced = save(path, bytes).map_err(ApiError::store_failed)?;
        *self.current.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(next);
        synced.map_err(|error| {
            self.writer.close();
            ApiError::store_failed(error)
        })
    }
}

/// The most entries a signed edit may carry.
const MAX_PER_EDIT: usize = 100;
/// How far ahead a signed request may set its expiry, in seconds.
const MAX_EXPIRY: u64 = 300;
/// Whether a signed request's expiry is neither past nor too far ahead.
fn fresh(expires: u64, now: u64) -> bool {
    expires >= now && expires <= now.saturating_add(MAX_EXPIRY)
}

/// Declarations live beside the policy file: `host-policy.json` keeps them in
/// `host-policy.declarations.json`.
fn declarations_path(policy_file: &std::path::Path) -> PathBuf {
    policy_file.with_extension("declarations.json")
}
impl Policy {
    pub async fn open(config: Option<Config>) -> anyhow::Result<Self> {
        let Some(config) = config else {
            return Ok(Self::default());
        };
        let origin = reqwest::Url::parse(&config.origin)?;
        anyhow::ensure!(
            matches!(origin.scheme(), "http" | "https")
                && origin.origin().ascii_serialization() == config.origin,
            "host_policy.origin must be a browser origin without a trailing slash"
        );
        anyhow::ensure!(config.admin != Address::ZERO, "host_policy.admin is zero");
        let lists = blohard_policy::Reader::new(Some(config.file.clone()))
            .read()
            .await?;
        let declarations = Declarations::read(&declarations_path(&config.file))?;
        let taken_down = takedowns::Takedowns::read(&takedowns::path(&config.file))?;
        Ok(Self(Arc::new(Inner {
            config: Some(config),
            lists: Durable::new(lists),
            declarations: Durable::new(declarations),
            takedowns: Durable::new(taken_down),
        })))
    }
    fn config(&self) -> Option<&Config> {
        self.0.config.as_ref()
    }
    pub fn enabled(&self) -> bool {
        self.config().is_some()
    }
    pub fn snapshot(&self) -> Arc<Lists> {
        self.0.lists.snapshot()
    }
    pub fn declarations(&self) -> Arc<Declarations> {
        self.0.declarations.snapshot()
    }
    pub fn blocked(&self, author: &[u8; 20]) -> bool {
        self.snapshot().blocked(author)
    }
    pub fn takedowns(&self) -> Arc<takedowns::Takedowns> {
        self.0.takedowns.snapshot()
    }
    /// Whether the admin has taken down the record `digest`: this host
    /// neither serves nor accepts it.
    pub fn taken_down(&self, digest: &[u8; 32]) -> bool {
        self.takedowns().contains(digest)
    }
    // The handler verifies the signature first. Expiry and revision are checked
    // again inside the writer so concurrent requests cannot apply stale edits.
    async fn apply(&self, edit: Edit) -> Result<(), ApiError> {
        let policy = self.clone();
        let refusals = Refusals {
            busy: "policy_busy",
            closed: "policy_write_failed_restart_required",
            pending: "policy_write_pending",
        };
        self.0
            .lists
            .write(refusals, Duration::ZERO, move || {
                let config = policy.0.config.as_ref().expect("configured writer");
                let current = policy.snapshot();
                edit.message(&config.origin, now())?;
                if current.revision != edit.revision {
                    return Err(ApiError::new(StatusCode::CONFLICT, "policy_changed"));
                }
                let mut next = (*current).clone();
                let members = match edit.list {
                    List::Curated => &mut next.curated,
                    List::Blocked => &mut next.blocked,
                    List::Bots => &mut next.bots,
                };
                for address in edit.addresses {
                    let address = alloy_primitives::Address::from(address.0);
                    match edit.action {
                        Action::Add => {
                            members.insert(address);
                        }
                        Action::Remove => {
                            members.remove(&address);
                        }
                    }
                }
                next.validate()
                    .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "invalid_policy"))?;
                next.revision = next.revision.checked_add(1).ok_or_else(|| {
                    ApiError::new(StatusCode::CONFLICT, "policy_revision_exhausted")
                })?;
                // Refuse an edit the host and the relayer could not read back.
                let bytes = next
                    .encode()
                    .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "policy_too_large"))?;
                policy.0.lists.save_and_publish(&config.file, &bytes, next)
            })
            .await
    }
}

impl Policy {
    /// One writer saves the declarations, then publishes them, as for edits.
    async fn declare(
        &self,
        account: alloy_primitives::Address,
        statement: Statement,
    ) -> Result<(), ApiError> {
        let config = self.0.config.as_ref().expect("configured declarations");
        let path = declarations_path(&config.file);
        let policy = self.clone();
        let refusals = Refusals {
            busy: "declarations_busy",
            closed: "declarations_write_failed_restart_required",
            pending: "declarations_write_pending",
        };
        // Declaring is free and may be slow to sign, so a declaration waits a
        // little for the writer instead of being turned away at once.
        self.0
            .declarations
            .write(refusals, Duration::from_secs(5), move || {
                let next = policy
                    .declarations()
                    .accept(account, statement, now())
                    .map_err(refused)?;
                let bytes = next.encode().map_err(ApiError::store_failed)?;
                policy.0.declarations.save_and_publish(&path, &bytes, next)
            })
            .await
    }
}

impl Policy {
    /// One writer saves the takedown list, then publishes it, as for edits.
    /// The handler checked the signature; the revision is checked again here.
    async fn write_takedowns(&self, edit: takedowns::Edit) -> Result<(), ApiError> {
        let config = self.0.config.as_ref().expect("configured takedowns");
        let path = takedowns::path(&config.file);
        let policy = self.clone();
        let refusals = Refusals {
            busy: "takedowns_busy",
            closed: "takedowns_write_failed_restart_required",
            pending: "takedowns_write_pending",
        };
        self.0
            .takedowns
            .write(refusals, Duration::ZERO, move || {
                let current = policy.takedowns();
                if current.revision != edit.revision {
                    return Err(ApiError::new(StatusCode::CONFLICT, "takedowns_changed"));
                }
                let next = current.after(&edit)?;
                let bytes = next.encode().map_err(ApiError::store_failed)?;
                policy.0.takedowns.save_and_publish(&path, &bytes, next)
            })
            .await
    }
}

fn save(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<std::io::Result<()>> {
    let temp = path.with_extension(format!("tmp-{}", std::process::id()));
    // Only this process writes policy. A leftover from a crashed process is
    // safe to replace; the live file is never truncated.
    let result = (|| {
        let directory = std::fs::File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(std::path::Path::new(".")),
        )?;
        let mut file = std::fs::File::create(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, path)?;
        Ok(directory.sync_all())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum List {
    Curated,
    Blocked,
    Bots,
}
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum Action {
    Add,
    Remove,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edit {
    #[serde(with = "crate::chain::decimal")]
    revision: u64,
    expires: u64,
    list: List,
    action: Action,
    addresses: Vec<Address>,
    signature: String,
}
impl Edit {
    /// The text the admin signs, after the request's own checks.
    fn message(&self, origin: &str, now: u64) -> Result<String, ApiError> {
        if !fresh(self.expires, now)
            || self.addresses.is_empty()
            || self.addresses.len() > MAX_PER_EDIT
            || self.addresses.contains(&Address::ZERO)
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_policy_edit",
            ));
        }
        let list = match self.list {
            List::Curated => "curated",
            List::Blocked => "blocked",
            List::Bots => "bots",
        };
        let action = match self.action {
            Action::Add => "add",
            Action::Remove => "remove",
        };
        Ok(format!(
            "Host policy update\nHost: {origin}\nRevision: {}\nExpires: {}\nList: {list}\nAction: {action}\nAddresses:\n{}",
            self.revision,
            self.expires,
            self.addresses
                .iter()
                .map(|a| format!("0x{}", hex::encode(a.0)))
                .collect::<Vec<_>>()
                .join("\n")
        ))
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn refused(refusal: Refusal) -> ApiError {
    match refusal {
        Refusal::Stale => ApiError::new(StatusCode::CONFLICT, "stale_declaration"),
        Refusal::Full => ApiError::new(StatusCode::CONFLICT, "declarations_full"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REFUSALS: Refusals = Refusals {
        busy: "busy",
        closed: "closed",
        pending: "pending",
    };

    /// A held writer turns a write that will not wait away at once, and one
    /// that waits runs once the writer is free; a closed writer says so.
    #[tokio::test]
    async fn a_write_gets_the_writer_within_its_wait() {
        let durable = Arc::new(Durable::new(()));
        let held = durable.writer.clone().acquire_owned().await.unwrap();
        let refused = durable
            .write(REFUSALS, Duration::ZERO, || Ok(()))
            .await
            .unwrap_err();
        assert_eq!(
            (refused.status, refused.code),
            (StatusCode::SERVICE_UNAVAILABLE, "busy")
        );

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(held);
        });
        durable
            .write(REFUSALS, Duration::from_secs(5), || Ok(()))
            .await
            .expect("the writer freed within the wait");
        durable
            .write(REFUSALS, Duration::ZERO, || Ok(()))
            .await
            .expect("a free writer");

        durable.writer.close();
        let refused = durable
            .write(REFUSALS, Duration::ZERO, || Ok(()))
            .await
            .unwrap_err();
        assert_eq!(
            (refused.status, refused.code),
            (StatusCode::SERVICE_UNAVAILABLE, "closed")
        );
    }
}
