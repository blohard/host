//! Disposable link metadata. Images stay on their origin; this service never
//! downloads them. No chain reads or persistent writes belong on this path.
mod http;
pub use http::{authorize_or_cached_response, draft, lookup};
mod metadata;
mod outbound;
use crate::{
    expiring::{Expiring, NEGATIVE_LIFETIME, SUCCESS_LIFETIME},
    flights::Flights,
    token_bucket::Bucket,
};
use anyhow::Result;
pub use outbound::Config;
use reqwest::Url;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug, Serialize, PartialEq)]
struct Metadata {
    title: String,
    description: String,
    image: Option<String>,
}
#[derive(Clone)]
struct Entry {
    value: Result<Option<Metadata>, ()>,
    until: Instant,
}

const POST_WINDOW: Duration = Duration::from_secs(300);
const POST_BUDGET_CAP: usize = 4096;
type Post = (u64, u64);

/// Where a preview request comes from: a link in an accepted post, or the
/// composer before publishing.
#[derive(Clone, Copy)]
enum PreviewSource {
    Post { chain: u64, id: u64 },
    Draft,
}

/// The time a fetch, its parse and its image check have in all.
const FETCH_DEADLINE: Duration = Duration::from_secs(10);

// Retry delays, in seconds, for refusals that are this host's own. They are
// never cached against a link.
/// Both draft slots are in use.
const RETRY_DRAFT_LANE: u64 = 2;
/// The draft allowance is spent, or the link's host already has two fetches.
const RETRY_BUSY: u64 = 5;
/// The host's list of its own addresses is out of date.
const RETRY_DISCOVERY: u64 = 15;
/// No shared fetch slot came free within three seconds, or too many links
/// are waiting.
const RETRY_NO_SLOT: u64 = 60;

#[derive(Default)]
struct PostBudget(HashMap<Post, [Option<Instant>; 2]>);
impl PostBudget {
    fn take(&mut self, post: Post, now: Instant) -> Result<(), u64> {
        let remaining = |at: Instant| POST_WINDOW.saturating_sub(now.saturating_duration_since(at));
        let seconds = |wait: Duration| wait.as_secs() + u64::from(wait.subsec_nanos() != 0);
        if !self.0.contains_key(&post) && self.0.len() >= POST_BUDGET_CAP {
            // Evict only fully replenished allowances. Churning other post ids
            // must not reset a live post's rate limit.
            self.0
                .retain(|_, times| !remaining(times[1].unwrap()).is_zero());
            if self.0.len() >= POST_BUDGET_CAP {
                let wait = self
                    .0
                    .values()
                    .map(|times| remaining(times[1].unwrap()))
                    .min()
                    .unwrap();
                return Err(seconds(wait));
            }
        }
        let times = self.0.entry(post).or_insert([None, None]);
        if let Some(first) = times[0] {
            let wait = remaining(first);
            if !wait.is_zero() {
                return Err(seconds(wait));
            }
        }
        *times = [times[1], Some(now)];
        Ok(())
    }
}

/// The slots a fetch holds. Its detached worker owns them until the work
/// ends: through the blocking parse even after the waiter gives up at the
/// deadline (see `blocking_with`), and through the image check.
struct FetchPermits {
    _shared: OwnedSemaphorePermit,
    _draft: Option<OwnedSemaphorePermit>,
    _visitor: Option<Arc<OwnedSemaphorePermit>>,
}

pub struct Previews {
    fetcher: outbound::Fetcher,
    // With 2 KiB URL keys and bounded fields, 1024 entries hold under 6 MiB
    // of strings. CLOCK eviction keeps a one-off URL flood bounded.
    cache: Expiring<String, Entry>,
    flights: Flights<(bool, String), Result<Entry, u64>>,
    // Uncached draft work shares one host-wide allowance, regardless of visitors.
    draft_budget: Mutex<Bucket>,
    post_budget: Mutex<PostBudget>,
    slots: Arc<Semaphore>,
    // Unpublished links may occupy only two of the shared fetch slots.
    drafts: Arc<Semaphore>,
}
impl Previews {
    /// A preview found for `link`, as a fetch would leave it, for tests.
    #[cfg(test)]
    pub(crate) fn seed_for_test(&self, link: &str, title: &str) {
        let lifetime = Duration::from_secs(60);
        let entry = Entry {
            value: Ok(Some(Metadata {
                title: title.into(),
                description: String::new(),
                image: None,
            })),
            until: Instant::now() + lifetime,
        };
        self.cache.put(link.into(), entry, lifetime);
    }

    pub fn new(config: Config) -> Result<Self> {
        Ok(Self {
            fetcher: outbound::Fetcher::new(config)?,
            cache: Expiring::new(1024),
            flights: Flights::new(40),
            draft_budget: Mutex::new(Bucket::new(20.0, 1.0 / 5.0, Instant::now())),
            post_budget: Mutex::new(PostBudget::default()),
            slots: Arc::new(Semaphore::new(8)),
            drafts: Arc::new(Semaphore::new(2)),
        })
    }
    pub async fn run(&self) {
        self.fetcher.run().await;
    }

    pub async fn ready(&self) -> bool {
        self.fetcher.ready().await
    }

    /// Keep a published link's answer until it expires. A draft's answer is
    /// never kept: a link its writer has not published must not be
    /// discoverable through the public cache.
    fn remember(&self, source: PreviewSource, key: String, entry: &Entry) {
        if let PreviewSource::Post { .. } = source {
            let lifetime = entry.until.saturating_duration_since(Instant::now());
            self.cache.put(key, entry.clone(), lifetime);
        }
    }

    #[cfg(test)]
    async fn read(self: &Arc<Self>, url: Url, draft: bool) -> Result<Entry, u64> {
        let source = if draft {
            PreviewSource::Draft
        } else {
            PreviewSource::Post { chain: 1, id: 1 }
        };
        self.read_admitted(url, source, None).await
    }

    /// One detached worker per link admits and fetches it, and the readers of
    /// that link wait for its answer. Refusals that are this host's own carry
    /// their retry delay in seconds and are never cached.
    async fn read_admitted(
        self: &Arc<Self>,
        url: Url,
        source: PreviewSource,
        visitor: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Entry, u64> {
        let key = url.to_string();
        // Drafts and published links never share a fetch, so neither inherits
        // the other's refusals. Readers of a published link share one fetch,
        // whichever post they came from.
        let flight = (matches!(source, PreviewSource::Draft), key.clone());
        let service = self.clone();
        let worker_key = key.clone();
        self.flights
            .run(flight, || self.cache.get(&key).map(Ok), async move {
                // The worker owns the permits until it finishes: a reader that
                // disconnects cannot free capacity its fetch still uses.
                let (destination, permits) = service.admit(&url, source, visitor).await?;
                let deadline = Instant::now() + FETCH_DEADLINE;
                let entry = service
                    .fetch_card(url, destination, permits, deadline)
                    .await?;
                service.remember(source, worker_key, &entry);
                Ok(entry)
            })
            .await
            .map_err(|_| RETRY_NO_SLOT)?
    }

    /// Admission, in the order https://github.com/blohard/host/blob/main/GATEWAY_API.md promises,
    /// so that a refusal never
    /// spends what a later step takes: for a draft, a draft slot and then the
    /// draft allowance; for every link, a slot at its host and then a shared
    /// slot; and last, for a post, one of its two attempts.
    async fn admit(
        &self,
        url: &Url,
        source: PreviewSource,
        visitor: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<(outbound::Destination, FetchPermits), u64> {
        let draft = match source {
            PreviewSource::Draft => {
                let slot = self
                    .drafts
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| RETRY_DRAFT_LANE)?;
                if !self
                    .draft_budget
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take(Instant::now())
                {
                    return Err(RETRY_BUSY);
                }
                Some(slot)
            }
            PreviewSource::Post { .. } => None,
        };
        // A busy host is not a failed page: the refusal reaches the readers
        // waiting now, but is not cached against the link.
        let (destination, shared) = self.fetcher.admit(url, &self.slots).await?;
        if let PreviewSource::Post { chain, id } = source {
            self.post_budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take((chain, id), Instant::now())?;
        }
        let permits = FetchPermits {
            _shared: shared,
            _draft: draft,
            _visitor: visitor,
        };
        Ok((destination, permits))
    }

    /// Fetch the page, parse its head and check its image, all before
    /// `deadline`, and make the outcome an entry.
    async fn fetch_card(
        &self,
        url: Url,
        destination: outbound::Destination,
        permits: FetchPermits,
        deadline: Instant,
    ) -> Result<Entry, u64> {
        let fetched = tokio::time::timeout_at(deadline.into(), async {
            let mut page = self.fetcher.fetch(url, destination).await?;
            let mut bytes = Vec::new();
            let mut permits = permits;
            let mut limit = outbound::FIRST_HTML_BYTES;
            let parsed = loop {
                let ended = page.read_until(&mut bytes, limit).await?;
                let base = page.url.clone();
                // Doubling checkpoints bounds re-parsing to about twice the
                // final prefix. Each parse owns the permits and shares the deadline.
                let ((answer, retained), returned) = blocking_with(permits, move || {
                    (metadata::parse(&bytes, &base, deadline), bytes)
                })
                .await?;
                bytes = retained;
                permits = returned;
                let parsed = answer?;
                if parsed.finished
                    || ended
                    || bytes.len() == outbound::HTML_BYTES
                    // Give early titles a chance to acquire the rest of their
                    // metadata; read beyond 512 KiB only if none was found.
                    || (bytes.len() >= outbound::PREVIEW_HTML_BYTES && parsed.metadata.is_some())
                {
                    break parsed.metadata;
                }
                // A response chunk can cross several checkpoints. Skip those
                // rather than parsing the same prefix again.
                limit = (bytes.len() + 1)
                    .next_power_of_two()
                    .min(outbound::HTML_BYTES);
            };
            drop(page);
            drop(bytes);
            // The image check is outbound work too, so it keeps the permits.
            let card = match parsed {
                Some(mut metadata) => {
                    let retry_image = self.check_image(&mut metadata).await?;
                    Some((metadata, retry_image))
                }
                None => None,
            };
            if Instant::now() >= deadline {
                return Err(outbound::Failure::timeout());
            }
            drop(permits);
            Ok(card)
        })
        .await
        .unwrap_or_else(|_| Err(outbound::Failure::timeout()));
        Entry::from_fetch(fetched)
    }

    /// Keep the card's image only if its address passes the check. True when
    /// the check failed in a way that may recover, so the card is looked at
    /// again within the hour.
    async fn check_image(&self, metadata: &mut Metadata) -> Result<bool, outbound::Failure> {
        let Some(image) = metadata.image.take() else {
            return Ok(false);
        };
        match self.fetcher.image(&image).await {
            Ok(url) => {
                metadata.image = Some(url.to_string());
                Ok(false)
            }
            Err(outbound::Failure::DiscoveryUnavailable) => {
                Err(outbound::Failure::DiscoveryUnavailable)
            }
            Err(outbound::Failure::Temporary(_)) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

/// Run blocking work that owns the fetch's permits until it ends, then hand
/// them back. Blocking work cannot be stopped midway, so a waiter that gives
/// up at the deadline must not free the slots while the work still runs.
async fn blocking_with<T: Send + 'static>(
    permits: FetchPermits,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<(T, FetchPermits), outbound::Failure> {
    tokio::task::spawn_blocking(move || (work(), permits))
        .await
        .map_err(|_| outbound::Failure::temporary())
}

// Strip presentation controls without removing joining characters used in
// scripts and emoji (U+200C/U+200D).
fn invisible(ch: char) -> bool {
    matches!(ch, '\u{00ad}' | '\u{061c}' | '\u{180e}' | '\u{200b}'
        | '\u{200e}'..='\u{200f}' | '\u{202a}'..='\u{202e}'
        | '\u{2060}'..='\u{206f}' | '\u{feff}' | '\u{fff9}'..='\u{fffb}')
}

fn text(value: &str, cap: usize) -> String {
    let mut result = String::new();
    let mut space = false;
    for ch in value.chars() {
        if ch.is_whitespace() {
            space = !result.is_empty();
        } else if !ch.is_control() && !invisible(ch) {
            if result.len() + usize::from(space) + ch.len_utf8() > cap {
                break;
            }
            if space {
                result.push(' ');
                space = false;
            }
            result.push(ch);
        }
    }
    result
}

fn link_url(link: &str) -> Result<Url> {
    anyhow::ensure!(link.len() <= outbound::URL_BYTES, "link too long");
    // URL parsing rewrites whitespace/control characters. Accept a single
    // link token, never a span of prose or markup that becomes another URL.
    anyhow::ensure!(
        !link.chars().any(|c| c.is_whitespace()
            || c.is_control()
            || invisible(c)
            || "<>\"`\\“”‘’«»‹›「」『』【】".contains(c)),
        "invalid link text"
    );
    if link
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("http://"))
        || link
            .get(..8)
            .is_some_and(|s| s.eq_ignore_ascii_case("https://"))
    {
        outbound::url(link)
    } else {
        outbound::url(&format!("https://{link}"))
    }
}

impl Entry {
    /// What a fetch's outcome becomes: an entry that lasts its lifetime, or a
    /// refusal that is this host's own. A card lives a day, or an hour when
    /// the flag beside it says its image check may recover; a page without a
    /// card, or a refusal of it, an hour; a failure that may recover, its
    /// cooldown.
    fn from_fetch(
        fetched: Result<Option<(Metadata, bool)>, outbound::Failure>,
    ) -> Result<Self, u64> {
        let (value, lifetime) = match fetched {
            Ok(Some((metadata, retry_image))) => {
                let lifetime = if retry_image {
                    NEGATIVE_LIFETIME
                } else {
                    SUCCESS_LIFETIME
                };
                (Ok(Some(metadata)), lifetime)
            }
            Ok(None) | Err(outbound::Failure::NoPreview) => (Ok(None), NEGATIVE_LIFETIME),
            Err(outbound::Failure::Temporary(wait)) => (Err(()), wait),
            Err(outbound::Failure::DiscoveryUnavailable) => return Err(RETRY_DISCOVERY),
            Err(outbound::Failure::Busy) => return Err(RETRY_BUSY),
        };
        Ok(Self {
            value,
            until: Instant::now() + lifetime,
        })
    }
}

#[cfg(test)]
mod tests;
