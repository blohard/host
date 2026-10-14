//! Peers: other hosts this one knows. Their URLs are published in the status,
//! so a client that knows one host learns the rest, and they are asked for a
//! record this host lacks when a reader names the message that anchors it
//! (`GET /api/content/{hash}?chain_id=…&message_id=…`,
//! https://github.com/blohard/host/blob/main/STORE_API.md). What a
//! peer returns is treated exactly like an upload: hashed, checked against
//! the chain, admitted by this host's own policy, stored or held. A peer
//! therefore needs no trust; it is only a place where bytes may be found.
//! The index's body sweep and readers share the same backoff for missing
//! bodies. Local storage is always checked before asking peers.
//!
//! A peer is asked with a bare `GET /api/content/{hash}`, never with hints,
//! so two hosts that list each other cannot send one miss round in a loop.
//! Each peer gets a few seconds, and an answer is read only up to this
//! host's own size cap, so a slow or misbehaving peer costs a reader little.
//! Readers and the sweep share outstanding fetches. Eight distinct fetches
//! may run at once; each has a thirty-second budget and survives cancellation
//! of its readers. Bounded, expiring retry positions let each lookup resume
//! beyond peers already tried. Completed answers are left to the content store.

use crate::expiring::Expiring;
use crate::flights::{FlightError, Flights};
use crate::keccak::{self, Digest};
use crate::{
    acceptance::Acceptance,
    envelope,
    http::ApiError,
    publish::{Outcome, PublishParams, accept, body_of},
    server::{AppState, StoreState},
    store::Kind,
    verifier::Verifier,
};
use axum::http::StatusCode;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// How long one peer may take to answer.
const PEER_TIMEOUT: Duration = Duration::from_secs(5);
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ACTIVE: usize = 8;
const RETRY_MIN: Duration = Duration::from_secs(30);
const RETRY_MAX: Duration = Duration::from_secs(30 * 60);
// Keep retry positions beyond the maximum backoff.
const PROGRESS_TTL: Duration = Duration::from_secs(60 * 60);
const MAX_PROGRESS: usize = 10_000;
/// How long the answer to "does the chain vouch for this hash under that
/// message" is remembered, so a reader retrying a miss costs no RPC calls.
const VOUCH_TTL: Duration = Duration::from_secs(10);
const MAX_VOUCHES: usize = 10_000;

/// What a vouch is about: (chain, message, hash).
pub type VouchKey = (u64, u64, [u8; 32]);

type Answer = Option<(bytes::Bytes, usize)>;

#[derive(Clone)]
struct Progress {
    next: usize,
    /// Until when no peer is asked at all: after a miss, or after a refusal
    /// of the bytes whatever their role.
    after: Instant,
    delay: Duration,
    /// Until when the bytes are not asked for as a body: after this host
    /// refused them as one. Stands on its own: asking as an attachment
    /// neither clears nor renews it.
    refused_as_body: Option<Instant>,
    /// The same for asks as an attachment: over the attachment cap, or not a
    /// type this host stores.
    refused_as_attachment: Option<Instant>,
}

impl Default for Progress {
    fn default() -> Self {
        Self {
            next: 0,
            after: Instant::now(),
            delay: Duration::ZERO,
            refused_as_body: None,
            refused_as_attachment: None,
        }
    }
}

impl Progress {
    /// Only a definite miss, every asked peer answering 404, grows the delay.
    /// A peer that could not be reached, timed out or answered otherwise says
    /// nothing about the record, so the lookup keeps the shortest delay.
    fn completed(&mut self, found: bool, missing: bool, now: Instant) {
        self.delay = if found {
            Duration::ZERO
        } else if missing {
            (self.delay * 2).max(RETRY_MIN).min(RETRY_MAX)
        } else {
            RETRY_MIN
        };
        self.after = now + self.delay;
    }

    /// Whether a fetch of the bytes as `role` must wait.
    fn holds(&self, role: Kind, now: Instant) -> bool {
        let refused = match role {
            Kind::Bodies => self.refused_as_body,
            Kind::Attachments => self.refused_as_attachment,
        };
        now < self.after || refused.is_some_and(|until| now < until)
    }
}

struct Fetches {
    urls: Vec<String>,
    /// The most bytes a peer's answer may have: the host's largest upload.
    cap: usize,
    progress: Expiring<Digest, Progress>,
    client: reqwest::Client,
    flights: Flights<Digest, Answer>,
}

/// Background copying (the index sweep) shares this many of the peer fetch
/// slots across every chain, leaving the rest to readers.
const BACKGROUND_FETCHES: usize = 2;

pub struct Peers {
    fetches: Arc<Fetches>,
    background: Arc<tokio::sync::Semaphore>,
    /// What the chain said about a key, for a few seconds.
    vouches: Expiring<VouchKey, Vouch>,
}

impl Peers {
    /// `cap` is the most bytes a peer's answer may have: the host's largest
    /// upload, so anything it could admit.
    pub fn new(urls: Vec<String>, cap: usize, client: reqwest::Client) -> Self {
        let urls = urls
            .into_iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
        Self {
            fetches: Arc::new(Fetches {
                urls,
                cap,
                progress: Expiring::new(MAX_PROGRESS),
                client,
                flights: Flights::new(MAX_ACTIVE),
            }),
            vouches: Expiring::new(MAX_VOUCHES),
            background: Arc::new(tokio::sync::Semaphore::new(BACKGROUND_FETCHES)),
        }
    }

    /// A turn for background copying, shared by every chain's sweep.
    pub async fn background(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.background
            .clone()
            .acquire_owned()
            .await
            .expect("the background semaphore is never closed")
    }

    pub fn urls(&self) -> &[String] {
        &self.fetches.urls
    }

    pub fn is_empty(&self) -> bool {
        self.fetches.urls.is_empty()
    }

    /// The remembered answer for this (chain, message, hash), if recent.
    pub fn recall(&self, key: VouchKey) -> Option<Vouch> {
        self.vouches.get(&key)
    }

    pub fn remember(&self, key: VouchKey, vouched: Vouch) {
        self.vouches.put(key, vouched, VOUCH_TTL)
    }

    /// Whether a lookup of this hash as `role` is backing off, so asking now
    /// would answer nothing without trying a peer.
    pub fn waiting(&self, digest: &Digest, role: Kind) -> bool {
        self.fetches
            .progress
            .get(digest)
            .is_some_and(|p| p.holds(role, Instant::now()))
    }

    /// Move only the retry deadline; preserve the delay and next-peer position.
    #[cfg(test)]
    pub(crate) fn retry_now(&self, hash: Digest) {
        let mut progress = self.fetches.progress.get(&hash).unwrap();
        progress.after = Instant::now();
        self.fetches.progress.put(hash, progress, PROGRESS_TTL);
    }

    /// A peer had the record, but this host refused it whatever the post or
    /// role. The successful download reset the backoff, so set it again: the
    /// longest delay for a refusal the bytes earn, the shortest for one that
    /// passes.
    fn refused(&self, digest: &Digest, delay: Duration) {
        let mut progress = self.fetches.progress.get(digest).unwrap_or_default();
        progress.delay = delay;
        progress.after = Instant::now() + delay;
        self.fetches.progress.put(*digest, progress, PROGRESS_TTL);
    }

    /// A peer had the record, but this host refused it as `role`: no ask in
    /// that role for the longest delay. The fetch's own backoff stays reset,
    /// so an ask in the other role goes to a peer at once.
    fn refused_as(&self, digest: &Digest, role: Kind) {
        let mut progress = self.fetches.progress.get(digest).unwrap_or_default();
        let until = Some(Instant::now() + RETRY_MAX);
        match role {
            Kind::Bodies => progress.refused_as_body = until,
            Kind::Attachments => progress.refused_as_attachment = until,
        }
        self.fetches.progress.put(*digest, progress, PROGRESS_TTL);
    }

    /// Identical hashes share a bounded task, even when the initiating reader
    /// leaves. Lookups every peer answered with a 404 back
    /// off from thirty seconds to thirty minutes; any other failure waits
    /// thirty seconds. Callers check local storage before entering here.
    /// A full pool refuses new work without queuing.
    pub async fn fetch(
        &self,
        digest: &Digest,
        role: Kind,
    ) -> Result<Option<(bytes::Bytes, &str)>, &'static str> {
        let key = *digest;
        let fetches = self.fetches.clone();
        let result = self
            .fetches
            .flights
            .run(
                key,
                || self.waiting(&key, role).then_some(None),
                async move {
                    let (result, missing) =
                        tokio::time::timeout(FETCH_TIMEOUT, fetches.fetch(&key))
                            .await
                            .unwrap_or((None, false));
                    let mut progress = fetches.progress.get(&key).unwrap_or_default();
                    progress.completed(result.is_some(), missing, Instant::now());
                    fetches.progress.put(key, progress, PROGRESS_TTL);
                    result
                },
            )
            .await
            .map_err(|error| match error {
                FlightError::Busy => "the host is busy; try again shortly",
                FlightError::Stopped => "peer lookup unavailable; try again shortly",
            })?;
        Ok(result.map(|(bytes, peer)| (bytes, self.fetches.urls[peer].as_str())))
    }
}

/// What a peer pull came to.
pub(crate) enum Pulled {
    /// Fetched and stored.
    Stored(bytes::Bytes),
    /// Fetched, but only held until its message is accepted.
    Held,
    /// A peer had it, and this host refuses it for what it is.
    Refused,
    /// The post the reader named cannot vouch for it after all.
    Unvouched,
    /// Not vouched for, or no peer had it.
    Missing,
}

impl Pulled {
    /// The bytes, when the pull stored them.
    pub(crate) fn stored(self) -> Option<bytes::Bytes> {
        match self {
            Self::Stored(bytes) => Some(bytes),
            Self::Held | Self::Refused | Self::Unvouched | Self::Missing => None,
        }
    }
}

/// What a refusal by [`accept`] is about, which decides what backs off.
enum Refusal {
    /// The post the reader named: deleted or removed while the bytes were on
    /// the way, or by a blocked author. The bytes may still be what another
    /// post needs, so nothing backs off.
    Post,
    /// The bytes in the role they were asked in: this host's body cap,
    /// attachment cap or attachment types. Asked for in the other role they
    /// are not held back; an attachment cap can be under the body cap as
    /// well as over it.
    Role,
    /// The bytes, whatever the post or role: any other client error.
    Bytes,
    /// The moment: holding full, busy, a timeout, a conflict or a rate
    /// limit. Tried again soon.
    Moment,
}

fn refusal(error: &ApiError) -> Refusal {
    match error.code {
        "message_gone" | "host_blocked" => Refusal::Post,
        "body_too_large" | "attachment_too_large" | "attachment_type_not_stored" => Refusal::Role,
        _ if error.status.is_client_error()
            && ![
                StatusCode::REQUEST_TIMEOUT,
                StatusCode::CONFLICT,
                StatusCode::TOO_MANY_REQUESTS,
            ]
            .contains(&error.status) =>
        {
            Refusal::Bytes
        }
        _ => Refusal::Moment,
    }
}

/// A record this host lacks, fetched from a peer on a reader's word that
/// message `message_id` on `chain_id` anchors it
/// (https://github.com/blohard/host/blob/main/STORE_API.md). Peers are
/// asked only once the chain, or a body this host has, vouches for the hash,
/// so a stranger cannot make this host fetch arbitrary hashes; what a peer
/// returns then goes through [`accept`] like any upload. Says what came of
/// it, with the bytes when they were stored, so the reader is served at
/// once ([`Pulled::stored`]). An error means the chain could not be asked,
/// which the reader should hear.
pub(crate) async fn pull(
    state: &AppState,
    store: &StoreState,
    chain_id: u64,
    message_id: u64,
    digest: Digest,
) -> Result<Pulled, ApiError> {
    if store.peers.is_empty() {
        return Ok(Pulled::Missing);
    }
    // Nothing is fetched that this host has taken down.
    if state.policy.taken_down(&digest.0) {
        return Ok(Pulled::Refused);
    }
    // An index read is already cheap and must observe a stopped index or a
    // newly accepted message immediately. Store-only hosts cache RPC vouches.
    let key = (chain_id, message_id, digest.0);
    let vouched = if state.gateway.is_some() {
        vouched(state, store, chain_id, message_id, &digest).await?
    } else {
        match store.peers.recall(key) {
            Some(v) => v,
            None => {
                let v = vouched(state, store, chain_id, message_id, &digest).await?;
                store.peers.remember(key, v);
                v
            }
        }
    };
    let role = match vouched {
        Vouch::For(role) => role,
        Vouch::NotThis => return Ok(Pulled::Missing),
        Vouch::Nothing => return Ok(Pulled::Unvouched),
    };
    let Some((bytes, peer)) =
        store.peers.fetch(&digest, role).await.map_err(|message| {
            ApiError::said(StatusCode::SERVICE_UNAVAILABLE, "host_busy", message)
        })?
    else {
        return Ok(Pulled::Missing);
    };
    let hash = digest.to_hex();
    match accept(
        state,
        store,
        PublishParams::by_message(chain_id, message_id),
        digest,
        bytes.clone(),
    )
    .await
    {
        Ok(Outcome::Stored { kind, .. }) => {
            tracing::info!(chain_id, message_id, peer, hash = %hash, kind = kind.label(), "pulled from peer");
            Ok(Pulled::Stored(bytes))
        }
        Ok(Outcome::Held { .. }) => {
            tracing::info!(chain_id, message_id, peer, hash = %hash, "pulled from peer; held until the message is final");
            Ok(Pulled::Held)
        }
        Err(error) => match refusal(&error) {
            Refusal::Post => {
                tracing::info!(chain_id, message_id, peer, hash = %hash, status = error.status.as_u16(), "pulled record refused for this post: {}", error.message);
                Ok(Pulled::Unvouched)
            }
            Refusal::Role => {
                tracing::warn!(chain_id, message_id, peer, hash = %hash, status = error.status.as_u16(), "pulled record refused as {role:?}: {}", error.message);
                store.peers.refused_as(&digest, role);
                Ok(Pulled::Refused)
            }
            Refusal::Bytes => {
                tracing::warn!(chain_id, message_id, peer, hash = %hash, status = error.status.as_u16(), "pulled record refused: {}", error.message);
                store.peers.refused(&digest, RETRY_MAX);
                Ok(Pulled::Refused)
            }
            Refusal::Moment => {
                tracing::info!(chain_id, message_id, peer, hash = %hash, status = error.status.as_u16(), "pulled record not admitted yet: {}", error.message);
                store.peers.refused(&digest, RETRY_MIN);
                Err(error)
            }
        },
    }
}

/// What the post named says about a hash.
#[derive(Clone, Copy)]
pub(crate) enum Vouch {
    /// Its body, or one of its attachments.
    For(Kind),
    /// The post stands, but neither is nor names the hash.
    NotThis,
    /// Deleted, removed or by a blocked author: it vouches for nothing.
    Nothing,
}

/// Does the chain, or a body this host has, say `digest` belongs to that
/// message, and as what? `Err` when the chain could not be asked.
async fn vouched(
    state: &AppState,
    store: &StoreState,
    chain_id: u64,
    message_id: u64,
    digest: &Digest,
) -> Result<Vouch, ApiError> {
    if matches!(state.verifier, Verifier::AllowAll) {
        // Dev mode has no chain to ask: with peers configured, any hash a
        // reader names is fetched, as a body. Never the case on a real
        // deployment.
        return Ok(Vouch::For(Kind::Bodies));
    }
    let Some(m) = Acceptance::new(state)
        .for_peer(chain_id, message_id)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(Vouch::NotThis);
    };
    // Answering before fetching, rather than refusing what a peer sends,
    // costs no download and leaves the hash free for another post.
    if m.withdrawn || state.policy.blocked(&m.author) {
        return Ok(Vouch::Nothing);
    }
    if m.content_hash == digest.0 {
        return Ok(Vouch::For(Kind::Bodies));
    }
    let Some((_, policy)) = &store.attachments else {
        return Ok(Vouch::NotThis);
    };
    let Some((body, _)) = body_of(store, chain_id, &Digest(m.content_hash)).await? else {
        return Ok(Vouch::NotThis);
    };
    Ok(
        if envelope::references_within(&body, &digest.0, policy.max_per_message) {
            Vouch::For(Kind::Attachments)
        } else {
            Vouch::NotThis
        },
    )
}

impl Fetches {
    /// Fetch at most `cap` bytes, verifying the hash before sharing them,
    /// and whether every peer asked answered a definite 404.
    async fn fetch(&self, digest: &Digest) -> (Answer, bool) {
        let hash = digest.to_hex();
        if self.urls.is_empty() {
            return (None, false);
        }
        let mut missing = true;
        let key = *digest;
        let mut progress = self.progress.get(&key).unwrap_or_default();
        let start = progress.next;

        for offset in 0..self.urls.len() {
            let index = (start + offset) % self.urls.len();
            let peer = &self.urls[index];
            // Advance before waiting: even the whole-fetch timeout must leave
            // the next retry past this peer. Other hashes have their own place.
            progress.next = (index + 1) % self.urls.len();
            self.progress.put(key, progress.clone(), PROGRESS_TTL);
            let resp = match self
                .client
                .get(format!("{peer}/api/content/{hash}"))
                .timeout(PEER_TIMEOUT)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::info!(peer, "peer unreachable: {e}");
                    missing = false;
                    continue;
                }
            };
            if resp.status() != reqwest::StatusCode::OK {
                tracing::debug!(peer, status = %resp.status(), "peer lacks the record");
                missing &= resp.status() == reqwest::StatusCode::NOT_FOUND;
                continue;
            }
            missing = false;
            let Some(bytes) = read_capped(resp, self.cap).await else {
                tracing::warn!(
                    peer,
                    "peer answer over this host's size cap or unreadable; ignored"
                );
                continue;
            };
            if keccak::hash(&[bytes.as_ref()]) != *digest {
                tracing::warn!(
                    peer,
                    "peer returned bytes that do not hash to the record; ignored"
                );
                continue;
            }
            return (Some((bytes.into(), index)), false);
        }
        (None, missing)
    }
}

/// The whole body, or `None` once it grows past `cap` (or cannot be read), so a
/// peer cannot make this host buffer more than it would accept as an upload.
async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Option<Vec<u8>> {
    if resp.content_length().is_some_and(|n| n > cap as u64) {
        return None;
    }
    let mut out = Vec::new();
    while let Some(chunk) = resp.chunk().await.ok()? {
        if out.len() + chunk.len() > cap {
            return None;
        }
        out.extend_from_slice(&chunk);
    }
    Some(out)
}

#[cfg(test)]
mod tests;
