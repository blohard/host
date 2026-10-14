//! In-memory uploads awaiting the host's configured confirmation depth.
//!
//! A record arrives before its message is `confirmations` deep — or before
//! the transaction has even been mined, proved by the transaction hash. It is
//! kept here, in memory, bounded by total bytes, total entries, entries per
//! signer, bytes per signer, and age, and is **never served**: serving would make the host a
//! free short-lived file drop. A watcher promotes confirmed entries into the
//! content stores, which commit periodically, and drops reverted or expired
//! entries. Confirmation depth is a host policy, not protocol finality.
//!
//! Losing this area on restart is acceptable: clients keep their own copy of
//! anything a host has not confirmed and retry.

use crate::{keccak::Digest, store::Kind};
use std::{
    collections::HashMap,
    fmt,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct HoldingConfig {
    /// Total bytes held across all chains.
    pub max_bytes: usize,
    /// Total records held across all chains.
    pub max_entries: usize,
    /// Held records per signer per chain: one post plus its attachments, a few
    /// times over.
    pub max_per_signer: usize,
    /// Held bytes per signer per chain: a maximum-sized post, a 1 MiB body
    /// plus four 2 MiB attachments, with headroom. An upload proved by an
    /// unmined transaction has spent no gas, so without this a signer or two
    /// could fill the whole area for the ttl. Fairness only: more identities
    /// can still fill it.
    pub max_bytes_per_signer: usize,
    /// How long an entry may wait for its message to become accepted.
    pub ttl: Duration,
}

impl Default for HoldingConfig {
    fn default() -> Self {
        Self {
            max_bytes: 64 << 20,
            max_entries: 4096,
            max_per_signer: 16,
            max_bytes_per_signer: 12 << 20,
            ttl: Duration::from_secs(30 * 60),
        }
    }
}

/// A record handed to the holding area.
#[derive(Debug, Clone)]
pub struct Held {
    pub chain_id: u64,
    pub digest: Digest,
    pub bytes: Vec<u8>,
    pub kind: Kind,
    /// The body this record belongs to (itself, for a body).
    pub body_digest: Digest,
    /// The transaction that anchors the body, when known before the receipt.
    pub tx_hash: Option<[u8; 32]>,
    /// The authenticated post author, charged against the per-signer cap.
    pub signer: [u8; 20],
    /// Known once the receipt (or the chain at `latest`) names it.
    pub message_id: Option<u64>,
}

/// A held record without its bytes: what the watcher reads on every pass
/// without copying the area. (The body a record belongs to is implied by
/// the query that returned it.)
#[derive(Debug, Clone)]
pub struct HeldMeta {
    pub chain_id: u64,
    pub digest: Digest,
    /// How many bytes the record is.
    pub len: usize,
    pub kind: Kind,
    pub tx_hash: Option<[u8; 32]>,
    pub signer: [u8; 20],
    pub message_id: Option<u64>,
    /// When it arrived.
    pub since: Instant,
    /// Asking for its transaction's receipt.
    pub receipt: Backoff,
    /// The accepted block at which the message was last read and found not
    /// yet accepted; while the accepted head stays there, reading again
    /// cannot answer differently.
    pub checked_at: Option<u64>,
    /// Asking whether its message is accepted, after a failed lookup.
    pub acceptance: Backoff,
}

/// A fresh transaction is expected to mine within seconds, so its receipt is
/// asked for on every pass this long after the upload arrived.
const RECEIPT_GRACE: Duration = Duration::from_secs(10);
/// Receipt misses after their grace and failed acceptance reads share a
/// six-second delay doubled per attempt, capped at one minute.
fn retry_delay(misses: u32) -> Duration {
    Duration::from_secs(6)
        .saturating_mul(1 << misses.saturating_sub(1).min(8))
        .min(Duration::from_secs(60))
}

/// A lookup that keeps failing waits longer each time: how often it has
/// missed, and when it may be tried again.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    misses: u32,
    not_before: Instant,
}

impl Backoff {
    fn new(now: Instant) -> Self {
        Self {
            misses: 0,
            not_before: now,
        }
    }

    /// Count a miss at `now`: the next try waits `retry_delay`, or not at all
    /// when `wait` is false.
    fn missed(&mut self, now: Instant, wait: bool) {
        self.misses += 1;
        self.not_before = now
            + if wait {
                retry_delay(self.misses)
            } else {
                Duration::ZERO
            };
    }

    /// Whether the lookup may be tried again at `now`.
    pub fn due(&self, now: Instant) -> bool {
        self.not_before <= now
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum HoldError {
    /// Promotion (or expiry) removed the body while this attachment's
    /// admission was in flight. Retry against the body's current state.
    BodyNotHeld,
    /// The area holds its maximum bytes; the client should retry once its
    /// message is accepted.
    Full { max_bytes: usize },
    /// The area holds its maximum number of records; likewise.
    TooManyEntries { max_entries: usize },
    /// This signer already has as many held records as allowed.
    TooManyForSigner { max: usize },
    /// This signer already has as many held bytes as allowed.
    TooManyBytesForSigner { max_bytes: usize },
}

impl fmt::Display for HoldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BodyNotHeld => write!(f, "the body is no longer held; retry the upload"),
            Self::Full { max_bytes } => write!(
                f,
                "the host is holding its maximum of {max_bytes} unconfirmed bytes; retry once the message is final"
            ),
            Self::TooManyEntries { max_entries } => write!(
                f,
                "the host is holding its maximum of {max_entries} unconfirmed records; retry once the message is final"
            ),
            Self::TooManyForSigner { max } => write!(
                f,
                "this signer already has {max} unconfirmed records held; wait for them to be confirmed"
            ),
            Self::TooManyBytesForSigner { max_bytes } => write!(
                f,
                "this signer already has {max_bytes} unconfirmed bytes held; wait for them to be confirmed"
            ),
        }
    }
}

struct Entry {
    held: Held,
    since: Instant,
    /// The receipt and acceptance lookups' misses, which live and die with
    /// the entry.
    receipt: Backoff,
    checked_at: Option<u64>,
    acceptance: Backoff,
}

impl Entry {
    fn meta(&self) -> HeldMeta {
        let h = &self.held;
        HeldMeta {
            chain_id: h.chain_id,
            digest: h.digest,
            len: h.bytes.len(),
            kind: h.kind,
            tx_hash: h.tx_hash,
            signer: h.signer,
            message_id: h.message_id,
            since: self.since,
            receipt: self.receipt,
            checked_at: self.checked_at,
            acceptance: self.acceptance,
        }
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<(u64, Digest), Entry>,
    bytes: usize,
}

pub struct Holding {
    cfg: HoldingConfig,
    inner: Mutex<Inner>,
}

impl Holding {
    pub fn new(cfg: HoldingConfig) -> Self {
        Self {
            cfg,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn config(&self) -> &HoldingConfig {
        &self.cfg
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock leaves plain data behind; keep serving.
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Hold a record, timestamped now. Idempotent by (chain, digest): a
    /// duplicate returns `Ok(false)` without counting against any cap.
    pub fn insert(&self, mut held: Held) -> Result<bool, HoldError> {
        // The caps count length, so nothing is kept with spare capacity, as
        // an upload's buffer can have. A boxed slice is exactly its length.
        held.bytes = std::mem::take(&mut held.bytes)
            .into_boxed_slice()
            .into_vec();
        let mut g = self.lock();
        let key = (held.chain_id, held.digest);
        if g.entries.contains_key(&key) {
            return Ok(false);
        }
        if held.kind == Kind::Attachments
            && !g
                .entries
                .get(&(held.chain_id, held.body_digest))
                .is_some_and(|entry| entry.held.kind == Kind::Bodies)
        {
            return Err(HoldError::BodyNotHeld);
        }
        if g.bytes + held.bytes.len() > self.cfg.max_bytes {
            return Err(HoldError::Full {
                max_bytes: self.cfg.max_bytes,
            });
        }
        if g.entries.len() >= self.cfg.max_entries {
            return Err(HoldError::TooManyEntries {
                max_entries: self.cfg.max_entries,
            });
        }
        // Records and bytes per signer are checked together, under the one
        // lock, so a signer cannot slip past either cap between two uploads.
        let (by_signer, signer_bytes) = g
            .entries
            .values()
            .filter(|e| e.held.chain_id == held.chain_id && e.held.signer == held.signer)
            .fold((0usize, 0usize), |(n, bytes), e| {
                (n + 1, bytes + e.held.bytes.len())
            });
        if by_signer >= self.cfg.max_per_signer {
            return Err(HoldError::TooManyForSigner {
                max: self.cfg.max_per_signer,
            });
        }
        if signer_bytes + held.bytes.len() > self.cfg.max_bytes_per_signer {
            return Err(HoldError::TooManyBytesForSigner {
                max_bytes: self.cfg.max_bytes_per_signer,
            });
        }
        g.bytes += held.bytes.len();
        g.entries.insert(
            key,
            Entry {
                held,
                since: Instant::now(),
                receipt: Backoff::new(Instant::now()),
                checked_at: None,
                acceptance: Backoff::new(Instant::now()),
            },
        );
        Ok(true)
    }

    /// A copy of one record, bytes included.
    pub fn get(&self, chain_id: u64, digest: &Digest) -> Option<Held> {
        self.lock()
            .entries
            .get(&(chain_id, *digest))
            .map(|e| e.held.clone())
    }

    /// Take one record out, bytes included.
    pub fn remove(&self, chain_id: u64, digest: &Digest) -> Option<Held> {
        let mut g = self.lock();
        let entry = g.entries.remove(&(chain_id, *digest))?;
        g.bytes -= entry.held.bytes.len();
        Some(entry.held)
    }

    /// Finish promotion only after every attachment has left holding. The
    /// check and removal share insert's lock: an attachment arriving late
    /// either keeps its body here for the next pass or is refused for retry.
    /// No I/O or cancellable work happens inside this handoff.
    pub fn finish_promotion(&self, chain_id: u64, digest: &Digest) -> bool {
        let mut g = self.lock();
        if g.entries.values().any(|entry| {
            entry.held.chain_id == chain_id
                && entry.held.kind == Kind::Attachments
                && entry.held.body_digest == *digest
        }) {
            return false;
        }
        if let Some(entry) = g.entries.remove(&(chain_id, *digest)) {
            g.bytes -= entry.held.bytes.len();
        }
        true
    }

    /// Every record of a body (the body and its attachments) learns the id.
    pub fn set_message_id(&self, chain_id: u64, body_digest: &Digest, message_id: u64) {
        let mut g = self.lock();
        for e in g.entries.values_mut() {
            if e.held.chain_id == chain_id && e.held.body_digest == *body_digest {
                e.held.message_id = Some(message_id);
            }
        }
    }

    /// The receipt for this body's transaction was asked for at `now` and was
    /// not there. Within the grace after arrival it is asked for again on the
    /// next pass; after that each miss doubles the wait, up to a minute, since
    /// a transaction still pending by then is underpriced or stuck and a host
    /// full of them must not spend a call on each every few seconds.
    pub fn defer_receipt(&self, chain_id: u64, digest: &Digest, now: Instant) {
        let mut g = self.lock();
        let Some(e) = g.entries.get_mut(&(chain_id, *digest)) else {
            return;
        };
        let within_grace = now.saturating_duration_since(e.since) < RECEIPT_GRACE;
        e.receipt.missed(now, !within_grace);
    }

    /// The message was read at accepted block `block` and found not yet
    /// accepted. It is not read again until the accepted head moves.
    pub fn set_checked_at(&self, chain_id: u64, digest: &Digest, block: u64) {
        let mut g = self.lock();
        if let Some(e) = g.entries.get_mut(&(chain_id, *digest)) {
            e.checked_at = Some(block);
        }
    }

    /// Asking whether the message is accepted failed at `now`: busy or
    /// unreachable upstream. Nothing is recorded about the block, and the
    /// entry waits before it is asked about again.
    pub fn defer_acceptance(&self, chain_id: u64, digest: &Digest, now: Instant) {
        let mut g = self.lock();
        let Some(e) = g.entries.get_mut(&(chain_id, *digest)) else {
            return;
        };
        e.acceptance.missed(now, true);
    }

    /// Bodies first, then attachments — the order promotion wants.
    pub fn snapshot(&self) -> Vec<HeldMeta> {
        let g = self.lock();
        let mut v: Vec<HeldMeta> = g.entries.values().map(Entry::meta).collect();
        v.sort_by_key(|h| (h.kind != Kind::Bodies, h.since));
        v
    }

    /// Attachments held for `body_digest` on `chain_id`.
    pub fn attachments_of(&self, chain_id: u64, body_digest: &Digest) -> Vec<HeldMeta> {
        let g = self.lock();
        g.entries
            .values()
            .filter(|e| {
                e.held.chain_id == chain_id
                    && e.held.kind == Kind::Attachments
                    && e.held.body_digest == *body_digest
            })
            .map(Entry::meta)
            .collect()
    }

    /// Remove everything older than the ttl and return what it was.
    pub fn expire(&self, now: Instant) -> Vec<HeldMeta> {
        let mut g = self.lock();
        let ttl = self.cfg.ttl;
        let expired: Vec<(u64, Digest)> = g
            .entries
            .iter()
            .filter(|(_, e)| now.duration_since(e.since) > ttl)
            .map(|(k, _)| *k)
            .collect();
        let mut out = Vec::new();
        for k in expired {
            if let Some(e) = g.entries.remove(&k) {
                g.bytes -= e.held.bytes.len();
                out.push(e.meta());
            }
        }
        out
    }

    /// (entries, bytes) currently held.
    pub fn stats(&self) -> (usize, usize) {
        let g = self.lock();
        (g.entries.len(), g.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keccak;

    fn held(chain: u64, bytes: &[u8], signer: u8) -> Held {
        let digest = keccak::hash(&[bytes]);
        Held {
            chain_id: chain,
            digest,
            bytes: bytes.to_vec(),
            kind: Kind::Bodies,
            body_digest: digest,
            tx_hash: None,
            signer: [signer; 20],
            message_id: None,
        }
    }

    /// The area's caps count length, so it keeps no spare capacity: a record
    /// arriving in a larger allocation, as an upload's buffer can be, is kept
    /// in one of exactly its length.
    #[test]
    fn a_held_record_keeps_no_spare_capacity() {
        let h = Holding::new(HoldingConfig::default());
        let mut record = held(1, b"a short body in a roomy buffer", 1);
        let mut roomy = Vec::with_capacity(8192);
        roomy.extend_from_slice(&record.bytes);
        record.bytes = roomy;
        let (len, digest) = (record.bytes.len(), record.digest);
        h.insert(record).unwrap();
        let g = h.lock();
        let kept = &g.entries[&(1, digest)].held.bytes;
        assert_eq!((kept.len(), kept.capacity()), (len, len));
        assert_eq!(g.bytes, len);
    }

    #[test]
    fn promotion_and_late_attachments_have_one_handoff() {
        let h = Holding::new(HoldingConfig::default());
        let body = held(1, b"body", 1);
        let hash = body.digest;
        let attachment = |bytes: &[u8]| Held {
            kind: Kind::Attachments,
            body_digest: hash,
            ..held(1, bytes, 1)
        };
        h.insert(body).unwrap();
        let first = attachment(b"first");
        h.insert(first.clone()).unwrap();
        let captured = h.attachments_of(1, &hash);
        assert_eq!(captured.len(), 1);
        // A second upload lands while promotion writes its snapshot. Even
        // if that pass is cancelled after the first write, both the body
        // and the unprocessed attachment remain available to the next pass.
        let late = attachment(b"late");
        h.insert(late.clone()).unwrap();
        h.remove(1, &first.digest);
        assert!(!h.finish_promotion(1, &hash));
        assert!(h.get(1, &hash).is_some());
        assert_eq!(h.attachments_of(1, &hash)[0].digest, late.digest);
        h.remove(1, &late.digest);
        assert!(h.finish_promotion(1, &hash));
        assert_eq!(h.stats(), (0, 0));
        // Opposite ordering: completion wins before the upload's insert.
        assert_eq!(h.insert(attachment(b"later")), Err(HoldError::BodyNotHeld));
        assert_eq!(h.stats(), (0, 0), "a refused upload consumes no capacity");
    }

    #[test]
    fn caps_and_idempotence() {
        let h = Holding::new(HoldingConfig {
            max_bytes: 100,
            max_entries: 5,
            max_per_signer: 2,
            max_bytes_per_signer: 100,
            ttl: Duration::from_secs(60),
        });
        assert_eq!(h.insert(held(1, b"aaaa", 1)), Ok(true));
        assert_eq!(
            h.insert(held(1, b"aaaa", 1)),
            Ok(false),
            "duplicate is a no-op"
        );
        assert_eq!(h.insert(held(1, b"bbbb", 1)), Ok(true));
        assert_eq!(
            h.insert(held(1, b"cccc", 1)),
            Err(HoldError::TooManyForSigner { max: 2 })
        );
        // Another signer, or another chain, is counted separately.
        assert_eq!(h.insert(held(1, b"cccc", 2)), Ok(true));
        assert_eq!(h.insert(held(2, b"dddd", 1)), Ok(true));
        // Total bytes.
        assert_eq!(
            h.insert(held(3, &[0u8; 90], 9)),
            Err(HoldError::Full { max_bytes: 100 })
        );
        assert_eq!(h.stats(), (4, 16));
        // Total entries.
        assert_eq!(h.insert(held(3, b"e", 3)), Ok(true));
        assert_eq!(
            h.insert(held(3, b"f", 4)),
            Err(HoldError::TooManyEntries { max_entries: 5 })
        );
        assert_eq!(
            h.insert(held(3, b"e", 3)),
            Ok(false),
            "a duplicate is still a no-op when full"
        );
        // Remove frees bytes and a slot; the bytes come back with the record.
        let gone = h.remove(1, &keccak::hash(&[b"aaaa"])).unwrap();
        assert_eq!(gone.bytes, b"aaaa");
        assert_eq!(h.stats(), (4, 13));
        assert!(h.remove(1, &keccak::hash(&[b"aaaa"])).is_none());
        assert_eq!(h.insert(held(3, b"f", 4)), Ok(true));
    }

    #[test]
    fn bytes_per_signer_are_capped_per_chain_and_checked_with_the_record_cap() {
        let h = Holding::new(HoldingConfig {
            max_bytes: 1000,
            max_entries: 50,
            max_per_signer: 3,
            max_bytes_per_signer: 10,
            ttl: Duration::from_secs(60),
        });
        assert_eq!(h.insert(held(1, b"aaaaaa", 1)), Ok(true));
        assert_eq!(
            h.insert(held(1, b"bbbbb", 1)),
            Err(HoldError::TooManyBytesForSigner { max_bytes: 10 }),
            "six held plus five would exceed ten"
        );
        assert_eq!(
            h.insert(held(1, b"cccc", 1)),
            Ok(true),
            "exactly ten is allowed"
        );
        assert_eq!(
            h.insert(held(1, b"d", 1)),
            Err(HoldError::TooManyBytesForSigner { max_bytes: 10 })
        );
        assert_eq!(
            h.insert(held(1, b"bbbbb", 2)),
            Ok(true),
            "another signer has its own budget"
        );
        assert_eq!(
            h.insert(held(2, b"bbbbb", 1)),
            Ok(true),
            "the same signer on another chain too"
        );
        // The record cap still applies first when both would be exceeded.
        let tight = Holding::new(HoldingConfig {
            max_bytes: 1000,
            max_entries: 50,
            max_per_signer: 1,
            max_bytes_per_signer: 1,
            ttl: Duration::from_secs(60),
        });
        assert_eq!(tight.insert(held(1, b"a", 1)), Ok(true));
        assert_eq!(
            tight.insert(held(1, b"b", 1)),
            Err(HoldError::TooManyForSigner { max: 1 })
        );
    }

    #[test]
    fn expiry_and_message_ids() {
        let h = Holding::new(HoldingConfig {
            ttl: Duration::from_secs(10),
            ..HoldingConfig::default()
        });
        // `since` is stamped on insert; nothing is old yet, and eleven
        // seconds on everything held so far is.
        h.insert(held(1, b"old", 1)).unwrap();
        assert!(h.expire(Instant::now()).is_empty());
        let old_digest = keccak::hash(&[b"old"]);
        assert_eq!(h.snapshot()[0].digest, old_digest);
        assert_eq!(h.snapshot()[0].len, 3);
        let expired = h.expire(Instant::now() + Duration::from_secs(11));
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].digest, old_digest);
        assert_eq!(h.stats(), (0, 0));

        // An attachment learns the id along with its body, and `get` still
        // hands back the bytes.
        h.insert(held(1, b"new", 1)).unwrap();
        let body = keccak::hash(&[b"new"]);
        let mut att = held(1, b"att", 1);
        att.kind = Kind::Attachments;
        att.body_digest = body;
        h.insert(att).unwrap();
        h.set_message_id(1, &body, 42);
        assert!(h.snapshot().iter().all(|x| x.message_id == Some(42)));
        assert_eq!(h.attachments_of(1, &body).len(), 1);
        assert_eq!(h.get(1, &body).unwrap().bytes, b"new");
        assert!(h.get(2, &body).is_none());
        // Bodies sort first.
        assert_eq!(h.snapshot()[0].kind, Kind::Bodies);
    }
}
