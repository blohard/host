//! Reconciliation needs no reliable wakeup from uploads: startup, backfill and
//! periodic sweeps all visit the same bounded work sets, reconstructed by replay.
//! A body that comes into the store is also noted as an arrival, so the next
//! pass takes it up ahead of that round-robin, which can be long when many
//! posts' bodies never come.
//!
//! With peers, the sweep also makes this host a copy: it fetches the
//! attachments each analysed body references, and once per process it walks
//! every message, noting a body lost from the store and the attachments of
//! those it has. Missing records live in RAM only and are fetched in turn,
//! through the host's shared background allowance; the walk after a restart
//! finds whatever is still missing.

use super::{
    Copied, Host, Index,
    model::Update,
    references::{self, QuoteKey, Reference, Resolution, WORK_BATCH},
};
use crate::chain::Hash;
use crate::keccak::Digest;
use crate::store::Kind;
use anyhow::Result;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    ops::Bound::{Excluded, Included, Unbounded},
    sync::{Arc, PoisonError},
    time::Duration,
};

/// Missing records fetched per pass. Every chain's sweep shares the host's
/// background allowance, two of the eight peer fetch slots, so readers keep
/// the rest; each fetch is bounded at thirty seconds.
const COPIES_PER_PASS: usize = 4;
/// Pending records looked at per pass to find ones not backing off.
const COPY_SCAN: usize = 256;
/// Of each pass's analyses and bindings, arrivals take at most this many,
/// fewer than `WORK_BATCH`, so the round-robin gets some of every pass
/// however many arrive.
const ARRIVALS_PER_PASS: usize = 24;
const _: () = assert!(
    ARRIVALS_PER_PASS < WORK_BATCH,
    "the round-robin gets some of every pass"
);
/// Arrivals waiting at once per index; past it, the round-robin finds the
/// rest, as it does after a restart.
const MAX_ARRIVALS: usize = 1024;

/// An index's arrivals: bodies that came into this host's store and that the
/// index still has work for, analysing them or binding their posts. Oldest
/// first, each hash once while it waits, so the same bytes uploaded again
/// neither add an entry nor move one. An arrival a pass is working on still
/// counts as waiting. In memory only. Whoever looks at the index to decide
/// what to do with the queue keeps the index's lock until the queue is
/// updated, always taking the index's lock first, so no post can be added
/// between the decision and the update.
#[derive(Default)]
pub(crate) struct Arrivals {
    queue: VecDeque<Hash>,
    /// Every arrival waiting, in the queue or taken by a pass.
    waiting: BTreeSet<Hash>,
}

impl Index {
    /// Note that `hash`'s bytes are in this host's store, if this index still
    /// has work for them.
    pub fn body_arrived(&self, hash: Hash) {
        let view = self.view.read().expect("index lock");
        let refs = &view.model.references;
        let waiting = refs.pending_bodies.contains(&hash) || refs.ready_bodies.contains(&hash);
        let mut arrivals = self.arrivals.lock().unwrap_or_else(PoisonError::into_inner);
        if waiting && arrivals.waiting.len() < MAX_ARRIVALS && arrivals.waiting.insert(hash) {
            arrivals.queue.push_back(hash);
        }
    }

    /// The oldest arrivals, at most `limit`, for one pass. They still count
    /// as waiting until [`Self::return_arrivals`].
    pub(super) fn take_arrivals(&self, limit: usize) -> Vec<Hash> {
        let mut arrivals = self.arrivals.lock().unwrap_or_else(PoisonError::into_inner);
        let count = limit.min(arrivals.queue.len());
        arrivals.queue.drain(..count).collect()
    }

    /// Arrivals back from a pass. One whose posts are not all bound goes to
    /// the end of the queue, keeping its priority; the rest leave. One still
    /// waiting to be analysed leaves too, for the round-robin to try in its
    /// turn, so a body that cannot be read holds no place.
    pub(super) fn return_arrivals(&self, taken: Vec<Hash>) {
        // The index's lock first, held until the queue is updated, so no post
        // reusing a finished body can be indexed in between.
        let view = self.view.read().expect("index lock");
        let ready = &view.model.references.ready_bodies;
        let mut arrivals = self.arrivals.lock().unwrap_or_else(PoisonError::into_inner);
        for hash in taken {
            if ready.contains(&hash) {
                arrivals.queue.push_back(hash);
            } else {
                arrivals.waiting.remove(&hash);
            }
        }
    }

    #[cfg(test)]
    pub fn arrivals_for_test(&self) -> Vec<Hash> {
        let arrivals = self.arrivals.lock().unwrap();
        arrivals.queue.iter().copied().collect()
    }
}

/// A body is in this host's store, newly or again. The same bytes can back
/// posts on several chains, so every chain's index waiting on them takes
/// them up first.
pub fn arrived(indexes: &HashMap<u64, Index>, hash: Hash) {
    for index in indexes.values() {
        index.body_arrived(hash);
    }
}

/// Records remembered as missing. Posts cost gas and each body names at most
/// the store's per-message limit, so this is far above honest use; past it,
/// new ones wait for the walk after a restart.
const MAX_PENDING: usize = 100_000;

#[derive(Default)]
pub struct Sweep {
    body_after: Option<Hash>,
    binding_after: Option<Hash>,
    quote_after: Option<QuoteKey>,
    /// The repair walk's position among message ids, until it has seen them all.
    walk_after: Option<u64>,
    walked: bool,
    /// Go round once more when the walk under way ends: a record was let go
    /// since it began, and posts it has passed may still need it.
    rewalk_requested: bool,
    /// Missing records, each with a message that vouches for it.
    missing: BTreeMap<[u8; 32], (u64, Kind)>,
    missing_after: Option<[u8; 32]>,
}

impl Sweep {
    /// How many attachments are still to be fetched.
    #[cfg(test)]
    pub fn pending_attachments(&self) -> usize {
        self.missing
            .values()
            .filter(|(_, r)| *r == Kind::Attachments)
            .count()
    }

    /// How many lost bodies are still to be fetched.
    #[cfg(test)]
    pub fn pending_repairs(&self) -> usize {
        self.missing
            .values()
            .filter(|(_, r)| *r == Kind::Bodies)
            .count()
    }

    /// Remember a missing record and the message that vouches for it.
    fn note(&mut self, chain: u64, id: u64, digest: [u8; 32], kind: Kind) {
        if self.missing.len() >= MAX_PENDING {
            tracing::warn!(
                chain,
                "too many records missing; the rest wait for the next walk"
            );
            return;
        }
        self.missing.entry(digest).or_insert((id, kind));
    }

    /// Remember the attachments `body` references, for message `id`.
    fn note_attachments(&mut self, chain: u64, id: u64, body: &[u8], limit: usize) {
        for digest in crate::envelope::attachments(body, limit) {
            self.note(chain, id, digest, Kind::Attachments);
        }
    }
}

/// A body to analyse this pass, and the post asked to vouch for it.
struct Pick {
    hash: Hash,
    id: u64,
    vouched: bool,
    arrived: bool,
}

/// Whether a post can vouch for copying its records: not deleted or removed,
/// and not by an author this host blocks.
fn vouches(host: &impl Host, message: &super::types::Message) -> bool {
    message.stands_alone() && !host.blocked(&message.author.0)
}

/// The quoted post, once it is in its chain's index here and that index has
/// caught up.
fn resolution(host: &impl Host, key: QuoteKey) -> Option<Resolution> {
    let target = host.indexes()?.get(&key.quote.chain)?;
    let target = target.view.read().expect("index lock");
    if !target.state().is_ready() {
        return None;
    }
    let message = target.model.messages.get(&key.quote.id)?.clone();
    Some(Resolution {
        key,
        deployment: target.model.deployment.clone(),
        at: target.model.at.clone(),
        message,
    })
}

/// Try the quotes of messages just bound, at once: a quote of a post this
/// host already has is resolved in the pass that binds it, rather than
/// waiting its turn behind quotes that may never resolve. Those it cannot
/// resolve yet stay queued for the round-robin. A full batch of bindings can
/// bring eight quotes each, so they are applied in updates of `WORK_BATCH`.
async fn resolve_quotes_of(host: &impl Host, index: &Index, ids: &[u64]) -> Result<bool> {
    let keys = {
        let view = index.view.read().expect("index lock");
        let refs = &view.model.references;
        let mut keys = Vec::new();
        for id in ids {
            let Some(analysis) = view
                .model
                .messages
                .get(id)
                .and_then(|m| refs.analyses.get(&m.content_hash))
            else {
                continue;
            };
            for reference in &analysis.references {
                if let Reference::Quote { target } = reference {
                    let key = QuoteKey {
                        source: *id,
                        quote: *target,
                    };
                    if refs.pending_quotes.contains(&key) {
                        keys.push(key);
                    }
                }
            }
        }
        keys
    };
    let resolutions = keys
        .into_iter()
        .filter_map(|key| resolution(host, key))
        .collect::<Vec<_>>();
    for batch in resolutions.chunks(WORK_BATCH) {
        index.update(Update::Resolve(batch.to_vec())).await?;
    }
    Ok(!resolutions.is_empty())
}

/// Fairness even when the first missing body or target never becomes available.
fn next<T: Ord + Clone>(set: &BTreeSet<T>, after: Option<&T>) -> Vec<T> {
    match after {
        None => set.iter().take(WORK_BATCH).cloned().collect(),
        Some(after) => set
            .range((Excluded(after), Unbounded))
            .chain(set.range((Unbounded, Included(after))))
            .take(WORK_BATCH)
            .cloned()
            .collect(),
    }
}

impl Sweep {
    /// One pass of each job in turn: analyse the bodies the index is waiting
    /// for, bind analysed bodies to their messages and try their quotes at
    /// once, resolve other quotes, then copy missing records from peers.
    /// Arrivals go first, each in this one pass. True when the index changed,
    /// so the next pass runs at once.
    pub async fn run(&mut self, host: &impl Host, chain: u64, index: &Index) -> Result<bool> {
        if !index.state().is_established() {
            return Ok(false);
        }
        let arrived = index.take_arrivals(ARRIVALS_PER_PASS);
        let pass = self.pass(host, chain, index, &arrived).await;
        // Back whatever the pass did, even if it failed partway.
        index.return_arrivals(arrived);
        pass
    }

    /// One pass's work; [`Self::run`] returns its arrivals afterwards.
    async fn pass(
        &mut self,
        host: &impl Host,
        chain: u64,
        index: &Index,
        arrived: &[Hash],
    ) -> Result<bool> {
        let analysed = self.analyse_bodies(host, chain, index, arrived).await?;
        let bound = self.bind_bodies(index, arrived).await?;
        let resolved_new = resolve_quotes_of(host, index, &bound).await?;
        let resolved = self.resolve_quotes(host, index).await?;
        // Repair work changes no index state, so it never counts as progress:
        // it moves a batch per pass, at the sweep's own pace. Without peers
        // there is nothing to copy from.
        if host.has_bodies() && host.has_peers() {
            self.walk(host, chain, index).await;
            self.fetch_missing(host, chain).await;
        }
        Ok(analysed || !bound.is_empty() || resolved_new || resolved)
    }

    /// The bodies one pass analyses: arrivals still pending first, then the
    /// round-robin's, which moves past its own work only. Each comes with the
    /// post asked to vouch for it: posts sharing a body vouch for it together,
    /// so one that can speaks for it. A deleted or removed post, or a blocked
    /// author's, fetches nothing, and only lets bytes already here be
    /// analysed.
    fn pick_bodies(&mut self, host: &impl Host, index: &Index, arrived: &[Hash]) -> Vec<Pick> {
        let view = index.view.read().expect("index lock");
        let refs = &view.model.references;
        let first = arrived
            .iter()
            .filter(|hash| refs.pending_bodies.contains(hash))
            .copied()
            .collect::<Vec<_>>();
        let rest = next(&refs.pending_bodies, self.body_after.as_ref())
            .into_iter()
            .filter(|hash| !first.contains(hash))
            .take(WORK_BATCH - first.len())
            .collect::<Vec<_>>();
        if let Some(last) = rest.last() {
            self.body_after = Some(*last);
        }
        let arrivals = first.len();
        first
            .into_iter()
            .chain(rest)
            .enumerate()
            .map(|(n, hash)| {
                let posts = &refs.unbound[&hash];
                let live = posts
                    .iter()
                    .find(|id| vouches(host, &view.model.messages[id]))
                    .copied();
                Pick {
                    hash,
                    id: live.unwrap_or(*posts.first().expect("pending source")),
                    vouched: live.is_some(),
                    arrived: n < arrivals,
                }
            })
            .collect()
    }

    /// Analyse a batch of the bodies the index is waiting for: arrivals first,
    /// read from this host's store, then the round-robin's, each read from the
    /// store or, within the background allowance, from a peer. The attachments
    /// an analysed body references are noted for copying.
    async fn analyse_bodies(
        &mut self,
        host: &impl Host,
        chain: u64,
        index: &Index,
        arrived: &[Hash],
    ) -> Result<bool> {
        if !host.has_bodies() {
            return Ok(false);
        }
        let picks = self.pick_bodies(host, index, arrived);
        let mut analyses = Vec::new();
        for Pick {
            hash,
            id,
            vouched,
            arrived,
        } in picks
        {
            let digest = Digest(hash.0);
            let analysis = async {
                let bytes = if arrived {
                    // In the store already: no waiting on a peer.
                    host.stored_body(digest).await?
                } else {
                    // Local first; from a peer only within the host's background allowance.
                    match host.copy(chain, id, digest, Kind::Bodies).await? {
                        Copied::Stored(Some(bytes)) => Some(bytes),
                        _ => None,
                    }
                };
                let Some(bytes) = bytes else {
                    return Ok(None);
                };
                references::analyse(hash, &bytes).map(|a| Some((a, bytes)))
            }
            .await;
            match analysis {
                Ok(Some((analysis, bytes))) => {
                    // Without peers nothing could fetch them, and nothing is
                    // fetched for a body only deleted or removed posts carry.
                    if vouched
                        && host.has_peers()
                        && let Some(limit) = host.attachment_limit()
                    {
                        self.note_attachments(chain, id, &bytes, limit);
                    }
                    analyses.push(analysis);
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(chain, ?hash, ?error, "body analysis will retry"),
            }
        }
        if analyses.is_empty() {
            return Ok(false);
        }
        index.update(Update::Analyze(analyses)).await?;
        Ok(true)
    }

    /// Apply analysed bodies to a batch of the messages that carry them: each
    /// message's mentions are recorded and its quotes queued for resolving.
    /// Posts of arrivals go first, one from each in turn, up to their share.
    /// Returns the messages bound.
    async fn bind_bodies(&mut self, index: &Index, arrived: &[Hash]) -> Result<Vec<u64>> {
        let ids = {
            let view = index.view.read().expect("index lock");
            let refs = &view.model.references;
            let mut ids = Vec::new();
            let mut turns = arrived
                .iter()
                .filter(|hash| refs.ready_bodies.contains(hash))
                .map(|hash| refs.unbound[hash].iter())
                .collect::<Vec<_>>();
            while ids.len() < ARRIVALS_PER_PASS && !turns.is_empty() {
                turns.retain_mut(|posts| {
                    if ids.len() == ARRIVALS_PER_PASS {
                        return true;
                    }
                    posts.next().map(|id| ids.push(*id)).is_some()
                });
            }
            let from_arrivals = ids.len();
            for hash in next(&refs.ready_bodies, self.binding_after.as_ref()) {
                let more = refs.unbound[&hash]
                    .iter()
                    .filter(|id| !ids[..from_arrivals].contains(id))
                    .take(WORK_BATCH - ids.len())
                    .copied()
                    .collect::<Vec<_>>();
                ids.extend(more);
                self.binding_after = Some(hash);
                if ids.len() == WORK_BATCH {
                    break;
                }
            }
            ids
        };
        if ids.is_empty() {
            return Ok(ids);
        }
        index.update(Update::Bind(ids.clone())).await?;
        Ok(ids)
    }

    /// Resolve a batch of queued quotes, each once the quoted post is in its
    /// chain's index here and that index has caught up.
    async fn resolve_quotes(&mut self, host: &impl Host, index: &Index) -> Result<bool> {
        let pending = {
            let view = index.view.read().expect("index lock");
            next(
                &view.model.references.pending_quotes,
                self.quote_after.as_ref(),
            )
        };
        let mut resolutions = Vec::new();
        for key in pending {
            self.quote_after = Some(key.clone());
            resolutions.extend(resolution(host, key));
        }
        if resolutions.is_empty() {
            return Ok(false);
        }
        index.update(Update::Resolve(resolutions)).await?;
        Ok(true)
    }

    /// One batch of the repair walk, from this host's store only: a body the
    /// index has analysed but the store has lost is noted for copying, and
    /// the attachments of each stored body are noted too.
    async fn walk(&mut self, host: &impl Host, chain: u64, index: &Index) {
        if self.walked {
            return;
        }
        let batch = {
            let view = index.view.read().expect("index lock");
            let after = self.walk_after.map_or(Unbounded, Excluded);
            let refs = &view.model.references;
            view.model
                .messages
                .range((after, Unbounded))
                .take(WORK_BATCH)
                // An unanalysed body is the analysis loop's to fetch.
                .map(|(&id, m)| {
                    (
                        id,
                        m.content_hash,
                        !refs.pending_bodies.contains(&m.content_hash),
                        vouches(host, m),
                    )
                })
                .collect::<Vec<_>>()
        };
        let last = batch.len() < WORK_BATCH;
        let limit = host.attachment_limit();
        for (id, hash, analysed, vouched) in batch {
            // A post that cannot vouch notes nothing; one that can, with the
            // same bytes, notes them itself.
            if vouched {
                match host.stored_body(Digest(hash.0)).await {
                    Ok(Some(bytes)) => {
                        if let Some(limit) = limit {
                            self.note_attachments(chain, id, &bytes, limit);
                        }
                    }
                    Ok(None) if analysed => self.note(chain, id, hash.0, Kind::Bodies),
                    Ok(None) => {}
                    Err(error) => {
                        // Try this message again on the next pass.
                        tracing::warn!(chain, id, ?error, "body walk will retry");
                        return;
                    }
                }
            }
            self.walk_after = Some(id);
        }
        if last {
            if self.rewalk_requested {
                self.rewalk_requested = false;
                self.walk_after = None;
            } else {
                self.walked = true;
            }
        }
    }

    /// A record was let go because its post can no longer vouch for it. A
    /// walk that starts after now visits every post, so any other post still
    /// needing it notes it again: one now if none is under way, otherwise one
    /// more when the current walk ends.
    fn walk_again(&mut self) {
        if self.walked {
            self.walked = false;
            self.walk_after = None;
        } else {
            self.rewalk_requested = true;
        }
    }

    /// Copy a few missing records, taking turns so that one no peer has never
    /// holds up the rest, and passing over those still backing off.
    async fn fetch_missing(&mut self, host: &impl Host, chain: u64) {
        if self.missing.is_empty() {
            return;
        }
        // Past the last one looked at, then around from the start, each once.
        let around: Vec<([u8; 32], (u64, Kind))> = match self.missing_after {
            None => self
                .missing
                .iter()
                .take(COPY_SCAN)
                .map(|(&d, &v)| (d, v))
                .collect(),
            Some(after) => self
                .missing
                .range::<[u8; 32], _>((Excluded(after), Unbounded))
                .chain(
                    self.missing
                        .range::<[u8; 32], _>((Unbounded, Included(after))),
                )
                .take(COPY_SCAN)
                .map(|(&d, &v)| (d, v))
                .collect(),
        };
        let mut batch = Vec::new();
        for (digest, entry) in around {
            self.missing_after = Some(digest);
            if !host.peer_waiting(&Digest(digest), entry.1) {
                batch.push((digest, entry));
                if batch.len() == COPIES_PER_PASS {
                    break;
                }
            }
        }
        // The host's background allowance paces these, across every chain.
        let results = futures::future::join_all(
            batch
                .iter()
                .map(|&(digest, (id, kind))| host.copy(chain, id, Digest(digest), kind)),
        )
        .await;
        let limit = host.attachment_limit();
        for ((digest, (id, kind)), result) in batch.into_iter().zip(results) {
            match result {
                Ok(Copied::Stored(body)) => {
                    self.missing.remove(&digest);
                    if let (Some(body), Some(limit)) = (body, limit) {
                        self.note_attachments(chain, id, &body, limit);
                    }
                }
                Ok(Copied::Refused) => {
                    // Kept by peers this host does not agree with; a restart's walk may note it again.
                    tracing::debug!(
                        chain,
                        id,
                        ?kind,
                        "refused by this host's policy; not asked again"
                    );
                    self.missing.remove(&digest);
                }
                Ok(Copied::Unvouched) => {
                    tracing::debug!(
                        chain,
                        id,
                        ?kind,
                        "this post can no longer vouch; walking again for another"
                    );
                    self.missing.remove(&digest);
                    self.walk_again();
                }
                Ok(Copied::Missing) => {}
                Err(error) => tracing::debug!(chain, id, ?kind, ?error, "copy will retry"),
            }
        }
    }
}

pub fn start<H: Host>(host: Arc<H>) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(indexes) = host.indexes() else {
        return Vec::new();
    };
    let mut sweeps = Vec::new();
    for (&chain, index) in indexes {
        let index = index.clone();
        let host = host.clone();
        sweeps.push(tokio::spawn(async move {
            let mut sweep = Sweep::default();
            loop {
                if index.state().is_stopped() {
                    break;
                }
                match sweep.run(&*host, chain, &index).await {
                    Ok(true) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => tracing::warn!(chain, ?error, "body reconciliation incomplete"),
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }));
    }
    sweeps
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A host whose attachment fetches answer from a script, recording each ask.
    struct Scripted {
        verifier: crate::verifier::Verifier,
        available: Mutex<BTreeSet<[u8; 32]>>,
        failing: BTreeSet<[u8; 32]>,
        refused: BTreeSet<[u8; 32]>,
        waiting: BTreeSet<[u8; 32]>,
        /// Posts that can no longer vouch for anything.
        unvouched: BTreeSet<u64>,
        asked: Mutex<Vec<[u8; 32]>>,
    }
    impl Scripted {
        fn new(
            available: impl IntoIterator<Item = u8>,
            failing: impl IntoIterator<Item = u8>,
        ) -> Self {
            Self {
                verifier: crate::verifier::Verifier::AllowAll,
                available: Mutex::new(available.into_iter().map(|b| [b; 32]).collect()),
                failing: failing.into_iter().map(|b| [b; 32]).collect(),
                refused: BTreeSet::new(),
                waiting: BTreeSet::new(),
                unvouched: BTreeSet::new(),
                asked: Mutex::new(Vec::new()),
            }
        }
    }
    impl Host for Scripted {
        fn verifier(&self) -> &crate::verifier::Verifier {
            &self.verifier
        }
        fn indexes(&self) -> Option<&std::collections::HashMap<u64, Index>> {
            None
        }
        fn has_bodies(&self) -> bool {
            true
        }
        fn has_peers(&self) -> bool {
            true
        }
        fn attachment_limit(&self) -> Option<usize> {
            Some(4)
        }
        async fn stored_body(&self, _: Digest) -> Result<Option<bytes::Bytes>> {
            Ok(None)
        }
        fn peer_waiting(&self, digest: &Digest, _: Kind) -> bool {
            self.waiting.contains(&digest.0)
        }
        fn blocked(&self, _: &[u8; 20]) -> bool {
            false
        }
        async fn copy(&self, _: u64, id: u64, digest: Digest, kind: Kind) -> Result<Copied> {
            self.asked.lock().unwrap().push(digest.0);
            anyhow::ensure!(!self.failing.contains(&digest.0), "peer unreachable");
            Ok(if self.unvouched.contains(&id) {
                Copied::Unvouched
            } else if self.refused.contains(&digest.0) {
                Copied::Refused
            } else if self.available.lock().unwrap().contains(&digest.0) {
                // A copied body names one more attachment, [0xab; 32].
                Copied::Stored((kind == Kind::Bodies).then(|| body([0xab]).into()))
            } else {
                Copied::Missing
            })
        }
    }

    fn body(references: impl IntoIterator<Item = u8>) -> Vec<u8> {
        let mut text = String::from("version/1 text/plain\n");
        for b in references {
            text.push_str(&format!("record:0x{}\n", hex::encode([b; 32])));
        }
        text.into_bytes()
    }

    #[test]
    fn a_body_contributes_its_first_distinct_references_up_to_the_limit() {
        let mut sweep = Sweep::default();
        sweep.note_attachments(1, 7, &body([1, 2, 1, 3, 4, 5]), 4);
        assert_eq!(
            sweep.missing.keys().map(|k| k[0]).collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert!(
            sweep
                .missing
                .values()
                .all(|&(id, r)| id == 7 && r == Kind::Attachments)
        );
        // Only text/plain bodies carry references.
        let mut other = Sweep::default();
        other.note_attachments(1, 7, b"version/1 image/png\nrecord:0x00", 4);
        assert_eq!(other.pending_attachments(), 0);
    }

    #[test]
    fn pending_attachments_stay_bounded() {
        let mut sweep = Sweep::default();
        for n in 0..(MAX_PENDING as u32 + 10) {
            let mut digest = [0; 32];
            digest[..4].copy_from_slice(&n.to_be_bytes());
            sweep
                .missing
                .entry(digest)
                .or_insert((1, Kind::Attachments));
            if sweep.missing.len() >= MAX_PENDING {
                break;
            }
        }
        sweep.note_attachments(1, 2, &body([0xee]), 4);
        assert_eq!(sweep.pending_attachments(), MAX_PENDING);
    }

    #[tokio::test]
    async fn missing_attachments_take_turns_and_only_a_fetch_removes_one() {
        // Twenty pending: one no peer has, one whose peer fails, the rest available.
        let host = Scripted::new(2..20, [1]);
        let mut sweep = Sweep::default();
        for b in 0..20 {
            sweep.missing.insert([b; 32], (1, Kind::Attachments));
        }
        for _ in 0..6 {
            let before = host.asked.lock().unwrap().len();
            sweep.fetch_missing(&host, 1).await;
            assert!(host.asked.lock().unwrap().len() - before <= COPIES_PER_PASS);
        }
        assert_eq!(
            sweep.missing.keys().map(|k| k[0]).collect::<Vec<_>>(),
            [0, 1],
            "fetched ones go; the rest wait"
        );
        // The two that remain are still asked about on later passes.
        let before = host.asked.lock().unwrap().len();
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(host.asked.lock().unwrap()[before..].len(), 2);
        host.available.lock().unwrap().insert([0; 32]);
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(sweep.pending_attachments(), 1);
    }

    #[tokio::test]
    async fn a_lost_body_is_copied_in_turn_and_brings_its_attachments() {
        let host = Scripted::new([7], []);
        let mut sweep = Sweep::default();
        sweep.missing.insert([7; 32], (3, Kind::Bodies));
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(sweep.pending_repairs(), 0);
        assert_eq!(
            sweep.missing.get(&[0xab; 32]),
            Some(&(3, Kind::Attachments)),
            "the copied body's attachments are noted for its message"
        );
    }

    /// Four chains' sweeps share the host's background allowance, so a
    /// reader still gets a peer fetch slot while every peer stalls.
    #[tokio::test]
    async fn every_chain_sweep_shares_one_background_allowance() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct PeerHost {
            verifier: crate::verifier::Verifier,
            peers: crate::peers::Peers,
        }
        impl Host for PeerHost {
            fn verifier(&self) -> &crate::verifier::Verifier {
                &self.verifier
            }
            fn indexes(&self) -> Option<&std::collections::HashMap<u64, Index>> {
                None
            }
            fn has_bodies(&self) -> bool {
                true
            }
            fn has_peers(&self) -> bool {
                true
            }
            fn attachment_limit(&self) -> Option<usize> {
                Some(4)
            }
            async fn stored_body(&self, _: Digest) -> Result<Option<bytes::Bytes>> {
                Ok(None)
            }
            fn peer_waiting(&self, _: &Digest, _: Kind) -> bool {
                false
            }
            fn blocked(&self, _: &[u8; 20]) -> bool {
                false
            }
            // As the real host copies: a background turn, then the shared fetch pool.
            async fn copy(&self, _: u64, _: u64, d: Digest, kind: Kind) -> Result<Copied> {
                let _turn = self.peers.background().await;
                self.peers
                    .fetch(&d, kind)
                    .await
                    .map_err(anyhow::Error::msg)?;
                Ok(Copied::Missing)
            }
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in listener.incoming() {
                held.push(stream);
                count.fetch_add(1, Ordering::SeqCst);
            }
        });
        let host = Arc::new(PeerHost {
            verifier: crate::verifier::Verifier::AllowAll,
            peers: crate::peers::Peers::new(vec![url], 1024, reqwest::Client::new()),
        });
        let mut work = Vec::new();
        for chain in 1..=4u8 {
            let host = host.clone();
            work.push(tokio::spawn(async move {
                let mut sweep = Sweep::default();
                for offset in 0..4 {
                    sweep
                        .missing
                        .insert([chain * 10 + offset; 32], (1, Kind::Attachments));
                }
                sweep.fetch_missing(&*host, u64::from(chain)).await;
            }));
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while connections.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            connections.load(Ordering::SeqCst),
            2,
            "background copying holds two connections in all"
        );
        let reader = tokio::time::timeout(
            Duration::from_secs(1),
            host.peers
                .fetch(&Digest([99; 32]), crate::store::Kind::Bodies),
        )
        .await;
        for task in work {
            task.abort();
        }
        // The reader is admitted (it then waits on the stalled peer like everyone else).
        assert!(
            !matches!(reader, Ok(Err(_))),
            "a reader was refused a peer fetch slot: {reader:?}"
        );
    }

    /// The record is let go, and a walk is due: at once when none is under
    /// way, or again from the start when the one under way ends.
    #[tokio::test]
    async fn a_post_that_cannot_vouch_lets_its_record_go_and_walks_again() {
        let mut host = Scripted::new([9], []);
        host.unvouched.insert(1);
        let mut sweep = Sweep {
            walked: true,
            walk_after: Some(40),
            ..Sweep::default()
        };
        sweep.missing.insert([9; 32], (1, Kind::Attachments));
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(sweep.pending_attachments(), 0);
        assert!(
            !sweep.walked && sweep.walk_after.is_none(),
            "walking from the start"
        );
        assert!(!sweep.rewalk_requested);

        sweep.walk_after = Some(12);
        sweep.missing.insert([9; 32], (1, Kind::Attachments));
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(sweep.walk_after, Some(12), "the walk under way goes on");
        assert!(
            sweep.rewalk_requested,
            "and goes round once more when it ends"
        );
    }

    #[tokio::test]
    async fn a_refused_attachment_is_not_asked_for_again() {
        let mut host = Scripted::new([], []);
        host.refused.insert([5; 32]);
        let mut sweep = Sweep::default();
        sweep.missing.insert([5; 32], (1, Kind::Attachments));
        sweep.fetch_missing(&host, 1).await;
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(sweep.pending_attachments(), 0);
        assert_eq!(host.asked.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn attachments_backing_off_are_passed_over_without_using_a_turn() {
        // Ten backing off sort first; the two after them are fetched in the same pass.
        let mut host = Scripted::new([20, 21], []);
        host.waiting = (0..10).map(|b| [b; 32]).collect();
        let mut sweep = Sweep::default();
        for b in (0..10).chain([20, 21]) {
            sweep.missing.insert([b; 32], (1, Kind::Attachments));
        }
        sweep.fetch_missing(&host, 1).await;
        assert_eq!(
            host.asked
                .lock()
                .unwrap()
                .iter()
                .map(|d| d[0])
                .collect::<Vec<_>>(),
            [20, 21]
        );
        assert_eq!(sweep.pending_attachments(), 10);
    }
}
