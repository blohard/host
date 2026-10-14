//! Promote held uploads at the configured confirmation depth; drop reverted
//! and expired entries. Promoted bytes become durable at the next store commit.
//!
//! `main` runs [`promote_once`] every few seconds; the tests call it directly,
//! or [`promote_at`] with a time of their choosing.
//!
//! A held body whose transaction is still pending costs one receipt call per
//! pass, and a host holding thousands would spend its budget on them and keep
//! every other upload waiting behind the answers. So a pending receipt is
//! asked for on every pass only while the upload is fresh; after that the
//! asking backs off per entry, up to a minute, while bodies whose message id
//! is already known are checked against the index or the chain as before.

use crate::abi::hex0x;
use crate::acceptance::Acceptance;
use crate::envelope::references_within;
use crate::holding::HeldMeta;
use crate::keccak::Digest;
use crate::server::{AppState, StoreState};
use crate::store::Kind;
use std::collections::HashSet;
use std::time::Instant;

/// One pass of the watcher: drop expired entries, learn message ids from
/// receipts, and promote every held body (with its attachments) whose message
/// is accepted. Returns how many records were promoted.
pub async fn promote_once(state: &AppState) -> usize {
    promote_at(state, Instant::now()).await
}

/// [`promote_once`] as of `now`: what has expired, and whose receipt is due.
pub async fn promote_at(state: &AppState, now: Instant) -> usize {
    let Some(store) = state.store.as_ref() else {
        return 0; // a gateway holds nothing
    };
    let mut promoted = 0;
    // `now` is the pass's clock; a backoff after a slow lookup is measured
    // from when that lookup finished, not from when the pass began.
    let started = Instant::now();
    // The confirmed head of each chain, looked up once per pass, not once per
    // held upload.
    let mut acceptance = Acceptance::new(state);
    let mut reported = HashSet::new();
    for h in store.holding.expire(now) {
        tracing::warn!(chain = h.chain_id, hash = %h.digest.to_hex(), signer = %hex0x(&h.signer), "held upload expired without a final message; dropped");
    }
    for held in store.holding.snapshot() {
        if state.policy.blocked(&held.signer) {
            drop_body(store, &held, "author blocked by host");
            continue;
        }
        // Discard what is taken down as soon as a pass sees it, whether or not
        // its post is ever accepted, so it holds no slot until expiry: a body
        // goes with its attachments, an attachment alone.
        if state.policy.taken_down(&held.digest.0) {
            if held.kind == Kind::Bodies {
                drop_body(store, &held, "taken down by the host");
            } else {
                drop_attachment(store, held.chain_id, &held.digest, "taken down by the host");
            }
            continue;
        }
        if held.kind != Kind::Bodies {
            continue; // attachments ride with their body
        }
        if held.message_id.is_none() && !held.receipt.due(now) {
            continue; // its receipt was missing recently; asked again later
        }
        let Some(id) = message_id_of(state, store, &held, now).await else {
            continue;
        };
        if !held.acceptance.due(now) {
            continue; // its acceptance lookup failed recently; asked again later
        }
        // On a store-only host each body costs a Board read at the accepted
        // head. A body found unaccepted at this very head cannot answer
        // differently until the head moves, so it is not read again before.
        let block = match acceptance.confirmed(held.chain_id).await {
            Ok(block) => block,
            Err(e) => {
                // A cached head failure affects every held body on the chain;
                // report it once per pass, not once per upload.
                if reported.insert(held.chain_id) {
                    tracing::warn!(chain = held.chain_id, "acceptance lookup failed: {e}");
                }
                continue;
            }
        };
        if block.is_some() && block == held.checked_at {
            continue;
        }
        match acceptance.accepted(held.chain_id, id).await {
            Ok(Some(m)) if m.withdrawn => {
                drop_body(store, &held, "its post has been deleted or removed")
            }
            // A failed store leaves the entry and records nothing, so the
            // next pass promotes again without waiting for a new block.
            Ok(Some(m)) if m.content_hash == held.digest.0 => {
                promoted += promote(state, store, &held, id, now, started).await
            }
            Ok(Some(_)) => drop_body(
                store,
                &held,
                "the accepted message commits to different bytes",
            ),
            Ok(None) => {
                if let Some(block) = block {
                    store
                        .holding
                        .set_checked_at(held.chain_id, &held.digest, block);
                }
            }
            Err(e) => {
                store.holding.defer_acceptance(
                    held.chain_id,
                    &held.digest,
                    now + started.elapsed(),
                );
                if reported.insert(held.chain_id) {
                    tracing::warn!(chain = held.chain_id, "acceptance lookup failed: {e}");
                }
            }
        }
    }
    promoted
}

/// The id of the message a held body names: known already, or learned from
/// its transaction's receipt and remembered. `None` while the transaction is
/// pending or the receipt cannot be read; a body whose transaction reverted
/// or posted nothing is dropped here.
async fn message_id_of(
    state: &AppState,
    store: &StoreState,
    held: &HeldMeta,
    now: Instant,
) -> Option<u64> {
    if let Some(id) = held.message_id {
        return Some(id);
    }
    let tx = held.tx_hash?;
    match state.verifier.receipt(held.chain_id, tx).await {
        Ok(Some(r)) if !r.succeeded => {
            drop_body(store, held, "its transaction reverted");
            None
        }
        Ok(Some(r)) => match r.message_id {
            Some(id) => {
                store
                    .holding
                    .set_message_id(held.chain_id, &held.digest, id);
                Some(id)
            }
            None => {
                drop_body(store, held, "its transaction posted nothing");
                None
            }
        },
        Ok(None) => {
            store
                .holding
                .defer_receipt(held.chain_id, &held.digest, now);
            None // still pending
        }
        Err(e) => {
            tracing::warn!(chain = held.chain_id, "receipt lookup failed: {e}");
            store
                .holding
                .defer_receipt(held.chain_id, &held.digest, now);
            None
        }
    }
}

/// Move one accepted body into the store, then every held attachment its bytes
/// reference, within the store's caps. An entry leaves the holding area only
/// once its bytes are stored or it is deliberately dropped, and the body's
/// entry goes last, so a failed write keeps the whole promotion for a later
/// pass; that pass is backed off like a failed lookup rather than retried
/// every few seconds, measured from when the failed write finished. Returns
/// how many records moved.
async fn promote(
    state: &AppState,
    store: &StoreState,
    held: &HeldMeta,
    id: u64,
    pass: Instant,
    started: Instant,
) -> usize {
    let Some(body) = store.holding.get(held.chain_id, &held.digest) else {
        return 0; // gone meanwhile
    };
    // Checked again as each record is stored, since a takedown may have landed
    // while this pass waited on the chain: nothing taken down is stored.
    if state.policy.taken_down(&held.digest.0) {
        drop_body(store, held, "taken down by the host");
        return 0;
    }
    if let Err(e) = store.bodies.put(body.bytes.clone()).await {
        tracing::error!("storing promoted body failed: {e}");
        store
            .holding
            .defer_acceptance(held.chain_id, &held.digest, pass + started.elapsed());
        return 0;
    }
    // Before its attachments, which may take a while to write.
    state.body_arrived(held.digest);
    let mut moved = 1;
    let mut failed = false;
    for a in store.holding.attachments_of(held.chain_id, &held.digest) {
        let Some(att) = store.holding.get(held.chain_id, &a.digest) else {
            continue;
        };
        if state.policy.taken_down(&a.digest.0) {
            drop_attachment(store, held.chain_id, &a.digest, "taken down by the host");
            continue;
        }
        match &store.attachments {
            Some((target, rules))
                if a.len <= target.max_size()
                    && references_within(&body.bytes, &a.digest.0, rules.max_per_message) =>
            {
                match target.put(att.bytes).await {
                    Ok(_) => {
                        store.holding.remove(held.chain_id, &a.digest);
                        moved += 1;
                    }
                    Err(e) => {
                        tracing::error!("storing promoted attachment failed: {e}");
                        failed = true;
                    }
                }
            }
            _ => drop_attachment(
                store,
                held.chain_id,
                &a.digest,
                "not referenced by its final body",
            ),
        }
    }
    if failed {
        // The body is stored and served; its entry stays so the attachments
        // that failed are tried again, after a pause.
        store
            .holding
            .defer_acceptance(held.chain_id, &held.digest, pass + started.elapsed());
        return moved;
    }
    if store.holding.finish_promotion(held.chain_id, &held.digest) {
        tracing::info!(chain = held.chain_id, message_id = id, hash = %held.digest.to_hex(), "held upload promoted: message is final");
    }
    moved
}

/// Drop one held attachment.
fn drop_attachment(store: &StoreState, chain_id: u64, digest: &Digest, why: &str) {
    store.holding.remove(chain_id, digest);
    tracing::warn!(chain = chain_id, hash = %digest.to_hex(), "held attachment dropped: {why}");
}

/// Drop a held body and everything held for it.
fn drop_body(store: &StoreState, held: &HeldMeta, why: &str) {
    store.holding.remove(held.chain_id, &held.digest);
    for a in store.holding.attachments_of(held.chain_id, &held.digest) {
        store.holding.remove(held.chain_id, &a.digest);
    }
    tracing::warn!(chain = held.chain_id, hash = %held.digest.to_hex(), "held upload dropped: {why}");
}
