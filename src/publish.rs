//! Content admission shared by HTTP uploads and records copied from peers.
//!
//! Each record names a chain and either a message id or the transaction that
//! posts it. The checks identify a body or referenced attachment, verify its
//! anchoring and this host's content limits, then store or hold the bytes.
//! `--allow-all` skips chain verification. [`http`] receives uploads and builds
//! their HTTP responses; [`crate::peers::pull`] applies the same checks to copies.

pub(crate) mod http;

use crate::abi::decode_hex;
use crate::abi::{posted_content_hash, posted_reply_gate};
use crate::acceptance::Acceptance;
use crate::envelope;
use crate::holding::{Held, HoldError};
use crate::host_policy::{Policy, takedowns};
use crate::http::ApiError;
use crate::keccak::Digest;
use crate::server::{AppState, Ctx, StoreState};
use crate::store::{ContentStore, Kind};
use crate::verifier::{SignedAuthor, Verifier, VerifyError};
use axum::http::StatusCode;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct PublishParams {
    /// EIP-155 id of the chain the message was posted on.
    chain_id: Option<u64>,
    /// The message id on that chain. For a body, its contentHash must equal
    /// the upload's hash; for an attachment, that message's body must
    /// reference the upload's hash.
    message_id: Option<u64>,
    /// Alternatively, the hash of the transaction that posts this record.
    /// The host checks the transaction's calldata commits to the upload and
    /// holds the bytes until the message is accepted
    /// (https://github.com/blohard/host/blob/main/STORE_API.md).
    tx_hash: Option<String>,
}

impl PublishParams {
    /// The proof a reader gives when asking for a record by hash: the message
    /// that anchors it.
    pub(crate) fn by_message(chain_id: u64, message_id: u64) -> Self {
        Self {
            chain_id: Some(chain_id),
            message_id: Some(message_id),
            tx_hash: None,
        }
    }
}

/// Where an accepted upload ended up.
pub enum Outcome {
    /// In the durable store: its message is accepted. `newly_stored` is false
    /// when identical bytes were already there.
    Stored { kind: Kind, newly_stored: bool },
    /// In the holding area, waiting for acceptance; not served yet.
    Held { kind: Kind, message_id: Option<u64> },
}

const PROOF_REQUIRED: &str =
    "chain_id and either message_id or tx_hash are required: upload only after the post is sent";

/// The refusal for content of a post its author deleted or its parent's
/// author removed. The protocol lets hosts stop serving such content, and
/// this host takes none of it.
fn gone() -> ApiError {
    ApiError::said(
        StatusCode::GONE,
        "message_gone",
        "this post has been deleted or removed, so this host takes none of its content",
    )
}

/// Every check an upload goes through, then the store, the holding area, or
/// a refusal. Uploads and records pulled from peers both come through here.
pub(crate) async fn accept(
    state: &AppState,
    store: &StoreState,
    params: PublishParams,
    digest: Digest,
    body: bytes::Bytes,
) -> Result<Outcome, ApiError> {
    let outcome = check_and_keep(state, store, params, digest, body).await;
    // A body in the store, new or already there, may be what an index is
    // waiting for, on any chain.
    if let Ok(Outcome::Stored {
        kind: Kind::Bodies, ..
    }) = &outcome
    {
        state.body_arrived(digest);
    }
    outcome
}

async fn check_and_keep(
    state: &AppState,
    store: &StoreState,
    params: PublishParams,
    digest: Digest,
    body: bytes::Bytes,
) -> Result<Outcome, ApiError> {
    if body.is_empty() {
        return Err(ApiError::said(
            StatusCode::BAD_REQUEST,
            "empty_content",
            "empty content",
        ));
    }
    let max_upload = state.max_upload();
    if body.len() > max_upload {
        return Err(ApiError::too_large("content", max_upload));
    }
    // Bytes must declare what they are (protocol SPEC.md §6), checked before
    // any network round trip. Every stored record carries the header.
    if let Err(e) = envelope::parse(&body) {
        return Err(ApiError::said(
            StatusCode::BAD_REQUEST,
            "invalid_envelope",
            format!("invalid content envelope: {e}"),
        ));
    }
    // Under any post, and before anything is asked of the chain.
    if state.policy.taken_down(&digest.0) {
        return Err(takedowns::refused());
    }

    // Dev mode: no chain to ask. Small uploads are bodies, larger ones
    // attachments, no questions asked.
    if matches!(state.verifier, Verifier::AllowAll) {
        let (target, kind) = match &store.attachments {
            Some((s, _)) if body.len() > store.bodies.max_size() => (s, Kind::Attachments),
            _ => (&store.bodies, Kind::Bodies),
        };
        return persist(target, &state.policy, kind, body, digest).await;
    }

    // The proof of anchoring: a message id, or the transaction that posts it.
    let chain_id = params
        .chain_id
        .ok_or_else(|| ApiError::said(StatusCode::BAD_REQUEST, "proof_required", PROOF_REQUIRED))?;
    if let Some(index) = state.index(chain_id)
        && !index.state().is_established()
    {
        return Err(ApiError::from(VerifyError::IndexUnavailable));
    }
    let anchor = match (params.message_id, params.tx_hash) {
        (Some(id), _) => anchor_by_message(state, chain_id, id).await?,
        (None, Some(h)) => {
            let tx: [u8; 32] = decode_hex(&h)
                .and_then(|v| v.try_into().ok())
                .ok_or_else(|| {
                    ApiError::said(
                        StatusCode::BAD_REQUEST,
                        "malformed_request",
                        "tx_hash must be 32 bytes of hex",
                    )
                })?;
            anchor_by_tx(state, chain_id, tx).await?
        }
        (None, None) => {
            return Err(ApiError::said(
                StatusCode::BAD_REQUEST,
                "proof_required",
                PROOF_REQUIRED,
            ));
        }
    };
    state.policy.allow(&anchor.author)?;
    if anchor.withdrawn {
        return Err(gone());
    }
    admit(store, &state.policy, &anchor, digest, body).await
}

/// A message's body as this host has it: stored, or merely held while its
/// message is not accepted (`true` when stored). `None` when it has neither.
pub(crate) async fn body_of(
    store: &StoreState,
    chain_id: u64,
    body_digest: &Digest,
) -> Result<Option<(Vec<u8>, bool)>, ApiError> {
    match store.bodies.get(body_digest).await {
        Err(e) => Err(ApiError::store_failed(e)),
        Ok(Some(b)) => Ok(Some((b, true))),
        Ok(None) => Ok(store
            .holding
            .get(chain_id, body_digest)
            .map(|h| (h.bytes, false))),
    }
}

/// Evidence for a post, independent of which of its records is being uploaded.
enum AnchorProof {
    Confirmed(u64),
    Included(u64),
    Pending([u8; 32]),
}

struct Anchor {
    chain_id: u64,
    author: [u8; 20],
    body_digest: [u8; 32],
    proof: AnchorProof,
    /// Deleted or removed, or a pending post created already deleted.
    withdrawn: bool,
}

impl Anchor {
    fn message_id(&self) -> Option<u64> {
        match self.proof {
            AnchorProof::Confirmed(id) | AnchorProof::Included(id) => Some(id),
            AnchorProof::Pending(_) => None,
        }
    }

    fn is_final(&self) -> bool {
        matches!(self.proof, AnchorProof::Confirmed(_))
    }

    fn held(&self, kind: Kind, digest: Digest, bytes: bytes::Bytes) -> Held {
        Held {
            chain_id: self.chain_id,
            digest,
            bytes: Vec::from(bytes),
            kind,
            body_digest: Digest(self.body_digest),
            signer: self.author,
            message_id: self.message_id(),
            tx_hash: match (&self.proof, kind) {
                (AnchorProof::Pending(tx), Kind::Bodies) => Some(*tx),
                _ => None,
            },
        }
    }
}

/// Prefer confirmation; a message present only at latest is still provisional.
async fn anchor_by_message(state: &AppState, chain_id: u64, id: u64) -> Result<Anchor, ApiError> {
    let (message, proof) = match Acceptance::new(state)
        .accepted(chain_id, id)
        .await
        .map_err(ApiError::from)?
    {
        Some(message) => (message, AnchorProof::Confirmed(id)),
        // A mined post ahead of acceptance may occupy holding slots. Only
        // the acceptance source above can authorize serving it later.
        None => (
            state
                .verifier
                .message_at(chain_id, id, None)
                .await
                .map_err(ApiError::from)?,
            AnchorProof::Included(id),
        ),
    };
    Ok(Anchor {
        chain_id,
        author: message.author,
        body_digest: message.content_hash,
        proof,
        withdrawn: message.withdrawn,
    })
}

/// A mined post uses the receipt before any current signature check: a key
/// revoked after inclusion cannot invalidate the chain's earlier judgement.
async fn anchor_by_tx(
    state: &AppState,
    chain_id: u64,
    tx_hash: [u8; 32],
) -> Result<Anchor, ApiError> {
    let contract = state
        .verifier
        .contract(chain_id)
        .ok_or_else(|| ApiError::from(VerifyError::UnsupportedChain(chain_id)))?;
    let tx = state
        .verifier
        .transaction(chain_id, tx_hash)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::said(
                StatusCode::NOT_FOUND,
                "transaction_not_found",
                format!(
                    "chain {chain_id} has no transaction 0x{}: send it first, then upload",
                    hex::encode(tx_hash)
                ),
            )
        })?;
    if tx.to != Some(contract) {
        return Err(ApiError::said(
            StatusCode::FORBIDDEN,
            "not_board_transaction",
            format!(
                "transaction 0x{} is not sent to the board",
                hex::encode(tx_hash)
            ),
        ));
    }
    let Some(committed) = posted_content_hash(&tx.input) else {
        return Err(ApiError::said(
            StatusCode::FORBIDDEN,
            "not_a_post",
            format!("transaction 0x{} is not a post", hex::encode(tx_hash)),
        ));
    };
    if tx.block_number.is_some()
        && let Some(r) = state
            .verifier
            .receipt(chain_id, tx_hash)
            .await
            .map_err(ApiError::from)?
    {
        if !r.succeeded {
            return Err(ApiError::said(
                StatusCode::FORBIDDEN,
                "transaction_reverted",
                format!("transaction 0x{} reverted", hex::encode(tx_hash)),
            ));
        }
        if let Some(id) = r.message_id {
            return anchor_by_message(state, chain_id, id).await;
        }
    }
    // Still pending, so the chain has not judged it yet. Whose post this is:
    // the sender for `post`; for `postBySig`, whose sender is only the relayer
    // paying, the author the calldata names, once their signature checks out
    // the way the Board will check it. Taking the calldata's word would let
    // anyone fill another author's holding slots. (A mined transaction took the
    // receipt path above: the chain's judgement stands, even if the key that
    // signed has since been revoked.)
    let author = match state
        .verifier
        .signed_author(chain_id, &tx.input)
        .await
        .map_err(ApiError::from)?
    {
        SignedAuthor::NotBySig => tx.from,
        SignedAuthor::Author(a) => a,
        SignedAuthor::Invalid => {
            return Err(ApiError::said(
                StatusCode::FORBIDDEN,
                "signature_invalid",
                format!(
                    "transaction 0x{} names an author whose signature does not verify",
                    hex::encode(tx_hash)
                ),
            ));
        }
    };
    Ok(Anchor {
        chain_id,
        author,
        body_digest: committed,
        proof: AnchorProof::Pending(tx_hash),
        // A post may be created already deleted; nothing else can change its
        // gate before it is mined.
        withdrawn: posted_reply_gate(&tx.input).is_some_and(crate::index::types::withdrawn),
    })
}

/// The message's own body, or else an attachment of it. A body goes to the
/// store once its message is final and waits in holding until then.
async fn admit(
    store: &StoreState,
    policy: &Policy,
    anchor: &Anchor,
    digest: Digest,
    body: bytes::Bytes,
) -> Result<Outcome, ApiError> {
    if anchor.body_digest != digest.0 {
        return attach(store, policy, anchor, digest, body).await;
    }
    if body.len() > store.bodies.max_size() {
        return Err(ApiError::body_too_large(store.bodies.max_size()));
    }
    if anchor.is_final() {
        persist(&store.bodies, policy, Kind::Bodies, body, digest).await
    } else {
        hold(store, policy, anchor.held(Kind::Bodies, digest, body))
    }
}

/// An upload that is not the message's body must be an attachment its body
/// references. The body may be stored or merely held; the attachment follows
/// it: stored when both are accepted, held otherwise.
async fn attach(
    store: &StoreState,
    policy: &Policy,
    anchor: &Anchor,
    digest: Digest,
    bytes: bytes::Bytes,
) -> Result<Outcome, ApiError> {
    let chain_id = anchor.chain_id;
    let body_digest = anchor.body_digest;
    let Some((target, rules)) = &store.attachments else {
        return Err(ApiError::said(
            StatusCode::FORBIDDEN,
            "not_message_body",
            format!(
                "this upload is not the message's body (the message commits to 0x{}), and this host stores no attachments",
                hex::encode(body_digest)
            ),
        ));
    };
    let body_digest = Digest(body_digest);
    let which = anchor
        .message_id()
        .map(|i| format!("message {i}"))
        .unwrap_or_else(|| "pending message".into());
    let Some((parent, parent_stored)) = body_of(store, chain_id, &body_digest).await? else {
        return Err(ApiError::said(
            StatusCode::CONFLICT,
            "body_not_held",
            format!(
                "the body of chain {chain_id} {which} is not on this host yet: upload it before its attachments"
            ),
        ));
    };
    if !envelope::references_within(&parent, &digest.0, rules.max_per_message) {
        return Err(ApiError::said(
            StatusCode::FORBIDDEN,
            "attachment_not_referenced",
            format!(
                "chain {chain_id} {which} does not reference {} within its first {} record references",
                digest.to_hex(),
                rules.max_per_message
            ),
        ));
    }
    if let Some(allowed) = &rules.media_types {
        let declared = envelope::parse(&bytes)
            .map(|h| h.media_type.to_string())
            .unwrap_or_default();
        if !allowed.iter().any(|t| t.eq_ignore_ascii_case(&declared)) {
            return Err(ApiError::said(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "attachment_type_not_stored",
                format!(
                    "this host stores attachments of these types only: {}",
                    allowed.join(", ")
                ),
            ));
        }
    }
    if bytes.len() > target.max_size() {
        return Err(ApiError::attachment_too_large(target.max_size()));
    }
    if anchor.is_final() && parent_stored {
        return persist(target, policy, Kind::Attachments, bytes, digest).await;
    }
    hold(store, policy, anchor.held(Kind::Attachments, digest, bytes))
}

/// Hold a record until its message is accepted. Like [`persist`], the last
/// step before the bytes are kept, so it checks the takedown list itself:
/// whatever the upload waited on before, nothing taken down is kept.
fn hold(store: &StoreState, policy: &Policy, held: Held) -> Result<Outcome, ApiError> {
    if policy.taken_down(&held.digest.0) {
        return Err(takedowns::refused());
    }
    let (kind, message_id) = (held.kind, held.message_id);
    match store.holding.insert(held) {
        Ok(_) => Ok(Outcome::Held { kind, message_id }),
        Err(e @ HoldError::BodyNotHeld) => Err(ApiError::said(
            StatusCode::SERVICE_UNAVAILABLE,
            "body_not_held",
            e.to_string(),
        )),
        Err(e @ (HoldError::Full { .. } | HoldError::TooManyEntries { .. })) => {
            Err(ApiError::said(
                StatusCode::INSUFFICIENT_STORAGE,
                "holding_full",
                e.to_string(),
            ))
        }
        Err(e @ (HoldError::TooManyForSigner { .. } | HoldError::TooManyBytesForSigner { .. })) => {
            Err(ApiError::said(
                StatusCode::TOO_MANY_REQUESTS,
                "signer_holding_limit",
                e.to_string(),
            ))
        }
    }
}

/// Write `body` to `store` — the caller has decided which one — and report
/// whether it was new there. The last step before the bytes are kept, so it
/// checks the takedown list itself: whatever the upload waited on before,
/// nothing taken down is written. A write already under way when a takedown
/// lands still completes, but those bytes are never served.
async fn persist(
    store: &ContentStore<Ctx>,
    policy: &Policy,
    kind: Kind,
    body: bytes::Bytes,
    digest: Digest,
) -> Result<Outcome, ApiError> {
    if policy.taken_down(&digest.0) {
        return Err(takedowns::refused());
    }
    if body.len() > store.max_size() {
        return Err(match kind {
            Kind::Bodies => ApiError::body_too_large(store.max_size()),
            Kind::Attachments => ApiError::attachment_too_large(store.max_size()),
        });
    }
    // Handed over without a copy when nothing else holds these bytes, as
    // with an upload, so they are not held twice while the write waits.
    match store.put(Vec::from(body)).await {
        Ok((stored_digest, newly_stored)) => {
            debug_assert_eq!(stored_digest, digest);
            Ok(Outcome::Stored { kind, newly_stored })
        }
        Err(e) => Err(ApiError::store_failed(e)),
    }
}
