//! Host-owned indexes: one durable journal and one ordered writer per chain.
//! RAM maps are disposable; replay is the only recovery path.

pub mod bodies;
pub mod changes;
pub mod follow;
mod journal;
pub mod model;
pub mod query;
mod references;
#[cfg(test)]
pub(crate) use references::analyse;
pub mod types;

pub use journal::{Index, State};

/// What the index's background work (following each chain, reconciling
/// bodies and quotes) needs from the process around it. The index names these
/// and the host implements them (server.rs), so the index uses no server type.
pub trait Host: Send + Sync + 'static {
    /// Chain access: the configured chains, their RPC upstream and checks.
    fn verifier(&self) -> &crate::verifier::Verifier;
    /// Every chain's index, also for resolving quotes across chains; none on a store-only host.
    fn indexes(&self) -> Option<&std::collections::HashMap<u64, Index>>;
    /// Whether bodies can be read at all: the host has a content store.
    fn has_bodies(&self) -> bool;
    /// Whether the store has peers to copy from.
    fn has_peers(&self) -> bool;
    /// How many of a body's `record:` references are its attachments, when
    /// the host stores attachments at all.
    fn attachment_limit(&self) -> Option<usize>;
    /// A body from this host's store only, never asking a peer.
    fn stored_body(
        &self,
        digest: crate::keccak::Digest,
    ) -> impl std::future::Future<Output = anyhow::Result<Option<bytes::Bytes>>> + Send;
    /// Whether a peer lookup of `digest` is backing off, so asking now would
    /// try no peer.
    fn peer_waiting(&self, digest: &crate::keccak::Digest, role: crate::store::Kind) -> bool;
    /// Whether this host's lists block `author`, so their posts vouch for no
    /// copying.
    fn blocked(&self, author: &[u8; 20]) -> bool;
    /// Copy the record `digest` from a peer, as background work. A record
    /// this host already stores is answered at once; otherwise a turn of the
    /// host-wide background allowance comes first, then the reader's path,
    /// vouched for by message `id` on `chain`.
    fn copy(
        &self,
        chain: u64,
        id: u64,
        digest: crate::keccak::Digest,
        kind: crate::store::Kind,
    ) -> impl std::future::Future<Output = anyhow::Result<Copied>> + Send;
}

/// Where a record stands after this host asked for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Copied {
    /// Stored, with a body's bytes.
    Stored(Option<bytes::Bytes>),
    /// No peer had it, or it is held until its message is accepted.
    Missing,
    /// A peer had it and this host refuses it for what it is: not worth asking again.
    Refused,
    /// The message given cannot vouch for it: deleted, removed or by a blocked
    /// author. Another post that needs the same bytes still may.
    Unvouched,
}

/// A shared host is a host, so callers holding one pass it as it is.
impl<T: Host> Host for std::sync::Arc<T> {
    fn verifier(&self) -> &crate::verifier::Verifier {
        (**self).verifier()
    }
    fn indexes(&self) -> Option<&std::collections::HashMap<u64, Index>> {
        (**self).indexes()
    }
    fn has_bodies(&self) -> bool {
        (**self).has_bodies()
    }
    fn has_peers(&self) -> bool {
        (**self).has_peers()
    }
    fn attachment_limit(&self) -> Option<usize> {
        (**self).attachment_limit()
    }
    fn stored_body(
        &self,
        digest: crate::keccak::Digest,
    ) -> impl std::future::Future<Output = anyhow::Result<Option<bytes::Bytes>>> + Send {
        (**self).stored_body(digest)
    }
    fn peer_waiting(&self, digest: &crate::keccak::Digest, role: crate::store::Kind) -> bool {
        (**self).peer_waiting(digest, role)
    }
    fn blocked(&self, author: &[u8; 20]) -> bool {
        (**self).blocked(author)
    }
    fn copy(
        &self,
        chain: u64,
        id: u64,
        digest: crate::keccak::Digest,
        kind: crate::store::Kind,
    ) -> impl std::future::Future<Output = anyhow::Result<Copied>> + Send {
        (**self).copy(chain, id, digest, kind)
    }
}

/// A proven history replacement invalidates cross-chain associations too.
#[derive(Debug)]
pub struct HistoryChanged;
impl std::fmt::Display for HistoryChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("accepted history changed; rebuild required")
    }
}
impl std::error::Error for HistoryChanged {}

#[cfg(test)]
mod reference_tests;
#[cfg(test)]
mod sweep_tests;
#[cfg(test)]
mod tests;
