//! The server's state and construction: what `main` and the tests both build.
//!
//! [`open_state`] builds a [`StoreState`] and/or a [`GatewayState`], one per
//! role the config enables. [`crate::routes::router`] mounts HTTP on that state.

use crate::config::{AttachmentsConfig, GatewayConfig, StoreConfig};
use crate::holding::{Holding, HoldingConfig};
use crate::names::Names;
use crate::peers::Peers;
use crate::store::{ContentStore, Kind, MAX_CONTENT_SIZE};
use crate::verifier::Verifier;
use commonware_runtime::{Supervisor as _, tokio as cw_tokio};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// The runtime context the stores run on.
pub type Ctx = cw_tokio::Context;

/// The content-store role: bodies and attachments by hash, plus the holding
/// area for uploads whose message is not accepted yet.
pub struct StoreState {
    /// Taken before buffering an upload; owned through verification and storage.
    pub uploads: crate::publish::http::Uploads,
    /// Message bodies: hash committed on-chain.
    pub bodies: ContentStore<Ctx>,
    /// Attachments: hash referenced by an accepted stored body. `None` = bodies-only host.
    pub attachments: Option<(ContentStore<Ctx>, AttachmentsConfig)>,
    /// Uploads waiting for their message to become accepted (never served).
    pub holding: Holding,
    /// Other hosts, asked for records this one lacks (`peers.rs`).
    pub peers: Peers,
}

impl StoreState {
    /// Make everything taken so far durable, both stores.
    pub async fn commit(&self) -> anyhow::Result<()> {
        self.bodies.commit().await?;
        if let Some((attachments, _)) = &self.attachments {
            attachments.commit().await?;
        }
        Ok(())
    }
}

/// The gateway role: what the web app reads the chain and names through.
pub struct GatewayState {
    pub indexes: HashMap<u64, crate::index::Index>,
    pub names: Option<Arc<Names>>,
    pub link_previews: Option<Arc<crate::link_preview::Previews>>,
}

/// Everything the handlers share. The two roles are optional and separate,
/// so a host can be a content store, a gateway, or both, and either can
/// become its own service without the API changing.
pub struct AppState {
    pub policy: crate::host_policy::Policy,
    pub gate_checks: crate::gate_validation::Validated,
    pub keyrings: BTreeMap<u64, Arc<crate::keyring::Keyring>>,
    pub verifier: Verifier,
    /// The operator's one-line notice for visitors, if any (config `notice`).
    pub notice: Option<String>,
    pub store: Option<StoreState>,
    pub gateway: Option<GatewayState>,
}

/// The index's background work reads the chain through the verifier, other
/// chains' indexes for quotes, and bodies from the store or, missing there,
/// from a peer (crate::index::Host).
impl crate::index::Host for AppState {
    fn verifier(&self) -> &Verifier {
        &self.verifier
    }
    fn indexes(&self) -> Option<&HashMap<u64, crate::index::Index>> {
        self.gateway.as_ref().map(|g| &g.indexes)
    }
    fn has_bodies(&self) -> bool {
        self.store.is_some()
    }
    fn has_peers(&self) -> bool {
        self.store
            .as_ref()
            .is_some_and(|store| !store.peers.is_empty())
    }
    fn attachment_limit(&self) -> Option<usize> {
        let (_, policy) = self.store.as_ref()?.attachments.as_ref()?;
        Some(policy.max_per_message)
    }
    async fn stored_body(
        &self,
        digest: crate::keccak::Digest,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        let Some(store) = &self.store else {
            return Ok(None);
        };
        Ok(store.bodies.get(&digest).await?.map(bytes::Bytes::from))
    }
    fn peer_waiting(&self, digest: &crate::keccak::Digest, role: crate::store::Kind) -> bool {
        self.store
            .as_ref()
            .is_some_and(|store| store.peers.waiting(digest, role))
    }
    fn blocked(&self, author: &[u8; 20]) -> bool {
        self.policy.blocked(author)
    }
    async fn copy(
        &self,
        chain: u64,
        id: u64,
        digest: crate::keccak::Digest,
        kind: crate::store::Kind,
    ) -> anyhow::Result<crate::index::Copied> {
        use crate::{index::Copied, peers::Pulled};
        let message = self.indexed_message(chain, id).ok().flatten();
        if message.is_some_and(|m| self.policy.blocked(&m.author)) {
            return Ok(Copied::Unvouched);
        }
        let Some(store) = &self.store else {
            return Ok(Copied::Missing);
        };
        let local = match kind {
            Kind::Bodies => store.bodies.get(&digest).await?,
            Kind::Attachments => match &store.attachments {
                Some((attachments, _)) => attachments.get(&digest).await?,
                None => return Ok(Copied::Missing),
            },
        };
        if let Some(bytes) = local {
            return Ok(Copied::Stored(
                (kind == Kind::Bodies).then(|| bytes::Bytes::from(bytes)),
            ));
        }
        if store.peers.is_empty() {
            return Ok(Copied::Missing);
        }
        // Every chain's sweep shares this allowance, so readers keep most peer slots.
        let _turn = store.peers.background().await;
        // The peer path vouches for the hash through the chain or the body first.
        Ok(
            match crate::peers::pull(self, store, chain, id, digest)
                .await
                .map_err(|error| anyhow::anyhow!(error.message))?
            {
                Pulled::Stored(bytes) => Copied::Stored((kind == Kind::Bodies).then_some(bytes)),
                Pulled::Refused => Copied::Refused,
                Pulled::Unvouched => Copied::Unvouched,
                Pulled::Held | Pulled::Missing => Copied::Missing,
            },
        )
    }
}

/// What a store route answers on a host that is a gateway only.
pub const NO_STORE: &str = "this host stores no content";

impl AppState {
    /// Read a stored body, asking peers only on a local miss. Peer bytes still
    /// pass normal content admission; held bytes are never returned.
    pub async fn body_from_store_or_peer(
        &self,
        chain: u64,
        id: u64,
        digest: crate::keccak::Digest,
    ) -> anyhow::Result<Option<bytes::Bytes>> {
        let Some(store) = &self.store else {
            return Ok(None);
        };
        match store.bodies.get(&digest).await? {
            Some(bytes) => Ok(Some(bytes::Bytes::from(bytes))),
            None if !store.peers.is_empty() => crate::peers::pull(self, store, chain, id, digest)
                .await
                .map(crate::peers::Pulled::stored)
                .map_err(|error| anyhow::anyhow!(error.message)),
            None => Ok(None),
        }
    }

    /// Store policy and peer anchoring use the same accepted facts as readers.
    /// This host's index for `chain`. There is none on a store-only host,
    /// which has no gateway, or for a chain this host does not serve.
    pub fn index(&self, chain: u64) -> Option<&crate::index::Index> {
        self.gateway.as_ref()?.indexes.get(&chain)
    }

    /// A body is in the store, newly or again: any chain's index still
    /// waiting on it takes it up first.
    pub fn body_arrived(&self, digest: crate::keccak::Digest) {
        if let Some(gateway) = &self.gateway {
            crate::index::bodies::arrived(&gateway.indexes, crate::chain::Hex(digest.0));
        }
    }

    /// A stopped or warming index must never fall back to a different history.
    pub fn read_index<T>(
        &self,
        chain: u64,
        read: impl FnOnce(&crate::index::model::Model) -> T,
    ) -> Result<T, crate::verifier::VerifyError> {
        use crate::verifier::VerifyError;
        let index = self
            .index(chain)
            .ok_or(VerifyError::UnsupportedChain(chain))?;
        index.read_ready(read).ok_or(VerifyError::IndexUnavailable)
    }

    pub fn indexed_message(
        &self,
        chain: u64,
        id: u64,
    ) -> Result<Option<crate::verifier::MessageInfo>, crate::verifier::VerifyError> {
        self.read_index(chain, |model| {
            model
                .messages
                .get(&id)
                .map(|m| crate::verifier::MessageInfo {
                    author: m.author.0,
                    content_hash: m.content_hash.0,
                    withdrawn: !m.stands_alone(),
                })
        })
    }

    /// The largest upload any handler should read.
    pub fn max_upload(&self) -> usize {
        self.store
            .as_ref()
            .map_or(MAX_CONTENT_SIZE, |s| largest_upload(s.attachments.as_ref()))
    }
}

/// The largest record a store takes: an attachment up to its cap, or any
/// record up to the content ceiling, whichever is more.
fn largest_upload(attachments: Option<&(ContentStore<Ctx>, AttachmentsConfig)>) -> usize {
    attachments.map_or(MAX_CONTENT_SIZE, |(s, _)| {
        s.max_size().max(MAX_CONTENT_SIZE)
    })
}

/// What `open_state` takes: the two role sections of the config, as parsed,
/// plus what the command line and the defaults add.
pub struct Options {
    pub policy: Option<crate::host_policy::Config>,
    /// The content-store role, when the config enables it.
    pub store: Option<StoreConfig>,
    /// The gateway role, when the config enables it.
    pub gateway: Option<GatewayConfig>,
    pub holding: HoldingConfig,
    /// One operator line, published in the status.
    pub notice: Option<String>,
    /// The account-key markets to index; needs the gateway role (Config::validate).
    pub keyrings: Vec<crate::keyring::Config>,
}

/// Attachment caps used in `--allow-all` mode, where there is no config.
pub fn dev_attachment_policy() -> AttachmentsConfig {
    AttachmentsConfig {
        max_bytes: 2 << 20,
        max_per_message: 4,
        media_types: None,
    }
}

/// Open the stores, indexes and name service. This is the one place the
/// production wiring lives, and the tests build their servers through it
/// too, so a bad partition name, label, or cap fails in `cargo test` rather
/// than at deploy time. Private chain and name reads use `client`, the
/// same one the verifier was built with.
pub async fn open_state(
    context: Ctx,
    verifier: Verifier,
    options: Options,
    client: reqwest::Client,
) -> anyhow::Result<AppState> {
    let policy = crate::host_policy::Policy::open(options.policy).await?;
    // Without an index there is no follower to establish chain identity.
    if options.gateway.is_none() {
        verifier.check_chains().await?;
    }
    let store = match options.store {
        Some(s) => Some(open_store(&context, s, options.holding).await?),
        None => {
            tracing::info!("no [store]: this host stores no content");
            None
        }
    };
    let gateway = match options.gateway {
        Some(g) => Some(open_gateway(&context, &verifier, g, &client).await?),
        None => {
            tracing::info!("no [gateway]: no index, name service or web app");
            None
        }
    };
    // A keyring that cannot open is reported as stopped; the host serves the rest.
    let mut keyrings = BTreeMap::new();
    for config in options.keyrings {
        let chain = config.chain;
        let index = match crate::keyring::Keyring::open(context.child("keyring"), config.clone())
            .await
        {
            Ok(index) => index,
            Err(error) => {
                tracing::error!(
                    chain,
                    "keyring disabled; move its journal aside or fix its configuration, then restart: {error:#}"
                );
                crate::keyring::Keyring::unavailable(config)
            }
        };
        keyrings.insert(chain, index);
    }
    Ok(AppState {
        policy,
        gate_checks: Default::default(),
        keyrings,
        verifier,
        notice: options.notice,
        store,
        gateway,
    })
}

async fn open_store(
    context: &Ctx,
    cfg: StoreConfig,
    holding: HoldingConfig,
) -> anyhow::Result<StoreState> {
    if !cfg.peers.is_empty() {
        tracing::info!(peers = ?cfg.peers, "peers: asked for records this host lacks");
    }
    let page_cache = cfg.page_cache_bytes();
    let bodies = ContentStore::init_with(
        context.child("bodies"),
        Kind::Bodies,
        cfg.body_cap(),
        page_cache,
    )
    .await?;
    tracing::info!(max_body_bytes = cfg.body_cap(), "message bodies enabled");
    let attachments = match cfg.attachments {
        Some(policy) => {
            let store = ContentStore::init_with(
                context.child("attachments"),
                Kind::Attachments,
                policy.max_bytes,
                page_cache,
            )
            .await?;
            tracing::info!(
                max_bytes = policy.max_bytes,
                max_per_message = policy.max_per_message,
                "attachments enabled"
            );
            Some((store, policy))
        }
        None => {
            tracing::info!("attachments disabled: bodies-only host");
            None
        }
    };
    // Peers get a client of their own that follows no redirect: a peer
    // may send the host anywhere, its own network included, and a record
    // is fetched from the peer named or not at all. Each request sets its
    // own timeout (peers.rs).
    let peer_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let peers = Peers::new(cfg.peers, largest_upload(attachments.as_ref()), peer_client);
    Ok(StoreState {
        uploads: crate::publish::http::Uploads::new(),
        bodies,
        attachments,
        holding: Holding::new(holding),
        peers,
    })
}

async fn open_gateway(
    context: &Ctx,
    verifier: &Verifier,
    cfg: GatewayConfig,
    client: &reqwest::Client,
) -> anyhow::Result<GatewayState> {
    // Each chain owns its journal and replayed RAM index. Opening needs no
    // chain access: the follower confirms the chain on its first successful
    // poll, retrying a slow chain rather than giving up.
    let mut indexes = HashMap::new();
    for (id, chain) in verifier.chains() {
        use crate::chain::Hex;
        use crate::index::{Index, types::Deployment};
        let deployment = Deployment {
            chain: id,
            board: Hex(chain.contract),
            start_block: chain.start_block,
            index_start_block: chain.index_start_block,
            registries: chain
                .blocking
                .iter()
                .map(|b| Hex(b.registry))
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        };
        // A new journal records this random generation, which marks paging
        // cursors from any earlier journal stale; an existing journal keeps
        // the one it recorded.
        let mut generation = [0u8; 32];
        rand_core::Rng::fill_bytes(&mut context.child("generation"), &mut generation);
        let generation = Hex(generation);
        let index = match Index::open(context.child("index"), deployment.clone(), generation).await
        {
            Ok(index) => index,
            Err(error) => {
                tracing::error!(
                    chain = id,
                    ?error,
                    "index journal could not open; chain unavailable"
                );
                Index::unavailable(deployment, generation)
            }
        };
        indexes.insert(id, index);
    }
    let names = cfg
        .names
        .map(|n| {
            let upstream = verifier
                .upstream()
                .cloned()
                .unwrap_or_else(|| crate::rpc::Upstream::unlimited(client.clone()));
            Names::new(n, upstream).map(Arc::new)
        })
        .transpose()?;
    let link_previews = cfg
        .link_previews
        .map(crate::link_preview::Previews::new)
        .transpose()?
        .map(Arc::new);
    Ok(GatewayState {
        indexes,
        names,
        link_previews,
    })
}
