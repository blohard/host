//! Startup configuration: which chains this host serves, how to reach each
//! one, and which of the two roles it plays.
//!
//! A host is a **content store** (`[store]`: bodies and attachments by hash,
//! stored once the chain commits to them), a **gateway** (`[gateway]`: the
//! accepted index and verified names), or
//! both. The roles share the chain list and one process today, and are kept
//! apart in the config, the state and the status so they can become separate
//! services without the API changing.
//!
//! Every Board deployment is its own message namespace (protocol SPEC.md §9), so
//! a proof of anchoring is the pair (chain id, message id) and the host must
//! know, per chain, which contract to ask and through which endpoint.

use anyhow::{Context as _, Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

/// Wallet metadata for the chain's native gas token, independent of AccountKeys pricing.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCurrency {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
}

impl Default for NativeCurrency {
    fn default() -> Self {
        Self {
            name: "Ether".into(),
            symbol: "ETH".into(),
            decimals: 18,
        }
    }
}

impl NativeCurrency {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.is_empty()
                && self.name.len() <= 64
                && self.name.trim() == self.name
                && self.name.bytes().all(|b| b.is_ascii_graphic() || b == b' '),
            "native_currency.name must be 1-64 printable ASCII characters without surrounding spaces"
        );
        ensure!(
            !self.symbol.is_empty()
                && self.symbol.len() <= 16
                && self.symbol.bytes().all(|b| b.is_ascii_graphic()),
            "native_currency.symbol must be 1-16 printable ASCII characters without spaces"
        );
        Ok(())
    }
}

/// The config file (TOML). See `config.example.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Local operator lists and the wallet allowed to edit them.
    pub host_policy: Option<crate::host_policy::Config>,
    /// Account-key holdings indexes, at most one per served chain.
    #[serde(default)]
    pub keyrings: Vec<crate::keyring::Config>,
    /// Chains this host serves: the store verifies anchoring against them and
    /// the gateway indexes their history. A publish naming any other chain id is
    /// rejected.
    #[serde(default)]
    pub chains: Vec<ChainConfig>,
    /// The content-store role. Present (even empty) to store bodies; absent
    /// for a host that is a gateway only.
    pub store: Option<StoreConfig>,
    /// The gateway role. Present (even empty) for indexed application reads;
    /// absent for a content store only. Serving --web-dir also requires this role.
    pub gateway: Option<GatewayConfig>,
    /// One line shown to every visitor, in the app's rail and on its About
    /// page: for instance that this is a test deployment whose posts can
    /// disappear. Published by `/api/status`; absent means none.
    pub notice: Option<String>,
    /// How many calls a minute this host may make to its upstreams, all
    /// chains and name lookups together, and how many of those only ingestion
    /// may spend. Absent: the defaults below.
    #[serde(default)]
    pub upstream: UpstreamConfig,
}

/// The budget for calls to upstream RPC providers. One bucket meters every
/// chain and the names service, since they share one subscription as a rule.
/// Client work (admission, estimates, receipts, uploads) may spend down to
/// `reserved_for_ingestion` tokens and is answered busy below that. Name
/// lookups stop earlier, halfway between that reserve and a full bucket, so a
/// flood of them leaves the headroom below to other client work. The follower
/// may spend the reserve too, so a flood of client traffic cannot stall
/// indexing. Size `calls_per_minute` to the provider's plan; a rebuild goes
/// faster with a larger budget for the duration. Zero means no limit.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    #[serde(default = "default_calls_per_minute")]
    pub calls_per_minute: u32,
    #[serde(default = "default_reserved_for_ingestion")]
    pub reserved_for_ingestion: u32,
}

fn default_calls_per_minute() -> u32 {
    1_200
}
fn default_reserved_for_ingestion() -> u32 {
    300
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            calls_per_minute: default_calls_per_minute(),
            reserved_for_ingestion: default_reserved_for_ingestion(),
        }
    }
}

/// The content-store role: what this host stores besides message bodies.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    /// Attachment policy (protocol SPEC.md §7). Absent: this host stores
    /// message bodies only and refuses attachments.
    pub attachments: Option<AttachmentsConfig>,
    /// Other hosts this one knows, as base URLs. Published in the status, and
    /// asked for a record this host lacks when a reader names the message
    /// that anchors it; what they return is checked like an upload.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Memory for the key index of each content store (bodies, attachments),
    /// in MiB: how much of the index stays in RAM between reads. Default 64.
    #[serde(default = "default_cache_mib")]
    pub page_cache_mib: u64,
    /// The largest message body this host stores, in bytes, envelope header
    /// included. Published in the status as `max_content_size`, so the
    /// composer stops there before any gas is spent. Zero means the default,
    /// 4000 bytes: about two dense paragraphs. Never above 1 MiB.
    #[serde(default)]
    pub max_body_bytes: usize,
}

/// The body cap a host applies when its config names none: about fourteen
/// tweets, which is a post and not an essay.
pub const DEFAULT_BODY_BYTES: usize = 4000;

/// The memory defaults, sized for a small host; a large one raises them.
fn default_cache_mib() -> u64 {
    64
}

/// A configured size in MiB as bytes. Zero, which a config built in code
/// gets for a field it left out, means the default.
fn mib_or_default(mib: u64) -> usize {
    let mib = if mib == 0 { default_cache_mib() } else { mib };
    mib_bytes(mib).expect("store.page_cache_mib is checked when the config loads")
}

/// A size in MiB as bytes, if this machine can count that many.
fn mib_bytes(mib: u64) -> Option<usize> {
    usize::try_from(mib).ok()?.checked_mul(1 << 20)
}

impl StoreConfig {
    pub fn page_cache_bytes(&self) -> usize {
        mib_or_default(self.page_cache_mib)
    }

    /// The configured body cap, or the default when the config left it out.
    pub fn body_cap(&self) -> usize {
        if self.max_body_bytes == 0 {
            DEFAULT_BODY_BYTES
        } else {
            self.max_body_bytes
        }
    }
}

/// The gateway role: what this host offers clients besides chain reads.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    /// Optional outbound link metadata. Omitted means no third-party page fetches.
    pub link_previews: Option<crate::link_preview::Config>,
    /// Private upstreams for the host's verified name service. No name RPC
    /// URL is exposed to browsers; omitted means display addresses only.
    pub names: Option<NamesConfig>,
}

/// Private name-service upstreams: ENS on Ethereum and Basenames on Base.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamesConfig {
    pub ethereum_upstream: Option<String>,
    pub base_upstream: Option<String>,
}

/// Attachment policy: size cap per attachment, `record:` references honoured
/// per body, allowed media types.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachmentsConfig {
    /// Largest attachment accepted, in bytes (at most 16 MiB).
    pub max_bytes: usize,
    /// How many `record:` references of a body this host honours, in order of
    /// appearance. Further references are ignored, never stored.
    pub max_per_message: usize,
    /// Media types this host stores as attachments. Absent: any type.
    /// Published in the status so clients know before they attach.
    #[serde(default)]
    pub media_types: Option<Vec<String>>,
}

/// One blohard deployment.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    /// EIP-155 chain id. Checked against the endpoint's `eth_chainId` at
    /// startup so a misconfigured endpoint fails loudly.
    pub id: u64,
    /// Human-readable label for logs and the status endpoint.
    pub name: Option<String>,
    /// JSON-RPC endpoint. May carry an API key; the server never exposes it.
    pub rpc_url: String,
    /// blohard core contract address on this chain (0x-prefixed hex).
    pub contract: String,
    /// An RPC URL that is safe to hand to browsers (no API key). When set it is
    /// published by `/api/status` so the web app can add the chain to a wallet
    /// for signing and submission. Leave unset to publish nothing.
    pub public_rpc_url: Option<String>,
    /// Native gas token advertised to wallets. Defaults to Ether/ETH/18.
    #[serde(default)]
    pub native_currency: NativeCurrency,
    /// Block at which the core contract was deployed. Ingestion starts no
    /// later than this block.
    #[serde(default)]
    pub start_block: u64,
    /// Earliest block needed by the Board and configured registries. Defaults
    /// to genesis when a registry is configured, otherwise the Board's start.
    pub index_start_block: Option<u64>,
    /// Accepted depth behind the head. A contradiction stops the index;
    /// select a depth appropriate for the chain. Zero suits anvil.
    #[serde(default)]
    pub confirmations: u64,
    /// Seconds between caught-up ingestion polls; private operator setting.
    /// Backfill proceeds without this delay. RPC failures use their own backoff.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u32,
    /// The blocking setup this host suggests to clients: a shared author gate
    /// that refuses repliers on the parent author's list `list` in `registry`
    /// (`AuthorBlocklistGate` in the protocol's src/ListGates.sol). Status publishes its
    /// registry and list immediately as `blocklist` for display filtering, and
    /// publishes `blocking` with the gate after verifying code and settings.
    pub blocking: Option<BlockingConfig>,
    /// Verified shared key gates, published for retrospective reply folding.
    pub key_gates: Option<KeyGatesConfig>,
    /// Optional deny-all gate, verified before offering reversible thread closing.
    pub closed_gate: Option<String>,
}

/// Known key gates bound to this Board. The combined gate must use the
/// registry and list in `blocking`; arbitrary custom gates are not recognised.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyGatesConfig {
    pub holders: Option<String>,
    pub holders_blocklist: Option<String>,
}

/// A shared author-blocklist gate and the registry list it reads.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockingConfig {
    pub gate: String,
    pub registry: String,
    pub list: String,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("in config {}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self> {
        let cfg: Self = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Every rule a config must meet, checked when it loads; what builds on
    /// it (Verifier::from_config) may assume them.
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(!self.chains.is_empty(), "config lists no chains");
        if let Some(policy) = &self.host_policy {
            ensure!(
                self.chains.iter().any(|c| c.id == policy.admin_chain),
                "host_policy.admin_chain is not served"
            );
        }
        ensure!(
            self.upstream.calls_per_minute == 0
                || self.upstream.reserved_for_ingestion < self.upstream.calls_per_minute,
            "upstream.reserved_for_ingestion must be below upstream.calls_per_minute"
        );
        ensure!(
            self.store.is_some() || self.gateway.is_some(),
            "config enables neither role: add a [store] section, a [gateway] section, or both"
        );
        ensure!(
            self.keyrings.is_empty() || self.gateway.is_some(),
            "a [[keyrings]] entry needs the [gateway] role, which follows its chain and serves it"
        );
        let mut markets = HashSet::new();
        for market in &self.keyrings {
            ensure!(
                markets.insert(market.chain),
                "only one keyring per chain is supported"
            );
            ensure!(
                self.chains.iter().any(|chain| chain.id == market.chain),
                "keyring chain {} is not served",
                market.chain
            );
            ensure!(
                market.market != crate::chain::Address::ZERO && market.start_block > 0,
                "keyring requires a market and its deployment block"
            );
        }
        for c in &self.chains {
            http_url(&format!("chains[{}].rpc_url", c.id), &c.rpc_url)?;
        }
        for p in self.store.iter().flat_map(|s| &s.peers) {
            http_url("store.peers", p)?;
        }
        if let Some(names) = self.gateway.as_ref().and_then(|g| g.names.as_ref()) {
            for (what, value) in [
                ("gateway.names.ethereum_upstream", &names.ethereum_upstream),
                ("gateway.names.base_upstream", &names.base_upstream),
            ] {
                if let Some(value) = value {
                    http_url(what, value)?;
                }
            }
        }
        if let Some(s) = self.store.as_ref() {
            ensure!(
                mib_bytes(s.page_cache_mib).is_some(),
                "store.page_cache_mib is too large to count in bytes"
            );
            ensure!(
                s.max_body_bytes == 0 || s.max_body_bytes >= crate::envelope::MAX_HEADER_LEN,
                "max_body_bytes must leave room for a record: at least {} bytes",
                crate::envelope::MAX_HEADER_LEN
            );
            ensure!(
                s.max_body_bytes <= crate::store::MAX_CONTENT_SIZE,
                "max_body_bytes exceeds the hard ceiling of {} bytes",
                crate::store::MAX_CONTENT_SIZE
            );
        }
        if let Some(a) = self.store.as_ref().and_then(|s| s.attachments.as_ref()) {
            ensure!(a.max_bytes > 0, "attachments.max_bytes must be positive");
            ensure!(
                a.max_bytes <= crate::store::MAX_VALUE_SIZE,
                "attachments.max_bytes exceeds the hard ceiling of {} bytes",
                crate::store::MAX_VALUE_SIZE
            );
            ensure!(
                a.max_per_message > 0,
                "attachments.max_per_message must be positive"
            );
        }
        let mut seen = HashSet::new();
        for c in &self.chains {
            ensure!(
                c.poll_interval_secs > 0,
                "chain {}: poll_interval_secs must be positive",
                c.id
            );
            ensure!(
                seen.insert(c.id),
                "chain id {} is listed more than once",
                c.id
            );
            c.validate().with_context(|| format!("chain {}", c.id))?;
            if let Some(url) = &c.public_rpc_url {
                // A path such as "/rpc/810337" means "on this host": the client
                // resolves it against the origin it loaded the app from, so the
                // host's own name never has to appear in its config.
                let is_url = ["http://", "https://", "ws://", "wss://"]
                    .iter()
                    .any(|p| url.starts_with(p));
                // "//other-host/rpc" is a protocol-relative URL, not a path.
                let is_path = url.starts_with('/') && !url.starts_with("//");
                ensure!(
                    is_url || is_path,
                    "chain {}: public_rpc_url must be an http(s) or ws(s) URL, or a path on this host",
                    c.id
                );
            }
        }
        Ok(())
    }
}

impl ChainConfig {
    /// The rules for one chain's section, beside those that compare chains.
    fn validate(&self) -> Result<()> {
        self.native_currency.validate()?;
        crate::abi::parse_address(&self.contract).context("bad contract address")?;
        if let Some(start) = self.index_start_block {
            ensure!(
                start <= self.start_block,
                "index_start_block must not follow the Board deployment (start_block)"
            );
        }
        if let Some(gate) = &self.closed_gate {
            gate_address(gate, "closed gate")?;
        }
        let blocking_gate = match &self.blocking {
            Some(b) => {
                crate::abi::parse_address(&b.registry).context("bad blocking registry address")?;
                Some(gate_address(&b.gate, "blocking gate")?)
            }
            None => None,
        };
        if let Some(g) = &self.key_gates {
            ensure!(
                g.holders_blocklist.is_none() || self.blocking.is_some(),
                "combined key gate requires the matching blocking configuration"
            );
            let holders = g
                .holders
                .as_deref()
                .map(|v| gate_address(v, "key gate"))
                .transpose()?;
            let holders_blocklist = g
                .holders_blocklist
                .as_deref()
                .map(|v| gate_address(v, "key gate"))
                .transpose()?;
            ensure!(
                holders.is_none() || holders != holders_blocklist,
                "key gate kinds require distinct addresses"
            );
            if let Some(block_gate) = blocking_gate {
                ensure!(
                    ![holders, holders_blocklist].contains(&Some(block_gate)),
                    "key gate cannot also be the plain blocklist gate"
                );
            }
        }
        Ok(())
    }
}

/// A configured gate contract: never one of the values the Board keeps for
/// no gate (0x0), a deleted post (0x1) or a removed one (0x2).
fn gate_address(value: &str, what: &str) -> Result<[u8; 20]> {
    let address =
        crate::abi::parse_address(value).with_context(|| format!("bad {what} address"))?;
    ensure!(
        address[..19] != [0; 19] || address[19] > 2,
        "invalid {what} address: 0x0, 0x1 and 0x2 are reserved by the Board"
    );
    Ok(address)
}

/// Operator-configured endpoints are parsed as URLs, not prefix-matched, so a
/// typo surfaces at startup rather than as repeated failures at runtime.
fn http_url(what: &str, value: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(value)
        .map_err(|e| anyhow!("{what}: {value} is not a valid URL ({e})"))?;
    ensure!(
        matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
        "{what}: {value} is not an http(s) URL"
    );
    Ok(())
}

fn default_poll_interval_secs() -> u32 {
    1
}

#[cfg(test)]
impl Config {
    /// A host config for tests that serves `chains` and nothing more: no
    /// role, keyring or notice, and the default upstream budget.
    pub fn for_test(chains: Vec<ChainConfig>) -> Self {
        Self {
            keyrings: Default::default(),
            host_policy: None,
            chains,
            store: None,
            gateway: None,
            notice: None,
            upstream: Default::default(),
        }
    }
}

#[cfg(test)]
impl ChainConfig {
    /// A bare chain entry for tests: no name, no list, no blocking, block 0,
    /// no confirmations.
    pub fn for_test(id: u64, url: &str, contract: &str) -> Self {
        Self {
            id,
            name: None,
            rpc_url: url.to_string(),
            contract: contract.to_string(),
            public_rpc_url: None,
            native_currency: NativeCurrency::default(),
            start_block: 0,
            index_start_block: None,
            confirmations: 0,
            poll_interval_secs: default_poll_interval_secs(),
            blocking: None,
            key_gates: None,
            closed_gate: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A page cache too large to count in bytes stops the host, where it
    /// once wrapped around into a far smaller cache.
    #[test]
    fn a_page_cache_too_large_to_count_is_refused() {
        let chain = r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
        "#;
        let error = Config::parse(&format!(
            "[store]\npage_cache_mib = 17592186044416\n{chain}"
        ))
        .unwrap_err();
        assert!(format!("{error:#}").contains("page_cache_mib"), "{error:#}");
        let cfg = Config::parse(&format!("[store]\npage_cache_mib = 512\n{chain}")).unwrap();
        assert_eq!(cfg.store.unwrap().page_cache_bytes(), 512 << 20);
    }

    #[test]
    fn keyrings_are_unique_and_belong_to_served_chains() {
        let base = r#"
            [gateway]
            [[chains]]
            id = 8453
            rpc_url = "http://base"
            contract = "0x0000000000000000000000000000000000000001"
            [[chains]]
            id = 84532
            rpc_url = "http://sepolia"
            contract = "0x0000000000000000000000000000000000000001"
            [[keyrings]]
            chain = 8453
            market = "0x00000000000000000000000000000000000000aa"
            start_block = 10
            [[keyrings]]
            chain = 84532
            market = "0x00000000000000000000000000000000000000aa"
            start_block = 20
        "#;
        Config::parse(base).unwrap();
        for bad in [
            base.replace("chain = 84532", "chain = 8453"),
            base.replace("chain = 84532", "chain = 1"),
            base.replace("start_block = 20", "start_block = 0"),
        ] {
            assert!(Config::parse(&bad).is_err());
        }
    }

    #[test]
    fn a_keyring_needs_the_gateway_role() {
        let keyring = r#"
            [[keyrings]]
            chain = 84532
            market = "0x00000000000000000000000000000000000000aa"
            start_block = 1
            [[chains]]
            id = 84532
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
        "#;
        let error = Config::parse(&format!("[store]\n{keyring}")).unwrap_err();
        assert!(format!("{error:#}").contains("[gateway]"), "{error:#}");
        Config::parse(&format!("[gateway]\n{keyring}")).unwrap();
    }

    #[test]
    fn ingestion_poll_interval_defaults_to_one_and_must_be_positive() {
        let base = r#"
            [gateway]
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
        "#;
        assert_eq!(Config::parse(base).unwrap().chains[0].poll_interval_secs, 1);
        assert_eq!(
            Config::parse(&format!("{base}\npoll_interval_secs = 5"))
                .unwrap()
                .chains[0]
                .poll_interval_secs,
            5
        );
        for value in ["0", "-1", "1.5", "4294967296"] {
            assert!(Config::parse(&format!("{base}\npoll_interval_secs = {value}")).is_err());
        }
    }

    #[test]
    fn example_config_parses() {
        let cfg = Config::parse(include_str!("../config.example.toml")).unwrap();
        assert_eq!(cfg.chains.len(), 1);
        assert_eq!(cfg.chains[0].id, 810337);
        assert_eq!(cfg.chains[0].name.as_deref(), Some("anvil"));
        assert_eq!(
            cfg.chains[0].public_rpc_url.as_deref(),
            Some("http://127.0.0.1:8545")
        );
        assert_eq!(cfg.chains[0].start_block, 1);
        let a = cfg
            .store
            .as_ref()
            .and_then(|s| s.attachments.as_ref())
            .expect("example enables attachments");
        assert!(cfg.gateway.is_some(), "example enables the gateway");
        assert_eq!(a.max_bytes, 2 << 20);
        assert_eq!(a.max_per_message, 4);
    }

    #[test]
    fn attachments_are_optional_and_bounded() {
        let base = r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
        "#;
        let store_only = format!("{base}\n[store]\n");
        let cfg = Config::parse(&store_only).unwrap();
        assert!(cfg.store.as_ref().unwrap().attachments.is_none());
        assert!(cfg.gateway.is_none());
        // The body cap: the default when unset, the configured value otherwise,
        // and never above the store's hard ceiling or below one envelope header.
        assert_eq!(cfg.store.as_ref().unwrap().body_cap(), DEFAULT_BODY_BYTES);
        let capped = Config::parse(&format!("{base}\n[store]\nmax_body_bytes = 4000\n")).unwrap();
        assert_eq!(capped.store.as_ref().unwrap().body_cap(), 4000);
        for bad in [(1 << 20) + 1, 64] {
            assert!(
                Config::parse(&format!("{base}\n[store]\nmax_body_bytes = {bad}\n")).is_err(),
                "{bad}"
            );
        }
        let too_big = format!(
            "{base}\n[store.attachments]\nmax_bytes = {}\nmax_per_message = 4\n",
            (16 << 20) + 1
        );
        assert!(
            Config::parse(&too_big)
                .unwrap_err()
                .to_string()
                .contains("hard ceiling")
        );
        let zero = format!("{base}\n[store.attachments]\nmax_bytes = 1024\nmax_per_message = 0\n");
        assert!(
            Config::parse(&zero)
                .unwrap_err()
                .to_string()
                .contains("max_per_message")
        );
    }

    #[test]
    fn a_host_plays_at_least_one_role_and_rejects_unknown_sections() {
        let chain = r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
        "#;
        let err = Config::parse(chain).unwrap_err().to_string();
        assert!(err.contains("neither role"), "{err}");
        let err = Config::parse(&format!("{chain}\n[unknown]\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown field"), "{err}");
        // Either role alone is a valid host.
        assert!(
            Config::parse(&format!("{chain}\n[gateway]\n"))
                .unwrap()
                .store
                .is_none()
        );
        assert!(
            Config::parse(&format!("{chain}\n[store]\n"))
                .unwrap()
                .gateway
                .is_none()
        );
        // Peers are base URLs.
        let cfg = Config::parse(&format!(
            "{chain}\n[store]\npeers = [\"https://other.example\"]\n"
        ))
        .unwrap();
        assert_eq!(cfg.store.unwrap().peers, vec!["https://other.example"]);
        let err = Config::parse(&format!("{chain}\n[store]\npeers = [\"other.example\"]\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("store.peers"), "{err}");
    }

    #[test]
    fn native_currency_can_be_configured() {
        let cfg = Config::parse(
            r#"
            [[chains]]
            id = 424242
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
            native_currency = { name = "COIN", symbol = "COIN", decimals = 18 }
            [gateway]
            "#,
        );
        let cfg = cfg.unwrap();
        assert_eq!(cfg.chains[0].native_currency.name, "COIN");
        assert_eq!(cfg.chains[0].native_currency.symbol, "COIN");
        assert_eq!(cfg.chains[0].native_currency.decimals, 18);
    }

    #[test]
    fn native_currency_rejects_invalid_wallet_metadata() {
        let chain = r#"
            [[chains]]
            id = 424242
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
        "#;
        for currency in [
            r#"{ name = "", symbol = "COIN", decimals = 18 }"#.to_string(),
            r#"{ name = " COIN", symbol = "COIN", decimals = 18 }"#.into(),
            r#"{ name = "COIN", symbol = "C OIN", decimals = 18 }"#.into(),
            r#"{ name = "COIN", symbol = "", decimals = 18 }"#.into(),
            r#"{ name = "COIN", symbol = "COIN", decimals = -1 }"#.into(),
            r#"{ name = "COIN", symbol = "COIN", decimals = 256 }"#.into(),
            r#"{ name = "COIN", symbol = "COIN", decimals = 1.5 }"#.into(),
            r#"{ name = "COIN", symbol = "COIN" }"#.into(),
            r#"{ name = "COIN", symbol = "COIN", decimals = 18, typo = true }"#.into(),
            r#"{ name = "COIN\ncoin", symbol = "COIN", decimals = 18 }"#.into(),
            format!(
                r#"{{ name = "{}", symbol = "COIN", decimals = 18 }}"#,
                "x".repeat(65)
            ),
            format!(
                r#"{{ name = "COIN", symbol = "{}", decimals = 18 }}"#,
                "x".repeat(17)
            ),
        ] {
            assert!(
                Config::parse(&format!("{chain}\nnative_currency = {currency}\n[gateway]"))
                    .is_err(),
                "{currency}"
            );
        }
        for decimals in [0, 6, 18, 255] {
            let currency =
                format!(r#"{{ name = "Example coin", symbol = "COIN", decimals = {decimals} }}"#);
            let cfg = Config::parse(&format!("{chain}\nnative_currency = {currency}\n[gateway]"))
                .unwrap();
            assert_eq!(cfg.chains[0].native_currency.decimals, decimals);
        }
    }

    #[test]
    fn published_fields_are_optional() {
        let cfg = Config::parse(
            r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
            [gateway]
        "#,
        )
        .unwrap();
        assert!(cfg.chains[0].public_rpc_url.is_none());
        assert_eq!(cfg.chains[0].native_currency.name, "Ether");
        assert_eq!(cfg.chains[0].native_currency.symbol, "ETH");
        assert_eq!(cfg.chains[0].native_currency.decimals, 18);
        assert_eq!(cfg.chains[0].start_block, 0);
        let err = Config::parse(
            r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            public_rpc_url = "ftp://a"
            contract = "0x0000000000000000000000000000000000000001"
            [gateway]
        "#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("public_rpc_url"), "{err}");
        let cfg = Config::parse(
            r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            public_rpc_url = "/rpc/1"
            contract = "0x0000000000000000000000000000000000000001"
            [gateway]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.chains[0].public_rpc_url.as_deref(), Some("/rpc/1"));
        // "//host/path" would send wallets to another host.
        let err = Config::parse(
            r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            public_rpc_url = "//other.example/rpc"
            contract = "0x0000000000000000000000000000000000000001"
            [gateway]
        "#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("public_rpc_url"), "{err}");
    }

    #[test]
    fn rejects_bad_configs() {
        // The whole chain of causes, as startup prints it.
        let err = |text: &str| format!("{:#}", Config::parse(text).unwrap_err());

        assert!(err("").contains("no chains"));

        let dup = r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
            [[chains]]
            id = 1
            rpc_url = "http://b"
            contract = "0x0000000000000000000000000000000000000002"
            [gateway]
        "#;
        assert!(err(dup).contains("more than once"), "{}", err(dup));

        let bad_addr = r#"
            [[chains]]
            id = 1
            rpc_url = "http://a"
            contract = "0x1234"
            [gateway]
        "#;
        assert!(err(bad_addr).contains("bad contract address"));

        let bad_url = r#"
            [[chains]]
            id = 1
            rpc_url = "ws://a"
            contract = "0x0000000000000000000000000000000000000001"
            [gateway]
        "#;
        assert!(err(bad_url).contains("http(s)"));

        // A typo in a field name must not be silently ignored.
        let typo = r#"
            [[chains]]
            id = 1
            rpc_uri = "http://a"
            contract = "0x0000000000000000000000000000000000000001"
            [gateway]
        "#;
        assert!(err(typo).contains("rpc_uri") || err(typo).contains("unknown field"));
    }
}
