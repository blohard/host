//! Address → verified display name and avatar reference. Only configured RPCs
//! and ENS's fixed batch gateway are contacted, never resolver-supplied URLs.
//! https://docs.ens.domains/resolvers/universal/
mod forward;
pub use forward::resolve;

use crate::{
    chain::Address,
    config::NamesConfig,
    expiring::{NEGATIVE_LIFETIME, SUCCESS_LIFETIME},
    flights::{FlightError, Flights},
    http::{JsonPath, json_error, json_ok},
    rpc,
    server::AppState,
};
use alloy_sol_types::{SolCall, SolError, sol};
use anyhow::{Result, anyhow, ensure};
use axum::{Extension, extract::State, http::StatusCode, response::Response};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, Instant},
};

sol! {
    function reverseWithGateways(bytes lookupAddress, uint256 coinType, string[] gateways) external view returns (string primary, address resolver, address reverseResolver);
    function resolveWithGateways(bytes name, bytes data, string[] gateways) external view returns (bytes result, address resolver);
    function callback(bytes response, bytes extra) external;
    function resolver(bytes32 node) external view returns (address);
    function nameForAddr(address addr) external view returns (string);
    function name(bytes32 node) external view returns (string);
    function addr(bytes32 node) external view returns (address);
    function text(bytes32 node, string key) external view returns (string);
    error OffchainLookup(address sender, string[] urls, bytes callData, bytes4 callbackFunction, bytes extraData);
    error ResolverNotFound(bytes name);
    error ReverseAddressMismatch(string primary, bytes primaryAddress);
}
const UNIVERSAL: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const LEGACY_BASE_RESOLVER: &str = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
// https://github.com/base/basenames#contract-addresses
const BASE_REGISTRY: &str = "0xb94704422c2a1e396835a571837aa5ae53285a95";
// https://docs.ens.domains/registry/reverse/
const BASE_REVERSE_REGISTRAR: &str = "0x0000000000d8e504002cc26e3ec46d81971c1664";

#[derive(Clone, Copy)]
enum CallPolicy {
    Universal,
    Fixed,
    // A name owner may choose an EOA or a resolver returning empty data.
    Resolver,
}

const BATCH_GATEWAY: &str = "https://ccip-v3.ens.xyz";
const MAX_BYTES: usize = 64 << 10;
const MAX_NAME: usize = 255;
const MAX_AVATAR: usize = 2048;
/// Lookups running at once. They share the host's client chain-call slots
/// (32 less ingestion's 4) with receipts and upload checks, so they stop
/// short of filling them: a cold burst of names then leaves posting alone.
const MAX_ACTIVE: usize = 24;
/// Distinct addresses waiting for a lookup; beyond them, busy at once.
const MAX_PENDING: usize = 96;
/// A day's distinct names. An entry is an address, a name of at most 255
/// bytes and an avatar URL of at most 2 KiB; typical ones are a few hundred bytes.
const CACHE_ENTRIES: usize = 100_000;

#[derive(Clone, Debug, Serialize)]
pub struct Entry {
    pub(crate) name: Option<String>,
    source: Option<&'static str>,
    avatar: Option<String>,
    /// A read behind the entry failed, the provider's or the name's own: on
    /// the avatar, or on ENS ahead of a basename. The entry is shown, but
    /// kept only as long as a negative answer, and says so, so a browser does
    /// the same.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    partial: bool,
}
impl Entry {
    fn absent() -> Self {
        Self {
            name: None,
            source: None,
            avatar: None,
            partial: false,
        }
    }
}

/// A day only for a whole found name; a negative or partial answer an hour.
/// A day of staleness is fine for a display name, and matches what the app
/// keeps in each browser.
fn lifetime(entry: &Entry) -> Duration {
    if entry.name.is_some() && !entry.partial {
        SUCCESS_LIFETIME
    } else {
        NEGATIVE_LIFETIME
    }
}
type Answer = Result<Entry, &'static str>;

fn flight_error(error: FlightError) -> &'static str {
    match error {
        FlightError::Busy => "host_busy",
        FlightError::Stopped => "name_unavailable",
    }
}

pub struct Names {
    ethereum: Option<String>,
    base: Option<String>,
    client: rpc::Upstream,
    offchain: reqwest::Client,
    #[cfg(test)]
    gateway_url: String,
    cache: crate::expiring::Expiring<Address, Entry>,
    flights: Flights<Address, Answer>,
    forward_cache: crate::expiring::Expiring<String, forward::Resolved>,
    forward_flights: Flights<String, Result<forward::Resolved, &'static str>>,
    active: Arc<tokio::sync::Semaphore>,
    /// An upstream that failed is left alone until the instant recorded here.
    /// The public endpoints hosts tend to use rate-limit bursts; asking a
    /// failing one once per lookup turns one refusal into a hundred.
    paused: Mutex<HashMap<&'static str, Instant>>,
    pause: Duration,
}

/// How long a failing name upstream is left alone.
const UPSTREAM_PAUSE: Duration = Duration::from_secs(30);

impl Names {
    pub fn cached(&self, address: Address) -> Option<Entry> {
        self.cache.get(&address)
    }

    pub fn new(config: NamesConfig, client: rpc::Upstream) -> Result<Self> {
        Ok(Self {
            ethereum: config.ethereum_upstream,
            base: config.base_upstream,
            client,
            paused: Mutex::new(HashMap::new()),
            pause: UPSTREAM_PAUSE,
            offchain: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            #[cfg(test)]
            gateway_url: BATCH_GATEWAY.into(),
            cache: crate::expiring::Expiring::new(CACHE_ENTRIES),
            flights: Flights::new(MAX_PENDING),
            forward_cache: crate::expiring::Expiring::new(2048),
            forward_flights: Flights::new(MAX_PENDING),
            active: Arc::new(tokio::sync::Semaphore::new(MAX_ACTIVE)),
        })
    }

    /// An address shares one flight. Cancellation detaches a reader; at most
    /// MAX_ACTIVE tasks and a bounded queue continue after a reader disconnects.
    pub fn lookup(self: &Arc<Self>, address: Address) -> impl Future<Output = Answer> + Send {
        self.lookup_admitted(address, None)
    }

    async fn lookup_admitted(
        self: &Arc<Self>,
        address: Address,
        permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Answer {
        if let Some(entry) = self.cache.get(&address) {
            return Ok(entry);
        }
        let service = self.clone();
        self.flights
            .run(address, || self.cache.get(&address).map(Ok), async move {
                service
                    .run_lookup(permit, async {
                        let entry = service.fetch_display_identity(address).await?;
                        service.cache.put(address, entry.clone(), lifetime(&entry));
                        Ok(entry)
                    })
                    .await
            })
            .await
            .map_err(flight_error)?
    }

    /// Run inside the detached flight, keeping both slots through cache publication.
    async fn run_lookup<T: Send>(
        &self,
        permit: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
        work: impl Future<Output = Result<T>> + Send,
    ) -> Result<T, &'static str> {
        let _permit = permit;
        let _active =
            tokio::time::timeout(Duration::from_secs(3), self.active.clone().acquire_owned())
                .await
                .map_err(|_| "host_busy")?
                .map_err(|_| "name_unavailable")?;
        tokio::time::timeout(Duration::from_secs(30), work)
            .await
            .map_err(anyhow::Error::from)
            .and_then(|answer| answer)
            .map_err(|error| {
                // Upstream failures are logged where they pause that upstream.
                tracing::debug!(?error, "name resolution unavailable");
                "name_unavailable"
            })
    }

    async fn fetch_display_identity(&self, address: Address) -> Result<Entry> {
        // ENS failing ahead of a basename leaves it partial, whether the
        // provider or the address's own records failed. Only the provider
        // failing leaves no answer to keep.
        let mut ens_failed = None;
        if let Some(url) = &self.ethereum {
            match self.ask("ens", || self.ens(url, address)).await {
                Ok(Some(found)) => return Ok(found),
                Ok(None) => {}
                Err(error) => ens_failed = Some(error),
            }
        }
        if let Some(url) = &self.base
            && let Some(found) = self
                .ask("basenames", || self.basename(url, address))
                .await?
        {
            // ENS wins: while it failed, the basename is shown as a partial answer.
            return Ok(Entry {
                partial: found.partial || ens_failed.is_some(),
                ..found
            });
        }
        match ens_failed {
            Some(error) if !error.is::<RecordFailed>() => Err(error),
            _ => Ok(Entry::absent()),
        }
    }

    /// One question to `upstream`, unless it failed within the pause: then it
    /// is not asked, and the lookup is unavailable at once. A failure starts
    /// the pause and is logged once.
    async fn ask<T, F: Future<Output = Result<T>>>(
        &self,
        upstream: &'static str,
        question: impl FnOnce() -> F,
    ) -> Result<T> {
        let now = Instant::now();
        let paused = self
            .paused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(upstream)
            .is_some_and(|until| now < *until);
        if paused {
            return Err(anyhow!("{upstream} upstream paused after a failure"));
        }
        match question().await {
            Ok(answer) => Ok(answer),
            Err(error) => {
                // Only the provider failing pauses it.
                if blames_the_provider(&error) && !self.pause.is_zero() {
                    self.paused
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(upstream, now + self.pause);
                    tracing::warn!(upstream, %error, pause_secs = self.pause.as_secs(), "name upstream failed; not asked again until the pause ends");
                }
                Err(error)
            }
        }
    }

    /// `Ok(None)` when the address has no name this host can verify: its
    /// resolver said so or refused, or answered with data this host will not
    /// follow or cannot read. That is the address's own record, cached like
    /// any absent name. `Err(RecordFailed)` when a name's own resolver broke
    /// while running, or the wait for its offchain records timed out; the
    /// caller decides what that means for its answer, and it never pauses an
    /// upstream. Any other `Err` is the provider or the gateway failing, which
    /// pauses it for every address.
    async fn call(
        &self,
        url: &str,
        target: &str,
        data: Vec<u8>,
        policy: CallPolicy,
    ) -> Result<Option<Vec<u8>>> {
        let mut data = data;
        // Oversized data is the record's doing, not the provider's.
        let fits = |hex: &str| hex.len() <= 2 + MAX_BYTES * 2;
        for _ in 0..5 {
            if data.len() > MAX_BYTES {
                return Ok(None);
            }
            let answer = rpc::call_as(
                rpc::Class::Names,
                &self.client,
                url,
                "eth_call",
                json!([{
                "to": target, "data": crate::abi::hex0x(&data), "gas": format!("0x{:x}", rpc::SIM_GAS_CAP)
            }, "latest"]),
                rpc::MAX_RESPONSE,
            )
            .await?;
            if let Some(result) = answer.get("result") {
                let text = result
                    .as_str()
                    .ok_or_else(|| anyhow!("invalid name result"))?;
                if !fits(text) {
                    return Ok(None);
                }
                let bytes =
                    crate::abi::decode_hex(text).ok_or_else(|| anyhow!("invalid name result"))?;
                // Fixed infrastructure returns ABI data even for missing records.
                // A custom resolver's empty answer is only that name's failure.
                ensure!(
                    !bytes.is_empty() || matches!(policy, CallPolicy::Resolver),
                    "empty name result"
                );
                return Ok(Some(bytes));
            }
            let error = &answer["error"];
            let bytes = match rpc::error_data(error) {
                Some(text) if !fits(text) => return Ok(None),
                Some(text) => crate::abi::decode_hex(text),
                None => None,
            };
            let Some(bytes) = bytes else {
                // Without revert bytes, a call that reverted or ran out of the
                // lookup's gas is still this name's record refusing or costing
                // too much. A name's own resolver may also break, which is that
                // name's failure too. Any other error is the provider's.
                if matches!(policy, CallPolicy::Resolver) && resolver_broke(error) {
                    return Err(RecordFailed.into());
                }
                ensure!(
                    rpc::is_revert(error) || out_of_gas(error),
                    "name call failed"
                );
                return Ok(None);
            };
            if !matches!(policy, CallPolicy::Universal)
                || bytes.starts_with(&ResolverNotFound::SELECTOR)
                || bytes.starts_with(&ReverseAddressMismatch::SELECTOR)
            {
                return Ok(None);
            }
            // Any other revert, or an offchain lookup this host will not
            // follow, leaves the address without a verifiable name.
            let Ok(request) = OffchainLookup::abi_decode_validate(&bytes) else {
                return Ok(None);
            };
            if !(target == UNIVERSAL
                && request.sender.as_slice() == crate::abi::parse_address(UNIVERSAL)?
                && request.urls.iter().any(|u| u == BATCH_GATEWAY))
            {
                return Ok(None);
            }
            let Some(response) = self.gateway(&request.callData).await? else {
                return Ok(None);
            };
            let mut callback = callbackCall {
                response: response.into(),
                extra: request.extraData,
            }
            .abi_encode();
            callback[..4].copy_from_slice(request.callbackFunction.as_slice());
            data = callback;
        }
        // A resolver that keeps redirecting has no name to give.
        Ok(None)
    }

    /// The batch gateway's answer, or `None` when it is too large: the
    /// gateway relays what the name's own offchain resolver returned.
    async fn gateway(&self, data: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(test)]
        let gateway_url = &self.gateway_url;
        #[cfg(not(test))]
        let gateway_url = BATCH_GATEWAY;
        let mut response = self
            .offchain
            .post(gateway_url)
            .json(&json!({"sender": UNIVERSAL, "data": crate::abi::hex0x(data)}))
            .send()
            .await
            .map_err(gateway_error)?;
        ensure!(response.status().is_success(), "ENS gateway failed");
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BYTES as u64)
        {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(gateway_error)? {
            if bytes.len() + chunk.len() > MAX_BYTES {
                return Ok(None);
            }
            bytes.extend_from_slice(&chunk);
        }
        let answer: Value = serde_json::from_slice(&bytes)?;
        answer["data"]
            .as_str()
            .and_then(crate::abi::decode_hex)
            .map(Some)
            .ok_or_else(|| anyhow!("invalid ENS gateway answer"))
    }

    async fn ens(&self, url: &str, address: Address) -> Result<Option<Entry>> {
        let call = reverseWithGatewaysCall {
            lookupAddress: address.0.to_vec().into(),
            coinType: alloy_primitives::U256::from(60),
            gateways: vec![BATCH_GATEWAY.into()],
        };
        // Failed records stay an error here: a basename shown in their place
        // is then partial, so a recovered ENS name is not hidden for a day.
        let Some(bytes) = self
            .call(url, UNIVERSAL, call.abi_encode(), CallPolicy::Universal)
            .await?
        else {
            return Ok(None);
        };
        // An answer that does not decode (a name that is not UTF-8), or a name
        // that is not a normalized DNS name, is the owner's record: no name to show.
        let Ok(reverse) = reverseWithGatewaysCall::abi_decode_returns_validate(&bytes) else {
            return Ok(None);
        };
        let primary = reverse.primary;
        if primary.is_empty() {
            return Ok(None);
        }
        let Ok(packet) = dns(&primary) else {
            return Ok(None);
        };
        if !normalized(&primary) {
            return Ok(None);
        }
        let text = textCall {
            node: node(&primary).into(),
            key: "avatar".into(),
        }
        .abi_encode();
        let call = resolveWithGatewaysCall {
            name: packet.into(),
            data: text.into(),
            gateways: vec![BATCH_GATEWAY.into()],
        };
        // A missing or unreadable record is the name having no avatar. A
        // failure, the provider's or the name's own, leaves the name partial.
        let avatar = self
            .call(url, UNIVERSAL, call.abi_encode(), CallPolicy::Universal)
            .await
            .map(|bytes| {
                let resolved =
                    resolveWithGatewaysCall::abi_decode_returns_validate(&bytes?).ok()?;
                textCall::abi_decode_returns_validate(&resolved.result).ok()
            });
        Ok(Some(Entry {
            name: Some(primary),
            source: Some("ens"),
            partial: avatar.is_err(),
            avatar: avatar.ok().flatten().and_then(avatar_url),
        }))
    }

    /// Shared by display names and recipient lookups. The registry, not a
    /// historical default resolver, chooses where a name's records live.
    async fn base_resolver(&self, url: &str, name: &str) -> Result<Option<String>> {
        let bytes = self
            .call(
                url,
                BASE_REGISTRY,
                resolverCall {
                    node: node(name).into(),
                }
                .abi_encode(),
                CallPolicy::Fixed,
            )
            .await?;
        Ok(bytes
            .and_then(|b| resolverCall::abi_decode_returns_validate(&b).ok())
            .filter(|a| !a.is_zero())
            .map(|a| format!("{a:#x}")))
    }

    async fn basename(&self, url: &str, address: Address) -> Result<Option<Entry>> {
        let Some(bytes) = self
            .call(
                url,
                BASE_REVERSE_REGISTRAR,
                nameForAddrCall {
                    addr: alloy_primitives::Address::from(address.0),
                }
                .abi_encode(),
                CallPolicy::Fixed,
            )
            .await?
        else {
            return Ok(None);
        };
        let Ok(mut primary) = nameForAddrCall::abi_decode_returns_validate(&bytes) else {
            return Ok(None);
        };
        // Only an explicitly empty modern record falls back. Invalid records
        // or a provider failure must not resurrect an older primary name.
        if primary.is_empty() {
            let reverse = format!("{}.80002105.reverse", hex::encode(address.0));
            let Some(legacy) = self
                .call(
                    url,
                    LEGACY_BASE_RESOLVER,
                    nameCall {
                        node: node(&reverse).into(),
                    }
                    .abi_encode(),
                    CallPolicy::Fixed,
                )
                .await?
            else {
                return Ok(None);
            };
            let Ok(name) = nameCall::abi_decode_returns_validate(&legacy) else {
                return Ok(None);
            };
            primary = name;
        }
        // Only a normalized DNS name under base.eth counts. Anything else the
        // owner set is their record, not the provider failing.
        if primary.is_empty()
            || dns(&primary).is_err()
            || !primary.ends_with(".base.eth")
            || !normalized(&primary)
        {
            return Ok(None);
        }
        let Some(resolver) = self.base_resolver(url, &primary).await? else {
            return Ok(None);
        };
        let Some(bytes) = absent_if_record_failed(
            self.call(
                url,
                &resolver,
                addrCall {
                    node: node(&primary).into(),
                }
                .abi_encode(),
                CallPolicy::Resolver,
            )
            .await,
        )?
        else {
            return Ok(None);
        };
        if addrCall::abi_decode_returns_validate(&bytes)
            .map_or(true, |forward| forward.as_slice() != address.0)
        {
            return Ok(None);
        }
        // A resolver that breaks here leaves the name verified but partial.
        let avatar = self
            .call(
                url,
                &resolver,
                textCall {
                    node: node(&primary).into(),
                    key: "avatar".into(),
                }
                .abi_encode(),
                CallPolicy::Resolver,
            )
            .await
            .map(|bytes| textCall::abi_decode_returns_validate(&bytes?).ok());
        Ok(Some(Entry {
            name: Some(primary),
            source: Some("basenames"),
            partial: avatar.is_err(),
            avatar: avatar.ok().flatten().and_then(avatar_url),
        }))
    }
}

/// How Base's providers report a name's own resolver breaking while it runs,
/// other than reverting or running out of gas: code -32003 with one of these
/// messages. The owner chooses the resolver, so any of them is the name's own
/// failure: code that halts, a precompile that refuses the call, or code that
/// asks for more memory than allowed. Checked on 2026-10-13 against Coinbase
/// Developer Platform's endpoint, mainnet.base.org and base-rpc.publicnode.com,
/// which agree. Other wording, from these or other providers, stays a provider
/// failure.
const RESOLVER_BREAKS: [&str; 9] = [
    "EVM error: InvalidFEOpcode",
    "EVM error: OpcodeNotFound",
    "EVM error: StackUnderflow",
    "EVM error: StackOverflow",
    "EVM error: InvalidJump",
    "EVM error: OutOfOffset",
    "EVM error: CreateInitCodeSizeLimit",
    "EVM error: PrecompileError",
    "out of memory: memory limit exceeded during memory expansion",
];

fn resolver_broke(error: &Value) -> bool {
    error["code"].as_i64() == Some(-32003)
        && error["message"]
            .as_str()
            .is_some_and(|message| RESOLVER_BREAKS.contains(&message))
}

/// A name's own records failed to answer: its resolver broke while running,
/// or the wait for its offchain records timed out. This host counts that as
/// the name's failure, not the provider's, so `ask` never pauses an upstream
/// for it, and each caller decides what it means for its answer.
#[derive(Debug)]
struct RecordFailed;

impl std::fmt::Display for RecordFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the name's records failed to answer")
    }
}

impl std::error::Error for RecordFailed {}

/// Whether a failed question says the provider is failing. This host's own
/// limiter turning the call away says nothing about the provider, and a
/// name's own records failing is that name's alone.
fn blames_the_provider(error: &anyhow::Error) -> bool {
    !error.is::<RecordFailed>()
        && !matches!(
            error.downcast_ref::<rpc::RpcError>(),
            Some(rpc::RpcError::Busy)
        )
}

/// Turns a name's own records failing into `Ok(None)`: for a call the answer
/// depends on, such a name has nothing this host can verify, like one whose
/// resolver refused. Any other error passes through.
fn absent_if_record_failed(answer: Result<Option<Vec<u8>>>) -> Result<Option<Vec<u8>>> {
    match answer {
        Err(error) if error.is::<RecordFailed>() => Ok(None),
        answer => answer,
    }
}

/// ENS's batch gateway waits about 30 seconds for a name's own offchain server
/// (measured 2026-10-13), longer than this host waits for the gateway, so a
/// name owner can make the host time out at will. A timeout, before or during
/// the gateway's answer, is therefore counted as that name's failure, though
/// it does not prove the owner's server was slow. That is a choice: if the
/// gateway hangs for everyone, each address loses its ENS name for an hour,
/// instead of ENS pausing. Any other error reaching the gateway still counts
/// as the gateway failing.
fn gateway_error(error: reqwest::Error) -> anyhow::Error {
    if error.is_timeout() {
        RecordFailed.into()
    } else {
        anyhow!(error.without_url())
    }
}

/// A call that ran out of the lookup's gas allowance: the record's cost, such
/// as a very long name, not the provider failing. Clients word it differently
/// ("out of gas", "OutOfGas"), in the message or the data.
fn out_of_gas(error: &Value) -> bool {
    [&error["message"], &error["data"]]
        .into_iter()
        .filter_map(Value::as_str)
        .any(|text| {
            let text = text.to_ascii_lowercase();
            text.contains("out of gas") || text.contains("outofgas")
        })
}

fn node(name: &str) -> [u8; 32] {
    let mut node = [0u8; 32];
    for label in name.rsplit('.') {
        let hash = crate::keccak::hash(&[label.as_bytes()]).0;
        node = crate::keccak::hash(&[&node, &hash]).0;
    }
    node
}
fn dns(name: &str) -> Result<Vec<u8>> {
    ensure!(
        !name.is_empty() && name.len() <= MAX_NAME && !name.chars().any(char::is_control),
        "invalid name"
    );
    let mut packet = Vec::new();
    for label in name.split('.') {
        ensure!(
            !label.is_empty() && label.len() <= 255,
            "invalid name label"
        );
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    Ok(packet)
}
/// ENS's normalizer (ENSIP-15), built on first use: about 40 ms and 40 MB,
/// then a few microseconds a name. Its free functions rebuild it every call.
static NORMALIZER: LazyLock<ens_normalize_rs::EnsNameNormalizer> = LazyLock::new(Default::default);

/// Whether `name` is already in the normalized form ENS resolves. The
/// registries accept any string, so a capital, an invisible character or a
/// label mixing alphabets could otherwise pass for another account's name.
fn normalized(name: &str) -> bool {
    NORMALIZER.normalize(name).is_ok_and(|n| n == name)
}
fn avatar_url(value: String) -> Option<String> {
    if value.len() > MAX_AVATAR {
        return None;
    }
    if let Some(path) = value.strip_prefix("ipfs://") {
        return Some(format!(
            "https://ipfs.io/ipfs/{}",
            path.strip_prefix("ipfs/").unwrap_or(path)
        ));
    }
    // As the parser writes it back: scheme and host in lower case, the
    // path's characters encoded one way. Not every spelling of one URL comes
    // out the same, but the owner's spelling is not passed through.
    let url = reqwest::Url::parse(&value).ok()?;
    (url.scheme() == "https" && url.host_str().is_some()).then(|| url.to_string())
}

/// Reverse-name cache hits do not spend a visitor allowance. Path extraction
/// is shared with the handler, so this shortcut accepts the same addresses.
pub async fn cached_response(
    request: &mut axum::extract::Request,
    state: &AppState,
) -> Option<Response> {
    use axum::RequestExt as _;
    let names = state.gateway.as_ref()?.names.as_ref()?;
    let JsonPath(address) = request.extract_parts::<JsonPath<Address>>().await.ok()?;
    Some(json_ok(json!(names.cached(address)?)))
}

pub async fn lookup(
    State(state): State<Arc<AppState>>,
    JsonPath(address): JsonPath<Address>,
    permit: Option<Extension<Arc<tokio::sync::OwnedSemaphorePermit>>>,
) -> Response {
    let Some(names) = state.gateway.as_ref().and_then(|g| g.names.as_ref()) else {
        return json_ok(json!(Entry::absent()));
    };
    match names.lookup_admitted(address, permit.map(|p| p.0)).await {
        Ok(entry) => json_ok(json!(entry)),
        Err(code) => json_error(StatusCode::SERVICE_UNAVAILABLE, code),
    }
}

#[cfg(test)]
mod tests;
