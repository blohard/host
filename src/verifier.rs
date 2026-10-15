//! The chain side of publishing: is this upload anchored, and by whom?
//!
//! Private node access for content anchoring and live transaction services.
//! Gateway-backed uploads prefer the accepted index; store-only hosts call
//! `getMessage` at their configured confirmation depth. Pending uploads use
//! transaction calldata and receipts, with signature authentication before a
//! claimed author can occupy holding slots. `acceptance` chooses the source of
//! accepted facts; `publish` keeps provisional evidence separate.
//!
//! "Confirmed" here means `confirmations` blocks behind the sampled head, not
//! consensus finality. Chain id scopes every proof to its Board deployment.

use crate::abi::{
    content_hash_from_return, get_message_calldata, post_by_sig_digest, post_by_sig_parts,
    reply_gate_calldata,
};
use crate::abi::{hex0x, parse_address, parse_quantity, selector};
use crate::config::Config;
use crate::keccak;
use crate::rpc::{self, Budget, RpcError, Upstream, result_bytes};
use anyhow::{Context as _, Result, anyhow, bail, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt, time::Duration};

/// How the CDN decides whether a publish is anchored on-chain.
pub enum Verifier {
    /// Accept everything (development mode — no chain running).
    AllowAll,
    /// Verify against the board of whichever configured chain the publisher
    /// names.
    Chains {
        client: Upstream,
        chains: BTreeMap<u64, Chain>,
        /// Chain ids in configured order. The status lists chains this way,
        /// and the app opens the first one, so the operator chooses the default.
        order: Vec<u64>,
    },
}

/// One configured deployment.
pub struct Chain {
    pub name: Option<String>,
    /// JSON-RPC endpoint; may embed an API key, so never surface it.
    pub url: String,
    /// 20-byte board contract address.
    pub contract: [u8; 20],
    /// Browser-safe RPC URL, if the operator chose to publish one.
    pub public_rpc_url: Option<String>,
    /// Block the board was deployed in.
    pub start_block: u64,
    pub index_start_block: u64,
    /// Blocks behind the head treated as accepted.
    pub confirmations: u64,
    /// Delay between ingestion polls once caught up.
    pub poll_interval: Duration,
    /// The suggested blocking setup, if configured.
    pub blocking: Option<Blocking>,
    pub key_gates: Option<KeyGates>,
    pub closed_gate: Option<[u8; 20]>,
    /// Receipt checks in flight, shared by identical requests and never kept
    /// once answered (services::transaction).
    pub receipts: crate::services::Receipts,
}

/// Verified gate addresses published to clients, scoped to this Board.
#[derive(Debug, Clone)]
pub struct KeyGates {
    pub holders: Option<[u8; 20]>,
    pub holders_blocklist: Option<[u8; 20]>,
}

/// A shared author-blocklist gate and the registry list it reads.
#[derive(Debug, Clone)]
pub struct Blocking {
    pub gate: [u8; 20],
    pub registry: [u8; 20],
    pub list: String,
}

/// Why a publish was refused. The HTTP layer maps each variant to a status.
#[derive(Debug, Clone)]
pub enum VerifyError {
    /// The request named a chain this host is not configured for.
    UnsupportedChain(u64),
    /// The chain says the message does not exist or commits to other bytes.
    NotAnchored(String),
    /// The chain's RPC could not be reached or answered unintelligibly.
    Rpc(String),
    /// The accepted index is starting or has stopped on an inconsistency.
    IndexUnavailable,
    /// Too many calls to the node are out; try again shortly.
    Busy,
    /// The provider requested a pause; preserve its wait through upload checks.
    RateLimited(Duration),
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedChain(id) => write!(f, "chain {id} is not served by this host"),
            Self::NotAnchored(msg) | Self::Rpc(msg) => f.write_str(msg),
            Self::IndexUnavailable => {
                f.write_str("the chain index is unavailable; try again shortly")
            }
            Self::Busy => f.write_str("the host is busy; try again shortly"),
            Self::RateLimited(wait) => RpcError::RateLimited(*wait).fmt(f),
        }
    }
}

impl std::error::Error for VerifyError {}

impl From<RpcError> for VerifyError {
    fn from(e: RpcError) -> Self {
        match e {
            RpcError::Busy => Self::Busy,
            RpcError::RateLimited(wait) => Self::RateLimited(wait),
            other => Self::Rpc(other.to_string()),
        }
    }
}

/// What a message commits to, as read from the board.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageInfo {
    pub author: [u8; 20],
    pub content_hash: [u8; 32],
    /// Deleted by its author or removed by its parent's author. The host takes
    /// no more content for such a message.
    pub withdrawn: bool,
}

/// A transaction as the chain reports it (`eth_getTransactionByHash`).
#[derive(Debug, Clone)]
pub struct TxInfo {
    pub from: [u8; 20],
    pub to: Option<[u8; 20]>,
    pub input: Vec<u8>,
    /// `None` while the transaction is still pending.
    pub block_number: Option<u64>,
}

/// A mined transaction's outcome (`eth_getTransactionReceipt`).
#[derive(Debug, Clone)]
pub struct ReceiptInfo {
    pub succeeded: bool,
    /// The id from the board's `Posted` event, if this transaction posted.
    pub message_id: Option<u64>,
}

/// The address a 65-byte `r‖s‖v` signature over `digest` recovers to, with the
/// same refusals as the Board's ECDSA (a high-s twin, a bad v, wrong length).
pub fn recover_signer(digest: &[u8; 32], sig: &[u8]) -> Option<[u8; 20]> {
    use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
    if sig.len() != 65 {
        return None;
    }
    // The Board hands v to ecrecover as is, which only knows 27 and 28.
    let v = match sig[64] {
        27 | 28 => sig[64] - 27,
        _ => return None,
    };
    let signature = Signature::from_slice(&sig[..64]).ok()?;
    if signature.normalize_s().is_some() {
        return None; // the malleable twin: refused, as OpenZeppelin refuses it
    }
    let key =
        VerifyingKey::recover_from_prehash(digest, &signature, RecoveryId::from_byte(v)?).ok()?;
    let point = key.to_encoded_point(false);
    Some(
        keccak::hash(&[&point.as_bytes()[1..]]).0[12..]
            .try_into()
            .expect("20 bytes"),
    )
}

/// What a pending `postBySig` transaction says about its author.
#[derive(Debug, PartialEq, Eq)]
pub enum SignedAuthor {
    /// Not a `postBySig` call: the author is the transaction's sender.
    NotBySig,
    /// The named author's signature does not verify; the Board will refuse it.
    Invalid,
    /// The named author stands behind the post.
    Author([u8; 20]),
}

#[derive(Debug)]
pub struct ChainMismatch {
    expected: u64,
    reported: u64,
}

impl std::fmt::Display for ChainMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "chain id mismatch: config says {} but its endpoint reports {}",
            self.expected, self.reported
        )
    }
}
impl std::error::Error for ChainMismatch {}

impl Verifier {
    /// Build from a validated config (Config::validate holds the rules)
    /// without network access. Indexed hosts verify each endpoint in the
    /// follower; store-only hosts call [`Verifier::check_chains`] before
    /// serving.
    pub fn from_config(cfg: &Config, client: reqwest::Client) -> Result<Self> {
        let client = Upstream::new(
            client,
            Budget::new(
                cfg.upstream.calls_per_minute,
                cfg.upstream.reserved_for_ingestion,
            ),
        );
        let mut chains = BTreeMap::new();
        let mut order = Vec::new();
        for c in &cfg.chains {
            let index_start_block = c.index_start_block.unwrap_or_else(|| {
                if c.blocking.is_some() {
                    0
                } else {
                    c.start_block
                }
            });
            if cfg.gateway.is_some() && c.index_start_block.is_none() && index_start_block == 0 {
                // The loud warning belongs to a genuinely fresh index; the
                // follower emits it when it finds no checkpoint to resume from.
                tracing::info!(
                    chain = c.id,
                    name = c.name.as_deref().unwrap_or("-"),
                    "index_start_block is not configured: an existing index resumes from its checkpoint, a fresh one would scan from genesis"
                );
            }
            let chain = Chain {
                receipts: Default::default(),
                name: c.name.clone(),
                url: c.rpc_url.clone(),
                contract: parse_address(&c.contract)?,
                public_rpc_url: c.public_rpc_url.clone(),
                start_block: c.start_block,
                index_start_block,
                confirmations: c.confirmations,
                poll_interval: Duration::from_secs(u64::from(c.poll_interval_secs)),
                closed_gate: c.closed_gate.as_deref().map(parse_address).transpose()?,
                key_gates: c
                    .key_gates
                    .as_ref()
                    .map(|g| {
                        Ok::<_, anyhow::Error>(KeyGates {
                            holders: g.holders.as_deref().map(parse_address).transpose()?,
                            holders_blocklist: g
                                .holders_blocklist
                                .as_deref()
                                .map(parse_address)
                                .transpose()?,
                        })
                    })
                    .transpose()?,
                blocking: c
                    .blocking
                    .as_ref()
                    .map(|b| {
                        Ok::<_, anyhow::Error>(Blocking {
                            gate: parse_address(&b.gate)?,
                            registry: parse_address(&b.registry)?,
                            list: b.list.clone(),
                        })
                    })
                    .transpose()?,
            };
            chains.insert(c.id, chain);
            order.push(c.id);
        }
        Ok(Self::Chains {
            client,
            chains,
            order,
        })
    }

    pub fn mode(&self) -> &'static str {
        match self {
            Self::AllowAll => "allow-all (dev)",
            Self::Chains { .. } => "eth-rpc",
        }
    }

    /// The configured chains, in configured order. Callers must omit private
    /// RPC URLs from public responses.
    pub fn chains(&self) -> Vec<(u64, &Chain)> {
        match self {
            Self::AllowAll => Vec::new(),
            Self::Chains { chains, order, .. } => order
                .iter()
                .filter_map(|id| chains.get(id).map(|c| (*id, c)))
                .collect(),
        }
    }

    /// Configuration only; this does not establish endpoint identity or index readiness.
    pub fn config(&self, chain_id: u64) -> Option<&Chain> {
        match self {
            Self::AllowAll => None,
            Self::Chains { chains, .. } => chains.get(&chain_id),
        }
    }

    /// Configured transport and chain; callers still check identity and readiness.
    pub(crate) fn chain(&self, chain_id: u64) -> Result<(&Upstream, &Chain), VerifyError> {
        match self {
            Self::AllowAll => None,
            Self::Chains { client, chains, .. } => chains.get(&chain_id).map(|c| (client, c)),
        }
        .ok_or(VerifyError::UnsupportedChain(chain_id))
    }

    /// The metered transport to the configured chains; none in development mode.
    pub fn upstream(&self) -> Option<&Upstream> {
        match self {
            Self::AllowAll => None,
            Self::Chains { client, .. } => Some(client),
        }
    }

    /// Store-only hosts verify endpoints before serving. Indexed hosts perform
    /// the same check per chain in the follower, retrying transport failures.
    pub async fn check_chains(&self) -> Result<()> {
        for (id, _) in self.chains() {
            self.check_chain(id, rpc::Class::Ingestion).await?;
        }
        Ok(())
    }

    pub async fn check_chain(&self, id: u64, class: rpc::Class) -> Result<()> {
        let (client, chain) = self.chain(id)?;
        let resp = rpc::call_as(
            class,
            client,
            &chain.url,
            "eth_chainId",
            json!([]),
            rpc::MAX_RESPONSE,
        )
        .await
        .with_context(|| format!("chain {id}"))?;
        if let Some(err) = resp.get("error") {
            bail!("chain {id}: eth_chainId failed: {err}");
        }
        let reported = resp
            .get("result")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("chain {id}: eth_chainId response missing result"))?;
        let reported = parse_quantity(reported).map_err(|e| anyhow!("chain {id}: {e}"))?;
        if reported != id {
            return Err(ChainMismatch {
                expected: id,
                reported,
            }
            .into());
        }
        tracing::info!(
            chain = id,
            name = chain.name.as_deref().unwrap_or("-"),
            contract = %hex0x(&chain.contract),
            confirmations = chain.confirmations,
            "chain endpoint verified"
        );
        Ok(())
    }

    /// Authenticate the author a pending `postBySig` transaction names, the way
    /// the Board will at inclusion: the signature recovers to the author, or
    /// the author has code and accepts it through ERC-1271. A host that took
    /// the calldata's word for it would let anyone fill another author's holding
    /// slots with transactions that later revert.
    pub async fn signed_author(
        &self,
        chain_id: u64,
        input: &[u8],
    ) -> Result<SignedAuthor, VerifyError> {
        let Some((words, sig)) = post_by_sig_parts(input) else {
            return Ok(SignedAuthor::NotBySig);
        };
        let author: [u8; 20] = words[0][12..].try_into().expect("20 bytes");
        let (client, chain) = self.chain(chain_id)?;
        let digest = post_by_sig_digest(chain_id, &chain.contract, &words);
        if recover_signer(&digest, sig) == Some(author) {
            return Ok(SignedAuthor::Author(author));
        }
        // A contract account is asked itself, as the Board asks it: the same
        // calldata, from the Board's address, and the answer must be the
        // magic value as a full word (trailing bytes ignored, as the Board
        // copies 32 and compares).
        let code = rpc::call(
            client,
            &chain.url,
            "eth_getCode",
            json!([hex0x(&author), "latest"]),
        )
        .await?;
        if let Some(error) = code.get("error") {
            return Err(VerifyError::Rpc(format!("eth_getCode failed: {error}")));
        }
        if result_bytes(&code)?.is_empty() {
            return Ok(SignedAuthor::Invalid);
        }
        let data = crate::abi::is_valid_signature_calldata(digest, sig);
        // Code the uploader chose runs here, so the call carries the same
        // ceiling as contract-wallet/name calls; Board admission and post estimates
        // use a smaller bound. The Board itself
        // gives a signer no more than the post's gas.
        let resp = rpc::call(
            client,
            &chain.url,
            "eth_call",
            json!([{
                "from": hex0x(&chain.contract),
                "to": hex0x(&author),
                "data": hex0x(&data),
                "gas": format!("0x{:x}", rpc::SIM_GAS_CAP),
            }, "latest"]),
        )
        .await?;
        if let Some(error) = resp.get("error") {
            return if rpc::is_revert(error) {
                Ok(SignedAuthor::Invalid)
            } else {
                Err(VerifyError::Rpc(format!(
                    "isValidSignature failed: {error}"
                )))
            };
        }
        let mut magic_word = [0u8; 32];
        magic_word[..4].copy_from_slice(&selector("isValidSignature(bytes32,bytes)"));
        let bytes = result_bytes(&resp)?;
        let magic = bytes.len() >= 32 && bytes[..32] == magic_word;
        Ok(if magic {
            SignedAuthor::Author(author)
        } else {
            SignedAuthor::Invalid
        })
    }

    /// The board contract on `chain_id`, if served.
    pub fn contract(&self, chain_id: u64) -> Option<[u8; 20]> {
        self.chain(chain_id).ok().map(|(_, c)| c.contract)
    }

    /// The newest block this host treats as accepted: head minus `confirmations`.
    pub async fn confirmed_block(&self, chain_id: u64) -> Result<u64, VerifyError> {
        let (client, chain) = self.chain(chain_id)?;
        let head = rpc::head(client, &chain.url, rpc::Class::Client).await?;
        Ok(head.saturating_sub(chain.confirmations))
    }

    /// `getMessage(message_id)` as of `block` (`None` = latest): the author and
    /// content hash, or `NotAnchored` if the board has no message at that block.
    pub async fn message_at(
        &self,
        chain_id: u64,
        message_id: u64,
        block: Option<u64>,
    ) -> Result<MessageInfo, VerifyError> {
        let (client, chain) = self.chain(chain_id)?;
        let tag = match block {
            Some(n) => json!(format!("0x{n:x}")),
            None => json!("latest"),
        };
        let call = |data: Vec<u8>| {
            let params = json!([{ "to": hex0x(&chain.contract), "data": hex0x(&data) }, tag]);
            rpc::call(client, &chain.url, "eth_call", params)
        };
        let resp = call(get_message_calldata(message_id)).await?;

        if let Some(err) = resp.get("error") {
            // A revert means the board has no such message at that block.
            // Anything else is the provider, not the board: rate limits and
            // outages must stay retryable, not read as a missing message.
            if !rpc::is_revert(err) {
                return Err(VerifyError::Rpc(format!(
                    "eth_call for message {message_id} on chain {chain_id} failed: {err}"
                )));
            }
            return Err(VerifyError::NotAnchored(format!(
                "chain {chain_id} has no message {message_id}{} (eth_call failed: {err})",
                block
                    .map(|b| format!(" as of block {b}"))
                    .unwrap_or_default()
            )));
        }
        let raw = result_bytes(&resp)?;
        let content_hash =
            content_hash_from_return(&raw).map_err(|e| VerifyError::Rpc(e.to_string()))?;
        let mut author = [0u8; 20];
        author.copy_from_slice(&raw[12..32]);
        // The gate, at the same block: getMessage leaves it out. The message
        // exists, so any failure here is the provider's and stays retryable.
        let resp = call(reply_gate_calldata(message_id)).await?;
        if let Some(err) = resp.get("error") {
            return Err(VerifyError::Rpc(format!(
                "eth_call for the gate of message {message_id} on chain {chain_id} failed: {err}"
            )));
        }
        let raw = result_bytes(&resp)?;
        let gate: [u8; 20] = raw
            .get(12..32)
            .and_then(|g| g.try_into().ok())
            .ok_or_else(|| {
                VerifyError::Rpc(format!(
                    "unexpected replyGate return size: {} bytes",
                    raw.len()
                ))
            })?;
        Ok(MessageInfo {
            author,
            content_hash,
            withdrawn: crate::index::types::withdrawn(gate),
        })
    }

    /// Look up a transaction, pending or mined. `Ok(None)` if the chain does
    /// not know it.
    pub async fn transaction(
        &self,
        chain_id: u64,
        tx_hash: [u8; 32],
    ) -> Result<Option<TxInfo>, VerifyError> {
        let (client, chain) = self.chain(chain_id)?;
        let resp = rpc::call(
            client,
            &chain.url,
            "eth_getTransactionByHash",
            json!([hex0x(&tx_hash)]),
        )
        .await?;
        let Some(tx) = rpc::optional_result(&resp)? else {
            return Ok(None);
        };
        let decoded = || -> Result<TxInfo> {
            use crate::abi::{quantity, text};
            use crate::chain::{Hash, Hex};
            ensure!(
                text(&tx["hash"])?.parse::<Hash>()? == Hex(tx_hash),
                "wrong transaction"
            );
            let from = parse_address(text(&tx["from"])?)?;
            let to = match tx.get("to") {
                Some(Value::Null) => None, // contract creation
                Some(value) => Some(parse_address(text(value)?)?),
                None => bail!("transaction missing to"),
            };
            let input = crate::abi::decode_hex(text(&tx["input"])?)
                .ok_or_else(|| anyhow!("invalid transaction input"))?;
            let block_number = match tx.get("blockNumber") {
                Some(Value::Null) => None, // still pending
                Some(value) => Some(quantity(value)?),
                None => bail!("transaction missing blockNumber"),
            };
            Ok(TxInfo {
                from,
                to,
                input,
                block_number,
            })
        };
        decoded()
            .map(Some)
            .map_err(|error| VerifyError::Rpc(error.to_string()))
    }

    /// A mined transaction's receipt, with the posted message id if the board
    /// emitted `Posted`. `Ok(None)` while pending.
    pub async fn receipt(
        &self,
        chain_id: u64,
        tx_hash: [u8; 32],
    ) -> Result<Option<ReceiptInfo>, VerifyError> {
        let (client, chain) = self.chain(chain_id)?;
        let resp = rpc::call(
            client,
            &chain.url,
            "eth_getTransactionReceipt",
            json!([hex0x(&tx_hash)]),
        )
        .await?;
        let Some(value) = rpc::optional_result(&resp)? else {
            return Ok(None);
        };
        let decoded = || -> Result<ReceiptInfo> {
            use crate::chain::Hex;
            use crate::index::model::Event;
            let receipt = crate::abi::Receipt::parse(value, Hex(tx_hash))?;
            // Uploads need the id, not event timestamps. Promotion still
            // verifies its commitment through the accepted index or Board.
            let events = receipt.events(Hex(chain.contract), &[], 0)?;
            let message_id = events.into_iter().find_map(|event| match event {
                Event::Posted { message } => Some(message.id),
                _ => None,
            });
            Ok(ReceiptInfo {
                succeeded: receipt.succeeded,
                message_id,
            })
        };
        decoded()
            .map(Some)
            .map_err(|error| VerifyError::Rpc(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChainConfig;
    use crate::keccak::Digest;
    use crate::mock_node::{self, Node};

    const CONTRACT_A: &str = "0x000000000000000000000000000000000000aaaa";
    const CONTRACT_B: &str = "0x000000000000000000000000000000000000bbbb";

    fn config(chains: &[(u64, &str, &str)]) -> Config {
        Config {
            // A config needs a role to be valid; the gateway changes nothing here.
            gateway: Some(crate::config::GatewayConfig::default()),
            ..Config::for_test(
                chains
                    .iter()
                    .map(|(id, url, contract)| ChainConfig::for_test(*id, url, contract))
                    .collect(),
            )
        }
    }

    fn verifier(chains: &[(u64, &str, &str)]) -> Result<Verifier> {
        build(&config(chains))
    }

    /// A config checked as it loads, then built.
    fn build(cfg: &Config) -> Result<Verifier> {
        cfg.validate()?;
        Verifier::from_config(cfg, reqwest::Client::new())
    }

    /// The one-call form the tests exercise: Ok if `digest` is what message
    /// `message_id` on `chain_id` commits to at the latest block.
    async fn verify(
        v: &Verifier,
        digest: &Digest,
        chain_id: u64,
        message_id: u64,
    ) -> Result<(), VerifyError> {
        match v.message_at(chain_id, message_id, None).await? {
            m if m.content_hash == digest.0 => Ok(()),
            m => Err(VerifyError::NotAnchored(format!(
                "content hash mismatch: chain {chain_id} message {message_id} commits to 0x{}, upload hashes to {}",
                hex::encode(m.content_hash),
                digest.to_hex(),
            ))),
        }
    }

    #[test]
    fn key_gate_configuration_is_optional_and_rejects_invalid_addresses() {
        use crate::config::{BlockingConfig, KeyGatesConfig};
        let mut cfg = config(&[(1, "http://localhost:8545", CONTRACT_A)]);
        assert!(build(&cfg).unwrap().chains()[0].1.key_gates.is_none());
        cfg.chains[0].blocking = Some(BlockingConfig {
            gate: "0x3333333333333333333333333333333333333333".into(),
            registry: CONTRACT_A.into(),
            list: "blocked".into(),
        });
        cfg.chains[0].key_gates = Some(KeyGatesConfig {
            holders: Some(CONTRACT_A.into()),
            holders_blocklist: Some(CONTRACT_B.into()),
        });
        let v = build(&cfg).unwrap();
        let gates = v.chains()[0].1.key_gates.as_ref().unwrap();
        assert_eq!(gates.holders, Some(parse_address(CONTRACT_A).unwrap()));
        assert_eq!(
            gates.holders_blocklist,
            Some(parse_address(CONTRACT_B).unwrap())
        );
        for collision in [CONTRACT_A, "0x3333333333333333333333333333333333333333"] {
            cfg.chains[0].key_gates.as_mut().unwrap().holders_blocklist = Some(collision.into());
            assert!(build(&cfg).is_err());
        }
        for field in [true, false] {
            for invalid in [
                "not-an-address",
                "0x0000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
            ] {
                let gates = cfg.chains[0].key_gates.as_mut().unwrap();
                gates.holders = Some(if field { invalid } else { CONTRACT_A }.into());
                gates.holders_blocklist = Some(if field { CONTRACT_B } else { invalid }.into());
                assert!(build(&cfg).is_err());
            }
        }
        cfg.chains[0].key_gates.as_mut().unwrap().holders_blocklist = Some(CONTRACT_B.into());
        for reserved in [
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000002",
        ] {
            cfg.chains[0].blocking.as_mut().unwrap().gate = reserved.into();
            let error = format!(
                "{:#}",
                build(&cfg)
                    .err()
                    .expect("a reserved blocking gate is refused")
            );
            assert!(error.contains("blocking gate"), "{reserved}: {error}");
        }
        cfg.chains[0].blocking = None;
        assert!(build(&cfg).is_err());
    }

    #[test]
    fn from_config_rejects_bad_input() {
        assert!(verifier(&[]).is_err());
        assert!(verifier(&[(1, "http://a", "0x12")]).is_err());
        let v = verifier(&[(1, "http://a", CONTRACT_A), (2, "http://b", CONTRACT_B)]).unwrap();
        assert_eq!(v.mode(), "eth-rpc");
        let ids: Vec<u64> = v.chains().iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![1, 2]);
        // The operator's order is the published order, whatever the ids say.
        let v = verifier(&[(2, "http://b", CONTRACT_B), (1, "http://a", CONTRACT_A)]).unwrap();
        let ids: Vec<u64> = v.chains().iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![2, 1]);
        assert!(verifier(&[(1, "http://a", CONTRACT_A), (1, "http://b", CONTRACT_B)]).is_err());
        assert_eq!(v.contract(2), Some(parse_address(CONTRACT_B).unwrap()));
        assert_eq!(v.contract(3), None);
    }

    #[tokio::test]
    async fn dev_mode_has_no_chain_verification() {
        let v = Verifier::AllowAll;
        assert_eq!(v.mode(), "allow-all (dev)");
        assert!(v.chains().is_empty());
        v.check_chains().await.unwrap();
        assert!(matches!(
            v.message_at(0, 0, None).await,
            Err(VerifyError::UnsupportedChain(0))
        ));
        assert!(matches!(
            v.confirmed_block(0).await,
            Err(VerifyError::UnsupportedChain(0))
        ));
        assert!(matches!(
            v.transaction(0, [0; 32]).await,
            Err(VerifyError::UnsupportedChain(0))
        ));
    }

    #[tokio::test]
    async fn only_a_revert_means_no_such_message() {
        let node = Node::new(1, CONTRACT_A, b"blohard on chain 1".to_vec());
        node.messages.store(1, std::sync::atomic::Ordering::Relaxed);
        let a = mock_node::spawn(node).await;
        let v = verifier(&[(1, &a.url, CONTRACT_A)]).unwrap();
        // The board reverts for an id it has never issued: not anchored.
        assert!(matches!(
            v.message_at(1, 7, None).await,
            Err(VerifyError::NotAnchored(_))
        ));
        // A provider refusing the call says nothing about the message.
        *a.node.simulation_error.lock().unwrap() =
            Some(json!({"code": -32005, "message": "rate limited"}));
        match v.message_at(1, 1, None).await {
            Err(VerifyError::RateLimited(wait)) => assert_eq!(wait, Duration::from_secs(2)),
            other => panic!("expected a retryable error, got {other:?}"),
        }
        // After the endpoint cooldown, a revert is still a missing message.
        v.chain(1).unwrap().0.expire_cooldown(&a.url);
        *a.node.simulation_error.lock().unwrap() =
            Some(json!({"code": 3, "message": "execution reverted", "data": "0x"}));
        assert!(matches!(
            v.message_at(1, 1, None).await,
            Err(VerifyError::NotAnchored(_))
        ));
    }

    #[tokio::test]
    async fn verifies_per_chain_against_nodes() {
        let a = mock_node::spawn(Node::new(1, CONTRACT_A, b"blohard on chain 1".to_vec())).await;
        let b = mock_node::spawn(Node::new(2, CONTRACT_B, b"blohard on chain 2".to_vec())).await;
        let v = verifier(&[(1, &a.url, CONTRACT_A), (2, &b.url, CONTRACT_B)]).unwrap();
        v.check_chains().await.unwrap();

        let on1 = keccak::hash(&[b"blohard on chain 1"]);
        let on2 = keccak::hash(&[b"blohard on chain 2"]);

        // Each chain's bytes verify on that chain and nowhere else: the
        // request is routed to the right endpoint and contract.
        verify(&v, &on1, 1, 7).await.unwrap();
        verify(&v, &on2, 2, 7).await.unwrap();
        verify(&v, &on1, 1, u64::MAX).await.unwrap();
        match verify(&v, &on1, 2, 7).await.unwrap_err() {
            VerifyError::NotAnchored(msg) => {
                assert!(msg.contains("content hash mismatch"), "{msg}");
                assert!(msg.contains(&hex::encode(on2.0)), "{msg}");
                assert!(msg.contains(&on1.to_hex()), "{msg}");
            }
            other => panic!("expected NotAnchored, got {other:?}"),
        }
        // The author comes back too: the mock's message i is by 0x…0i.
        let m = v.message_at(1, 7, None).await.unwrap();
        assert_eq!(m.author[19], 7);
        assert_eq!(m.content_hash, on1.0);

        // A chain this host does not serve.
        match verify(&v, &on1, 3, 7).await.unwrap_err() {
            VerifyError::UnsupportedChain(3) => {}
            other => panic!("expected UnsupportedChain(3), got {other:?}"),
        }
        assert!(
            VerifyError::UnsupportedChain(3)
                .to_string()
                .contains("chain 3")
        );

        // Nonexistent message (revert).
        match verify(&v, &on1, 1, 0).await.unwrap_err() {
            VerifyError::NotAnchored(msg) => assert!(msg.contains("no message 0"), "{msg}"),
            other => panic!("expected NotAnchored, got {other:?}"),
        }

        // Unreachable node: an Rpc error, not a panic — and chain 2 still works.
        let a_url = a.url.clone();
        a.stop().await;
        match verify(&v, &on1, 1, 7).await.unwrap_err() {
            VerifyError::Rpc(msg) => {
                assert!(msg.contains("rpc unreachable"), "{msg}");
                assert!(!msg.contains(&a_url), "endpoint leaked: {msg}");
            }
            other => panic!("expected Rpc, got {other:?}"),
        }
        verify(&v, &on2, 2, 7).await.unwrap();
        b.stop().await;
    }

    #[tokio::test]
    async fn check_chains_catches_misconfiguration() {
        // Endpoint reports chain 1 but config claims it is chain 5.
        let node = mock_node::spawn(Node::new(1, CONTRACT_A, b"x".to_vec())).await;
        let v = verifier(&[(5, &node.url, CONTRACT_A)]).unwrap();
        let err = v.check_chains().await.unwrap_err().to_string();
        assert!(err.contains("mismatch"), "{err}");
        assert!(err.contains("config says 5"), "{err}");
        assert!(err.contains("reports 1"), "{err}");

        // Correct id passes; a dead endpoint fails with the chain named.
        let v = verifier(&[(1, &node.url, CONTRACT_A)]).unwrap();
        v.check_chains().await.unwrap();
        let node_url = node.url.clone();
        node.stop().await;
        let error = v.check_chains().await.unwrap_err();
        assert!(matches!(
            error.downcast_ref::<RpcError>(),
            Some(RpcError::Unreachable(_))
        ));
        let err = format!("{error:#}");
        assert!(
            err.contains("chain 1") && err.contains("unreachable"),
            "{err}"
        );
        assert!(!err.contains(&node_url), "endpoint leaked: {err}");
    }

    #[test]
    fn recovery_round_trips_and_refuses_the_malformed() {
        use k256::ecdsa::SigningKey;
        let key = SigningKey::from_slice(&[7u8; 32]).unwrap();
        let point = key.verifying_key().to_encoded_point(false);
        let address: [u8; 20] = keccak::hash(&[&point.as_bytes()[1..]]).0[12..]
            .try_into()
            .unwrap();
        let digest = [0x42u8; 32];
        let (sig, rec) = key.sign_prehash_recoverable(&digest).unwrap();
        let mut bytes = sig.to_bytes().to_vec();
        bytes.push(27 + rec.to_byte());
        assert_eq!(recover_signer(&digest, &bytes), Some(address));
        // The other digest recovers to someone else; a short or odd-v signature to nobody.
        assert_ne!(recover_signer(&[0x43u8; 32], &bytes), Some(address));
        assert_eq!(recover_signer(&digest, &bytes[..64]), None);
        let mut bad_v = bytes.clone();
        bad_v[64] = 5;
        assert_eq!(recover_signer(&digest, &bad_v), None);
        // A raw 0/1 recovery id is refused too: ecrecover, and so the Board, wants 27/28.
        let mut raw_v = bytes.clone();
        raw_v[64] -= 27;
        assert_eq!(recover_signer(&digest, &raw_v), None);
    }
}
