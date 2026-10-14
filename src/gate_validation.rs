//! Validate optional shared gates away from request handling. Unavailable or mismatched
//! deployments never disable Board serving, and display reads never initiate RPCs.
//!
//! A gate is checked by what it answers, not by its code: its getters must name this
//! host's Board, key market and blocklist, and the getters of the other kinds must be
//! absent, so a combined gate cannot pass as a single one, nor any of them as the
//! closing gate. This catches a mistyped
//! address, another chain's or market's gate, or the wrong kind; a contract built to
//! answer the same getters with other rules is the operator's to rule out.
use crate::{
    abi::{address_word, hex0x, selector},
    chain::{Address, Hex},
    rpc,
    server::AppState,
};
use anyhow::{Result, ensure};
use serde_json::json;
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Blocklist,
    Keys,
    KeysBlocklist,
    Closed,
}
impl Kind {
    const ALL: [Self; 4] = [
        Self::Blocklist,
        Self::Keys,
        Self::KeysBlocklist,
        Self::Closed,
    ];

    fn configured(self, chain: &crate::verifier::Chain) -> Option<Address> {
        match self {
            Self::Closed => chain.closed_gate,
            Self::Blocklist => chain.blocking.as_ref().map(|b| b.gate),
            Self::Keys => chain.key_gates.as_ref().and_then(|g| g.holders),
            Self::KeysBlocklist => chain.key_gates.as_ref().and_then(|g| g.holders_blocklist),
        }
        .map(Hex)
    }
}

#[derive(Default)]
pub struct Validated(RwLock<BTreeSet<(u64, Address, Kind)>>);
impl Validated {
    /// A combined gate is usable only once its blocklist setup also validates.
    pub fn published(
        &self,
        id: u64,
        chain: &crate::verifier::Chain,
        kind: Kind,
    ) -> Option<Address> {
        let gate = kind.configured(chain)?;
        if !self.contains(id, gate, kind) {
            return None;
        }
        if kind == Kind::KeysBlocklist {
            self.published(id, chain, Kind::Blocklist)?;
        }
        Some(gate)
    }

    /// A configured gate this host has not validated yet: the status omits it
    /// for now, and says so, so a client asks again rather than keep the gap.
    pub fn pending(&self, id: u64, chain: &crate::verifier::Chain) -> bool {
        Kind::ALL.into_iter().any(|kind| {
            kind.configured(chain).is_some() && self.published(id, chain, kind).is_none()
        })
    }

    pub fn contains(&self, chain: u64, gate: Address, kind: Kind) -> bool {
        self.0
            .read()
            .expect("validated gates")
            .contains(&(chain, gate, kind))
    }
    pub(crate) fn insert(&self, chain: u64, gate: Address, kind: Kind) {
        self.0
            .write()
            .expect("validated gates")
            .insert((chain, gate, kind));
    }
}

/// `canReply(1, 0x0, "")`, which the closing gate answers false for anyone.
fn closed_probe() -> Vec<u8> {
    crate::abi::can_reply_calldata(1, Address::ZERO, &[])
}

/// What a gate answers at `block`: its return data, or None when it reverts, as a
/// contract without that function does. Any other failure leaves the gate unchecked.
async fn answer(
    client: &rpc::Upstream,
    chain: &crate::verifier::Chain,
    gate: Address,
    block: &str,
    data: &[u8],
) -> Result<Option<Vec<u8>>> {
    let result = rpc::call(
        client,
        &chain.url,
        "eth_call",
        json!([{"to":gate,"data":hex0x(data),"gas":"0x186a0"},block]),
    )
    .await?;
    if let Some(error) = result.get("error") {
        ensure!(rpc::is_revert(error), "gate call failed: {error}");
        return Ok(None);
    }
    Ok(Some(rpc::result_bytes(&result)?))
}

async fn verify(
    state: &AppState,
    id: u64,
    chain: &crate::verifier::Chain,
    gate: Address,
    kind: Kind,
) -> Result<()> {
    state.verifier.check_chain(id, rpc::Class::Client).await?;
    let client = state.verifier.upstream().unwrap();
    // Pin every read to the same accepted block, including during deployment.
    let head = rpc::head(client, &chain.url, rpc::Class::Client).await?;
    let block = format!("0x{:x}", head.saturating_sub(chain.confirmations));
    if kind == Kind::Closed {
        let closed = answer(client, chain, gate, &block, &closed_probe()).await?;
        ensure!(closed == Some(vec![0; 32]), "closing gate allows replies");
        // Other gates refuse an account like the zero address too: the closing
        // gate is the one with none of their getters.
        for getter in ["BOARD()", "KEYS()", "REGISTRY()"] {
            let got = answer(client, chain, gate, &block, &selector(getter)).await?;
            ensure!(got.is_none(), "gate has {getter}: not the closing gate");
        }
        return Ok(());
    }
    let mut bindings = vec![("BOARD()", address_word(Hex(chain.contract)))];
    let mut absent = vec![];
    if kind == Kind::Blocklist {
        absent.push("KEYS()");
    } else {
        let keys = state
            .keyrings
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("keyring not configured"))?;
        bindings.push(("KEYS()", address_word(keys.config().market)));
    }
    if kind == Kind::Keys {
        absent.push("REGISTRY()");
    } else {
        let blocking = chain
            .blocking
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("blocking not configured"))?;
        bindings.push(("REGISTRY()", address_word(Hex(blocking.registry))));
        bindings.push((
            "LIST_ID()",
            crate::keccak::hash(&[blocking.list.as_bytes()]).0,
        ));
    }
    for (getter, expected) in bindings {
        let got = answer(client, chain, gate, &block, &selector(getter)).await?;
        ensure!(
            got.as_deref() == Some(&expected[..]),
            "gate {getter} mismatch"
        );
    }
    for getter in absent {
        let got = answer(client, chain, gate, &block, &selector(getter)).await?;
        ensure!(got.is_none(), "gate has {getter}: another kind of gate");
    }
    Ok(())
}

/// Called once at startup and then once per minute until configured gates validate.
/// Success is retained: the shared gates' bindings are immutable and they cannot be upgraded.
pub async fn validate_pending(state: &AppState) {
    for (id, chain) in state.verifier.chains() {
        for kind in Kind::ALL {
            let Some(gate) = kind.configured(chain) else {
                continue;
            };
            if state.gate_checks.contains(id, gate, kind) {
                continue;
            }
            match verify(state, id, chain, gate, kind).await {
                Ok(()) => state.gate_checks.insert(id, gate, kind),
                Err(error) => {
                    tracing::warn!(chain=id, %gate, "shared gate disabled, retrying later: {error:#}")
                }
            }
        }
    }
}

pub fn start(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            validate_pending(&state).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_node::{self, Running};

    const REVERT: &str = "execution reverted";
    fn set(running: &Running, data: &[u8], answer: serde_json::Value) {
        running
            .node
            .response_overrides
            .lock()
            .unwrap()
            .insert(format!("eth_call:{}", hex0x(data)), answer);
    }
    fn word(bytes: &[u8]) -> serde_json::Value {
        json!({"result": hex0x(bytes)})
    }
    fn reverted() -> serde_json::Value {
        json!({"error": {"code": 3, "message": REVERT}})
    }

    #[tokio::test]
    async fn bindings_must_match_other_kinds_must_be_absent_and_failures_can_recover() {
        use crate::{
            config::{BlockingConfig, ChainConfig, Config, KeyGatesConfig},
            keyring::{Config as KeyringConfig, Keyring},
            mock_node::Node,
            verifier::Verifier,
        };
        for kind in [Kind::Blocklist, Kind::Keys, Kind::KeysBlocklist] {
            let board = Hex([1; 20]);
            let market = Hex([2; 20]);
            let registry = Hex([3; 20]);
            let gate = Hex([4; 20]);
            let running = mock_node::spawn(Node::new(84532, &board.to_string(), vec![])).await;
            running.node.set_head(20);
            let mut chain = ChainConfig::for_test(84532, &running.url, &board.to_string());
            chain.key_gates = (kind != Kind::Blocklist).then(|| KeyGatesConfig {
                holders: (kind == Kind::Keys).then(|| gate.to_string()),
                holders_blocklist: (kind == Kind::KeysBlocklist).then(|| gate.to_string()),
            });
            chain.blocking = (kind != Kind::Keys).then(|| BlockingConfig {
                gate: if kind == Kind::Blocklist {
                    gate
                } else {
                    Hex([5; 20])
                }
                .to_string(),
                registry: registry.to_string(),
                list: "blocked".into(),
            });
            let cfg = Config::for_test(vec![chain]);
            let keys = Keyring::detached(KeyringConfig {
                chain: 84532,
                market,
                start_block: 1,
            });
            let state = AppState {
                gate_checks: Default::default(),
                keyrings: (kind != Kind::Blocklist)
                    .then_some((84532, keys))
                    .into_iter()
                    .collect(),
                verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
                notice: None,
                policy: Default::default(),
                store: None,
                gateway: None,
            };
            // The mock knows no contract at this address: nothing it asks is answered.
            validate_pending(&state).await;
            assert!(!state.gate_checks.contains(84532, gate, kind));
            let all = [
                ("BOARD()", address_word(board)),
                ("KEYS()", address_word(market)),
                ("REGISTRY()", address_word(registry)),
                ("LIST_ID()", crate::keccak::hash(&[b"blocked"]).0),
            ];
            let has = |getter: &str| match kind {
                Kind::Blocklist => getter != "KEYS()",
                Kind::Keys => getter == "BOARD()" || getter == "KEYS()",
                _ => true,
            };
            for (getter, value) in &all {
                set(
                    &running,
                    &selector(getter),
                    if has(getter) { word(value) } else { reverted() },
                );
            }
            for (getter, value) in all.iter().filter(|(g, _)| has(g)) {
                set(&running, &selector(getter), word(&[9; 32]));
                validate_pending(&state).await;
                assert!(
                    !state.gate_checks.contains(84532, gate, kind),
                    "accepted wrong {getter}"
                );
                set(&running, &selector(getter), word(value));
            }
            // The getter that sets the combined gate apart from this kind.
            let other = |getter: &str| match kind {
                Kind::Blocklist => getter == "KEYS()",
                Kind::Keys => getter == "REGISTRY()",
                _ => false,
            };
            for (getter, value) in all.iter().filter(|(g, _)| other(g)) {
                // A combined gate answers every getter: it is not the plain kind.
                set(&running, &selector(getter), word(value));
                validate_pending(&state).await;
                assert!(
                    !state.gate_checks.contains(84532, gate, kind),
                    "accepted a gate with {getter}"
                );
                // An outage is not an absent getter.
                set(
                    &running,
                    &selector(getter),
                    json!({"error": {"code": -32000, "message": "header not found"}}),
                );
                validate_pending(&state).await;
                assert!(
                    !state.gate_checks.contains(84532, gate, kind),
                    "an outage proved {getter} absent"
                );
                set(&running, &selector(getter), reverted());
            }
            validate_pending(&state).await;
            assert!(state.gate_checks.contains(84532, gate, kind));
            let calls = running.node.counts();
            validate_pending(&state).await;
            if kind != Kind::KeysBlocklist {
                assert_eq!(
                    running.node.counts(),
                    calls,
                    "validated gates need no polling"
                );
                continue;
            }
            // A combined gate is published only once its blocklist gate validates too.
            let state = Arc::new(state);
            let response = crate::reads::status(axum::extract::State(state.clone())).await;
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let status: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(status["chains"][0]["blocking"].is_null());
            assert!(status["chains"][0]["key_gates"]["holders_blocklist"].is_null());
            state
                .gate_checks
                .insert(84532, Hex([5; 20]), Kind::Blocklist);
            let response = crate::reads::status(axum::extract::State(state.clone())).await;
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let status: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                status["chains"][0]["key_gates"]["holders_blocklist"],
                json!(gate)
            );
        }
    }

    #[tokio::test]
    async fn a_closing_gate_must_refuse_replies_and_have_no_other_gates_getters() {
        use crate::{
            config::{ChainConfig, Config},
            mock_node::Node,
            verifier::Verifier,
        };
        let board = Hex([1; 20]);
        let gate = Hex([7; 20]);
        let running = mock_node::spawn(Node::new(84532, &board.to_string(), vec![])).await;
        running.node.set_head(20);
        let mut chain = ChainConfig::for_test(84532, &running.url, &board.to_string());
        chain.closed_gate = Some(gate.to_string());
        let cfg = Config::for_test(vec![chain]);
        let state = AppState {
            gate_checks: Default::default(),
            keyrings: Default::default(),
            verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
            notice: None,
            policy: Default::default(),
            store: None,
            gateway: None,
        };
        // An address without code answers nothing; a gate that lets replies through is not closed.
        let mut allows = [0; 32];
        allows[31] = 1;
        for open in [json!({"result": "0x"}), word(&allows)] {
            set(&running, &closed_probe(), open);
            validate_pending(&state).await;
            assert!(!state.gate_checks.contains(84532, gate, Kind::Closed));
        }
        // Another kind of gate refuses the zero address too, but it has getters.
        set(&running, &closed_probe(), word(&[0; 32]));
        for getter in ["BOARD()", "KEYS()", "REGISTRY()"] {
            set(&running, &selector(getter), reverted());
        }
        for getter in ["BOARD()", "KEYS()", "REGISTRY()"] {
            set(
                &running,
                &selector(getter),
                word(&address_word(Hex([2; 20]))),
            );
            validate_pending(&state).await;
            assert!(
                !state.gate_checks.contains(84532, gate, Kind::Closed),
                "a gate with {getter} passed as the closing gate"
            );
            set(
                &running,
                &selector(getter),
                json!({"error": {"code": -32000, "message": "header not found"}}),
            );
            validate_pending(&state).await;
            assert!(
                !state.gate_checks.contains(84532, gate, Kind::Closed),
                "an outage proved {getter} absent"
            );
            set(&running, &selector(getter), reverted());
        }
        let state = Arc::new(state);
        let response = crate::reads::status(axum::extract::State(state.clone())).await;
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(status["chains"][0]["closed_gate"].is_null());
        assert_eq!(
            status["chains"][0]["gates_pending"],
            json!(true),
            "omitted, and said so"
        );
        set(&running, &closed_probe(), word(&[0; 32]));
        validate_pending(&state).await;
        assert!(state.gate_checks.contains(84532, gate, Kind::Closed));
        let response = crate::reads::status(axum::extract::State(state)).await;
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status["chains"][0]["closed_gate"], json!(gate));
        assert!(status["chains"][0]["blocking"].is_null());
        assert_eq!(status["chains"][0]["gates_pending"], json!(false));
    }
}
