//! A mock JSON-RPC node for tests, shared by the verifier, index ingestion and
//! the HTTP tests.
//!
//! It implements just enough of `eth_chainId`, `eth_blockNumber`,
//! `eth_getCode`, the block and transaction reads, `eth_getLogs`, and
//! `eth_call` for the board's reads, a registry's `contains` and a contract
//! account's `isValidSignature`: it reports the configured chain
//! id; message 0 reverts (does not exist); every other id has `contentHash =
//! keccak(anchored)` unless overridden. It also asserts on request shape — a
//! malformed call gets a descriptive JSON-RPC error, so `Ok` assertions in
//! tests fail with the reason visible.

use crate::abi::{hex0x, selector};
use crate::keccak;

use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    future::IntoFuture as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::task::JoinHandle;

pub struct Node {
    pub chain_id: u64,
    pub contract: String,
    /// The bytes every existing message commits to, unless overridden.
    pub anchored: Vec<u8>,
    /// Per-message overrides: (id, bytes that message commits to).
    pub extra: Vec<(u64, Vec<u8>)>,
    /// Head block number reported by eth_blockNumber (tests may move it).
    pub head: AtomicU64,
    /// Messages 1..=messages exist at `latest`; message i was posted in
    /// block 10 + i by author 0x…0i with no parent, and appears in
    /// eth_getLogs. An eth_call at a numeric block sees only messages
    /// mined at or before it.
    pub messages: AtomicU64,
    /// Per-method request counter (filled in by `spawn`).
    pub counts: Mutex<HashMap<String, usize>>,
    /// The params of the last request of each method, as they reached the node.
    pub last: Mutex<HashMap<String, Value>>,
    /// Delay before every answer, so tests can watch what happens while an
    /// answer is still out.
    pub latency_ms: AtomicU64,
    /// Explicit history replacements for checkpoint disagreement tests.
    pub block_hashes: Mutex<HashMap<u64, [u8; 32]>>,
    pub extra_logs: Mutex<Vec<Value>>,
    pub simulation_error: Mutex<Option<Value>>,
    /// Complete per-method answers for provider error and malformed-response tests.
    pub response_overrides: Mutex<HashMap<String, Value>>,
    /// Transactions the node knows (eth_getTransactionByHash / receipt).
    pub txs: Mutex<Vec<MockTx>>,
    /// Addresses a `ListRegistry.contains` call answers `true` for.
    pub members: Mutex<Vec<[u8; 20]>>,
    /// Each message's reply gate as `replyGate(id)` answers it; zero when absent.
    pub gates: Mutex<HashMap<u64, [u8; 20]>>,
    /// Addresses that have code and accept every signature (ERC-1271).
    pub contracts: Mutex<Vec<[u8; 20]>>,
    /// A provider's result limit on eth_getLogs: a query matching more logs
    /// than this is refused. Zero means unlimited.
    pub max_logs: AtomicU64,
    /// Whether eth_getBlockReceipts is served; a node without it answers
    /// "method not found".
    pub block_receipts: AtomicBool,
    /// How many upcoming eth_getLogs requests fail outright, with a plain-text
    /// 503 instead of a JSON-RPC answer, as an overloaded provider does.
    pub log_outage: AtomicU64,
    /// How many upcoming eth_getLogs requests are refused with a JSON-RPC
    /// error, as a provider imposing a log-query limit does.
    pub log_refusals: AtomicU64,
    /// How many of the next `eth_getLogs` answers carry a log whose block
    /// number is not a quantity, whatever the range.
    pub malformed_logs: AtomicU64,
    /// A history replacement applied once, right after the logs or receipts
    /// of a range covering `block` have been served from the old history:
    /// the block takes this hash and gains these logs. Models a reorg that
    /// lands between a range read and the header reads that follow it.
    pub replace_after_read: Mutex<Option<Replacement>>,
    /// Transactions whose receipts report failure, by hash.
    pub failed_txs: Mutex<std::collections::HashSet<String>>,
}

#[derive(Clone, Debug)]
pub struct Replacement {
    pub block: u64,
    pub hash: [u8; 32],
    pub logs: Vec<Value>,
}

#[derive(Clone, Debug)]
pub struct MockTx {
    pub hash: [u8; 32],
    pub from: [u8; 20],
    pub to: String,
    pub input: Vec<u8>,
    /// `None` while pending.
    pub block: Option<u64>,
    /// The message the receipt's `Posted` log names, once mined.
    pub message_id: Option<u64>,
    pub reverted: bool,
}

/// Minimal topic filter for the mock: null = any, string = exact, array = OR.
fn topic_ok(log: &Value, topics: &Value) -> bool {
    let Some(wanted) = topics.as_array() else {
        return true;
    };
    let have = log["topics"].as_array().cloned().unwrap_or_default();
    wanted.iter().enumerate().all(|(i, w)| match w {
        Value::Null => true,
        Value::String(s) => have
            .get(i)
            .and_then(Value::as_str)
            .is_some_and(|h| h.eq_ignore_ascii_case(s)),
        Value::Array(any) => have.get(i).and_then(Value::as_str).is_some_and(|h| {
            any.iter()
                .any(|a| a.as_str().is_some_and(|s| h.eq_ignore_ascii_case(s)))
        }),
        _ => false,
    })
}

impl Node {
    /// A node for `chain_id` whose board is at `contract` and whose messages
    /// all commit to `anchored`. Head 0, no messages, no transactions, no
    /// members: set what a test needs with struct update syntax.
    pub fn new(chain_id: u64, contract: &str, anchored: Vec<u8>) -> Self {
        Self {
            chain_id,
            contract: contract.to_string(),
            anchored,
            extra: Vec::new(),
            head: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            counts: Mutex::default(),
            last: Mutex::default(),
            latency_ms: AtomicU64::new(0),
            block_hashes: Mutex::default(),
            extra_logs: Mutex::default(),
            simulation_error: Mutex::default(),
            response_overrides: Mutex::default(),
            txs: Mutex::default(),
            members: Mutex::default(),
            gates: Mutex::default(),
            contracts: Mutex::default(),
            max_logs: AtomicU64::new(0),
            block_receipts: AtomicBool::new(true),
            log_outage: AtomicU64::new(0),
            log_refusals: AtomicU64::new(0),
            malformed_logs: AtomicU64::new(0),
            replace_after_read: Mutex::default(),
            failed_txs: Mutex::default(),
        }
    }

    /// How many requests of each method have reached the node.
    pub fn counts(&self) -> HashMap<String, usize> {
        self.counts.lock().unwrap().clone()
    }

    /// How many `method` requests have reached the node.
    pub fn count(&self, method: &str) -> usize {
        self.counts
            .lock()
            .unwrap()
            .get(method)
            .copied()
            .unwrap_or(0)
    }

    /// The params of the last `method` request, as they reached the node.
    pub fn last(&self, method: &str) -> Value {
        self.last
            .lock()
            .unwrap()
            .get(method)
            .cloned()
            .unwrap_or(Value::Null)
    }

    pub fn head(&self) -> u64 {
        self.head.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_head(&self, n: u64) {
        self.head.store(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn messages(&self) -> u64 {
        self.messages.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_messages(&self, n: u64) {
        self.messages.store(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn add_tx(&self, tx: MockTx) {
        self.txs.lock().unwrap().push(tx);
    }

    /// Mark a pending transaction as mined in `block`, posting `message_id`.
    pub fn mine_tx(&self, hash: [u8; 32], block: u64, message_id: u64) {
        for t in self.txs.lock().unwrap().iter_mut() {
            if t.hash == hash {
                t.block = Some(block);
                t.message_id = Some(message_id);
            }
        }
    }

    fn message_bytes(&self, id: u64) -> Vec<u8> {
        self.extra
            .iter()
            .find(|(i, _)| *i == id)
            .map(|(_, b)| b.clone())
            .unwrap_or_else(|| self.anchored.clone())
    }
}

pub struct Running {
    pub url: String,
    pub node: Arc<Node>,
    pub handle: JoinHandle<std::io::Result<()>>,
}

impl Running {
    pub async fn stop(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

/// Bind on an ephemeral port and serve the mock. Must be called from
/// within a tokio runtime.
pub async fn spawn(node: Node) -> Running {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    spawn_on(node, listener).await
}

/// Serve the mock on a listener the test bound itself, so a chain can be
/// unreachable at one moment and answering at the same address the next.
pub async fn spawn_on(node: Node, listener: tokio::net::TcpListener) -> Running {
    let url = format!("http://{}", listener.local_addr().unwrap());
    let node = Arc::new(node);
    let app = Router::new()
        .route("/", post(handle))
        .with_state(node.clone());
    let handle = tokio::spawn(axum::serve(listener, app).into_future());
    Running { url, node, handle }
}

fn posted_log(node: &Node, i: u64) -> Value {
    let sig = keccak::hash(&[crate::abi::POSTED_SIGNATURE]);
    let word = |n: u64| format!("0x{:064x}", n);
    // Data: id, contentHash, replyGate (= 0). Topics: author, parent (0: top-level), parent author (0).
    let mut data = [0u8; 32].to_vec();
    data[24..32].copy_from_slice(&i.to_be_bytes());
    data.extend_from_slice(&keccak::hash(&[node.message_bytes(i).as_slice()]).0);
    data.extend_from_slice(&[0u8; 32]);
    json!({
        "address": node.contract.to_lowercase(),
        "topics": [hex0x(&sig.0), word(i) /* author 0x…0i */, word(0), word(0)],
        "data": hex0x(&data),
        "blockNumber": format!("0x{:x}", 10 + i),
        "logIndex": "0x0",
        "transactionHash": word(1000 + i),
        "transactionIndex": "0x0",
        "blockHash": word(2000 + 10 + i),
        "removed": false,
    })
}

fn block_hash(node: &Node, number: u64) -> String {
    node.block_hashes
        .lock()
        .unwrap()
        .get(&number)
        .map(|h| hex0x(h))
        .unwrap_or_else(|| format!("0x{:064x}", 2000 + number))
}

/// Every log in `from..=to`: one Posted per message, then the extra logs.
fn logs_in(node: &Node, from: u64, to: u64) -> Vec<Value> {
    (1..=node.messages())
        .filter(|i| (10 + i) >= from && (10 + i) <= to)
        .map(|i| posted_log(node, i))
        .chain(
            node.extra_logs
                .lock()
                .unwrap()
                .iter()
                .filter(|log| {
                    log["blockNumber"]
                        .as_str()
                        .and_then(|s| crate::abi::parse_quantity(s).ok())
                        .is_some_and(|n| n >= from && n <= to)
                })
                .cloned(),
        )
        .collect()
}

/// Apply the pending history replacement if the range just served covers it.
fn replace_after(node: &Node, from: u64, to: u64) {
    let mut pending = node.replace_after_read.lock().unwrap();
    if pending
        .as_ref()
        .is_some_and(|r| r.block >= from && r.block <= to)
    {
        let replacement = pending.take().unwrap();
        node.block_hashes
            .lock()
            .unwrap()
            .insert(replacement.block, replacement.hash);
        node.extra_logs.lock().unwrap().extend(replacement.logs);
    }
}

fn rpc_error(code: i64, message: String) -> Json<Value> {
    Json(json!({"jsonrpc":"2.0","id":1,"error":{"code":code,"message":message}}))
}

async fn handle(State(node): State<Arc<Node>>, Json(req): Json<Value>) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    if req["method"] == "eth_getLogs" && node.log_outage.load(Ordering::Relaxed) > 0 {
        node.log_outage.fetch_sub(1, Ordering::Relaxed);
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "upstream outage",
        )
            .into_response();
    }
    answer(node, req).await.into_response()
}

async fn answer(node: Arc<Node>, req: Value) -> Json<Value> {
    if let Some(m) = req["method"].as_str() {
        *node
            .counts
            .lock()
            .unwrap()
            .entry(m.to_string())
            .or_default() += 1;
        node.last
            .lock()
            .unwrap()
            .insert(m.to_string(), req["params"].clone());
    }
    let latency = node.latency_ms.load(std::sync::atomic::Ordering::Relaxed);
    if latency > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(latency)).await;
    }
    let fail =
        |msg: String| Json(json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":msg}}));
    let ok = |result: String| Json(json!({"jsonrpc":"2.0","id":1,"result":result}));
    if matches!(req["method"].as_str(), Some("eth_call" | "eth_estimateGas")) {
        if let Some(error) = node.simulation_error.lock().unwrap().as_ref() {
            return Json(json!({"jsonrpc": "2.0", "id": 1, "error": error}));
        }
        if req["method"] == "eth_estimateGas"
            && req["params"][0]["data"]
                .as_str()
                .is_some_and(|d| d.starts_with(&hex0x(&selector(crate::abi::POST_SIGNATURE))))
        {
            return ok("0x186a0".into());
        }
        if req["params"][0]["data"]
            .as_str()
            .is_some_and(|d| d.starts_with(&hex0x(&selector("checkReply(uint64,address,bytes)"))))
        {
            return ok(format!("0x{}", "00".repeat(32)));
        }
    }
    if req["method"] == "eth_call" {
        let key = format!(
            "eth_call:{}",
            req["params"][0]["data"].as_str().unwrap_or("")
        );
        if let Some(answer) = node.response_overrides.lock().unwrap().get(&key).cloned() {
            return Json(answer);
        }
    }
    if let Some(answer) = req["method"]
        .as_str()
        .and_then(|method| node.response_overrides.lock().unwrap().get(method).cloned())
    {
        return Json(answer);
    }
    match req["method"].as_str() {
        Some("eth_chainId") => ok(format!("0x{:x}", node.chain_id)),
        Some("eth_blockNumber") => ok(format!("0x{:x}", node.head())),
        Some("eth_getTransactionByHash") => {
            let want = req["params"][0].as_str().unwrap_or("").to_lowercase();
            let txs = node.txs.lock().unwrap();
            let Some(t) = txs.iter().find(|t| hex0x(&t.hash) == want) else {
                return Json(json!({"jsonrpc":"2.0","id":1,"result":null}));
            };
            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                "hash": want,
                "from": hex0x(&t.from),
                "to": t.to,
                "input": hex0x(&t.input),
                "blockNumber": t.block.map(|b| format!("0x{b:x}")),
            }}))
        }
        Some("eth_getTransactionReceipt") => {
            let want = req["params"][0].as_str().unwrap_or("").to_lowercase();
            let txs = node.txs.lock().unwrap();
            let Some(t) = txs
                .iter()
                .find(|t| hex0x(&t.hash) == want && t.block.is_some())
            else {
                return Json(json!({"jsonrpc":"2.0","id":1,"result":null}));
            };
            let logs: Vec<Value> = match (t.reverted, t.message_id) {
                (false, Some(id)) => {
                    let mut log = posted_log(&node, id);
                    log["transactionHash"] = json!(want);
                    log["blockNumber"] = json!(format!("0x{:x}", t.block.unwrap()));
                    log["blockHash"] = json!(format!("0x{:064x}", 2000 + t.block.unwrap()));
                    vec![log]
                }
                _ => vec![],
            };
            Json(json!({"jsonrpc":"2.0","id":1,"result":{
                "status": if t.reverted { "0x0" } else { "0x1" },
                "transactionHash": want,
                "blockHash": format!("0x{:064x}", 2000 + t.block.unwrap()),
                "blockNumber": format!("0x{:x}", t.block.unwrap()),
                "logs": logs,
            }}))
        }
        Some("eth_getBlockByNumber") => {
            // By number: that block, or null past the head; a tag means the
            // head. Two seconds a block from an epoch.
            let head = node.head();
            let number = match req["params"][0].as_str() {
                Some(s) if s.starts_with("0x") => {
                    match u64::from_str_radix(s.trim_start_matches("0x"), 16) {
                        Ok(n) if n <= head => n,
                        _ => return Json(json!({"jsonrpc":"2.0","id":1,"result":null})),
                    }
                }
                _ => head,
            };
            Json(json!({
                "jsonrpc":"2.0","id":1,
                "result": { "number": format!("0x{number:x}"), "hash": block_hash(&node, number), "timestamp": format!("0x{:x}", 1_700_000_000u64 + 2 * number), "transactions": [] }
            }))
        }
        Some("eth_getBlockReceipts") => {
            if !node.block_receipts.load(Ordering::Relaxed) {
                return rpc_error(-32601, "Method not found".into());
            }
            let Some(number) = req["params"][0]
                .as_str()
                .and_then(|s| crate::abi::parse_quantity(s).ok())
                .filter(|n| *n <= node.head())
            else {
                return Json(json!({"jsonrpc":"2.0","id":1,"result":null}));
            };
            // One receipt per transaction, its logs in order, every one successful.
            let mut receipts: Vec<Value> = Vec::new();
            for log in logs_in(&node, number, number) {
                let tx = log["transactionHash"].clone();
                match receipts.iter_mut().find(|r| r["transactionHash"] == tx) {
                    Some(receipt) => receipt["logs"].as_array_mut().unwrap().push(log),
                    None => {
                        let failed = node
                            .failed_txs
                            .lock()
                            .unwrap()
                            .contains(tx.as_str().unwrap_or(""));
                        receipts.push(json!({
                        "status": if failed { "0x0" } else { "0x1" },
                        "transactionHash": tx,
                        "blockNumber": format!("0x{number:x}"), "blockHash": block_hash(&node, number),
                        "logs": [log],
                        }));
                    }
                }
            }
            replace_after(&node, number, number);
            Json(json!({"jsonrpc":"2.0","id":1,"result":receipts}))
        }
        Some("eth_getLogs") => {
            if node.log_refusals.load(Ordering::Relaxed) > 0 {
                node.log_refusals.fetch_sub(1, Ordering::Relaxed);
                return rpc_error(-32005, "query limit exceeded".into());
            }
            if node.malformed_logs.load(Ordering::Relaxed) > 0 {
                node.malformed_logs.fetch_sub(1, Ordering::Relaxed);
                return Json(
                    json!({"jsonrpc":"2.0","id":1,"result":[{"blockNumber":"malformed"}]}),
                );
            }
            let f = &req["params"][0];
            let q = |k: &str| {
                f[k].as_str()
                    .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
            };
            let from = q("fromBlock").unwrap_or(0);
            let to = q("toBlock").unwrap_or(node.head());
            let logs: Vec<Value> = logs_in(&node, from, to)
                .into_iter()
                .filter(|l| topic_ok(l, &f["topics"]))
                .filter(|l| match &f["address"] {
                    Value::String(address) => l["address"]
                        .as_str()
                        .is_some_and(|a| a.eq_ignore_ascii_case(address)),
                    Value::Array(addresses) => addresses.iter().any(|address| {
                        address.as_str().is_some_and(|a| {
                            l["address"]
                                .as_str()
                                .is_some_and(|b| a.eq_ignore_ascii_case(b))
                        })
                    }),
                    _ => true,
                })
                .collect();
            let cap = node.max_logs.load(Ordering::Relaxed);
            if cap != 0 && logs.len() as u64 > cap {
                return rpc_error(-32005, format!("query returned more than {cap} results"));
            }
            replace_after(&node, from, to);
            Json(json!({"jsonrpc":"2.0","id":1,"result":logs}))
        }
        Some("eth_getCode") => {
            let who = req["params"][0].as_str().unwrap_or("").to_lowercase();
            let has = node
                .contracts
                .lock()
                .unwrap()
                .iter()
                .any(|c| hex0x(c) == who);
            ok(if has {
                "0x6001".to_string()
            } else {
                "0x".to_string()
            })
        }
        Some("eth_call") => {
            // A contract account accepts every signature: isValidSignature returns its magic.
            if let Some(d) = req["params"][0]["data"].as_str()
                && let Ok(bytes) = hex::decode(d.trim_start_matches("0x"))
                && bytes.len() >= 4
                && bytes[..4] == selector("isValidSignature(bytes32,bytes)")
            {
                let to = req["params"][0]["to"].as_str().unwrap_or("").to_lowercase();
                // Only the Board's own call is honoured, as an account may insist.
                let from = req["params"][0]["from"]
                    .as_str()
                    .unwrap_or("")
                    .to_lowercase();
                let has = from == node.contract.to_lowercase()
                    && node
                        .contracts
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|c| hex0x(c) == to);
                return ok(if has {
                    "0x1626ba7e00000000000000000000000000000000000000000000000000000000".into()
                } else {
                    "0xffffffff00000000000000000000000000000000000000000000000000000000".into()
                });
            }
            // ListRegistry.contains(owner, listId, member): true iff `member` is in `members`.
            if let Some(d) = req["params"][0]["data"].as_str()
                && let Ok(bytes) = hex::decode(d.trim_start_matches("0x"))
                && bytes.len() == 100
                && bytes[..4] == selector("contains(address,bytes32,address)")
            {
                let member: [u8; 20] = bytes[80..100].try_into().unwrap();
                let yes = node.members.lock().unwrap().contains(&member);
                return ok(format!("0x{:064x}", yes as u8));
            }
            let call = &req["params"][0];
            if call["to"].as_str().map(str::to_lowercase) != Some(node.contract.to_lowercase()) {
                return fail(format!("unexpected to {}", call["to"]));
            }
            // Which block are we asked about? A number limits which
            // messages exist (message i is mined in block 10 + i).
            let at_block: Option<u64> = match req["params"][1].as_str() {
                Some("latest") | Some("pending") | Some("safe") | Some("finalized") | None => None,
                Some(q) => match u64::from_str_radix(q.trim_start_matches("0x"), 16) {
                    Ok(b) => Some(b),
                    Err(_) => return fail(format!("bad block parameter {q}")),
                },
            };
            let data = call["data"].as_str().unwrap_or("");
            let data = hex::decode(data.strip_prefix("0x").unwrap_or(data)).unwrap_or_default();
            if data.len() == 36 && data[..4] == selector("replyGate(uint64)") {
                let id = u64::from_be_bytes(data[28..36].try_into().unwrap());
                let gate = node
                    .gates
                    .lock()
                    .unwrap()
                    .get(&id)
                    .copied()
                    .unwrap_or_default();
                return ok(hex0x(&crate::abi::address_word(crate::chain::Hex(gate))));
            }
            if data.len() != 36 || data[..4] != selector("getMessage(uint64)") {
                // A real node reports a call the contract has no function for as a revert.
                return fail(format!(
                    "execution reverted (unexpected calldata {})",
                    hex0x(&data)
                ));
            }
            let id = u64::from_be_bytes(data[28..36].try_into().unwrap());
            let exists = id != 0
                && (node.messages() == 0 || id <= node.messages())
                && at_block.is_none_or(|b| 10 + id <= b);
            if !exists {
                return fail("execution reverted".into());
            }
            let bytes = node.message_bytes(id);
            let mut ret = vec![0u8; 128];
            ret[31] = id as u8; // author 0x…0<id>, matching posted_log
            ret[96..128].copy_from_slice(&keccak::hash(&[bytes.as_slice()]).0);
            ok(hex0x(&ret))
        }
        other => fail(format!("unexpected method {other:?}")),
    }
}
