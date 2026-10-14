//! End-to-end tests over a real listener: the server as `main` wires it,
//! driven with a plain HTTP client, against a mock chain where one is needed.

use crate::abi::{hex0x, selector};
use crate::config::{
    AttachmentsConfig, ChainConfig, Config, GatewayConfig, NamesConfig, StoreConfig, UpstreamConfig,
};
use crate::holding::HoldingConfig;
use crate::keccak;
use crate::mock_node::{self, MockTx, Node};
use crate::routes::router;
use crate::rpc::SIM_GAS_CAP;
use crate::server::{AppState, Options, open_state};
use crate::verifier::{SignedAuthor, Verifier};
use crate::watcher::promote_once;
use axum::http::header;
use commonware_runtime::{Runner as _, tokio as cw_tokio};
use serde_json::{Value, json};
use std::{future::IntoFuture as _, path::PathBuf, sync::Arc, time::Duration};

const TEST_ATTACHMENT_CAP: usize = 2 << 20;
const CONTRACT: &str = "0x5FbDB2315678afecb367f032d93F642f64180aa3";

/// How a test wants its server: everything `open_state` takes, plus the web
/// directory the router serves. The default is a dev-mode host with
/// attachments enabled.
struct Setup {
    verifier: Verifier,
    host_policy: Option<crate::host_policy::Config>,
    /// Which roles the host plays; both by default.
    store: bool,
    gateway: bool,
    attachments: Option<AttachmentsConfig>,
    /// The body cap; zero for the default.
    max_body_bytes: usize,
    holding: HoldingConfig,
    peers: Vec<String>,
    names: Option<NamesConfig>,
    link_previews: Option<crate::link_preview::Config>,
    web_dir: Option<PathBuf>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            verifier: Verifier::AllowAll,
            host_policy: None,
            store: true,
            gateway: true,
            attachments: Some(policy(4)),
            max_body_bytes: 0,
            holding: HoldingConfig::default(),
            peers: Vec::new(),
            names: None,
            link_previews: None,
            web_dir: None,
        }
    }
}

/// What a test gets to drive.
struct Server {
    base: String,
    client: reqwest::Client,
    state: Arc<AppState>,
    /// The mock chain, if the test asked for one.
    node: Option<mock_node::Running>,
}

fn policy(max_per_message: usize) -> AttachmentsConfig {
    AttachmentsConfig {
        max_bytes: TEST_ATTACHMENT_CAP,
        max_per_message,
        media_types: None,
    }
}

/// A verifier for one chain served by the mock node at `url`.
fn chain_verifier(url: &str, chain: ChainConfig) -> Verifier {
    chain_verifier_with(url, chain, UpstreamConfig::default())
}

/// `chain_verifier` with the test's own upstream budget.
fn chain_verifier_with(url: &str, chain: ChainConfig, upstream: UpstreamConfig) -> Verifier {
    Verifier::from_config(
        &Config {
            upstream,
            ..Config::for_test(vec![ChainConfig {
                rpc_url: url.to_string(),
                ..chain
            }])
        },
        reqwest::Client::new(),
    )
    .unwrap()
}

/// Run `f` against a freshly started server on an ephemeral port, on the
/// production tokio runtime with a throwaway storage directory. `node`, if
/// given, is spawned first and its URL handed to `setup`, which decides the
/// rest. Indexed chains are established but not backfilled before `f`.
fn with_server<S, F, Fut>(name: &str, node: Option<Node>, setup: S, f: F)
where
    S: FnOnce(Option<&str>) -> Setup + Send + 'static,
    F: FnOnce(Server) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let dir = std::env::temp_dir().join(format!("blohard-host-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = cw_tokio::Config::default().with_storage_directory(&dir);
    cw_tokio::Runner::new(cfg).start(|context| async move {
        let node = match node {
            Some(n) => Some(mock_node::spawn(n).await),
            None => None,
        };
        let setup = setup(node.as_ref().map(|n| n.url.as_str()));
        let state = Arc::new(
            open_state(
                context,
                setup.verifier,
                Options {
                    policy: setup.host_policy,
                    store: setup.store.then_some(StoreConfig {
                        page_cache_mib: 0,
                        max_body_bytes: setup.max_body_bytes,
                        attachments: setup.attachments,
                        peers: setup.peers,
                    }),
                    gateway: setup.gateway.then_some(GatewayConfig {
                        names: setup.names,
                        link_previews: setup.link_previews,
                    }),
                    holding: setup.holding,
                    notice: Some("a test host".into()),
                    keyrings: Default::default(),
                },
                reqwest::Client::new(),
            )
            .await
            .unwrap(),
        );
        // Verify identities as the follower does, but leave backfill under
        // the test's control so admission before acceptance can be exercised.
        for (id, _) in state.verifier.chains() {
            if let Some(index) = state.gateway.as_ref().and_then(|g| g.indexes.get(&id)) {
                state
                    .verifier
                    .check_chain(id, crate::rpc::Class::Ingestion)
                    .await
                    .unwrap();
                index.establish().unwrap();
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        // With each connection's address, as `main` serves, so visitors are
        // told apart (a forwarded address counts from this loopback peer).
        let app = router(state.clone(), setup.web_dir)
            .into_make_service_with_connect_info::<std::net::SocketAddr>();
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        f(Server {
            base,
            client: reqwest::Client::new(),
            state,
            node,
        })
        .await;
        server.abort();
    });
    let _ = std::fs::remove_dir_all(&dir);
}

async fn get_json(client: &reqwest::Client, url: &str) -> Value {
    client.get(url).send().await.unwrap().json().await.unwrap()
}

/// POST `body` as JSON and read the answer as JSON, whatever its status.
async fn post_json(client: &reqwest::Client, url: &str, body: Value) -> Value {
    client
        .post(url)
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Upload `body` with `query`, such as "chain_id=31337&message_id=1".
async fn publish(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    body: impl Into<reqwest::Body>,
) -> reqwest::Response {
    let url = match query {
        "" => format!("{base}/api/publish"),
        query => format!("{base}/api/publish?{query}"),
    };
    client.post(url).body(body).send().await.unwrap()
}

/// Wait up to `limit` for `done` to hold, failing the test with `what` if it
/// never does.
async fn wait_until(limit: Duration, what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(limit, async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting: {what}"));
}

/// A peer host for tests: it serves the records it holds by hash, answers
/// 404 for any other, and counts every request. It stops when dropped.
struct FakePeer {
    url: String,
    records: Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>>,
    hits: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    _stop: tokio::sync::oneshot::Sender<()>,
}

impl FakePeer {
    /// A peer holding `records`, each under its 0x hash.
    fn start(records: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        Self::start_with(records, || {})
    }

    /// Like [`Self::start`], running `on_request` as each request arrives,
    /// before the answer: a test can change the host while a fetch is out.
    fn start_with(
        records: impl IntoIterator<Item = (String, Vec<u8>)>,
        on_request: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        use axum::response::IntoResponse as _;
        let records = Arc::new(std::sync::Mutex::new(
            records
                .into_iter()
                .map(|(hash, bytes)| (hash.to_lowercase(), bytes))
                .collect::<std::collections::HashMap<_, _>>(),
        ));
        let hits = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let (served, counted) = (records.clone(), hits.clone());
        let on_request = Arc::new(on_request);
        let app = axum::Router::new().route(
            "/api/content/{hash}",
            axum::routing::get(
                move |axum::extract::Path(hash): axum::extract::Path<String>| {
                    on_request();
                    let hash = hash.to_lowercase();
                    *counted.lock().unwrap().entry(hash.clone()).or_default() += 1;
                    let bytes = served.lock().unwrap().get(&hash).cloned();
                    async move {
                        match bytes {
                            Some(bytes) => (axum::http::StatusCode::OK, bytes).into_response(),
                            None => axum::http::StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                },
            ),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop_sender, stop) = tokio::sync::oneshot::channel::<()>();
        // Its own thread and runtime, so a test can start it before its server.
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    tokio::select! {
                        _ = axum::serve(listener, app) => {}
                        _ = stop => {}
                    }
                });
        });
        Self {
            url,
            records,
            hits,
            _stop: stop_sender,
        }
    }

    /// Begin serving `bytes` under `hash`.
    fn serve(&self, hash: &str, bytes: Vec<u8>) {
        self.records
            .lock()
            .unwrap()
            .insert(hash.to_lowercase(), bytes);
    }

    /// Requests for `hash` so far, answered or not.
    fn hits(&self, hash: &str) -> usize {
        let hits = self.hits.lock().unwrap();
        hits.get(&hash.to_lowercase()).copied().unwrap_or(0)
    }

    /// Requests for any hash so far.
    fn total_hits(&self) -> usize {
        self.hits.lock().unwrap().values().sum()
    }
}

/// The six static words of a `postBySig` call, for the digest and the calldata.
fn post_words(author: [u8; 20], hash: [u8; 32], nonce: u64) -> [[u8; 32]; 6] {
    let word = |v: &[u8]| {
        let mut w = [0u8; 32];
        w[32 - v.len()..].copy_from_slice(v);
        w
    };
    [
        word(&author),
        hash,
        [0u8; 32],
        [0u8; 32],
        word(&nonce.to_be_bytes()),
        word(&1_800_000_000u64.to_be_bytes()),
    ]
}

/// `postBySig` calldata: the static words, then `sig` and empty `gateData`.
fn post_by_sig_input(words: &[[u8; 32]; 6], sig: &[u8]) -> Vec<u8> {
    let word = |n: u64| {
        let mut w = [0u8; 32];
        w[24..].copy_from_slice(&n.to_be_bytes());
        w
    };
    let padded = sig.len().div_ceil(32) * 32;
    let mut input = selector(crate::abi::POST_BY_SIG_SIGNATURE).to_vec();
    for w in words {
        input.extend_from_slice(w);
    }
    input.extend_from_slice(&word(8 * 32));
    input.extend_from_slice(&word((8 * 32 + 32 + padded) as u64));
    input.extend_from_slice(&word(sig.len() as u64));
    input.extend_from_slice(sig);
    input.resize(input.len() + padded - sig.len(), 0);
    input.extend_from_slice(&[0u8; 32]);
    input
}

/// An empty takedown list beside a test's host policy file, which a host
/// with lists needs to start.
fn write_no_takedowns(policy_file: &std::path::Path) {
    std::fs::write(
        crate::host_policy::takedowns::path(policy_file),
        r#"{"revision":0,"hashes":[]}"#,
    )
    .unwrap();
}

/// Mark a message deleted or removed in the index, as its gate event would.
fn set_indexed_gate(state: &AppState, chain: u64, id: u64, gate: crate::chain::Address) {
    let index = &state.gateway.as_ref().unwrap().indexes[&chain];
    index
        .view
        .write()
        .unwrap()
        .model
        .messages
        .get_mut(&id)
        .unwrap()
        .gate = gate;
}

fn record(media_type: &str, body: &[u8]) -> Vec<u8> {
    let mut v = format!("version/1 {media_type}\n").into_bytes();
    v.extend_from_slice(body);
    v
}

fn hx(r: &[u8]) -> String {
    keccak::hash(&[r]).to_hex()
}

mod admission;
mod arrivals;
mod attachment_sweep;
mod budget;
mod dense_blocks;
mod follow_fixtures;
mod held_acceptance;
mod held_receipts;
mod host_policy;
mod indexed_reads;
mod rejections;
mod reorg_race;

mod lifecycle;
mod paths;
mod peers;
mod services;
mod takedowns;
mod uploads;
mod web;

mod upload_validation;
