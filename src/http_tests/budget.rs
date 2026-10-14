//! The upstream budget: clients spend down to the reserve and are then
//! answered busy; ingestion spends the reserve and keeps the index moving;
//! tokens accrue again with time.

use super::*;
use crate::index::follow;
use std::sync::atomic::AtomicU64;

#[test]
fn a_stopped_index_receipt_check_preserves_busy_from_chain_verification() {
    with_server(
        "stopped-receipt-budget",
        Some(Node::new(31337, CONTRACT, Vec::new())),
        |url| Setup {
            verifier: chain_verifier_with(
                url.unwrap(),
                ChainConfig::for_test(31337, "", CONTRACT),
                UpstreamConfig {
                    calls_per_minute: 60,
                    reserved_for_ingestion: 59,
                },
            ),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             state,
             node,
         }| async move {
            state.gateway.as_ref().unwrap().indexes[&31337].stop("test stop");
            let upstream = state.verifier.upstream().unwrap();
            // Drain the client allowance, including any token accrued during startup.
            loop {
                match crate::rpc::call(
                    upstream,
                    &node.as_ref().unwrap().url,
                    "eth_chainId",
                    json!([]),
                )
                .await
                {
                    Ok(_) => {}
                    Err(crate::rpc::RpcError::Busy) => break,
                    Err(error) => panic!("{error}"),
                }
            }
            let response = client
                .get(format!(
                    "{base}/api/chains/31337/transaction/{}",
                    hex0x(&[7; 32])
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 503);
            assert_eq!(
                response.json::<Value>().await.unwrap()["error"],
                "host_busy"
            );
            // The same verification on the follower's startup path can use the reserve.
            state
                .verifier
                .check_chain(31337, crate::rpc::Class::Ingestion)
                .await
                .unwrap();
        },
    );
}

/// A receipt for a mined transaction takes two upstream calls, the receipt
/// and its block header. Running out of budget between them is the same
/// retryable busy as running out before the first.
#[test]
fn running_out_of_budget_between_the_receipt_and_its_header_is_busy_too() {
    // Two tokens a second and one token above the reserve: the receipt call
    // takes it, the header call finds none.
    let budget = UpstreamConfig {
        calls_per_minute: 120,
        reserved_for_ingestion: 119,
    };
    let body = record("text/plain", b"# mined\n");
    with_server(
        "budget-header",
        Some(Node {
            head: AtomicU64::new(20),
            messages: AtomicU64::new(1),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier_with(
                url.unwrap(),
                ChainConfig::for_test(31337, "", CONTRACT),
                budget,
            ),
            ..Setup::default()
        },
        move |Server {
                  base, client, node, ..
              }| async move {
            let node = node.unwrap();
            let hash = [7u8; 32];
            node.node.add_tx(MockTx {
                hash,
                from: [0xab; 20],
                to: CONTRACT.to_lowercase(),
                input: Vec::new(),
                block: Some(12),
                message_id: Some(1),
                reverted: false,
            });
            // Startup spent a token establishing the chain; let the bucket
            // fill back to capacity, which is the only way a client gets one.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let response = client
                .get(format!(
                    "{base}/api/chains/31337/transaction/{}",
                    hex0x(&hash)
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 503);
            assert_eq!(
                response.json::<Value>().await.unwrap()["error"],
                "host_busy"
            );
            let counts = node.node.counts();
            assert_eq!(
                counts.get("eth_getTransactionReceipt"),
                Some(&1),
                "the receipt was read"
            );
        },
    );
}

#[test]
fn client_traffic_stops_at_the_reserve_while_ingestion_keeps_moving() {
    // Twenty tokens a second, all but ten of a minute's worth reserved:
    // clients get about ten calls, then one more every twentieth of a second.
    let budget = UpstreamConfig {
        calls_per_minute: 1200,
        reserved_for_ingestion: 1190,
    };
    with_server(
        "budget",
        Some(Node {
            head: AtomicU64::new(15),
            messages: AtomicU64::new(3),
            ..Node::new(31337, CONTRACT, b"version/1 text/plain\nhello".to_vec())
        }),
        move |url| Setup {
            verifier: chain_verifier_with(
                url.unwrap(),
                ChainConfig {
                    start_block: 10,
                    confirmations: 2,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
                budget,
            ),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             state,
             node,
         }| async move {
            // A receipt for an unknown transaction costs one upstream call.
            let receipt = |i: u64| {
                client
                    .get(format!("{base}/api/chains/31337/transaction/0x{i:064x}"))
                    .send()
            };
            let mut answered = 0;
            let mut busy = None;
            for i in 1..=40 {
                let response = receipt(i).await.unwrap();
                match response.status().as_u16() {
                    200 => answered += 1,
                    503 => {
                        busy = Some(response.json::<Value>().await.unwrap());
                        break;
                    }
                    status => panic!("unexpected status {status}"),
                }
            }
            assert!(
                (5..=16).contains(&answered),
                "{answered} client calls before the reserve"
            );
            assert_eq!(busy.expect("a busy answer")["error"], "host_busy");
            // Ingestion draws on the reserve: the index catches up at once.
            follow::once(&state).await;
            follow::once(&state).await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            assert!(index.state().is_ready(), "{:?}", index.state());
            // Ingestion spent below the reserve, so clients wait until tokens
            // accrue back above it: about half a second here, then they are
            // served again.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            assert_eq!(receipt(99).await.unwrap().status().as_u16(), 200);
            let counts = node.unwrap().node.counts();
            assert!(counts.get("eth_getLogs").is_some_and(|n| *n >= 1));
            // The status counts what the budget did, per class.
            let status = get_json(&client, &format!("{base}/api/status")).await;
            let upstream = &status["upstream"];
            assert!(upstream["client"]["calls"].as_u64().unwrap() > answered as u64);
            assert!(upstream["client"]["busy"].as_u64().unwrap() >= 1);
            assert!(upstream["ingestion"]["calls"].as_u64().unwrap() >= 1);
            assert_eq!(upstream["ingestion"]["busy"], 0);
        },
    );
}

#[test]
fn publishing_has_its_own_visitor_budget() {
    with_server(
        "publish-visitor-budget",
        None,
        |_| Setup::default(),
        |Server { base, client, .. }| async move {
            for _ in 0..16 {
                let response = publish(&client, &base, "", "bad").await;
                assert_ne!(response.status().as_u16(), 429);
            }
            let response = publish(&client, &base, "", "bad").await;
            assert_eq!(response.status().as_u16(), 429);
            assert_eq!(
                response.json::<Value>().await.unwrap()["error"],
                "visitor_read_limit"
            );
            // Upload refusals must not exhaust receipt/admission checks.
            assert_ne!(
                client
                    .get(format!(
                        "{base}/api/chains/31337/transaction/{}",
                        hex0x(&[7; 32])
                    ))
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .as_u16(),
                429
            );
            assert_eq!(
                client
                    .get(format!("{base}/api/status"))
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .as_u16(),
                200
            );
        },
    );
}

#[test]
fn route_policies_keep_chain_reads_budgeted_and_index_reads_free() {
    with_server(
        "route-admission",
        None,
        |_| Setup::default(),
        |Server { base, client, .. }| async move {
            let receipt = "/api/chains/31337/transaction/bad";
            for _ in 0..16 {
                assert_ne!(
                    client
                        .get(format!("{base}{receipt}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    429
                );
            }
            for (method, path) in [
                (reqwest::Method::GET, receipt),
                (reqwest::Method::POST, "/api/chains/31337/admission"),
                (reqwest::Method::POST, "/api/chains/31337/post-estimate"),
            ] {
                let response = client
                    .request(method, format!("{base}{path}"))
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 429, "{path}");
                assert_eq!(response.headers()["retry-after"], "2");
            }
            // Invalid arguments may fail, but free index reads never spend the
            // exhausted chain-read allowance. Other work has its own budget.
            for (method, path) in [
                (reqwest::Method::GET, "/api/status"),
                (reqwest::Method::GET, "/api/content/bad"),
                (reqwest::Method::GET, "/api/chains/31337/keyring/bad"),
                (
                    reqwest::Method::GET,
                    "/api/chains/31337/keyring/bad/balances",
                ),
                (reqwest::Method::GET, "/api/chains/31337/keyring/bad/trades"),
                (reqwest::Method::GET, "/api/chains/31337/message/1"),
                (reqwest::Method::POST, "/api/chains/31337/key-holdings"),
                (reqwest::Method::POST, "/api/chains/31337/state"),
                (reqwest::Method::POST, "/api/chains/31337/list"),
                (reqwest::Method::POST, "/api/chains/31337/lookup"),
                (reqwest::Method::POST, "/api/chains/31337/feed"),
                (reqwest::Method::POST, "/api/chains/31337/replies"),
                (reqwest::Method::POST, "/api/chains/31337/changes"),
                (reqwest::Method::POST, "/api/chains/31337/references"),
                (reqwest::Method::POST, "/api/chains/31337/notifications"),
                (reqwest::Method::GET, "/api/names/bad"),
                (reqwest::Method::POST, "/api/publish"),
                (reqwest::Method::GET, "/api/unknown"),
            ] {
                let response = client
                    .request(method, format!("{base}{path}"))
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap();
                assert_ne!(response.status(), 429, "{path}");
            }
        },
    );
}

#[test]
fn cached_reverse_names_bypass_the_shared_forward_and_reverse_budget() {
    with_server(
        "name-admission-cache",
        None,
        |_| Setup {
            names: Some(NamesConfig {
                ethereum_upstream: None,
                base_upstream: None,
            }),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             state,
             ..
         }| async move {
            let address = "0x0000000000000000000000000000000000000001";
            state
                .gateway
                .as_ref()
                .unwrap()
                .names
                .as_ref()
                .unwrap()
                .lookup(address.parse().unwrap())
                .await
                .unwrap();
            for _ in 0..32 {
                assert_eq!(
                    client
                        .get(format!("{base}/api/names/bad"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    400
                );
            }
            for path in ["/api/names/bad", "/api/names/resolve/alice.eth"] {
                assert_eq!(
                    client
                        .get(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    429
                );
            }
            let response = client
                .get(format!("{base}/api/names/{address}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.json::<Value>().await.unwrap()["name"], Value::Null);
        },
    );
}

/// A Base name service on which every address claims alice.base.eth, whose
/// records point at `owner`: only `owner` has that name. A lookup of any
/// other address costs three calls. It runs on its own thread, so a test can
/// start it before its server, until the returned sender is dropped.
fn basenames(owner: [u8; 20]) -> (String, tokio::sync::oneshot::Sender<()>) {
    async fn answer(
        axum::extract::State(owner): axum::extract::State<[u8; 20]>,
        axum::Json(request): axum::Json<Value>,
    ) -> axum::Json<Value> {
        let data = request["params"][0]["data"].as_str().unwrap_or("");
        let data = hex::decode(data.trim_start_matches("0x")).unwrap_or_default();
        let word = |n: usize| {
            let mut w = [0u8; 32];
            w[24..].copy_from_slice(&(n as u64).to_be_bytes());
            w.to_vec()
        };
        let string = |s: &str| {
            let mut bytes = s.as_bytes().to_vec();
            bytes.resize(s.len().div_ceil(32) * 32, 0);
            [word(32), word(s.len()), bytes].concat()
        };
        let address = |a: [u8; 20]| [vec![0u8; 12], a.to_vec()].concat();
        let called = |signature: &str| data.starts_with(&selector(signature));
        let result = if called("nameForAddr(address)") {
            string("alice.base.eth")
        } else if called("resolver(bytes32)") {
            address([0x33; 20])
        } else if called("addr(bytes32)") {
            address(owner)
        } else if called("text(bytes32,string)") {
            string("")
        } else {
            return axum::Json(json!({"jsonrpc": "2.0", "id": 1,
                "error": {"code": 3, "message": "execution reverted"}}));
        };
        axum::Json(json!({"jsonrpc": "2.0", "id": 1, "result": hex0x(&result)}))
    }
    let app = axum::Router::new()
        .route("/", axum::routing::post(answer))
        .with_state(owner);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop_sender, stop) = tokio::sync::oneshot::channel::<()>();
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
    (url, stop_sender)
}

/// A flood of name lookups for random addresses, from enough networks that
/// no visitor limit stops it, is refused at the names' line with 503, not at
/// a visitor's limit with 429. An upload that must read the chain still gets
/// its call, from the headroom names leave. Once the budget refills, the name
/// and the recipient refused during the flood resolve: nothing was paused,
/// and no refusal was kept as a missing name.
#[test]
fn name_lookups_leave_posting_checks_their_headroom() {
    let owner = [0x44u8; 20];
    let body = record("text/plain", b"posted while names flood");
    let (names_url, _names) = basenames(owner);
    with_server(
        "names-headroom",
        Some(Node::new(31337, CONTRACT, body.clone())),
        move |url| Setup {
            verifier: chain_verifier_with(
                url.unwrap(),
                ChainConfig::for_test(31337, "", CONTRACT),
                // Two tokens a second, ten kept for ingestion: names stop at 65.
                UpstreamConfig {
                    calls_per_minute: 120,
                    reserved_for_ingestion: 10,
                },
            ),
            names: Some(NamesConfig {
                ethereum_upstream: None,
                base_upstream: Some(names_url),
            }),
            ..Setup::default()
        },
        move |Server {
                  base, client, node, ..
              }| async move {
            let node = node.unwrap();
            let from = |network: u32| format!("198.51.{}.{}", network / 250, network % 250 + 1);
            let lookup = |path: String, network: u32| {
                client
                    .get(format!("{base}{path}"))
                    .header("x-forwarded-for", from(network))
                    .send()
            };
            let mut refused = None;
            'flood: for network in 0..40u32 {
                for i in 0..20u32 {
                    let address = format!("0x{:040x}", 1_000 + network * 100 + i);
                    let response = lookup(format!("/api/names/{address}"), network)
                        .await
                        .unwrap();
                    match response.status().as_u16() {
                        200 => {}
                        status => {
                            refused = Some((status, response.json::<Value>().await.unwrap()));
                            break 'flood;
                        }
                    }
                }
            }
            let (status, answer) = refused.expect("the flood reached the names' line");
            assert_eq!(
                (status, answer["code"].as_str()),
                (503, Some("name_unavailable"))
            );
            let owner_address = hex0x(&owner);
            let display = lookup(format!("/api/names/{owner_address}"), 900)
                .await
                .unwrap();
            assert_eq!(display.status(), 503, "refused at the line too");
            let recipient = lookup("/api/names/resolve/alice.base.eth".into(), 901)
                .await
                .unwrap();
            assert_eq!(recipient.status(), 503, "recipients share the line");
            // A pending post's upload reads its transaction from the chain.
            let tx_hash = [7u8; 32];
            let mut input = selector(crate::abi::POST_SIGNATURE).to_vec();
            input.extend_from_slice(&keccak::hash(&[body.as_slice()]).0);
            input.extend_from_slice(&[0u8; 32 * 4]);
            node.node.add_tx(MockTx {
                hash: tx_hash,
                from: [0xab; 20],
                to: CONTRACT.to_lowercase(),
                input,
                block: None,
                message_id: None,
                reverted: false,
            });
            let reads = || node.node.count("eth_getTransactionByHash");
            assert_eq!(reads(), 0);
            let query = format!("chain_id=31337&tx_hash={}", hex0x(&tx_hash));
            let upload = publish(&client, &base, &query, body.clone()).await;
            assert_eq!(upload.status(), 202, "{}", upload.text().await.unwrap());
            assert_eq!(reads(), 1, "the upload read its transaction");
            let upstream =
                get_json(&client, &format!("{base}/api/status")).await["upstream"].clone();
            assert!(
                upstream["names"]["busy"].as_u64().unwrap() >= 1,
                "{upstream}"
            );
            assert_eq!(upstream["client"]["busy"], 0, "{upstream}");
            // The budget refills, and what was refused resolves. Wait once
            // rather than poll: each try would spend the token that just came
            // in and then be refused, so the balance would never climb. Four
            // seconds bring eight tokens, enough for the four calls of a
            // display lookup and the two of a recipient over the names' line.
            tokio::time::sleep(Duration::from_secs(4)).await;
            let display = lookup(format!("/api/names/{owner_address}"), 902)
                .await
                .unwrap();
            assert_eq!(display.status(), 200);
            assert_eq!(
                display.json::<Value>().await.unwrap()["name"],
                "alice.base.eth"
            );
            let recipient = lookup("/api/names/resolve/alice.base.eth".into(), 903)
                .await
                .unwrap();
            assert_eq!(recipient.status(), 200);
            assert_eq!(
                recipient.json::<Value>().await.unwrap()["address"],
                owner_address
            );
        },
    );
}
