//! Startup, history verification and fail-closed index states.

use super::*;

#[test]
fn ingestion_waits_only_after_catching_up() {
    use crate::index::follow;
    with_server(
        "poll-interval",
        Some(Node {
            head: 4_000.into(),
            ..Node::new(31337, CONTRACT, Vec::new())
        }),
        |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    poll_interval_secs: 2,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            store: false,
            ..Setup::default()
        },
        |s| async move {
            let node = &s.node.as_ref().unwrap().node;
            let index = &s.state.gateway.as_ref().unwrap().indexes[&31337];
            follow::start(s.state.clone());
            let wait_for = |block| {
                wait_until(
                    Duration::from_millis(1_500),
                    "ingestion reaches the head",
                    move || {
                        index
                            .view
                            .read()
                            .unwrap()
                            .model
                            .at
                            .checkpoint
                            .is_some_and(|c| c.block == block)
                    },
                )
            };
            wait_for(4_000).await;
            assert_eq!(
                node.count("eth_blockNumber"),
                3,
                "three backfill ranges run before the first poll delay"
            );
            node.set_head(8_000);
            tokio::time::sleep(Duration::from_millis(1_200)).await;
            assert_eq!(
                node.count("eth_blockNumber"),
                3,
                "a caught-up chain waits the configured two seconds, not one"
            );
            wait_for(8_000).await;
            assert_eq!(
                node.count("eth_blockNumber"),
                5,
                "a ready index that falls behind also catches up in contiguous ranges"
            );
            index.stop("test complete");
        },
    );
}

#[test]
fn a_chain_that_does_not_answer_at_startup_is_starting_until_it_does() {
    use crate::index::follow;
    let dir = std::env::temp_dir().join(format!("blohard-startup-waits-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            // Reserve an address and leave nothing listening on it: the chain
            // is down while the host boots.
            let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = reserved.local_addr().unwrap();
            drop(reserved);
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            let verifier = Verifier::from_config(
                &Config::for_test(vec![ChainConfig::for_test(
                    31337,
                    &format!("http://{addr}"),
                    CONTRACT,
                )]),
                client.clone(),
            )
            .unwrap();
            let state = Arc::new(
                open_state(
                    context,
                    verifier,
                    Options {
                        store: None,
                        gateway: Some(GatewayConfig::default()),
                        holding: HoldingConfig::default(),
                        notice: None,
                        policy: Default::default(),
                        keyrings: Default::default(),
                    },
                    client.clone(),
                )
                .await
                .expect("opening an index needs no chain access"),
            );
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let phase = || {
                let view = index.view.read().unwrap();
                (
                    view.state().is_established(),
                    view.state().is_ready(),
                    !view.state().is_stopped(),
                )
            };
            assert_eq!(phase(), (false, false, true), "starting, not stopped");
            follow::once(&state).await;
            assert_eq!(
                phase(),
                (false, false, true),
                "an unreachable chain is retried, never marked stopped"
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let server =
                tokio::spawn(axum::serve(listener, router(state.clone(), None)).into_future());
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["chains"][0]["index"]["state"], "starting");
            assert_eq!(
                client
                    .post(format!("{base}/api/chains/31337/feed"))
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                503
            );
            // The chain comes up at the same address; the next poll confirms
            // it and catches up. No restart.
            let node = mock_node::spawn_on(
                Node {
                    head: 2.into(),
                    ..Node::new(31337, CONTRACT, Vec::new())
                },
                tokio::net::TcpListener::bind(addr).await.unwrap(),
            )
            .await;
            follow::once(&state).await;
            assert_eq!(phase(), (true, true, true));
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["chains"][0]["index"]["state"], "ready");
            assert_eq!(
                client
                    .post(format!("{base}/api/chains/31337/feed"))
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            server.abort();
            node.stop().await;
        },
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn startup_isolates_a_journal_failure_but_stops_all_indexes_on_history_change() {
    use crate::chain::Hex;
    use crate::index::{Index, follow, types::Deployment};
    use commonware_runtime::Supervisor as _;
    // An incompatible journal stays intact for repair and stops only its own
    // chain; replaced history stops every index, since cross-chain
    // associations may depend on it.
    for history_changed in [false, true] {
        let dir = std::env::temp_dir().join(format!(
            "blohard-startup-isolation-{}-{history_changed}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
            |context| async move {
                let body = record("text/plain", b"still available");
                let a = mock_node::spawn(Node {
                    head: 2.into(),
                    ..Node::new(31337, CONTRACT, body.clone())
                })
                .await;
                let b = mock_node::spawn(Node {
                    head: 2.into(),
                    ..Node::new(31338, CONTRACT, body.clone())
                })
                .await;
                let client = reqwest::Client::new();
                let verifier = Verifier::from_config(
                    &Config::for_test(vec![
                        ChainConfig::for_test(31337, &a.url, CONTRACT),
                        ChainConfig::for_test(31338, &b.url, CONTRACT),
                    ]),
                    client.clone(),
                )
                .unwrap();
                let prior = Index::open(
                    context.child("prior"),
                    Deployment {
                        chain: 31337,
                        board: CONTRACT.parse().unwrap(),
                        start_block: u64::from(!history_changed),
                        index_start_block: u64::from(!history_changed),
                        registries: vec![],
                    },
                    Hex([1; 32]),
                )
                .await
                .unwrap();
                prior.establish().unwrap();
                if history_changed {
                    prior
                        .append(crate::index::model::Batch {
                            from: 0,
                            through: crate::chain::Checkpoint {
                                block: 1,
                                hash: Hex([99; 32]),
                            },
                            events: vec![],
                        })
                        .await
                        .unwrap();
                    // Empty progress stays in RAM until flushed; journal it.
                    prior.flush().await.unwrap();
                }
                drop(prior);
                tokio::time::sleep(Duration::from_millis(10)).await;
                let state = Arc::new(
                    open_state(
                        context,
                        verifier,
                        Options {
                            store: Some(StoreConfig {
                                page_cache_mib: 0,
                                max_body_bytes: 0,
                                attachments: None,
                                peers: vec![],
                            }),
                            gateway: Some(GatewayConfig::default()),
                            holding: HoldingConfig::default(),
                            notice: None,
                            policy: Default::default(),
                            keyrings: Default::default(),
                        },
                        client.clone(),
                    )
                    .await
                    .expect("one bad journal must not abort startup"),
                );
                follow::once(&state).await;
                let indexes = &state.gateway.as_ref().unwrap().indexes;
                assert!(indexes[&31337].state().is_stopped());
                assert_eq!(indexes[&31338].state().is_stopped(), history_changed);
                assert!(
                    indexes[&31337]
                        .append(crate::index::model::Batch {
                            from: 0,
                            through: crate::chain::Checkpoint {
                                block: 0,
                                hash: Hex([0; 32]),
                            },
                            events: vec![],
                        })
                        .await
                        .is_err(),
                    "failed index cannot accept writes"
                );
                state
                    .store
                    .as_ref()
                    .unwrap()
                    .bodies
                    .put(body.clone())
                    .await
                    .unwrap();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let base = format!("http://{}", listener.local_addr().unwrap());
                let server = tokio::spawn(axum::serve(listener, router(state, None)).into_future());
                for (chain, expected) in [
                    (31337, 503),
                    (31338, if history_changed { 503 } else { 200 }),
                ] {
                    assert_eq!(
                        client
                            .post(format!("{base}/api/chains/{chain}/feed"))
                            .json(&json!({}))
                            .send()
                            .await
                            .unwrap()
                            .status(),
                        expected
                    );
                }
                let status = client
                    .get(format!("{base}/api/status"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(status.status(), 503);
                let status: Value = status.json().await.unwrap();
                assert_eq!(
                    status["chains"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|c| c["id"] == 31337)
                        .unwrap()["index"]["state"],
                    "stopped"
                );
                assert_eq!(
                    client
                        .get(format!("{base}/api/content/{}", hx(&body)))
                        .send()
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap()
                        .as_ref(),
                    body.as_slice()
                );
                server.abort();
            },
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn apply_failure_returns_unavailable_instead_of_panicking_in_routes() {
    use crate::chain::{Checkpoint, Hex, Position};
    use crate::index::{
        model::{Batch, Event},
        types::REMOVED,
    };
    with_server(
        "apply-failure",
        Some(Node {
            head: 14.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, record("text/plain", b"hello"))
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             state,
             ..
         }| async move {
            crate::index::follow::once(&state).await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let block = {
                let mut view = index.view.write().unwrap();
                // Fault injection: a reply with no derived count to drop on removal.
                view.model.messages.get_mut(&2).unwrap().parent = 1;
                view.model.next_block().unwrap()
            };
            assert!(
                index
                    .append(Batch {
                        from: block,
                        through: Checkpoint {
                            block,
                            hash: Hex([55; 32])
                        },
                        events: vec![Event::ReplyGate {
                            id: 2,
                            old: Hex([0; 20]),
                            new: REMOVED,
                            position: Position { block, log: 0 }
                        }],
                    })
                    .await
                    .is_err()
            );
            let response = client
                .get(format!("{base}/api/chains/31337/message/2"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 503);
            assert!(response.text().await.unwrap().contains("index_unavailable"));
            let status = client
                .get(format!("{base}/api/status"))
                .send()
                .await
                .unwrap();
            assert_eq!(status.status(), 503);
            let status: Value = status.json().await.unwrap();
            assert_eq!(status["chains"][0]["index"]["state"], "stopped");
        },
    );
}

#[test]
fn a_wrong_chain_stops_only_that_index_and_refuses_live_work() {
    let dir = std::env::temp_dir().join(format!("blohard-wrong-chain-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(|context| async move {
        let node = mock_node::spawn(Node::new(31338, CONTRACT, vec![])).await;
        let client = reqwest::Client::new();
        let state = Arc::new(open_state(context, chain_verifier(&node.url, ChainConfig::for_test(31337, "", CONTRACT)),
            Options { store: None, gateway: Some(GatewayConfig::default()), holding: HoldingConfig::default(), notice: None, policy: Default::default(), keyrings: Default::default() }, client.clone()).await.unwrap());
        crate::index::follow::once(&state).await;
        let index = &state.gateway.as_ref().unwrap().indexes[&31337];
        assert!(matches!(index.state(), crate::index::State::Stopped(reason) if reason.contains("chain id mismatch")));
        assert!(index.view.read().unwrap().model.at.checkpoint.is_none());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(axum::serve(listener, router(state, None)).into_future());
        assert_eq!(client.get(format!("{base}/api/chains/31337/transaction/{}", hex0x(&[99;32]))).send().await.unwrap().status(), 502);
        assert_eq!(node.node.count("eth_getTransactionReceipt"), 0);
        server.abort(); node.stop().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
}

/// A clean stop journals the progress each index served from RAM and commits
/// what the stores took since their last commit: the next start resumes where
/// the host stopped serving, and every body stored before the stop is there.
#[test]
fn a_clean_stop_keeps_the_served_checkpoint_and_stored_bodies() {
    let dir = std::env::temp_dir().join(format!("blohard-clean-stop-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let body = record("text/plain", b"kept through a clean stop");
    let digest = crate::keccak::Digest::from_hex(&hx(&body)).unwrap();
    let options = || Options {
        store: Some(StoreConfig {
            page_cache_mib: 0,
            max_body_bytes: 0,
            attachments: None,
            peers: vec![],
        }),
        gateway: Some(GatewayConfig::default()),
        holding: HoldingConfig::default(),
        notice: None,
        policy: Default::default(),
        keyrings: Default::default(),
    };
    let verifier = |url: &str, client: &reqwest::Client| {
        Verifier::from_config(
            &Config::for_test(vec![ChainConfig::for_test(31337, url, CONTRACT)]),
            client.clone(),
        )
        .unwrap()
    };
    let stored = body.clone();
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            let node = mock_node::spawn(Node {
                head: 100.into(),
                messages: 1.into(),
                ..Node::new(31337, CONTRACT, stored.clone())
            })
            .await;
            let client = reqwest::Client::new();
            let state = Arc::new(
                open_state(context, verifier(&node.url, &client), options(), client)
                    .await
                    .unwrap(),
            );
            let background = crate::Background::start(&state);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let served = |block: u64| async move {
                let what = format!("the index serves block {block}");
                wait_until(Duration::from_secs(10), &what, || {
                    index
                        .view
                        .read()
                        .unwrap()
                        .model
                        .at
                        .checkpoint
                        .is_some_and(|c| c.block == block)
                })
                .await;
            };
            served(100).await;
            // Empty blocks past the last journaled batch are served from RAM only.
            node.node.set_head(250);
            served(250).await;
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(stored)
                .await
                .unwrap();
            background.stop(&state).await;
            node.stop().await;
        },
    );
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            let client = reqwest::Client::new();
            // No chain is needed to open the journals and stores again.
            let state = open_state(
                context,
                verifier("http://127.0.0.1:9", &client),
                options(),
                client,
            )
            .await
            .unwrap();
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            assert_eq!(
                index
                    .view
                    .read()
                    .unwrap()
                    .model
                    .at
                    .checkpoint
                    .map(|c| c.block),
                Some(250),
                "the restart resumes from the checkpoint the host last served"
            );
            assert_eq!(
                state
                    .store
                    .as_ref()
                    .unwrap()
                    .bodies
                    .get(&digest)
                    .await
                    .unwrap(),
                Some(body),
                "a body stored before the stop survives it"
            );
        },
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// An upload still being checked when the host stops is stored before the
/// final commit: it holds its slot until its write is done, after its client
/// has gone and the server has stopped, and the stop waits for every slot.
#[test]
fn a_clean_stop_waits_for_an_upload_still_running() {
    use tokio::io::AsyncWriteExt as _;
    let dir = std::env::temp_dir().join(format!("blohard-stop-upload-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let body = record("text/plain", b"stored while the host stops");
    let digest = crate::keccak::Digest::from_hex(&hx(&body)).unwrap();
    // A store-only host checks each upload with its node, which the test can slow.
    let open = |context, url: &str| {
        let client = reqwest::Client::new();
        let verifier = Verifier::from_config(
            &Config::for_test(vec![ChainConfig::for_test(31337, url, CONTRACT)]),
            client.clone(),
        )
        .unwrap();
        let options = Options {
            store: Some(StoreConfig {
                page_cache_mib: 0,
                max_body_bytes: 0,
                attachments: None,
                peers: vec![],
            }),
            gateway: None,
            holding: HoldingConfig::default(),
            notice: None,
            policy: Default::default(),
            keyrings: Default::default(),
        };
        open_state(context, verifier, options, client)
    };
    let upload = body.clone();
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            let node = mock_node::spawn(Node {
                head: 100.into(),
                messages: 1.into(),
                ..Node::new(31337, CONTRACT, upload.clone())
            })
            .await;
            let state = Arc::new(open(context, &node.url).await.unwrap());
            let background = crate::Background::start(&state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(axum::serve(listener, router(state.clone(), None)).into_future());
            // The upload's two reads, the confirmed head and then its message,
            // now take half a second each: well inside its two-second deadline.
            node.node
                .latency_ms
                .store(500, std::sync::atomic::Ordering::Relaxed);
            let reads = || node.node.count("eth_call");
            let before = reads();
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            let head = format!(
                "POST /api/publish?chain_id=31337&message_id=1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                upload.len()
            );
            client.write_all(head.as_bytes()).await.unwrap();
            client.write_all(&upload).await.unwrap();
            // Asking for its message, the upload has its bytes and is being
            // checked. Its client goes, and the server stops.
            wait_until(Duration::from_secs(5), "the upload reads its message", || {
                reads() != before
            })
            .await;
            drop(client);
            server.abort();
            let uploads = &state.store.as_ref().unwrap().uploads;
            let free = || uploads.small.slots.available_permits();
            assert_eq!(free(), crate::publish::http::MAX_SMALL_UPLOADS - 1, "the upload holds its slot");
            background.stop(&state).await;
            assert_eq!(free(), crate::publish::http::MAX_SMALL_UPLOADS, "the stop waited for the upload");
            node.stop().await;
        },
    );
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            // A store-only host checks its chain as it opens.
            let node = mock_node::spawn(Node::new(31337, CONTRACT, Vec::new())).await;
            let state = open(context, &node.url).await.unwrap();
            assert_eq!(
                state
                    .store
                    .as_ref()
                    .unwrap()
                    .bodies
                    .get(&digest)
                    .await
                    .unwrap(),
                Some(body),
                "the upload's bytes survive the stop"
            );
            node.stop().await;
        },
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A keyring journal that does not match the configuration leaves the keyring
/// stopped and the rest of the host serving.
#[test]
fn a_keyring_that_cannot_open_leaves_the_host_serving() {
    let dir = std::env::temp_dir().join(format!("blohard-keyring-open-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let open = |context, start_block: u64| {
        let client = reqwest::Client::new();
        let verifier = Verifier::from_config(
            &Config::for_test(vec![
                ChainConfig::for_test(84532, "http://127.0.0.1:9", CONTRACT),
                ChainConfig::for_test(8453, "http://127.0.0.1:9", CONTRACT),
            ]),
            client.clone(),
        )
        .unwrap();
        let options = Options {
            store: None,
            gateway: Some(GatewayConfig::default()),
            holding: HoldingConfig::default(),
            notice: None,
            policy: Default::default(),
            keyrings: [84532, 8453]
                .into_iter()
                .map(|chain| crate::keyring::Config {
                    chain,
                    market: "0x00000000000000000000000000000000000000aa"
                        .parse()
                        .unwrap(),
                    start_block: if chain == 84532 { start_block } else { 10 },
                })
                .collect(),
        };
        open_state(context, verifier, options, client)
    };
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            let state = open(context, 10).await.unwrap();
            assert_eq!(state.keyrings.len(), 2);
            assert_eq!(state.keyrings[&8453].status()["state"], "starting");
            assert_eq!(
                state.keyrings.get(&84532).unwrap().status()["state"],
                "starting"
            );
        },
    );
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(
        |context| async move {
            // The journal was bound to a market that starts at block 10.
            let state = open(context, 11).await.unwrap();
            assert_eq!(
                state.keyrings.get(&84532).unwrap().status()["state"],
                "stopped"
            );
            assert_eq!(state.keyrings[&8453].status()["state"], "starting");
            assert!(state.gateway.as_ref().unwrap().indexes.contains_key(&8453));
        },
    );
    let _ = std::fs::remove_dir_all(&dir);
}
