//! Bounded live transaction and name services.

use super::*;

#[test]
fn transaction_services_build_bounded_calls_and_decode_receipts() {
    use std::sync::atomic::AtomicU64;
    with_server(
        "transaction-services",
        Some(Node {
            head: AtomicU64::new(14),
            messages: AtomicU64::new(1),
            txs: std::sync::Mutex::new(vec![MockTx {
                hash: [33; 32],
                from: [1; 20],
                to: CONTRACT.into(),
                input: vec![],
                block: Some(11),
                message_id: Some(1),
                reverted: false,
            }]),
            ..Node::new(31337, CONTRACT, b"version/1 text/plain\nhello".to_vec())
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             node,
             state,
         }| async move {
            let node = node.unwrap();
            let path = format!("{base}/api/chains/31337");
            let replier = hex0x(&[1; 20]);
            let admission = post_json(
                &client,
                &format!("{path}/admission"),
                json!({"parent":"1", "replier": replier, "gate_data":"0x1234"}),
            )
            .await;
            assert_eq!(admission["gate"], hex0x(&[0; 20]));
            let request = node.node.last.lock().unwrap()["eth_call"].clone();
            assert_eq!(
                request[0]["gas"],
                format!("0x{:x}", crate::services::POST_GAS_LIMIT)
            );
            assert_eq!(request[0]["to"], CONTRACT.to_lowercase());
            let data = crate::abi::decode_hex(request[0]["data"].as_str().unwrap()).unwrap();
            assert_eq!(&data[..4], &selector("checkReply(uint64,address,bytes)"));
            assert_eq!(data[35], 1);
            assert_eq!(&data[48..68], &[1; 20]);
            assert_eq!(data[99], 96, "dynamic argument offset");
            assert_eq!(data[131], 2, "opaque proof length");
            assert_eq!(&data[132..134], &[0x12, 0x34]);
            let estimate = post_json(
                &client,
                &format!("{path}/post-estimate"),
                json!({"author": replier, "content_hash": hex0x(&[22;32]), "parent":"1"}),
            )
            .await;
            assert_eq!(estimate["gas"], "125000");
            let request = node.node.last.lock().unwrap()["eth_estimateGas"].clone();
            assert_eq!(
                request[0]["gas"],
                format!("0x{:x}", crate::services::POST_GAS_LIMIT)
            );
            let data = crate::abi::decode_hex(request[0]["data"].as_str().unwrap()).unwrap();
            assert_eq!(&data[..4], &selector(crate::abi::POST_SIGNATURE));
            assert_eq!(&data[4..36], &[22; 32]);
            assert_eq!(data[131], 128);
            assert_eq!(
                &data[68..100],
                &[0; 32],
                "omitted reply gate inherits the account rule"
            );
            let estimate = post_json(&client, &format!("{path}/post-estimate"), json!({"author": replier, "content_hash": hex0x(&[22;32]), "parent":"0", "reply_gate": hex0x(&[44;20])})).await;
            assert_eq!(estimate["gas"], "125000");
            let request = node.node.last.lock().unwrap()["eth_estimateGas"].clone();
            let data = crate::abi::decode_hex(request[0]["data"].as_str().unwrap()).unwrap();
            assert_eq!(
                &data[68..100],
                &crate::abi::address_word(crate::chain::Hex([44; 20])),
                "estimate the same gate the wallet will send"
            );

            let pending =
                get_json(&client, &format!("{path}/transaction/{}", hex0x(&[99; 32]))).await;
            assert_eq!(pending["status"], "pending");
            let receipt =
                get_json(&client, &format!("{path}/transaction/{}", hex0x(&[33; 32]))).await;
            assert_eq!(receipt["status"], "success");
            assert_eq!(receipt["block"], "11");
            assert_eq!(receipt["events"][0]["message"]["id"], "1");
            node.node.txs.lock().unwrap()[0].reverted = true;
            let receipt =
                get_json(&client, &format!("{path}/transaction/{}", hex0x(&[33; 32]))).await;
            assert_eq!(receipt["status"], "reverted");
            assert!(receipt["events"].as_array().unwrap().is_empty());

            *node.node.simulation_error.lock().unwrap() = Some(
                json!({"code":3, "message":format!("execution reverted at {}", node.url), "data":"0x12345678"}),
            );
            let refused = client
                .post(format!("{path}/admission"))
                .json(&json!({"parent":"1", "replier":replier}))
                .send()
                .await
                .unwrap();
            assert_eq!(refused.status(), 422);
            let body = refused.text().await.unwrap();
            assert!(!body.contains(&node.url));
            assert!(body.contains("0x12345678"));
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            assert_eq!(index.state(), crate::index::State::Backfilling);
            index.stop("test stop");
            let receipt =
                get_json(&client, &format!("{path}/transaction/{}", hex0x(&[33; 32]))).await;
            assert_eq!(
                receipt["status"], "reverted",
                "receipt recovery needs no readable index"
            );
            assert_eq!(
                client
                    .post(format!("{path}/admission"))
                    .json(&json!({"parent":"1", "replier":replier}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                503
            );
        },
    );
}

/// A host that proxies name lookups answers the three read methods from
/// its cache, refuses everything else, and tells clients where to go.
#[test]
fn names_are_an_address_query_not_a_public_rpc() {
    let node = Node::new(31337, CONTRACT, vec![]);
    with_server(
        "http-names",
        Some(node),
        |url| Setup {
            names: Some(NamesConfig {
                ethereum_upstream: Some(url.unwrap().into()),
                base_upstream: None,
            }),
            ..Setup::default()
        },
        move |Server {
                  base, client, node, ..
              }| async move {
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["gateway"]["names"], true);
            assert!(!status.to_string().contains(&node.as_ref().unwrap().url));
            let old = client
                .post(format!("{base}/api/names/ethereum/rpc"))
                .json(&json!({"method":"eth_call"}))
                .send()
                .await
                .unwrap();
            assert_eq!(old.status().as_u16(), 404);
            let bad = client
                .get(format!("{base}/api/names/not-an-address"))
                .send()
                .await
                .unwrap();
            assert_eq!(bad.status().as_u16(), 400);
        },
    );
}

#[test]
fn unverified_key_gate_configuration_is_not_published() {
    with_server(
        "key-gate-status",
        Some(Node::new(
            31337,
            CONTRACT,
            b"version/1 text/plain\nhello".to_vec(),
        )),
        |url| {
            // Parse the operator's TOML and check the public response, end to end.
            let cfg = Config::parse(&format!(
                r#"
                [gateway]
                [[chains]]
                id = 31337
                rpc_url = "{}"
                contract = "{CONTRACT}"
                blocking = {{ gate = "{}", registry = "{}", list = "blocked" }}
                key_gates = {{ holders = "{}", holders_blocklist = "{}" }}
            "#,
                url.unwrap(),
                hex0x(&[3; 20]),
                hex0x(&[4; 20]),
                hex0x(&[5; 20]),
                hex0x(&[6; 20])
            ))
            .unwrap();
            Setup {
                verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
                ..Setup::default()
            }
        },
        |Server { base, client, .. }| async move {
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(
                status["chains"][0]["key_gates"],
                json!({
                    "holders": null, "holders_blocklist": null,
                })
            );
            assert!(status["chains"][0]["blocking"].is_null());
            assert_eq!(
                status["chains"][0]["blocklist"],
                json!({"registry":hex0x(&[4; 20]),"list":"blocked"})
            );
        },
    );
}

#[test]
fn link_previews_are_opt_in_and_reject_unsafe_urls_before_fetching() {
    for (enabled, store) in [(false, true), (true, true), (true, false)] {
        with_server(
            if !store {
                "preview-without-store"
            } else if enabled {
                "preview-enabled"
            } else {
                "preview-disabled"
            },
            None,
            move |_| Setup {
                store,
                link_previews: enabled.then(|| crate::link_preview::Config {
                    public_names: vec!["operator.invalid".into()],
                }),
                ..Setup::default()
            },
            move |Server { client, base, .. }| async move {
                let status = get_json(&client, &format!("{base}/api/status")).await;
                assert_eq!(status["gateway"]["link_previews"], enabled && store);
                assert_eq!(status["gateway"]["link_previews_ready"], false);
                let response = client
                    .get(format!("{base}/api/link-preview"))
                    .header("x-blohard-preview", "1")
                    .query(&[
                        ("chain", "31337"),
                        ("id", "1"),
                        ("link", "http://127.1/private"),
                    ])
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status().as_u16(), if enabled { 400 } else { 503 });
                assert_eq!(response.headers()["cache-control"], "no-store");
            },
        );
    }
}

#[test]
fn link_preview_fetches_require_a_stored_indexed_post_with_that_link() {
    let body = b"version/1 text/plain\nread example.org/article#section example.org/deleted example.org/removed".to_vec();
    with_server(
        "preview-post-proof",
        Some(Node {
            head: std::sync::atomic::AtomicU64::new(14),
            messages: std::sync::atomic::AtomicU64::new(1),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            link_previews: Some(crate::link_preview::Config {
                public_names: vec!["operator.invalid".into()],
            }),
            ..Setup::default()
        },
        move |Server {
                  client,
                  base,
                  state,
                  node,
              }| async move {
            crate::index::follow::once(&state).await;
            let node = node.unwrap();
            let calls = node.node.counts();
            let get = |chain, id, link| {
                client
                    .get(format!("{base}/api/link-preview"))
                    .header("x-blohard-preview", "1")
                    .query(&[("chain", chain), ("id", id), ("link", link)])
                    .send()
            };
            assert_eq!(
                get("31337", "1", "example.org/article#section")
                    .await
                    .unwrap()
                    .status(),
                503,
                "missing bytes must not trigger a peer or chain read"
            );
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(body)
                .await
                .unwrap();
            assert_eq!(
                get("31337", "1", "example.org/other")
                    .await
                    .unwrap()
                    .status(),
                400
            );
            assert_eq!(
                get("31337", "999", "example.org/article#section")
                    .await
                    .unwrap()
                    .status(),
                404
            );
            assert_eq!(
                get("999", "1", "example.org/article#section")
                    .await
                    .unwrap()
                    .status(),
                404
            );
            // Only a posted link reaches the discovery check. No background
            // discovery has run, so this test never contacts a public page.
            let response = get("31337", "1", "example.org/article#section")
                .await
                .unwrap();
            assert_eq!(response.status(), 503);
            assert_eq!(response.headers()["cache-control"], "no-store");
            // Discovery failure creates no cached answer. Neither deletion nor
            // moderation may authorize fetching a link from these bytes.
            use crate::chain::{Address, Checkpoint, Hex, Position};
            use crate::index::{
                model::{Batch, Event},
                types::{REMOVED, TOMBSTONE},
            };
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let root = index
                .read_ready(|model| model.messages[&1].clone())
                .unwrap();
            let mut reply = root.clone();
            reply.id = 2;
            reply.parent = 1;
            reply.parent_author = root.author;
            reply.position = Position { block: 15, log: 0 };
            index
                .append(Batch {
                    from: 15,
                    through: Checkpoint {
                        block: 15,
                        hash: Hex([15; 32]),
                    },
                    events: vec![
                        Event::Posted { message: reply },
                        Event::ReplyGate {
                            id: 1,
                            old: Address::ZERO,
                            new: TOMBSTONE,
                            position: Position { block: 15, log: 1 },
                        },
                        Event::ReplyGate {
                            id: 2,
                            old: Address::ZERO,
                            new: REMOVED,
                            position: Position { block: 15, log: 2 },
                        },
                    ],
                })
                .await
                .unwrap();
            for (id, link) in [("1", "example.org/deleted"), ("2", "example.org/removed")] {
                assert_eq!(
                    get("31337", id, link).await.unwrap().status(),
                    404,
                    "hidden post {id} cannot authorize a fetch"
                );
            }
            assert_eq!(
                get("31337", "1", "example.org/article#section")
                    .await
                    .unwrap()
                    .status(),
                404,
                "local discovery failure must not create a cached answer"
            );
            assert_eq!(
                node.node.counts(),
                calls,
                "preview authorization performs no chain calls"
            );
        },
    );
}

#[test]
fn draft_previews_are_opt_in_and_have_their_own_stricter_visitor_budget() {
    for enabled in [false, true] {
        with_server(
            if enabled {
                "draft-preview-enabled"
            } else {
                "draft-preview-disabled"
            },
            None,
            move |_| Setup {
                link_previews: enabled.then(|| crate::link_preview::Config {
                    public_names: vec!["operator.invalid".into()],
                }),
                ..Setup::default()
            },
            move |Server { client, base, .. }| async move {
                // Unsafe destinations are refused without needing a chain, post,
                // draft upload or any outbound connection.
                for _ in 0..4 {
                    let response = client
                        .get(format!("{base}/api/link-preview/draft"))
                        .header("x-blohard-preview", "1")
                        .query(&[("link", "http://127.1/private")])
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(response.status().as_u16(), if enabled { 400 } else { 503 });
                    assert_eq!(response.headers()["cache-control"], "no-store");
                }
                let response = client
                    .get(format!("{base}/api/link-preview/draft"))
                    .header("x-blohard-preview", "1")
                    .query(&[("link", "https://example.org/")])
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 429);
                assert_eq!(response.headers()["retry-after"], "15");
                // Exhausting drafts cannot take the published-preview allowance.
                let response = client
                    .get(format!("{base}/api/link-preview"))
                    .header("x-blohard-preview", "1")
                    .query(&[
                        ("chain", "31337"),
                        ("id", "1"),
                        ("link", "http://127.1/private"),
                    ])
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status().as_u16(), if enabled { 400 } else { 503 });
            },
        );
    }
}

#[test]
fn link_preview_browser_admission_precedes_fetches_and_has_no_cross_origin_cors() {
    with_server(
        "preview-origin",
        None,
        |_| Setup {
            link_previews: Some(crate::link_preview::Config {
                public_names: vec!["operator.invalid".into()],
            }),
            ..Setup::default()
        },
        |Server { client, base, .. }| async move {
            for path in [
                "/api/link-preview?chain=1&id=1&link=http://127.1/",
                "/api/link-preview/draft?link=http://127.1/",
            ] {
                let target = format!("{base}{path}");
                // Simple browser requests (including no-cors images) cannot pass.
                assert_eq!(client.get(&target).send().await.unwrap().status(), 403);
                for (header, value) in [
                    ("origin", "https://attacker.example"),
                    ("origin", "null"),
                    ("origin", "http://operator.invalid"),
                    ("origin", "https://operator.invalid:444"),
                    ("sec-fetch-site", "cross-site"),
                    ("sec-fetch-site", "same-site"),
                ] {
                    let response = client
                        .get(&target)
                        .header("x-blohard-preview", "1")
                        .header(header, value)
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(response.status(), 403, "{header}: {value}");
                    assert!(
                        response
                            .headers()
                            .get("access-control-allow-origin")
                            .is_none()
                    );
                }
                for headers in [
                    vec![],
                    vec![("sec-fetch-site", "same-origin")],
                    vec![("origin", "https://operator.invalid")],
                ] {
                    let mut request = client.get(&target).header("x-blohard-preview", "1");
                    for (name, value) in headers {
                        request = request.header(name, value);
                    }
                    assert_eq!(
                        request.send().await.unwrap().status(),
                        400,
                        "valid callers reach URL validation"
                    );
                }
                let preflight = client
                    .request(reqwest::Method::OPTIONS, &target)
                    .header("origin", "https://attacker.example")
                    .header("access-control-request-method", "GET")
                    .header("access-control-request-headers", "x-blohard-preview")
                    .send()
                    .await
                    .unwrap();
                assert!(
                    preflight
                        .headers()
                        .get("access-control-allow-origin")
                        .is_none()
                );
            }
            // Other API clients keep their existing CORS access.
            let response = client
                .get(format!(
                    "{base}/api/names/0x0000000000000000000000000000000000000001"
                ))
                .header("origin", "https://attacker.example")
                .send()
                .await
                .unwrap();
            assert_eq!(response.headers()["access-control-allow-origin"], "*");
        },
    );
}

/// Identical receipt checks at the same moment, from two tabs, share one
/// read of the chain; a later check asks again, since pending is never kept.
#[test]
fn identical_receipt_checks_share_one_read_and_keep_nothing() {
    with_server(
        "shared-receipts",
        Some(Node::new(31337, CONTRACT, Vec::new())),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             state,
             node,
         }| async move {
            crate::index::follow::once(&state).await;
            let node = node.unwrap();
            let reads = || node.node.count("eth_getTransactionReceipt");
            let url = format!("{base}/api/chains/31337/transaction/{}", hex0x(&[9; 32]));
            // Two tabs ask while the node is slow (one visitor has two read slots).
            node.node
                .latency_ms
                .store(300, std::sync::atomic::Ordering::Relaxed);
            let before = reads();
            let ask = || client.get(&url).send();
            let (a, b) = tokio::join!(ask(), ask());
            for response in [a, b] {
                let answer = response.unwrap().json::<Value>().await.unwrap();
                assert_eq!(answer["status"], "pending", "{answer}");
            }
            assert_eq!(reads() - before, 1, "one read for both");
            node.node
                .latency_ms
                .store(0, std::sync::atomic::Ordering::Relaxed);
            let later = ask().await.unwrap().json::<Value>().await.unwrap();
            assert_eq!(later["status"], "pending");
            assert_eq!(reads() - before, 2, "a later check reads again");
        },
    );
}

/// A receipt check nobody waits for any more is dropped, with its chain calls,
/// rather than left running: a reader that leaves frees what it used.
#[test]
fn a_receipt_check_whose_readers_left_is_not_kept_running() {
    with_server(
        "abandoned-receipts",
        Some(Node::new(31337, CONTRACT, Vec::new())),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        |Server {
             base,
             client,
             state,
             node,
         }| async move {
            crate::index::follow::once(&state).await;
            let node = node.unwrap();
            let reads = || node.node.count("eth_getTransactionReceipt");
            let url = format!("{base}/api/chains/31337/transaction/{}", hex0x(&[10; 32]));
            node.node
                .latency_ms
                .store(400, std::sync::atomic::Ordering::Relaxed);
            let before = reads();
            // The only reader gives up before the slow node answers.
            let gone = client
                .get(&url)
                .timeout(Duration::from_millis(100))
                .send()
                .await;
            assert!(gone.is_err());
            tokio::time::sleep(Duration::from_millis(50)).await;
            let answer = client
                .get(&url)
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            assert_eq!(answer["status"], "pending", "{answer}");
            assert_eq!(reads() - before, 2, "the next reader starts its own read");
        },
    );
}
