//! Indexed queries and late body reconciliation.

use super::*;

#[test]
fn indexed_membership_includes_registry_history_before_the_board() {
    use std::sync::atomic::AtomicU64;
    const REGISTRY: &str = "0x1111111111111111111111111111111111111111";
    const OWNER: &str = "0x2222222222222222222222222222222222222222";
    let list = hex0x(&keccak::hash(&[b"whitelist"]).0);
    let event = |member: u64, present: bool, block: u64, log: u64| {
        json!({
            "address": REGISTRY,
            "topics": [hex0x(&keccak::hash(&[if present { b"Added(address,bytes32,address)" } else { b"Removed(address,bytes32,address)" }]).0),
                format!("0x{}{}", "00".repeat(12), &OWNER[2..]), list, format!("0x{member:064x}")],
            "data": "0x", "blockNumber": format!("0x{block:x}"), "logIndex": format!("0x{log:x}"),
            "blockHash": format!("0x{:064x}", 2000 + block), "removed": false,
        })
    };
    let events = vec![
        event(1, true, 5, 0),
        event(2, true, 6, 0),
        event(2, false, 12, 1),
    ];
    with_server(
        "indexed-membership",
        Some(Node {
            head: AtomicU64::new(14),
            messages: AtomicU64::new(2),
            extra_logs: std::sync::Mutex::new(events),
            ..Node::new(31337, CONTRACT, b"version/1 text/plain\nhello".to_vec())
        }),
        |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    start_block: 10,
                    confirmations: 2,
                    blocking: Some(crate::config::BlockingConfig {
                        registry: REGISTRY.into(),
                        gate: CONTRACT.into(),
                        list: "blocked".into(),
                    }),
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            crate::index::follow::once(&state).await;
            let node = node.unwrap();
            let calls = node.node.counts();
            let path = format!("{base}/api/chains/31337");
            let snapshots = post_json(&client, &format!("{path}/state"), json!({
            "ids": ["1", "999"], "accounts": [format!("0x{:040x}", 1), format!("0x{:040x}", 2)],
        })).await;
            assert_eq!(snapshots["items"][0]["id"], "1");
            assert!(snapshots["items"][1].is_null());
            assert!(snapshots["accounts"][0]["member"].is_null());
            assert!(snapshots["accounts"][1]["member"].is_null());
            let checks: Value = client
                .post(format!("{path}/state"))
                .json(&json!({
                    "memberships": ([1, 2].map(|member| json!({
                        "list": {"registry": REGISTRY, "owner": OWNER, "list": list},
                        "member": format!("0x{member:040x}"),
                    }))),
                }))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(checks["memberships"], json!([true, false]));
            let refused = client.post(format!("{path}/state")).json(&json!({
                "memberships": [{"list": {"registry": CONTRACT, "owner": OWNER, "list": list}, "member": OWNER}],
            })).send().await.unwrap();
            assert_eq!(refused.status(), reqwest::StatusCode::BAD_REQUEST);

            let members = post_json(
                &client,
                &format!("{path}/list"),
                json!({
                    "list": {"registry": REGISTRY, "owner": OWNER, "list": list},
                }),
            )
            .await;
            assert_eq!(members["items"], json!([format!("0x{:040x}", 1)]));
            let found = post_json(&client, &format!("{path}/lookup"), json!({
            "author": format!("0x{:040x}", 1), "content_hash": snapshots["items"][0]["content_hash"], "parent": "0",
        })).await;
            assert_eq!(found["items"][0]["id"], "1");
            for id in [2, 1] {
                let uploaded = publish(
                    &client,
                    &base,
                    &format!("chain_id=31337&message_id={id}"),
                    b"version/1 text/plain\nhello".to_vec(),
                )
                .await;
                assert_eq!(
                    uploaded.status(),
                    200,
                    "indexed membership does not restrict storage admission"
                );
            }
            assert_eq!(node.node.counts(), calls);
        },
    );
}

#[test]
fn reference_index_reconciles_a_late_body_without_a_new_chain_block() {
    use std::sync::atomic::AtomicU64;
    const ACCOUNT: &str = "0x1111111111111111111111111111111111111111";
    let bytes = format!("version/1 text/plain\nhello account://{ACCOUNT}").into_bytes();
    // A peer that has nothing.
    let peer = FakePeer::start([]);
    let peer_url = peer.url.clone();
    with_server(
        "late-reference-body",
        Some(Node {
            head: AtomicU64::new(14),
            messages: AtomicU64::new(2),
            ..Node::new(31337, CONTRACT, bytes.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 2,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            let pull = format!(
                "{base}/api/content/{}?chain_id=31337&message_id=1",
                hx(&bytes)
            );
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=1", bytes.clone())
                    .await
                    .status(),
                503
            );
            assert_eq!(client.get(&pull).send().await.unwrap().status(), 503);
            assert_eq!(peer.total_hits(), 0, "backfill refuses before asking peers");
            assert_eq!(state.store.as_ref().unwrap().holding.stats(), (0, 0));
            crate::index::follow::once(&state).await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            assert!(!sweep.run(&state, 31337, index).await.unwrap());
            assert!(!sweep.run(&state, 31337, index).await.unwrap());
            assert!(!sweep.run(&state, 31337, index).await.unwrap());
            assert_eq!(peer.total_hits(), 1, "missing shared hash backs off");
            let at = index.view.read().unwrap().model.at.clone();
            let path = format!("{base}/api/chains/31337/references");
            let empty = post_json(&client, &path, json!({"account": ACCOUNT})).await;
            assert!(empty["items"].as_array().unwrap().is_empty());
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(bytes.clone())
                .await
                .unwrap();
            let calls = node.as_ref().unwrap().node.counts();
            assert!(sweep.run(&state, 31337, index).await.unwrap());
            assert_eq!(peer.total_hits(), 1, "late local bytes bypass peer backoff");
            let found = post_json(&client, &path, json!({"account": ACCOUNT})).await;
            assert_eq!(
                found["items"].as_array().unwrap().len(),
                2,
                "both posts using the body are indexed"
            );
            assert_eq!(
                found["at"]["checkpoint"],
                serde_json::to_value(at.checkpoint).unwrap()
            );
            assert_ne!(found["at"]["sequence"], at.sequence.to_string());
            assert_eq!(
                found["items"][0]["reference"],
                json!({"kind": "mention", "account": ACCOUNT})
            );
            let notifications = post_json(
                &client,
                &format!("{base}/api/chains/31337/notifications"),
                json!({"account": ACCOUNT}),
            )
            .await;
            assert_eq!(
                notifications, found,
                "reference-only notifications keep the same records and reasons"
            );
            let changes = post_json(&client, &format!("{base}/api/chains/31337/changes"), json!({
                "generation": at.generation, "after": at.sequence.to_string(), "scope": {"account": ACCOUNT},
            })).await;
            assert_eq!(changes["references"].as_array().unwrap().len(), 2);
            assert_eq!(changes["references"][0]["reference"]["kind"], "mention");

            assert!(
                !sweep.run(&state, 31337, index).await.unwrap(),
                "repeat sweep is a no-op"
            );
            assert_eq!(
                node.as_ref().unwrap().node.counts(),
                calls,
                "stored-body analysis and reference queries use no RPC"
            );
            index.stop("test stop");
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=1", bytes)
                    .await
                    .status(),
                503
            );
            assert_eq!(
                client.get(&pull).send().await.unwrap().status(),
                200,
                "stopping the index does not hide stored bytes"
            );
            let missing = format!(
                "{base}/api/content/{}?chain_id=31337&message_id=1",
                hex0x(&[99; 32])
            );
            assert_eq!(client.get(missing).send().await.unwrap().status(), 503);
            assert_eq!(
                peer.total_hits(),
                1,
                "stopped index refuses before asking peers"
            );
            assert_eq!(state.store.as_ref().unwrap().holding.stats(), (0, 0));
        },
    );
}

#[test]
fn indexed_queries_need_no_rpc_and_stop_on_checkpoint_disagreement() {
    use std::sync::atomic::{AtomicU64, Ordering};
    with_server(
        "indexed-queries",
        Some(Node {
            head: AtomicU64::new(14),
            messages: AtomicU64::new(4),
            ..Node::new(31337, CONTRACT, b"version/1 text/plain\n\nhello".to_vec())
        }),
        |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 2,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
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
            let node = node.unwrap();
            let path = format!("{base}/api/chains/31337");
            assert_eq!(
                client
                    .post(format!("{path}/feed"))
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                503
            );
            crate::index::follow::once(&state).await;
            let before = node.node.counts();
            let first = client
                .post(format!("{path}/feed"))
                .json(&json!({"limit": 1}))
                .send()
                .await
                .unwrap();
            assert_eq!(first.status(), 200);
            let first: Value = first.json().await.unwrap();
            assert_eq!(first["at"]["checkpoint"]["block"], "12");
            assert_eq!(
                first["items"][0]["id"], "2",
                "unaccepted posts stay off the page"
            );
            let next = post_json(
                &client,
                &format!("{path}/feed"),
                json!({"cursor": first["next"]}),
            )
            .await;
            assert_eq!(next["items"][0]["id"], "1");
            assert!(next["next"].is_null());
            let message = get_json(&client, &format!("{path}/message/2?ancestors=32")).await;
            assert_eq!(message["message"]["id"], "2");
            let pin = message["message"]["commitment"].as_str().unwrap();
            assert_eq!(
                client
                    .get(format!("{path}/message/2?commit={pin}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            assert_eq!(
                client
                    .get(format!("{path}/message/2?commit=0x{}", "11".repeat(32)))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
            assert_eq!(
                client
                    .get(format!("{path}/message/3"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );
            let changes = post_json(
                &client,
                &format!("{path}/changes"),
                json!({"generation": first["at"]["generation"], "after": "0", "scope": {"feed": {}}}),
            )
            .await;
            assert_eq!(changes["changes"].as_array().unwrap().len(), 2);
            assert_eq!(changes["through"], "2");
            assert_eq!(changes["records"].as_array().unwrap().len(), 2);
            assert_eq!(changes["records"][0]["id"], "1");
            assert!(changes["records"][0]["replies"].is_string());
            assert_eq!(node.node.counts(), before, "all domain reads use RAM only");

            node.node.head.store(16, Ordering::SeqCst);
            crate::index::follow::once(&state).await;
            let refreshed = post_json(&client, &format!("{path}/feed"), json!({})).await;
            assert_eq!(refreshed["items"][0]["id"], "4");
            let continuation = post_json(
                &client,
                &format!("{path}/feed"),
                json!({"cursor": first["next"]}),
            )
            .await;
            assert_eq!(
                continuation["items"][0]["id"], "1",
                "older cursor retains its original upper bound"
            );

            node.node.block_hashes.lock().unwrap().insert(14, [99; 32]);
            crate::index::follow::once(&state).await;
            assert_eq!(
                client
                    .post(format!("{path}/feed"))
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                503
            );
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["chains"][0]["index"]["state"], "stopped");
        },
    );
}
