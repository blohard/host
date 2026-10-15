//! Provider failures and upload limits must not become evidence about a post.
use super::*;
use crate::config::DEFAULT_BODY_BYTES;

fn post_input(body: &[u8]) -> Vec<u8> {
    let mut input = selector(crate::abi::POST_SIGNATURE).to_vec();
    input.extend_from_slice(&keccak::hash(&[body]).0);
    input.extend_from_slice(&[0; 32 * 4]);
    input
}

#[test]
fn transaction_rpc_errors_are_retryable() {
    let body = record("text/plain", b"provider failure");
    let input = post_input(&body);
    with_server(
        "upload-tx-errors",
        Some(Node::new(31337, CONTRACT, body.clone())),
        |url| Setup {
            gateway: false,
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  node,
                  state,
              }| async move {
            let node = node.unwrap();
            let valid = json!({"hash":hex0x(&[7;32]), "from":hex0x(&[1;20]), "to":CONTRACT, "input":hex0x(&input), "blockNumber":null});
            let mut answers = vec![
                json!({"error":{"code":-32005,"message":"rate limited"}}),
                json!({}),
            ];
            for field in ["hash", "from", "to", "input", "blockNumber"] {
                let mut missing = valid.clone();
                missing.as_object_mut().unwrap().remove(field);
                answers.push(json!({"result":missing}));
                let mut malformed = valid.clone();
                malformed[field] = json!("bad");
                answers.push(json!({"result":malformed}));
            }
            let query = format!("chain_id=31337&tx_hash={}", hex0x(&[7; 32]));
            for answer in answers {
                node.node
                    .response_overrides
                    .lock()
                    .unwrap()
                    .insert("eth_getTransactionByHash".into(), answer.clone());
                let response = publish(&client, &base, &query, body.clone()).await;
                assert_eq!(
                    response.status(),
                    if answer.get("error").is_some() {
                        503
                    } else {
                        502
                    },
                    "{answer}"
                );
                assert_eq!(state.store.as_ref().unwrap().holding.stats(), (0, 0));
                if answer.get("error").is_some() {
                    state
                        .verifier
                        .chain(31337)
                        .unwrap()
                        .0
                        .expire_cooldown(&node.url);
                }
            }
            for (answer, status) in [
                (json!({"result":null}), 404),
                (json!({"result":valid}), 202),
            ] {
                node.node
                    .response_overrides
                    .lock()
                    .unwrap()
                    .insert("eth_getTransactionByHash".into(), answer);
                assert_eq!(
                    publish(&client, &base, &query, body.clone()).await.status(),
                    status
                );
            }
        },
    );
}

#[test]
fn malformed_receipts_keep_held_uploads() {
    use crate::{holding::Held, store::Kind};
    let attachment = record("image/png", b"held image");
    let body = record(
        "text/plain",
        format!("![image](record:{})", hx(&attachment)).as_bytes(),
    );
    let node = Node {
        head: 20.into(),
        messages: 1.into(),
        txs: vec![MockTx {
            hash: [7; 32],
            from: [1; 20],
            to: CONTRACT.into(),
            input: post_input(&body),
            block: Some(11),
            message_id: Some(1),
            reverted: false,
        }]
        .into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "upload-receipt-errors",
        Some(node),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  state,
                  node,
                  client,
                  base,
              }| async move {
            let node = node.unwrap();
            let store = state.store.as_ref().unwrap();
            let body_hash = keccak::hash(&[&body]);
            let held = |bytes: Vec<u8>, kind| Held {
                chain_id: 31337,
                digest: keccak::hash(&[&bytes]),
                bytes,
                kind,
                body_digest: body_hash,
                tx_hash: Some([7; 32]),
                signer: [1; 20],
                message_id: None,
            };
            store
                .holding
                .insert(held(body.clone(), Kind::Bodies))
                .unwrap();
            store
                .holding
                .insert(held(attachment.clone(), Kind::Attachments))
                .unwrap();
            let valid = post_json(
                &client,
                &node.url,
                json!({"method":"eth_getTransactionReceipt","params":[hex0x(&[7;32])]}),
            )
            .await;
            let mut answers = vec![
                json!({"error":{"code":-32005,"message":"rate limited"}}),
                json!({}),
            ];
            for field in [
                "status",
                "transactionHash",
                "blockNumber",
                "blockHash",
                "logs",
            ] {
                let mut missing = valid.clone();
                missing["result"].as_object_mut().unwrap().remove(field);
                answers.push(missing);
                let mut bad = valid.clone();
                bad["result"][field] = json!("bad");
                answers.push(bad);
            }
            for (field, value) in [
                ("data", json!("0x")),
                ("transactionHash", json!(hex0x(&[8; 32]))),
                ("blockHash", json!(hex0x(&[8; 32]))),
                ("removed", json!(true)),
            ] {
                let mut bad = valid.clone();
                bad["result"]["logs"][0][field] = value;
                answers.push(bad);
            }
            let start = std::time::Instant::now();
            let receipt_url = format!("{base}/api/chains/31337/transaction/{}", hex0x(&[7; 32]));
            for (i, answer) in answers.iter().enumerate() {
                node.node
                    .response_overrides
                    .lock()
                    .unwrap()
                    .insert("eth_getTransactionReceipt".into(), answer.clone());
                crate::watcher::promote_at(&state, start + Duration::from_secs(i as u64 * 61))
                    .await;
                assert_eq!(
                    store.holding.stats().0,
                    2,
                    "receipt failure must retain body and attachment: {answer}"
                );
                let response = client
                    .get(&receipt_url)
                    .header("Origin", "https://example.org")
                    .send()
                    .await
                    .unwrap();
                if answer.get("error").is_some() {
                    assert_eq!(response.headers()["retry-after"], "2");
                    assert_eq!(
                        response.headers()["access-control-expose-headers"],
                        "retry-after"
                    );
                }
                assert_eq!(
                    response.status(),
                    if answer.get("error").is_some() {
                        503
                    } else {
                        502
                    },
                    "{answer}"
                );
                if answer.get("error").is_some() {
                    state
                        .verifier
                        .chain(31337)
                        .unwrap()
                        .0
                        .expire_cooldown(&node.url);
                }
            }
            node.node.response_overrides.lock().unwrap().clear();
            crate::index::follow::once(&state).await;
            assert_eq!(
                crate::watcher::promote_at(
                    &state,
                    start + Duration::from_secs(answers.len() as u64 * 61)
                )
                .await,
                2
            );
            assert_eq!(store.holding.stats(), (0, 0));
            assert!(store.bodies.get(&body_hash).await.unwrap().is_some());
        },
    );
}

#[test]
fn oversized_bodies_are_refused_before_holding() {
    let body = record("text/plain", &vec![b'x'; DEFAULT_BODY_BYTES + 1]);
    with_server(
        "review-body-cap",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            gateway: false,
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
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
            let r = publish(&client, &base, "chain_id=31337&message_id=1", body).await;
            assert_eq!(
                r.status(),
                413,
                "a body larger than its store cap must not enter holding"
            );
            assert_eq!(state.store.as_ref().unwrap().holding.stats(), (0, 0));
            node.unwrap().node.set_head(20);
            // The same limit holds after confirmation.
            let body = record("text/plain", &vec![b'x'; DEFAULT_BODY_BYTES + 1]);
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=1", body)
                    .await
                    .status(),
                413
            );
        },
    );
}

#[test]
fn attachments_arriving_during_promotion_survive_completion_or_cancellation() {
    for cancel in [false, true] {
        let a = record("image/png", b"first image");
        let b = record("image/png", b"second image");
        let body = record(
            "text/plain",
            format!("![a](record:{}) ![b](record:{})", hx(&a), hx(&b)).as_bytes(),
        );
        with_server(
            if cancel {
                "cancelled-promotion"
            } else {
                "late-attachment"
            },
            Some(Node {
                head: 20.into(),
                messages: 1.into(),
                ..Node::new(31337, CONTRACT, body.clone())
            }),
            |url| Setup {
                gateway: false,
                verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
                ..Setup::default()
            },
            move |Server { state, .. }| async move {
                let store = state.store.as_ref().unwrap();
                let body_hash = keccak::hash(&[&body]);
                let held = |bytes: Vec<u8>, kind| crate::holding::Held {
                    chain_id: 31337,
                    digest: keccak::hash(&[&bytes]),
                    bytes,
                    kind,
                    body_digest: body_hash,
                    tx_hash: None,
                    signer: [1; 20],
                    message_id: Some(1),
                };
                store
                    .holding
                    .insert(held(body.clone(), crate::store::Kind::Bodies))
                    .unwrap();
                store
                    .holding
                    .insert(held(a.clone(), crate::store::Kind::Attachments))
                    .unwrap();
                let target = &store.attachments.as_ref().unwrap().0;
                let guard = target.hold_writes_for_test().await;
                let state2 = state.clone();
                let task = tokio::spawn(async move { promote_once(&state2).await });
                wait_until(Duration::from_secs(3), "promotion is writing", || {
                    target.writer_waiting_for_test()
                })
                .await;
                // Promotion is writing its captured attachment list. A concurrent upload,
                // authenticated while the post was provisional, reaches hold after that snapshot.
                let b_hash = keccak::hash(&[&b]);
                store
                    .holding
                    .insert(held(b, crate::store::Kind::Attachments))
                    .unwrap();
                if cancel {
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                } else {
                    drop(guard);
                    task.await.unwrap();
                    assert!(
                        store.holding.get(31337, &body_hash).is_some(),
                        "late attachment keeps its body promotable"
                    );
                    for _ in 0..3 {
                        promote_once(&state).await;
                    }
                    assert!(target.get(&b_hash).await.unwrap().is_some());
                    assert_eq!(store.holding.stats(), (0, 0));
                    return;
                }
                drop(guard);
                for _ in 0..3 {
                    promote_once(&state).await;
                }
                assert!(
                    target.get(&b_hash).await.unwrap().is_some(),
                    "an acknowledged attachment must remain promotable after its parent leaves holding"
                );
            },
        );
    }
}
