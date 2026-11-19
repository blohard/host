//! Receipts for held uploads: asked for on every pass while the upload is
//! fresh, then with a growing pause per entry, while another upload's
//! promotion never waits on them.

use super::*;
use crate::watcher::promote_at;
use std::time::Instant;

fn pending_tx(node: &mock_node::Running, hash: [u8; 32], body: &[u8]) {
    let mut input = selector(crate::abi::POST_SIGNATURE).to_vec();
    input.extend_from_slice(&keccak::hash(&[body]).0);
    input.extend_from_slice(&[0u8; 32 * 4]);
    node.node.add_tx(MockTx {
        hash,
        from: [0xab; 20],
        to: CONTRACT.to_lowercase(),
        input,
        block: None,
        message_id: None,
        reverted: false,
    });
}

#[test]
fn a_pending_receipt_is_asked_for_less_and_less_while_others_promote() {
    let stuck = record("text/plain", b"# stuck in the mempool\n");
    let quick = record("text/plain", b"# mined soon\n");
    with_server(
        "held-receipts",
        Some(Node {
            head: 20.into(),
            messages: 1.into(),
            extra: vec![(2, quick.clone())],
            ..Node::new(31337, CONTRACT, stuck.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            gateway: false,
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            let node = node.unwrap();
            let receipts = || node.node.count("eth_getTransactionReceipt");
            let publish = |hash: [u8; 32], body: Vec<u8>| {
                let client = client.clone();
                let url = format!("{base}/api/publish?chain_id=31337&tx_hash={}", hex0x(&hash));
                async move { client.post(url).body(body).send().await.unwrap().status() }
            };
            pending_tx(&node, [1u8; 32], &stuck);
            pending_tx(&node, [2u8; 32], &quick);
            assert_eq!(publish([1u8; 32], stuck.clone()).await, 202);
            assert_eq!(publish([2u8; 32], quick.clone()).await, 202);

            // Fresh uploads: both receipts are asked for on every pass.
            let start = Instant::now();
            let before = receipts();
            assert_eq!(promote_at(&state, start).await, 0);
            assert_eq!(receipts(), before + 2);
            assert_eq!(promote_at(&state, start + Duration::from_secs(3)).await, 0);
            assert_eq!(receipts(), before + 4);

            // One mines: it is promoted on the next pass, the other asked once more.
            node.node.mine_tx([2u8; 32], 12, 2);
            node.node.set_messages(2);
            assert_eq!(promote_at(&state, start + Duration::from_secs(6)).await, 1);
            assert_eq!(receipts(), before + 6);

            // Past the grace the stuck one is asked for once more, its fourth
            // miss, and then left alone for forty-eight seconds.
            assert_eq!(promote_at(&state, start + Duration::from_secs(12)).await, 0);
            assert_eq!(receipts(), before + 7);
            for seconds in [15, 18, 24, 30, 45, 59] {
                assert_eq!(
                    promote_at(&state, start + Duration::from_secs(seconds)).await,
                    0
                );
                assert_eq!(receipts(), before + 7, "no receipt call at {seconds}s");
            }
            // The fifth miss: the wait would double again but is capped at a minute.
            assert_eq!(promote_at(&state, start + Duration::from_secs(61)).await, 0);
            assert_eq!(receipts(), before + 8, "asked again once its wait is over");
            assert_eq!(
                promote_at(&state, start + Duration::from_secs(100)).await,
                0
            );
            assert_eq!(receipts(), before + 8, "waiting out the capped minute");
            assert_eq!(
                promote_at(&state, start + Duration::from_secs(122)).await,
                0
            );
            assert_eq!(receipts(), before + 9);

            // It is still held, not dropped, and another upload is not delayed.
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["store"]["holding"]["entries"], 1);
        },
    );
}

fn renumbered_post(was_mined_at_upload: bool) {
    let body = record("text/plain", b"good morning");
    let label = if was_mined_at_upload {
        "held-mined"
    } else {
        "held-pending"
    };
    with_server(
        label,
        Some(Node {
            head: 15.into(),
            messages: 4.into(),
            extra: vec![(3, body.clone()), (4, body.clone())],
            ..Node::new(31337, CONTRACT, b"other".to_vec())
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
            let node = node.unwrap();
            let tx = [1; 32];
            pending_tx(&node, tx, &body);
            if was_mined_at_upload {
                node.node.mine_tx(tx, 14, 4);
            }
            let proof = format!("chain_id=31337&tx_hash={}", hex0x(&tx));
            let response = publish(&client, &base, &proof, body.clone()).await;
            assert_eq!(response.status(), 202);
            let answer: Value = response.json().await.unwrap();
            assert_eq!(
                answer["message_id"],
                json!(was_mined_at_upload.then_some(4))
            );
            let start = Instant::now();
            for seconds in [0, 3, 6, 9, 12] {
                if seconds == 9 && !was_mined_at_upload {
                    node.node.mine_tx(tx, 14, 4);
                }
                assert_eq!(
                    promote_at(&state, start + Duration::from_secs(seconds)).await,
                    0
                );
            }

            // The canonical history has only three posts. The transaction is now #3.
            node.node.mine_tx(tx, 13, 3);
            node.node.set_messages(3);
            node.node.set_head(18);
            assert_eq!(promote_at(&state, start + Duration::from_secs(15)).await, 1);
            assert_eq!(
                state
                    .store
                    .as_ref()
                    .unwrap()
                    .bodies
                    .get(&keccak::hash(&[&body]))
                    .await
                    .unwrap(),
                Some(body)
            );
        },
    );
}

#[test]
fn a_pending_upload_follows_its_reincluded_transaction() {
    renumbered_post(false);
}

#[test]
fn a_mined_upload_follows_its_reincluded_transaction() {
    renumbered_post(true);
}

#[test]
fn a_second_pending_proof_for_the_same_bytes_is_retryable() {
    let body = record("text/plain", b"same text");
    with_server(
        "held-different-proof",
        Some(Node {
            head: 15.into(),
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
            let node = node.unwrap();
            let submit = |tx: u8| {
                let proof = format!("chain_id=31337&tx_hash={}", hex0x(&[tx; 32]));
                let bytes = body.clone();
                let (client, base) = (&client, &base);
                async move { publish(client, base, &proof, bytes).await }
            };
            pending_tx(&node, [1; 32], &body);
            pending_tx(&node, [2; 32], &body);
            assert_eq!(submit(1).await.status(), 202);
            assert_eq!(submit(1).await.status(), 202);
            let other = submit(2).await;
            assert_eq!(other.status(), 503);
            assert_eq!(
                other.json::<Value>().await.unwrap()["code"],
                "different_held_proof"
            );
            assert_eq!(state.store.as_ref().unwrap().holding.stats().0, 1);
            node.node.mine_tx([2; 32], 11, 1);
            node.node.set_head(16);
            assert_eq!(submit(2).await.status(), 200);
        },
    );
}

#[test]
fn a_late_image_and_body_retry_can_name_the_post_by_number_or_transaction() {
    for number_first in [false, true] {
        let image = record("image/png", b"late image");
        let body = record(
            "text/plain",
            format!("![image](record:{})", hx(&image)).as_bytes(),
        );
        with_server(
            if number_first {
                "held-number-first"
            } else {
                "held-tx-first"
            },
            Some(Node {
                head: 15.into(),
                messages: 2.into(),
                extra: vec![(2, body.clone())],
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
                let node = node.unwrap();
                let tx = [1; 32];
                let hash = format!("chain_id=31337&tx_hash={}", hex0x(&tx));
                let number = "chain_id=31337&message_id=1";
                let other = "chain_id=31337&message_id=2";
                pending_tx(&node, tx, &body);
                if number_first {
                    node.node.mine_tx(tx, 11, 1);
                    assert_eq!(
                        publish(&client, &base, number, body.clone()).await.status(),
                        202
                    );
                } else {
                    assert_eq!(
                        publish(&client, &base, &hash, body.clone()).await.status(),
                        202
                    );
                    node.node.mine_tx(tx, 11, 1);
                }
                let retry = if number_first { hash.as_str() } else { number };
                assert_eq!(
                    publish(&client, &base, retry, body.clone()).await.status(),
                    202
                );
                assert_eq!(
                    publish(&client, &base, other, image.clone()).await.status(),
                    503
                );
                node.node.set_head(16);
                assert_eq!(
                    publish(&client, &base, number, image.clone())
                        .await
                        .status(),
                    202
                );
                assert_eq!(promote_at(&state, Instant::now()).await, 2);
                let target = &state
                    .store
                    .as_ref()
                    .unwrap()
                    .attachments
                    .as_ref()
                    .unwrap()
                    .0;
                assert_eq!(
                    target.get(&keccak::hash(&[&image])).await.unwrap(),
                    Some(image)
                );
            },
        );
    }
}
