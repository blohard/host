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
