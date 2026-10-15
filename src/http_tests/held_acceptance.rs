//! Acceptance reads for held uploads on a store-only host: one Board read
//! per body when the accepted head has moved since it was last found
//! unaccepted, none while it has not, and a growing pause per entry after a
//! failed lookup. Receipt polling and expiry are untouched by any of this.

use super::*;
use crate::watcher::promote_at;
use std::time::Instant;

#[test]
fn a_held_body_is_read_again_only_when_the_accepted_head_moves() {
    let early = record("text/plain", b"# posted in block 12\n");
    let late = record("text/plain", b"# posted in block 13\n");
    with_server(
        "held-acceptance",
        Some(Node {
            head: 15.into(),
            messages: 3.into(),
            extra: vec![(2, early.clone()), (3, late.clone())],
            ..Node::new(31337, CONTRACT, b"other".to_vec())
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
            let calls = || node.node.count("eth_call");
            let publish = |id: u64, body: Vec<u8>| {
                let client = client.clone();
                let url = format!("{base}/api/publish?chain_id=31337&message_id={id}");
                async move { client.post(url).body(body).send().await.unwrap().status() }
            };
            let served = |body: &[u8]| {
                let client = client.clone();
                let url = format!("{base}/api/content/{}", hex0x(&keccak::hash(&[body]).0));
                async move { client.get(url).send().await.unwrap().status().as_u16() }
            };
            // Messages 2 and 3 exist at latest, in blocks 12 and 13, but the
            // accepted head is 10: both uploads are held.
            assert_eq!(publish(2, early.clone()).await, 202);
            assert_eq!(publish(3, late.clone()).await, 202);

            // First pass: one Board read per body, both found unaccepted.
            let start = Instant::now();
            let before = calls();
            assert_eq!(promote_at(&state, start).await, 0);
            assert_eq!(calls(), before + 2);
            // Same accepted head: nothing is read again.
            assert_eq!(promote_at(&state, start + Duration::from_secs(3)).await, 0);
            assert_eq!(promote_at(&state, start + Duration::from_secs(6)).await, 0);
            assert_eq!(calls(), before + 2, "an unchanged head costs no reads");

            // The head moves to 17, accepting block 12: both are read once
            // more, one is promoted, the other found unaccepted at 12. A
            // message found takes a second read, for its gate.
            node.node.set_head(17);
            assert_eq!(promote_at(&state, start + Duration::from_secs(9)).await, 1);
            assert_eq!(calls(), before + 5);
            assert_eq!(served(&early).await, 200);
            assert_eq!(served(&late).await, 404);
            assert_eq!(promote_at(&state, start + Duration::from_secs(12)).await, 0);
            assert_eq!(
                calls(),
                before + 5,
                "found unaccepted at 12; not read again at 12"
            );

            // A failed lookup records nothing about the block and the entry
            // waits six seconds before it is asked about again.
            node.node.set_head(18);
            *node.node.simulation_error.lock().unwrap() =
                Some(json!({"code": -32000, "message": "provider unavailable"}));
            assert_eq!(promote_at(&state, start + Duration::from_secs(15)).await, 0);
            assert_eq!(calls(), before + 6, "the read was attempted and failed");
            *node.node.simulation_error.lock().unwrap() = None;
            assert_eq!(promote_at(&state, start + Duration::from_secs(18)).await, 0);
            assert_eq!(calls(), before + 6, "backing off after the failure");
            assert_eq!(promote_at(&state, start + Duration::from_secs(22)).await, 1);
            assert_eq!(
                calls(),
                before + 8,
                "asked again once the wait is over, and promoted"
            );
            assert_eq!(served(&late).await, 200);
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["store"]["holding"]["entries"], 0);
        },
    );
}

#[test]
fn a_failed_lookup_waits_from_its_own_completion_not_from_the_pass_start() {
    let body = record("text/plain", b"# slow provider\n");
    with_server(
        "held-acceptance-slow",
        Some(Node {
            head: 15.into(),
            messages: 2.into(),
            extra: vec![(2, body.clone())],
            ..Node::new(31337, CONTRACT, b"other".to_vec())
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
            let calls = || node.node.count("eth_call");
            let status = publish(&client, &base, "chain_id=31337&message_id=2", body.clone())
                .await
                .status();
            assert_eq!(status, 202);

            // Every answer now takes half a second, and the Board read fails.
            // The pass spends about a second before the failure is recorded.
            node.node
                .latency_ms
                .store(500, std::sync::atomic::Ordering::Relaxed);
            *node.node.simulation_error.lock().unwrap() =
                Some(json!({"code": -32000, "message": "provider unavailable"}));
            let start = Instant::now();
            let before = calls();
            assert_eq!(promote_at(&state, start).await, 0);
            assert_eq!(calls(), before + 1);
            *node.node.simulation_error.lock().unwrap() = None;
            node.node
                .latency_ms
                .store(0, std::sync::atomic::Ordering::Relaxed);

            // Six seconds after the pass began is not six seconds after the
            // failed lookup finished: the entry is still waiting.
            assert_eq!(promote_at(&state, start + Duration::from_secs(6)).await, 0);
            assert_eq!(
                calls(),
                before + 1,
                "the wait runs from the lookup's completion"
            );
            node.node.set_head(17);
            assert_eq!(promote_at(&state, start + Duration::from_secs(8)).await, 1);
            assert_eq!(calls(), before + 3, "the message and its gate");
        },
    );
}
