//! Blocks too dense for `eth_getLogs`: read from receipts, or stopped with
//! the block named. Gas bounds a block's events only by the cost of the log
//! itself, so a looping contract can put thousands into one block cheaply.
//! Transient failures are none of this: they are retried and never stop.

use super::follow_fixtures::*;
use super::*;
use crate::block_logs::PERSISTENT_REFUSALS;
use crate::index::{
    State,
    follow::{self, Follower, Limits},
};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Messages 1..=3 in blocks 11..=13, and two more in block 13, which then
/// holds three Board events. Head 15 with two confirmations accepts block 13.
fn dense_node() -> Node {
    Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(3),
        extra_logs: std::sync::Mutex::new(vec![
            posted(4, 13, 1, ANCHORED),
            posted(5, 13, 2, ANCHORED),
        ]),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    }
}

fn receipt_calls(node: &mock_node::Running) -> usize {
    node.node.count("eth_getBlockReceipts")
}

#[test]
fn a_block_the_provider_will_not_answer_as_logs_is_read_from_its_receipts() {
    let node = dense_node();
    node.max_logs.store(2, Relaxed);
    with_server("dense-receipts", Some(node), setup, |server| async move {
        let Server {
            base,
            client,
            state,
            node,
        } = server;
        // Ranges halve down to block 13 alone, which still exceeds the
        // provider's limit; its receipts carry the same three events.
        for _ in 0..6 {
            follow::once(&state).await;
        }
        assert!(index_state(&state).is_ready(), "{:?}", index_state(&state));
        assert_eq!(receipt_calls(node.as_ref().unwrap()), 1);
        let (status, ids) = feed_ids(&client, &base).await;
        assert_eq!(status, 200);
        assert_eq!(ids, ["5", "4", "3", "2", "1"]);
    });
}

#[test]
fn a_block_whose_logs_exceed_the_response_bound_is_read_from_its_receipts() {
    let one_log = posted(1, 11, 0, ANCHORED).to_string().len();
    with_server(
        "dense-log-bound",
        Some(dense_node()),
        setup,
        move |server| async move {
            let Server {
                base,
                client,
                state,
                node,
            } = server;
            // One log fits the bound and two do not, so every range halves to
            // single blocks, and block 13 alone still does not fit as logs.
            let limits = Limits {
                log_response: 40 + one_log + one_log / 2,
                ..Limits::DEFAULT
            };
            for _ in 0..8 {
                follow::once_with(&state, limits).await;
            }
            assert!(index_state(&state).is_ready(), "{:?}", index_state(&state));
            assert_eq!(receipt_calls(node.as_ref().unwrap()), 1);
            let (status, ids) = feed_ids(&client, &base).await;
            assert_eq!(status, 200);
            assert_eq!(ids, ["5", "4", "3", "2", "1"]);
        },
    );
}

#[test]
fn a_block_beyond_the_event_capacity_stops_that_chain_and_names_the_block() {
    with_server(
        "dense-stop",
        Some(dense_node()),
        setup,
        |server| async move {
            let Server {
                base,
                client,
                state,
                node,
            } = server;
            // Blocks 11 and 12 land; block 13 holds three events against a
            // capacity of two. A successful log query already proves that, so no
            // receipts are asked for.
            let limits = Limits {
                max_events: 2,
                ..Limits::DEFAULT
            };
            for _ in 0..6 {
                follow::once_with(&state, limits).await;
            }
            let reason = stopped_reason(&state);
            assert!(reason.contains("block 13"), "{reason}");
            assert!(reason.contains("3 board and registry events"), "{reason}");
            assert_eq!(receipt_calls(node.as_ref().unwrap()), 0);
            assert_eq!(feed_ids(&client, &base).await.0, 503);
        },
    );
}

#[test]
fn a_block_whose_receipts_exceed_the_capacity_stops_that_chain() {
    let node = dense_node();
    node.max_logs.store(2, Relaxed);
    with_server(
        "dense-receipts-stop",
        Some(node),
        setup,
        |server| async move {
            let Server { state, node, .. } = server;
            let limits = Limits {
                max_events: 2,
                ..Limits::DEFAULT
            };
            for _ in 0..6 {
                follow::once_with(&state, limits).await;
            }
            let reason = stopped_reason(&state);
            assert!(reason.contains("block 13"), "{reason}");
            assert!(reason.contains("3 board and registry events"), "{reason}");
            assert_eq!(receipt_calls(node.as_ref().unwrap()), 1);
        },
    );
}

#[test]
fn a_block_whose_receipts_exceed_the_response_bound_stops_that_chain() {
    let node = dense_node();
    node.max_logs.store(2, Relaxed);
    with_server(
        "dense-receipts-bound",
        Some(node),
        setup,
        |server| async move {
            let Server { state, .. } = server;
            // Three receipts run past a bound of 512 bytes. The block will not
            // shrink, so there is nothing to retry.
            let limits = Limits {
                receipt_response: 512,
                ..Limits::DEFAULT
            };
            for _ in 0..6 {
                follow::once_with(&state, limits).await;
            }
            let reason = stopped_reason(&state);
            assert!(reason.contains("block 13"), "{reason}");
            assert!(reason.contains("response bound of 512 bytes"), "{reason}");
        },
    );
}

#[test]
fn a_transient_failure_on_one_block_neither_falls_back_nor_stops() {
    let node = dense_node();
    node.block_receipts.store(false, Relaxed);
    // Three failures cover the halving from four blocks down to block 10
    // alone; the next poll's first request succeeds.
    node.log_outage.store(3, Relaxed);
    with_server("dense-transient", Some(node), setup, |server| async move {
        let Server {
            base,
            client,
            state,
            node,
        } = server;
        follow::once(&state).await;
        assert!(
            matches!(index_state(&state), State::Backfilling),
            "{:?}",
            index_state(&state)
        );
        for _ in 0..3 {
            follow::once(&state).await;
        }
        assert!(index_state(&state).is_ready(), "{:?}", index_state(&state));
        assert_eq!(receipt_calls(node.as_ref().unwrap()), 0);
        assert_eq!(feed_ids(&client, &base).await.1, ["5", "4", "3", "2", "1"]);
    });
}

#[test]
fn a_block_persistently_refused_without_block_receipts_stops_that_chain() {
    let node = dense_node();
    node.max_logs.store(2, Relaxed);
    node.block_receipts.store(false, Relaxed);
    with_server("dense-unserved", Some(node), setup, |server| async move {
        let Server { state, node, .. } = server;
        // Two polls land blocks 11 and 12. Block 13 is then refused on every
        // poll; a refusal may be a rate limit, so the chain waits it out.
        let mut follower = Follower::new(Limits::DEFAULT);
        for _ in 0..2 + PERSISTENT_REFUSALS - 1 {
            follow::once_on(&state, &mut follower).await;
        }
        assert!(
            index_state(&state).is_established(),
            "{:?}",
            index_state(&state)
        );
        follow::once_on(&state, &mut follower).await;
        let reason = stopped_reason(&state);
        assert!(reason.contains("block 13"), "{reason}");
        assert!(
            reason.contains(&format!("{PERSISTENT_REFUSALS} polls in a row")),
            "{reason}"
        );
        assert!(reason.contains("eth_getBlockReceipts"), "{reason}");
        assert_eq!(
            receipt_calls(node.as_ref().unwrap()),
            PERSISTENT_REFUSALS as usize
        );
    });
}

/// Refusals of a block are counted until it is taken in: an answer that
/// then fails its checks does not start the count over. Otherwise a provider
/// alternating refusals with bad answers would keep the refusals from adding
/// up, and only its bad answers could stop the chain, as rejected history
/// rather than an unreadable block.
#[test]
fn refusals_of_a_block_count_until_it_is_taken_in() {
    let node = dense_node();
    node.max_logs.store(2, Relaxed);
    node.block_receipts.store(false, Relaxed);
    with_server(
        "dense-interrupted",
        Some(node),
        setup,
        |server| async move {
            let Server { state, node, .. } = server;
            let node = node.as_ref().unwrap();
            // Two polls land blocks 11 and 12, then three refuse block 13.
            let mut follower = Follower::new(Limits::DEFAULT);
            for _ in 0..5 {
                follow::once_on(&state, &mut follower).await;
            }
            assert_eq!(receipt_calls(node), 3);
            // One answer for block 13 that is no log at all, rejected.
            node.node.malformed_logs.store(1, Relaxed);
            follow::once_on(&state, &mut follower).await;
            assert_eq!(receipt_calls(node), 3);
            // Twelve more refusals make fifteen; the sixteenth stops the chain.
            for _ in 0..PERSISTENT_REFUSALS - 4 {
                follow::once_on(&state, &mut follower).await;
            }
            assert!(
                index_state(&state).is_established(),
                "{:?}",
                index_state(&state)
            );
            follow::once_on(&state, &mut follower).await;
            let reason = stopped_reason(&state);
            assert!(reason.contains("block 13"), "{reason}");
            assert!(
                reason.contains(&format!("{PERSISTENT_REFUSALS} polls in a row")),
                "{reason}"
            );
            assert_eq!(receipt_calls(node), PERSISTENT_REFUSALS as usize);
        },
    );
}
