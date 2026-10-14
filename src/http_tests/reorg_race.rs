//! A block replaced while its range is being read. The dangerous shape is an
//! old block that held none of our logs: the per-log hash checks then have
//! nothing to compare, so only the end header read before the range can
//! reveal that the range changed. The next poll reads the replacement whole.

use super::follow_fixtures::*;
use super::*;
use crate::chain::Hex;
use crate::index::{State, follow};
use crate::mock_node::Replacement;
use std::sync::atomic::AtomicU64;

const NEW_HASH: [u8; 32] = [0x13; 32];

/// A Posted log for `id` in the replacement block, carrying its new hash.
fn replacement_post(id: u64, block: u64) -> Value {
    let mut log = posted(id, block, 0, ANCHORED);
    log["blockHash"] = json!(hex0x(&NEW_HASH));
    log
}

#[test]
fn an_empty_end_block_replaced_during_the_range_read_is_not_committed() {
    // Messages 1 and 2 in blocks 11 and 12; block 13, the accepted head, is
    // empty until the logs for 10..13 have been served, then it becomes a
    // different block holding message 3.
    let node = Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(2),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    *node.replace_after_read.lock().unwrap() = Some(Replacement {
        block: 13,
        hash: NEW_HASH,
        logs: vec![replacement_post(3, 13)],
    });
    with_server(
        "reorg-race-logs",
        Some(node),
        setup_from(10),
        |server| async move {
            let Server {
                base,
                client,
                state,
                ..
            } = server;
            follow::once(&state).await;
            assert_eq!(
                committed(&state),
                0,
                "a range that changed during the read is not committed"
            );
            assert!(checkpoint(&state).is_none());
            assert!(matches!(index(&state).state(), State::Backfilling));
            follow::once(&state).await;
            assert!(index(&state).state().is_ready());
            assert_eq!(checkpoint(&state).unwrap().hash, Hex(NEW_HASH));
            assert_eq!(feed_ids(&client, &base).await.1, ["3", "2", "1"]);
        },
    );
}

#[test]
fn the_receipts_fallback_is_checked_against_the_header_read_before_it() {
    // Block 13 alone is the range. Its logs are refused once, so it is read
    // from receipts, which are served from the empty old block before the
    // replacement lands. The header read before the attempt exposes it.
    let node = Node {
        head: AtomicU64::new(15),
        log_refusals: AtomicU64::new(1),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    *node.replace_after_read.lock().unwrap() = Some(Replacement {
        block: 13,
        hash: NEW_HASH,
        logs: vec![replacement_post(1, 13)],
    });
    with_server(
        "reorg-race-receipts",
        Some(node),
        setup_from(13),
        |server| async move {
            let Server {
                base,
                client,
                state,
                node,
            } = server;
            follow::once(&state).await;
            let receipts = node.as_ref().unwrap().node.count("eth_getBlockReceipts");
            assert_eq!(receipts, 1, "the refused block was read from its receipts");
            assert_eq!(committed(&state), 0);
            assert!(matches!(index(&state).state(), State::Backfilling));
            follow::once(&state).await;
            assert!(index(&state).state().is_ready());
            assert_eq!(checkpoint(&state).unwrap().hash, Hex(NEW_HASH));
            assert_eq!(feed_ids(&client, &base).await.1, ["1"]);
        },
    );
}

#[test]
fn a_replaced_checkpoint_stops_the_chain_on_an_advancing_poll() {
    // Ready at block 13; the checkpoint block is then replaced while new
    // blocks arrive, so the next poll advances instead of idling.
    let node = Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(2),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    with_server(
        "reorg-checkpoint-advancing",
        Some(node),
        setup_from(10),
        |server| async move {
            let Server { state, node, .. } = server;
            let node = &node.as_ref().unwrap().node;
            follow::once(&state).await;
            assert!(index(&state).state().is_ready());
            assert_eq!(committed(&state), 2);
            node.block_hashes.lock().unwrap().insert(13, [77; 32]);
            node.set_head(17);
            node.set_messages(4);
            follow::once(&state).await;
            assert!(
                matches!(index(&state).state(), State::Stopped(reason) if reason.contains("history changed")),
                "{:?}",
                index(&state).state()
            );
            assert_eq!(
                committed(&state),
                2,
                "nothing is appended on top of a replaced checkpoint"
            );
        },
    );
}

#[test]
fn a_replaced_checkpoint_is_caught_even_when_the_new_range_keeps_failing() {
    // Ready at block 13. The checkpoint block is replaced and new blocks
    // arrive whose only Board log does not decode, so the advancing poll
    // fails before its own checkpoint read; the failure path still checks.
    let node = Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(2),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    with_server(
        "reorg-checkpoint-failing",
        Some(node),
        setup_from(10),
        |server| async move {
            let Server { state, node, .. } = server;
            let node = &node.as_ref().unwrap().node;
            follow::once(&state).await;
            assert!(index(&state).state().is_ready());
            node.block_hashes.lock().unwrap().insert(13, [77; 32]);
            node.set_head(17);
            let mut malformed = posted(3, 15, 0, ANCHORED);
            malformed["data"] = json!("0x00");
            node.extra_logs.lock().unwrap().push(malformed);
            follow::once(&state).await;
            assert!(
                matches!(index(&state).state(), State::Stopped(reason) if reason.contains("history changed")),
                "{:?}",
                index(&state).state()
            );
            assert_eq!(committed(&state), 2);
        },
    );
}
