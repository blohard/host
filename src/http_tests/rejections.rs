//! Ranges the provider keeps answering that the index cannot take in as
//! valid history. A rejection can be a reorg or a lagging replica for a
//! while, so the chain waits it out; only persistence stops it, with the
//! block and the reason named instead of a stale index labelled ready.

use super::follow_fixtures::*;
use super::*;
use crate::index::{
    State,
    follow::{self, Follower, Limits, PERSISTENT_REJECTIONS},
};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[test]
fn a_persistently_invalid_range_stops_the_chain_while_outages_and_progress_do_not_count() {
    // Messages 1 and 2 in blocks 11 and 12, then a post numbered 5 in block
    // 13: the batch fails "missing or duplicate post" every poll.
    let node = Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(2),
        extra_logs: std::sync::Mutex::new(vec![posted(5, 13, 0, ANCHORED)]),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    with_server("rejections", Some(node), setup, |server| async move {
        let Server { state, node, .. } = server;
        let node = &node.as_ref().unwrap().node;
        let mut follower = Follower::new(Limits::DEFAULT);
        for _ in 0..3 {
            follow::once_on(&state, &mut follower).await;
            assert!(matches!(index_state(&state), State::Backfilling));
        }
        // A provider outage in between is weather: it neither counts nor resets.
        node.log_outage.store(1, Relaxed);
        follow::once_on(&state, &mut follower).await;
        assert_eq!(node.log_outage.load(Relaxed), 0);
        assert!(matches!(index_state(&state), State::Backfilling));
        // The chain corrects itself: the range is valid and progress clears the count.
        node.extra_logs.lock().unwrap().clear();
        node.set_messages(3);
        follow::once_on(&state, &mut follower).await;
        assert!(index_state(&state).is_ready(), "{:?}", index_state(&state));
        assert_eq!(committed(&state), 3);
        // A new invalid range at a later block starts counting from one again.
        node.set_head(17);
        node.extra_logs
            .lock()
            .unwrap()
            .push(posted(9, 15, 0, ANCHORED));
        for _ in 1..PERSISTENT_REJECTIONS {
            follow::once_on(&state, &mut follower).await;
            assert!(index_state(&state).is_ready(), "{:?}", index_state(&state));
        }
        assert_eq!(
            committed(&state),
            3,
            "a rejected batch never advances the index"
        );
        follow::once_on(&state, &mut follower).await;
        let reason = stopped_reason(&state);
        assert!(reason.contains("block 14"), "{reason}");
        assert!(reason.contains("missing or duplicate post"), "{reason}");
        assert!(
            reason.contains(&format!("{PERSISTENT_REJECTIONS} polls")),
            "{reason}"
        );
    });
}

#[test]
fn an_event_that_does_not_decode_counts_as_a_rejection() {
    let mut malformed = posted(3, 13, 0, ANCHORED);
    malformed["data"] = json!("0x00");
    let node = Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(2),
        extra_logs: std::sync::Mutex::new(vec![malformed]),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    with_server(
        "rejections-decode",
        Some(node),
        setup,
        |server| async move {
            let Server { state, .. } = server;
            let mut follower = Follower::new(Limits::DEFAULT);
            for _ in 1..PERSISTENT_REJECTIONS {
                follow::once_on(&state, &mut follower).await;
                assert!(matches!(index_state(&state), State::Backfilling));
            }
            follow::once_on(&state, &mut follower).await;
            let reason = stopped_reason(&state);
            assert!(reason.contains("block 10"), "{reason}");
            assert_eq!(committed(&state), 0);
        },
    );
}

#[test]
fn a_log_whose_hash_disagrees_with_its_header_persistently_is_a_rejection() {
    let mut log = posted(3, 13, 0, ANCHORED);
    log["blockHash"] = json!(format!("0x{:064x}", 0xdead));
    let node = Node {
        head: AtomicU64::new(15),
        messages: AtomicU64::new(2),
        extra_logs: std::sync::Mutex::new(vec![log]),
        ..Node::new(31337, CONTRACT, ANCHORED.to_vec())
    };
    with_server("rejections-hash", Some(node), setup, |server| async move {
        let Server { state, .. } = server;
        let mut follower = Follower::new(Limits::DEFAULT);
        for _ in 1..PERSISTENT_REJECTIONS {
            follow::once_on(&state, &mut follower).await;
            assert!(matches!(index_state(&state), State::Backfilling));
        }
        follow::once_on(&state, &mut follower).await;
        let reason = stopped_reason(&state);
        assert!(reason.contains("log disagrees with header"), "{reason}");
        assert!(reason.contains("block 10"), "{reason}");
        assert_eq!(committed(&state), 0);
    });
}
