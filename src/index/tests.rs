use super::{Index, model::*, types::*};
use crate::chain::{Address, Checkpoint, Hash, Hex, Position};
use commonware_runtime::{Clock as _, Runner as _, Supervisor as _, deterministic};

fn address(n: u8) -> Address {
    Hex([n; 20])
}
fn hash(n: u8) -> Hash {
    Hex([n; 32])
}

#[test]
fn following_filter_fits_256_follows_plus_the_reader_and_replies_to_them() {
    use super::query::{Clause, Filter};
    let authors = (0u16..257)
        .map(|n| {
            let mut bytes = [0; 20];
            bytes[..2].copy_from_slice(&n.to_be_bytes());
            Hex(bytes)
        })
        .collect();
    let mut filter = Filter {
        policy: Default::default(),
        any: vec![
            Clause {
                authors,
                ..Default::default()
            },
            Clause {
                parent_authors: vec![address(9)],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    assert!(filter.key().is_ok());
    filter.any[0].authors.push(address(10));
    assert_eq!(filter.key().unwrap_err().to_string(), "too_many_authors");
}
fn deployment() -> Deployment {
    Deployment {
        chain: 42,
        board: address(42),
        start_block: 10,
        index_start_block: 10,
        registries: Vec::new(),
    }
}
/// Open, then establish, as the follower does once its first successful
/// poll has confirmed the chain; writes are refused before that.
async fn opened<E: commonware_storage::Context + commonware_runtime::Spawner>(
    ctx: E,
    deployment: Deployment,
    generation: Hash,
) -> Index {
    let index = Index::open(ctx, deployment, generation).await.unwrap();
    index.establish().unwrap();
    index
}

fn post(id: u64, parent: u64, log: u32) -> Event {
    Event::Posted {
        message: Message {
            id,
            parent,
            author: address(id as u8),
            parent_author: if parent == 0 {
                Address::ZERO
            } else {
                address(parent as u8)
            },
            timestamp: 1_000,
            content_hash: hash(id as u8),
            transaction: hash(200),
            gate: Address::ZERO,
            position: Position { block: 10, log },
        },
    }
}
fn batch(events: Vec<Event>) -> Batch {
    Batch {
        from: 10,
        through: Checkpoint {
            block: 10,
            hash: hash(10),
        },
        events,
    }
}

/// A root post by `author`, for scans longer than the u8 fixtures allow.
fn authored(id: u64, author: u8, log: u32) -> Event {
    let Event::Posted { mut message } = post(id, 0, log) else {
        unreachable!("post builds a Posted event")
    };
    message.author = address(author);
    Event::Posted { message }
}

#[test]
fn change_scopes_advance_over_noise_and_keep_their_captured_boundary() {
    use super::changes::{self, INSPECTED};
    let mut model = Model::new(deployment(), hash(2));
    let noise = INSPECTED as u64 + 30;
    let wanted = noise - 10;
    model.apply(batch(
        (1..=noise)
            .map(|id| authored(id, if id == wanted { 70 } else { 1 }, id as u32 - 1))
            .collect(),
    ));
    let first: changes::Request = serde_json::from_value(serde_json::json!({
        "generation": hash(2), "after": "0", "scope": {"feed": {"any": [{"authors": [address(70)]}]}}
    })).unwrap();
    let page = changes::read(&model, &first).unwrap();
    assert!(page.changes.is_empty());
    assert!(page.records.is_empty());
    assert_eq!(page.through, INSPECTED as u64);
    assert!(page.more);
    let until = page.until.clone();
    drop(page);
    let mut extra = authored(noise + 1, 1, 0);
    if let Event::Posted { message } = &mut extra {
        message.position.block = 11;
    }
    model.apply(Batch {
        from: 11,
        through: Checkpoint {
            block: 11,
            hash: hash(11),
        },
        events: vec![extra],
    });
    let next = changes::Request {
        after: INSPECTED as u64,
        until: Some(until.clone()),
        ..first
    };
    let page = changes::read(&model, &next).unwrap();
    assert_eq!(page.changes.len(), 1);
    assert_eq!(page.records[0].message.id, wanted);
    assert_eq!(page.through, noise);
    assert!(!page.more);
    assert_eq!(page.until, until);
    assert_eq!(
        page.at.sequence,
        noise + 1,
        "records still report the current snapshot"
    );
}

/// A page returns no more than the client accepts: fifty changes and a
/// hundred records. The entry that would pass either bound starts the next
/// page, so paging resumes with no gap and no repeat.
#[test]
fn a_page_stops_at_what_the_client_accepts_and_resumes_there() {
    use super::changes;
    let mut model = Model::new(deployment(), hash(2));
    model.apply(batch(
        (1..=120).map(|id| post(id, 0, id as u32 - 1)).collect(),
    ));
    fn read(model: &Model, after: u64, scope: serde_json::Value) -> (Vec<u64>, usize, u64, bool) {
        let req: changes::Request = serde_json::from_value(serde_json::json!({
            "generation": hash(2), "after": after.to_string(), "scope": scope
        }))
        .unwrap();
        let page = changes::read(model, &req).unwrap();
        (
            page.changes.iter().map(|c| c.sequence).collect(),
            page.records.len(),
            page.through,
            page.more,
        )
    }
    // Every post matches a timeline: fifty changes end each page.
    let (mut after, mut seen) = (0, Vec::new());
    loop {
        let (changes, records, through, more) = read(
            &model,
            after,
            serde_json::json!({"feed": {"any": [{"kind": "posts"}]}}),
        );
        assert!(changes.len() <= 50 && records <= 100);
        seen.extend(changes);
        after = through;
        if !more {
            break;
        }
    }
    assert_eq!(seen, (1..=120).collect::<Vec<_>>());

    // Gate changes on held posts carry records without being changes when
    // gates are off: a hundred records end the page on their own.
    let gates: Vec<_> = (1..=120)
        .map(|id| Event::ReplyGate {
            id,
            old: Address::ZERO,
            new: TOMBSTONE,
            position: Position {
                block: 11,
                log: id as u32,
            },
        })
        .collect();
    model.apply(Batch {
        from: 11,
        through: Checkpoint {
            block: 11,
            hash: hash(11),
        },
        events: gates,
    });
    let held: Vec<_> = (1..=120).map(|id| id.to_string()).collect();
    let (changes, records, through, more) = read(
        &model,
        120,
        serde_json::json!({"ids": held, "gates": false}),
    );
    assert!(changes.is_empty());
    assert_eq!(records, 100);
    assert_eq!(through, 220);
    assert!(more);
}

#[test]
fn changes_carry_only_relevant_posts_counts_and_gate_events() {
    use super::changes;
    let mut model = Model::new(deployment(), hash(2));
    model.apply(batch(vec![
        post(1, 0, 0),
        post(2, 1, 1),
        post(3, 2, 2),
        Event::ReplyGate {
            id: 2,
            old: Address::ZERO,
            new: TOMBSTONE,
            position: Position { block: 10, log: 3 },
        },
    ]));
    let req: changes::Request = serde_json::from_value(serde_json::json!({
        "generation": hash(2), "after": "1", "scope": {"parents": ["1"]}
    }))
    .unwrap();
    let page = changes::read(&model, &req).unwrap();
    assert_eq!(
        page.changes.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        [2, 4]
    );
    assert_eq!(
        page.records
            .iter()
            .map(|r| r.message.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        page.records[0].replies, 0,
        "deleted direct replies leave the count"
    );
    let req: changes::Request = serde_json::from_value(serde_json::json!({
        "generation": hash(2), "after": "1", "scope": {}
    }))
    .unwrap();
    let gates = changes::read(&model, &req).unwrap();
    assert_eq!(gates.changes.len(), 1, "empty scope follows gates alone");
    assert!(gates.records.is_empty());
    let req: changes::Request = serde_json::from_value(serde_json::json!({
        "generation": hash(2), "after": "1", "scope": {"feed": {"any": [{"kind": "posts"}]}, "gates": false}
    })).unwrap();
    let counts = changes::read(&model, &req).unwrap();
    assert!(
        counts.changes.is_empty(),
        "a parent count does not expose an unrelated child's event"
    );
    assert_eq!(counts.records.len(), 1);
    assert_eq!(counts.records[0].message.id, 1);
}

/// Post `id`'s gate flipping to `new` at `log`.
fn reply_gate(id: u64, new: Address, log: u32) -> Event {
    Event::ReplyGate {
        id,
        old: Address::ZERO,
        new,
        position: Position { block: 10, log },
    }
}
/// `author`'s gate flipping to `new` at `log`.
fn author_gate(author: Address, new: Address, log: u32) -> Event {
    Event::AuthorGate {
        author,
        old: Address::ZERO,
        new,
        position: Position { block: 10, log },
    }
}
fn change_page(model: &Model, after: u64, scope: serde_json::Value) -> super::changes::Page<'_> {
    let req: super::changes::Request = serde_json::from_value(serde_json::json!({
        "generation": hash(2), "after": after.to_string(), "scope": scope
    }))
    .unwrap();
    super::changes::read(model, &req).unwrap()
}
/// Walks a scope's pages from `after` to the end, counting them and
/// collecting every change's sequence, as the app does on a refresh.
fn walk_changes(model: &Model, after: u64, scope: serde_json::Value) -> (usize, Vec<u64>) {
    let (mut after, mut pages, mut seen) = (after, 0, Vec::new());
    loop {
        let page = change_page(model, after, scope.clone());
        pages += 1;
        seen.extend(page.changes.iter().map(|c| c.sequence));
        after = page.through;
        if !page.more {
            return (pages, seen);
        }
    }
}

/// A gate flipped many times in one page's scan comes back once, as its
/// newest change, and the page's scan passes every flip. Alternating between
/// two values is the same: the subject is the post, not the gate.
#[test]
fn a_page_keeps_the_newest_gate_change_per_subject() {
    use super::changes::INSPECTED;
    let mut model = Model::new(deployment(), hash(2));
    let mut events = vec![post(1, 0, 0), post(2, 0, 1)];
    for n in 0..INSPECTED as u32 {
        let gate = if n % 2 == 0 { address(50) } else { address(51) };
        events.push(reply_gate(1, gate, n + 2));
    }
    model.apply(batch(events));
    // Sequences: posts 1 and 2, then flips 3..=INSPECTED + 2.
    let page = change_page(&model, 2, serde_json::json!({}));
    assert_eq!(
        page.changes.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        [INSPECTED as u64 + 2]
    );
    assert_eq!(page.through, INSPECTED as u64 + 2);
    assert!(!page.more);
    let Some(&&Change {
        event: ChangeEvent::Chain {
            event: Event::ReplyGate { new, .. },
        },
        ..
    }) = page.changes.first()
    else {
        panic!("a reply gate change");
    };
    assert_eq!(new, address(51), "the newest value");
    // Ten thousand flips on two subjects: five pages, not two hundred.
    let mut model = Model::new(deployment(), hash(2));
    let mut events = vec![post(1, 0, 0), post(2, 0, 1)];
    for n in 0..10_000u32 {
        events.push(reply_gate(
            1 + u64::from(n % 2),
            address(50 + (n % 4) as u8),
            n + 2,
        ));
    }
    model.apply(batch(events));
    let (pages, seen) = walk_changes(&model, 2, serde_json::json!({}));
    assert_eq!(pages, 5);
    assert_eq!(seen.len(), 10, "two subjects per page");
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "in order");
}

/// A subject already on a full page is still replaced by its newer change;
/// a new subject starts the next page, and the scan stops before it.
#[test]
fn a_full_page_still_replaces_a_subject_it_holds() {
    use super::changes::MAX_CHANGES;
    let mut model = Model::new(deployment(), hash(2));
    let subjects = MAX_CHANGES as u64;
    let mut events = (1..=subjects + 1)
        .map(|id| post(id, 0, id as u32 - 1))
        .collect::<Vec<_>>();
    let mut log = subjects as u32 + 1;
    // Fifty subjects fill a page...
    for id in 1..=subjects {
        events.push(reply_gate(id, address(50), log));
        log += 1;
    }
    // ...one flips again: replaced, not a fifty-first change...
    events.push(reply_gate(7, address(51), log));
    log += 1;
    // ...and a fifty-first subject starts the next page.
    events.push(reply_gate(subjects + 1, address(50), log));
    model.apply(batch(events));
    let first_gate = subjects + 2;
    let page = change_page(&model, subjects + 1, serde_json::json!({}));
    assert_eq!(page.changes.len(), MAX_CHANGES);
    let again = first_gate + subjects;
    assert!(
        page.changes.iter().any(|c| c.sequence == again),
        "the newer flip"
    );
    assert!(
        !page.changes.iter().any(|c| c.sequence == first_gate + 6),
        "not the older"
    );
    assert_eq!(page.through, again);
    assert!(page.more);
    let next = change_page(&model, page.through, serde_json::json!({}));
    assert_eq!(
        next.changes.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        [again + 1]
    );
    assert!(!next.more);
}

/// Reply gates, author gates, posts and reference changes interleaved: each
/// gate subject once, at its newest, in sequence order among the rest, with
/// the records of the posts the scope asked for.
#[test]
fn grouped_gates_keep_their_order_among_other_changes() {
    let mut model = Model::new(deployment(), hash(2));
    model.apply(batch(vec![
        post(1, 0, 0),
        reply_gate(1, address(50), 1),
        author_gate(address(1), address(60), 2),
        post(2, 1, 3),
        reply_gate(1, TOMBSTONE, 4),
        author_gate(address(2), address(61), 5),
        author_gate(address(1), address(62), 6),
        post(3, 0, 7),
    ]));
    model.references.analyses.insert(
        hash(2),
        super::references::Analysis {
            hash: hash(2),
            parser: 1,
            references: vec![super::references::Reference::Mention {
                account: address(9),
            }],
        },
    );
    model.references.ready_bodies.insert(hash(2));
    model.apply_update(Update::Bind(vec![2]));
    // Sequences: 1 post1, 2 gate1, 3 author1, 4 post2, 5 gate1, 6 author2, 7 author1, 8 post3, 9 refs2.
    let page = change_page(
        &model,
        0,
        serde_json::json!({"ids": ["1", "2"], "account": address(9)}),
    );
    assert_eq!(
        page.changes.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        [1, 4, 5, 6, 7, 9]
    );
    assert_eq!(
        page.records
            .iter()
            .map(|r| r.message.id)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        page.records[0].message.gate, TOMBSTONE,
        "the record carries the current gate"
    );
    assert_eq!(page.references.len(), 1);
}

/// The newest change within the scanned range is kept, not the newest the
/// index has: a flip past `until` waits for the next refresh, while the
/// record snapshot already shows it.
#[test]
fn grouping_stops_at_the_captured_boundary() {
    let mut model = Model::new(deployment(), hash(2));
    model.apply(batch(vec![
        post(1, 0, 0),
        reply_gate(1, address(50), 1),
        reply_gate(1, address(51), 2),
    ]));
    let until = model.at.clone();
    model.apply(Batch {
        from: 11,
        through: Checkpoint {
            block: 11,
            hash: hash(11),
        },
        events: vec![Event::ReplyGate {
            id: 1,
            old: address(51),
            new: address(52),
            position: Position { block: 11, log: 0 },
        }],
    });
    let req: super::changes::Request = serde_json::from_value(serde_json::json!({
        "generation": hash(2), "after": "1", "until": until, "scope": {"ids": ["1"]}
    }))
    .unwrap();
    let page = super::changes::read(&model, &req).unwrap();
    let Some(&&Change {
        sequence,
        event: ChangeEvent::Chain {
            event: Event::ReplyGate { new, .. },
        },
    }) = page.changes.first()
    else {
        panic!("a reply gate change");
    };
    assert_eq!((sequence, new), (3, address(51)));
    assert_eq!(
        page.records[0].message.gate,
        address(52),
        "the snapshot is newer"
    );
    assert!(!page.more);
    let later = change_page(&model, 3, serde_json::json!({"ids": ["1"]}));
    assert_eq!(
        later.changes.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        [4]
    );
}

#[test]
fn change_requests_refuse_expired_cursors_and_excessive_scopes() {
    use super::changes;
    let mut model = Model::new(deployment(), hash(2));
    model.apply(batch(vec![post(1, 0, 0), post(2, 0, 1)]));
    model.changes.pop_front();
    let make = |after: &str, scope: serde_json::Value| -> changes::Request {
        serde_json::from_value(serde_json::json!({
            "generation": hash(2), "after": after, "scope": scope
        }))
        .unwrap()
    };
    assert!(changes::read(&model, &make("0", serde_json::json!({}))).is_err());
    assert!(changes::read(&model, &make("3", serde_json::json!({}))).is_err());
    assert!(
        changes::read(
            &model,
            &make("1", serde_json::json!({"ids": vec!["1"; 4097]}))
        )
        .is_err()
    );
}

#[test]
fn ordered_posts_and_gate_changes_have_one_deterministic_result() {
    let mut model = Model::new(deployment(), hash(2));
    let b = batch(vec![
        post(1, 0, 0),
        post(2, 1, 1),
        post(3, 1, 2),
        post(4, 2, 3),
        Event::ReplyGate {
            id: 2,
            old: Address::ZERO,
            new: TOMBSTONE,
            position: Position { block: 10, log: 4 },
        },
        Event::ReplyGate {
            id: 3,
            old: Address::ZERO,
            new: REMOVED,
            position: Position { block: 10, log: 5 },
        },
        Event::AuthorGate {
            author: address(1),
            old: Address::ZERO,
            new: address(9),
            position: Position { block: 10, log: 6 },
        },
    ]);
    model.validate(&b).unwrap();
    model.apply(b);
    assert_eq!(
        model.counts[&1], 0,
        "deleted and removed replies leave the count"
    );
    assert_eq!(
        model.counts[&2], 1,
        "deleting a parent does not delete its descendants"
    );
    assert_eq!(model.children[&1], vec![2, 3]);
    assert_eq!(model.parent_authors[&address(1)], vec![2, 3]);
    assert_eq!(model.effective_gate(&model.messages[&1]), address(9));
    assert_eq!(model.effective_gate(&model.messages[&2]), TOMBSTONE);
    assert_eq!(model.at.sequence, 7);
    assert_eq!(model.at.checkpoint.unwrap().block, 10);
}

#[test]
fn malformed_batch_does_not_apply_even_its_valid_prefix() {
    let model = Model::new(deployment(), hash(2));
    for b in [
        batch(vec![post(1, 0, 0), post(3, 1, 1)]),
        batch(vec![post(1, 0, 0), post(2, 2, 1)]),
        batch(vec![post(1, 0, 1), post(2, 1, 0)]),
        batch(vec![
            post(1, 0, 0),
            Event::ReplyGate {
                id: 1,
                old: address(3),
                new: TOMBSTONE,
                position: Position { block: 10, log: 1 },
            },
        ]),
        batch(vec![
            post(1, 0, 0),
            Event::ReplyGate {
                id: 1,
                old: Address::ZERO,
                new: TOMBSTONE,
                position: Position { block: 10, log: 1 },
            },
            Event::ReplyGate {
                id: 1,
                old: TOMBSTONE,
                new: Address::ZERO,
                position: Position { block: 10, log: 2 },
            },
        ]),
    ] {
        assert!(model.validate(&b).is_err());
        assert!(model.messages.is_empty());
        assert_eq!(model.at.sequence, 0);
    }
}

#[test]
fn commitment_covers_immutable_fields_only() {
    let Event::Posted { mut message } = post(1, 0, 0) else {
        unreachable!()
    };
    let pin = message.commitment();
    message.gate = TOMBSTONE;
    message.position.block += 1;
    message.id += 1;
    assert_eq!(message.commitment(), pin);
    for field in 0..4 {
        let mut altered = message.clone();
        match field {
            0 => altered.author = address(99),
            1 => altered.parent = 99,
            2 => altered.timestamp += 1,
            _ => altered.content_hash = hash(99),
        }
        assert_ne!(altered.commitment(), pin);
    }
    // Independent Solidity vector in Board.t.sol, including ABI word padding.
    message.author = address(0x11);
    message.parent = 7;
    message.timestamp = 0xabcdef01;
    message.content_hash = hash(0x22);
    assert_eq!(
        message.commitment().to_string(),
        "0x4db098ca2d19724366861cb45ccdc8f5ba82751be711c2d37aaa8bcb6daf56c7"
    );
}

#[test]
fn json_preserves_u64_ids_and_rejects_wrong_width_hashes() {
    let Event::Posted { mut message } = post(1, 0, 0) else {
        unreachable!()
    };
    message.id = u64::MAX;
    let value = serde_json::to_value(&message).unwrap();
    assert_eq!(value["id"], u64::MAX.to_string());
    assert_eq!(serde_json::from_value::<Message>(value).unwrap(), message);
    assert!("0x01".parse::<Hash>().is_err());
}

#[test]
fn journal_replay_restores_checkpoint_generation_and_indexes() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        let at = index
            .append(batch(vec![
                post(1, 0, 0),
                post(2, 1, 1),
                post(3, 1, 2),
                post(4, 2, 3),
                Event::ReplyGate {
                    id: 2,
                    old: Address::ZERO,
                    new: TOMBSTONE,
                    position: Position { block: 10, log: 4 },
                },
            ]))
            .await
            .unwrap();
        assert_eq!(index.view.read().unwrap().model.counts[&1], 1);
        drop(index);
        // Let the owner drain and close before another handle opens the partitions.
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let restored = Index::open(ctx.child("restart"), deployment(), hash(99))
            .await
            .unwrap();
        let view = restored.view.read().unwrap();
        assert_eq!(view.model.at, at);
        assert_eq!(view.model.children[&1], vec![2, 3]);
        assert_eq!(
            view.model.counts[&1], 1,
            "replay counts only the live sibling"
        );
        assert_eq!(
            view.model.counts[&2], 1,
            "the deleted reply retains its own children"
        );
        assert_eq!(view.model.messages[&2].gate, TOMBSTONE);
        assert!(
            !view.state().is_ready(),
            "restart requires checkpoint verification"
        );
    });
}

#[test]
fn journal_rejects_invalid_batch_then_accepts_a_valid_retry() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("index"), deployment(), hash(2)).await;
        assert!(
            index
                .append(batch(vec![post(1, 0, 0), post(3, 0, 1)]))
                .await
                .is_err()
        );
        assert!(index.view.read().unwrap().model.messages.is_empty());
        index.append(batch(vec![post(1, 0, 0)])).await.unwrap();
        assert!(index.append(batch(vec![post(1, 0, 0)])).await.is_err());
        assert_eq!(index.view.read().unwrap().model.messages.len(), 1);
    });
}

#[test]
fn cancelled_acknowledgement_does_not_cancel_the_durable_write() {
    use futures::FutureExt as _;
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("index"), deployment(), hash(2)).await;
        assert!(
            index
                .append(batch(vec![post(1, 0, 0)]))
                .now_or_never()
                .is_none()
        );
        ctx.sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(index.view.read().unwrap().model.at.sequence, 1);
        drop(index);
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let restored = Index::open(ctx.child("restart"), deployment(), hash(9))
            .await
            .unwrap();
        assert_eq!(restored.view.read().unwrap().model.at.sequence, 1);
    });
}

#[test]
fn apply_failure_stops_only_its_chain_without_poisoning_readers() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("index"), deployment(), hash(2)).await;
        index
            .append(batch(vec![post(1, 0, 0), post(2, 1, 1)]))
            .await
            .unwrap();
        let mut other_deployment = deployment();
        other_deployment.chain += 1;
        let other = opened(ctx.child("other"), other_deployment, hash(3)).await;
        {
            index.mark_ready();
            let mut view = index.view.write().unwrap();
            // Inject a disagreement between validated history and a derived map.
            // Applying a removal will fail after the preceding post was applied.
            view.model.counts.clear();
        }
        let mut posted = post(3, 0, 0);
        if let Event::Posted { message } = &mut posted {
            message.position.block = 11;
        }
        let update = Batch {
            from: 11,
            through: Checkpoint {
                block: 11,
                hash: hash(11),
            },
            events: vec![
                posted,
                Event::ReplyGate {
                    id: 2,
                    old: Address::ZERO,
                    new: REMOVED,
                    position: Position { block: 11, log: 1 },
                },
            ],
        };
        index.view.read().unwrap().model.validate(&update).unwrap();
        assert!(index.append(update).await.is_err());
        {
            let view = index.view.read().expect("apply must not poison the lock");
            assert!(
                matches!(view.state(), super::State::Stopped(reason) if reason.contains("apply"))
            );
            assert!(
                !view.state().is_ready(),
                "partially applied state must not be served"
            );
        }
        assert!(index.append(batch(vec![])).await.is_err());
        other.append(batch(vec![post(1, 0, 0)])).await.unwrap();
        assert!(!other.state().is_stopped());
    });
}

#[test]
fn failed_sync_never_publishes_the_batch_and_stops_the_writer() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("index"), deployment(), hash(2)).await;
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        assert!(index.append(batch(vec![post(1, 0, 0)])).await.is_err());
        {
            let view = index.view.read().unwrap();
            assert!(view.model.messages.is_empty());
            assert_eq!(view.model.at.sequence, 0);
            assert!(view.state().is_stopped());
            assert!(!view.state().is_ready());
        }
        assert!(index.append(batch(vec![post(1, 0, 0)])).await.is_err());
    });
}

/// A record that cannot be synced is not served, stops the writer, and is
/// gone after a restart, whatever kind of update it was; what was synced
/// before it replays.
#[test]
fn after_a_failed_write_a_restart_replays_only_synced_records() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        index
            .append(range(10, 10, vec![post_in(10, 1)]))
            .await
            .unwrap();
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        let analysis = Update::Analyze(vec![super::references::Analysis {
            hash: hash(1),
            parser: super::references::PARSER_VERSION,
            references: vec![],
        }]);
        assert!(index.update(analysis).await.is_err());
        assert!(index.state().is_stopped());
        assert!(
            !index
                .view
                .read()
                .unwrap()
                .model
                .references
                .analyses
                .contains_key(&hash(1))
        );
        ctx.storage_fault_config().write().sync_rate = None;
        assert!(
            index
                .append(range(11, 11, vec![post_in(11, 2)]))
                .await
                .is_err(),
            "a stopped writer takes no more work"
        );
        let index = reopen(&ctx, index, "restart").await;
        let view = index.view.read().unwrap();
        assert_eq!(view.model.at.checkpoint.unwrap().block, 10);
        assert_eq!(view.model.messages.len(), 1);
        assert!(!view.model.references.analyses.contains_key(&hash(1)));
    });
}

/// A clean stop journals the empty progress RAM holds. If that record cannot
/// be synced, the writer stops and refuses more work, and a restart replays
/// only what was durable before it.
#[test]
fn a_failed_flush_sync_stops_the_writer_and_replays_only_durable_progress() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        index
            .append(range(10, 10, vec![post_in(10, 1)]))
            .await
            .unwrap();
        index.append(range(11, 50, vec![])).await.unwrap();
        let served = index.view.read().unwrap().model.at.clone();
        assert_eq!(served.checkpoint.unwrap().block, 50);
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        assert!(index.flush().await.is_err());
        assert!(index.state().is_stopped());
        assert_eq!(
            index.view.read().unwrap().model.at,
            served,
            "the model is left as it was"
        );
        ctx.storage_fault_config().write().sync_rate = None;
        assert!(index.flush().await.is_err());
        assert!(index.append(range(51, 60, vec![])).await.is_err());
        let index = reopen(&ctx, index, "restart").await;
        let view = index.view.read().unwrap();
        assert_eq!(view.model.at.checkpoint.unwrap().block, 10);
        assert_eq!(view.model.messages.len(), 1);
    });
}

/// A fresh journal writes its identity when it opens. If that record cannot
/// be synced, opening fails; a later open writes it and works normally.
#[test]
fn a_failed_identity_sync_prevents_opening_and_can_be_retried() {
    deterministic::Runner::default().start(|ctx| async move {
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        assert!(
            Index::open(ctx.child("failed"), deployment(), hash(2))
                .await
                .is_err()
        );
        ctx.storage_fault_config().write().sync_rate = None;
        let index = opened(ctx.child("retry"), deployment(), hash(2)).await;
        index.append(batch(vec![post(1, 0, 0)])).await.unwrap();
        let index = reopen(&ctx, index, "restart").await;
        assert_eq!(index.view.read().unwrap().model.messages.len(), 1);
    });
}

#[test]
fn a_journal_waits_to_be_established_and_cannot_be_reused_for_another_start_block() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("index"), deployment(), hash(2)).await;
        drop(index);
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let reopened = Index::open(ctx.child("restart"), deployment(), hash(3))
            .await
            .unwrap();
        assert!(!reopened.state().is_established());
        assert!(
            reopened.append(batch(vec![post(1, 0, 0)])).await.is_err(),
            "no chain update is accepted before the follower confirms the chain"
        );
        reopened.establish().unwrap();
        drop(reopened);
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let mut other = deployment();
        other.start_block += 1;
        assert!(
            Index::open(ctx.child("wrong_start"), other, hash(3))
                .await
                .is_err()
        );
    });
}

#[test]
fn feed_cursor_survives_new_posts_and_binds_filter_and_generation() {
    use super::query::{self, Clause, Filter};
    let mut model = Model::new(deployment(), hash(2));
    let b = batch((1..=30).map(|id| post(id, 0, id as u32)).collect());
    model.validate(&b).unwrap();
    model.apply(b);
    let filter = Filter::default();
    let first = query::feed(&model, &filter, None, 20).unwrap();
    assert_eq!(first.items.first().unwrap().message.id, 30);
    assert_eq!(first.items.last().unwrap().message.id, 11);
    let cursor = first.next.unwrap();
    let mut added = post(31, 0, 0);
    if let Event::Posted { message } = &mut added {
        message.position.block = 11;
    }
    let b = Batch {
        from: 11,
        through: Checkpoint {
            block: 11,
            hash: hash(11),
        },
        events: vec![added],
    };
    model.validate(&b).unwrap();
    model.apply(b);
    let second = query::feed(&model, &filter, Some(&cursor), 20).unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|r| r.message.id)
            .collect::<Vec<_>>(),
        (1..=10).rev().collect::<Vec<_>>()
    );
    assert!(second.next.is_none());
    let different = Filter {
        policy: Default::default(),
        exclude_list: None,
        any: vec![Clause {
            authors: vec![address(1)],
            ..Default::default()
        }],
    };
    assert!(query::feed(&model, &different, Some(&cursor), 20).is_err());
    model.at.generation = hash(99);
    assert!(query::feed(&model, &filter, Some(&cursor), 20).is_err());
}

#[test]
fn following_merge_deduplicates_and_a_hidden_page_still_makes_progress() {
    use super::query::{self, Clause, Filter};
    let mut model = Model::new(deployment(), hash(2));
    let b = batch(vec![post(1, 0, 0), post(2, 1, 1), post(3, 1, 2)]);
    model.validate(&b).unwrap();
    model.apply(b);
    let filter = Filter {
        policy: Default::default(),
        exclude_list: None,
        any: vec![
            Clause {
                authors: vec![address(2), address(2)],
                ..Default::default()
            },
            Clause {
                parent_authors: vec![address(1)],
                ..Default::default()
            },
        ],
    };
    let page = query::feed(&model, &filter, None, 20).unwrap();
    assert_eq!(
        page.items.iter().map(|r| r.message.id).collect::<Vec<_>>(),
        vec![3, 2]
    );

    let mut model = Model::new(deployment(), hash(2));
    let b = batch(
        (1..=600)
            .map(|id| {
                let Event::Posted { mut message } = post(id, 0, id as u32) else {
                    unreachable!()
                };
                message.author = address(7);
                if id > 100 {
                    message.gate = TOMBSTONE;
                }
                Event::Posted { message }
            })
            .collect(),
    );
    model.validate(&b).unwrap();
    model.apply(b);
    let filter = Filter::default();
    let first = query::feed(&model, &filter, None, 20).unwrap();
    assert!(first.items.is_empty());
    let next = first.next.unwrap();
    assert_eq!(next.before, 101);
    let second = query::feed(&model, &filter, Some(&next), 20).unwrap();
    assert_eq!(second.items[0].message.id, 100);
}

#[test]
fn ended_replies_leave_lists_and_counts_but_preserve_direct_access() {
    use super::query::{self, Clause, Filter, Kind, Record};
    let ids = |items: &[Record<'_>]| items.iter().map(|r| r.message.id).collect::<Vec<_>>();
    let mut model = Model::new(deployment(), hash(2));
    let Event::Posted {
        message: mut born_deleted,
    } = post(7, 1, 6)
    else {
        unreachable!()
    };
    born_deleted.gate = TOMBSTONE;
    let b = batch(vec![
        post(1, 0, 0),
        post(2, 1, 1),
        post(3, 2, 2),
        post(4, 1, 3),
        post(5, 1, 4),
        post(6, 5, 5),
        Event::Posted {
            message: born_deleted,
        },
        Event::ReplyGate {
            id: 2,
            old: Address::ZERO,
            new: TOMBSTONE,
            position: Position { block: 10, log: 7 },
        },
        Event::ReplyGate {
            id: 5,
            old: Address::ZERO,
            new: REMOVED,
            position: Position { block: 10, log: 8 },
        },
    ]);
    model.validate(&b).unwrap();
    model.apply(b);
    let page = query::replies(&model, 1, None, 20).unwrap();
    assert_eq!(
        ids(&page.items),
        vec![4],
        "ended replies are omitted, including born-deleted ones, without promoting descendants"
    );
    assert_eq!(page.parent.replies, 1, "only live direct replies count");
    let under_deleted = query::replies(&model, 2, None, 20).unwrap();
    assert_eq!(
        ids(&under_deleted.items),
        vec![3],
        "a direct read of a deleted reply shows its surviving replies"
    );
    let under_removed = query::replies(&model, 5, None, 20).unwrap();
    assert_eq!(ids(&under_removed.items), vec![6]);
    assert_eq!(
        under_removed.parent.replies, 1,
        "removal hides a branch from the parent, it does not erase the replies"
    );
    let feed = query::feed(
        &model,
        &Filter {
            policy: Default::default(),
            exclude_list: None,
            any: vec![Clause {
                parent_authors: vec![address(1), address(2), address(5)],
                kind: Kind::Replies,
                ..Default::default()
            }],
        },
        None,
        20,
    )
    .unwrap();
    assert_eq!(
        ids(&feed.items),
        vec![6, 4, 3],
        "neither deleted nor removed replies stand alone in a feed"
    );
}

#[test]
fn feed_clauses_share_a_cursor_and_roots_do_not_scan_reply_history() {
    use super::query::{self, Clause, Filter, Kind};
    let mut model = Model::new(deployment(), hash(2));
    let events = std::iter::once(post(1, 0, 0))
        .chain((2..=601).map(|id| {
            let Event::Posted { mut message } = post(id, 1, id as u32) else {
                unreachable!()
            };
            message.author = address(if id == 2 { 2 } else { 7 });
            Event::Posted { message }
        }))
        .collect();
    let b = batch(events);
    model.validate(&b).unwrap();
    model.apply(b);
    let roots = Clause {
        kind: Kind::Posts,
        ..Default::default()
    };
    let page = query::feed(
        &model,
        &Filter {
            policy: Default::default(),
            exclude_list: None,
            any: vec![roots.clone()],
        },
        None,
        20,
    )
    .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].message.id, 1);
    assert!(page.next.is_none(), "roots use their own posting list");
    let filter = Filter {
        policy: Default::default(),
        exclude_list: None,
        any: vec![
            roots,
            Clause {
                authors: vec![address(2)],
                parent_authors: vec![address(1)],
                kind: Kind::Replies,
                ..Default::default()
            },
        ],
    };
    let first = query::feed(&model, &filter, None, 1).unwrap();
    assert_eq!(first.items[0].message.id, 2);
    let second = query::feed(&model, &filter, first.next.as_ref(), 1).unwrap();
    assert_eq!(second.items[0].message.id, 1);
    assert!(second.next.is_none());
    let empty = query::feed(
        &model,
        &Filter {
            policy: Default::default(),
            any: vec![],
            exclude_list: None,
        },
        None,
        20,
    )
    .unwrap();
    assert!(
        empty.items.is_empty() && empty.next.is_none(),
        "following nobody selects nobody"
    );
    let replies = query::replies(&model, 1, None, 20).unwrap();
    assert_eq!(replies.parent.message.id, 1);
    assert_eq!(replies.parent.replies, 600);
}

#[test]
fn lifecycle_guards_reads_and_never_revives_a_stopped_index() {
    use super::State;
    deterministic::Runner::default().start(|ctx| async move {
        let index = Index::open(ctx.child("index"), deployment(), hash(2))
            .await
            .unwrap();
        assert_eq!(index.state(), State::Starting);
        index.mark_ready();
        assert_eq!(index.state(), State::Starting);
        assert!(index.read_ready(|_| ()).is_none());
        assert!(index.append(batch(vec![])).await.is_err());
        index.establish().unwrap();
        assert_eq!(index.state(), State::Backfilling);
        assert!(index.read_ready(|_| ()).is_none());
        index.append(batch(vec![post(1, 0, 0)])).await.unwrap();
        index.mark_ready();
        assert_eq!(index.read_ready(|m| m.messages.len()), Some(1));
        index.stop("test stop");
        index.mark_ready();
        assert!(index.establish().is_err());
        assert!(index.read_ready(|_| ()).is_none());
        assert_eq!(index.state(), State::Stopped("test stop".into()));
    });
}

#[test]
fn record_queries_preserve_pins_ancestry_and_recovery_cursor_bounds() {
    use super::query;
    use serde_json::json;
    let mut model = Model::new(deployment(), hash(2));
    let events = (1..=502)
        .map(|id| {
            let mut e = post(id, id.saturating_sub(1), id as u32);
            if let Event::Posted { message } = &mut e {
                message.author = address(1);
                message.parent_author = if id == 1 { Address::ZERO } else { address(1) };
                message.content_hash = hash(1);
            }
            e
        })
        .collect();
    let b = batch(events);
    model.validate(&b).unwrap();
    model.apply(b);
    let req = serde_json::from_value(json!({"ancestors":2})).unwrap();
    let answer = query::message(&model, 4, &req).unwrap();
    assert_eq!(answer["ancestors"][0]["id"], "3");
    assert_eq!(answer["ancestors"][1]["id"], "2");
    assert!(query::message(&model, 0, &req).is_err());
    let bad = serde_json::from_value(json!({"commit": hash(9)})).unwrap();
    assert!(query::message(&model, 4, &bad).is_err());
    let mut request =
        json!({"author":address(1), "content_hash":hash(1), "parent":"0", "limit":20});
    let first = query::lookup(&model, &serde_json::from_value(request.clone()).unwrap()).unwrap();
    assert!(first["items"].as_array().unwrap().is_empty());
    assert_eq!(
        first["next"]["before"], "3",
        "cursor follows examined candidates, not matches"
    );
    request["cursor"] = first["next"].clone();
    let last = query::lookup(&model, &serde_json::from_value(request.clone()).unwrap()).unwrap();
    assert_eq!(last["items"][0]["id"], "1");
    assert!(last["next"].is_null());
    request["parent"] = json!("1");
    assert!(query::lookup(&model, &serde_json::from_value(request).unwrap()).is_err());
}

#[test]
fn notification_exclusions_filter_before_paging_without_erasing_references() {
    use super::{
        query::{self, Clause, Filter},
        references::{Analysis, PARSER_VERSION, Reference},
    };
    let registry = address(90);
    let list = ListKey {
        registry,
        owner: address(1),
        list: hash(91),
    };
    let mut d = deployment();
    d.registries.push(registry);
    let mut model = Model::new(d, hash(2));
    let mut events = vec![post(1, 0, 0)];
    for id in 2..=603 {
        let Event::Posted { mut message } = post(id, 1, id as u32) else {
            unreachable!()
        };
        message.author = address(if id <= 3 { 3 } else { 2 });
        message.content_hash = hash(7);
        events.push(Event::Posted { message });
    }
    events.push(Event::Membership {
        list,
        member: address(2),
        present: true,
        position: Position {
            block: 10,
            log: 604,
        },
    });
    let b = batch(events);
    model.validate(&b).unwrap();
    model.apply(b);
    let after = model.at.sequence;
    let analysis = Update::Analyze(vec![Analysis {
        hash: hash(7),
        parser: PARSER_VERSION,
        references: vec![Reference::Mention {
            account: address(1),
        }],
    }]);
    model.validate_update(&analysis).unwrap();
    model.apply_update(analysis);
    for start in (2..=603).step_by(32) {
        let bind = Update::Bind((start..=(start + 31).min(603)).collect());
        model.validate_update(&bind).unwrap();
        model.apply_update(bind);
    }
    let mut req: super::changes::Request = serde_json::from_value(serde_json::json!({
        "generation": model.at.generation, "after": after.to_string(),
        "scope": {"account": address(1), "exclude_list": list}
    }))
    .unwrap();
    let mut found = Vec::new();
    loop {
        let page = super::changes::read(&model, &req).unwrap();
        found.extend(page.references.iter().map(|r| r.record.message.id));
        req.after = page.through;
        if !page.more {
            break;
        }
    }
    assert_eq!(
        found,
        vec![2, 3],
        "late-body changes apply the same exclusion"
    );
    let filter = Filter {
        policy: Default::default(),
        any: vec![Clause {
            parent_authors: vec![address(1)],
            ..Default::default()
        }],
        exclude_list: Some(list),
    };
    let first = query::feed(&model, &filter, None, 20).unwrap();
    assert!(
        first.items.is_empty(),
        "the 500-candidate bound still applies"
    );
    let second = query::feed(&model, &filter, first.next.as_ref(), 20).unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|r| r.message.id)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    assert!(second.next.is_none());
    let first_refs = query::references(&model, address(1), Some(list), None, 20).unwrap();
    assert!(first_refs.items.is_empty());
    let second_refs =
        query::references(&model, address(1), Some(list), first_refs.next.as_ref(), 20).unwrap();
    assert_eq!(
        second_refs
            .items
            .iter()
            .map(|r| r.record.message.id)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    assert_eq!(
        model.references.accounts[&address(1)].len(),
        602,
        "index associations remain complete"
    );
    let other = Some(ListKey {
        owner: address(9),
        ..list
    });
    assert_eq!(
        query::references(&model, address(1), other, None, 20)
            .unwrap()
            .items
            .len(),
        20
    );
    assert!(
        query::references(&model, address(1), other, first_refs.next.as_ref(), 20).is_err(),
        "cursor binds the exclusion list"
    );
    assert!(
        query::references(
            &model,
            address(1),
            Some(ListKey {
                registry: address(99),
                ..list
            }),
            None,
            20
        )
        .is_err(),
        "unknown registries fail closed"
    );
    assert_eq!(
        query::replies(&model, 1, None, 20).unwrap().items.len(),
        20,
        "thread reads remain independent"
    );
}

#[test]
fn notification_changes_exclude_posts_and_reload_only_for_the_selected_list() {
    use super::{changes, query};
    fn apply(model: &mut Model, mut b: Batch) {
        for event in &mut b.events {
            if let Event::Posted { message } = event {
                message.position.block = b.through.block;
            }
        }
        model.validate(&b).unwrap();
        model.apply(b);
    }
    let registry = address(90);
    let list = ListKey {
        registry,
        owner: address(1),
        list: hash(91),
    };
    let mut d = deployment();
    d.registries.push(registry);
    let mut model = Model::new(d, hash(2));
    apply(
        &mut model,
        batch(vec![
            post(1, 0, 0),
            Event::Membership {
                list,
                member: address(2),
                present: true,
                position: Position { block: 10, log: 1 },
            },
        ]),
    );
    let after = model.at.sequence;
    apply(
        &mut model,
        Batch {
            from: 11,
            through: Checkpoint {
                block: 11,
                hash: hash(11),
            },
            events: vec![
                post(2, 1, 0),
                post(3, 1, 1),
                Event::AuthorGate {
                    author: address(2),
                    old: Address::ZERO,
                    new: TOMBSTONE,
                    position: Position { block: 11, log: 2 },
                },
            ],
        },
    );
    let mut req: changes::Request = serde_json::from_value(serde_json::json!({
        "generation": model.at.generation, "after": after.to_string(),
        "scope": {"feed": {"any": [{"parent_authors": [address(1)]}], "exclude_list": list}}
    }))
    .unwrap();
    let answer = changes::read(&model, &req).unwrap();
    assert_eq!(
        answer
            .records
            .iter()
            .map(|r| r.message.id)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(
        answer.changes.len(),
        2,
        "allowed post plus the gate event needed by the shared ledger"
    );
    req.after = model.at.sequence;
    apply(
        &mut model,
        Batch {
            from: 12,
            through: Checkpoint {
                block: 12,
                hash: hash(12),
            },
            events: vec![Event::Membership {
                list: ListKey {
                    owner: address(9),
                    ..list
                },
                member: address(2),
                present: true,
                position: Position { block: 12, log: 0 },
            }],
        },
    );
    assert!(
        changes::read(&model, &req).is_ok(),
        "another owner's list does not invalidate this view"
    );
    apply(
        &mut model,
        Batch {
            from: 13,
            through: Checkpoint {
                block: 13,
                hash: hash(13),
            },
            events: vec![Event::Membership {
                list,
                member: address(2),
                present: false,
                position: Position { block: 13, log: 0 },
            }],
        },
    );
    assert_eq!(
        changes::read(&model, &req).err().unwrap().to_string(),
        "reset_required"
    );
    let filter = req.scope.feed.take().unwrap();
    assert_eq!(
        query::feed(&model, &filter, None, 20).unwrap().items.len(),
        2,
        "unblock needs no reference rebuild"
    );
    req.scope.account = Some(address(1));
    req.scope.exclude_list = Some(list);
    assert_eq!(
        changes::read(&model, &req).err().unwrap().to_string(),
        "reset_required",
        "reference views reload too"
    );
}

#[test]
fn a_posts_clause_with_parent_authors_is_refused_before_any_scan() {
    use super::{
        changes,
        query::{self, Clause, Filter, Kind, Refusal},
    };
    let mut model = Model::new(deployment(), hash(2));
    model.apply(batch(vec![post(1, 0, 0), post(2, 1, 1)]));
    for parent_author in [address(1), Address::ZERO] {
        let filter = Filter {
            policy: Default::default(),
            exclude_list: None,
            any: vec![Clause {
                parent_authors: vec![parent_author],
                kind: Kind::Posts,
                ..Default::default()
            }],
        };
        let error = query::feed(&model, &filter, None, 20)
            .err()
            .expect("refused");
        assert_eq!(
            error.downcast_ref::<Refusal>().map(|r| r.0),
            Some("posts_have_no_parent_author")
        );
        let req: changes::Request = serde_json::from_value(serde_json::json!({
            "generation": hash(2), "after": "0",
            "scope": {"feed": serde_json::to_value(&filter).unwrap()}
        }))
        .unwrap();
        assert_eq!(
            changes::read(&model, &req)
                .err()
                .expect("refused")
                .to_string(),
            "posts_have_no_parent_author"
        );
    }
    let replies = Filter {
        policy: Default::default(),
        exclude_list: None,
        any: vec![Clause {
            parent_authors: vec![address(1)],
            kind: Kind::Replies,
            ..Default::default()
        }],
    };
    assert_eq!(
        query::feed(&model, &replies, None, 20).unwrap().items.len(),
        1
    );
}

/// A crash while the tail is being written leaves one of two things. Trailing
/// bytes after the last record are the unfinished write they look like, and
/// reopening drops them with nothing lost. A cut inside the last page loses
/// records the host had already acknowledged, and reopening must refuse the
/// journal rather than serve a shorter history as complete; the host then
/// marks the index unavailable until the operator rebuilds it. The journal
/// library owns both outcomes; this pins the host's contract with it.
#[test]
fn a_torn_journal_tail_is_dropped_and_a_cut_into_acknowledged_records_is_refused() {
    use commonware_runtime::{Blob as _, Storage as _, WriteOptions};
    fn batch_at(block: u64, id: u64) -> Batch {
        let mut event = post(id, 0, 0);
        if let Event::Posted { message } = &mut event {
            message.position.block = block;
        }
        Batch {
            from: block,
            through: Checkpoint {
                block,
                hash: hash(block as u8),
            },
            events: vec![event],
        }
    }
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        // Enough records to span more than one 4 KiB page of the data blob.
        let mut at = None;
        for block in 10..=25 {
            at = Some(index.append(batch_at(block, block - 9)).await.unwrap());
        }
        let at = at.unwrap();
        drop(index);
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let d = deployment();
        let partition = format!("index-{}-{}_data", d.chain, d.board);
        let names = ctx.scan(&partition).await.unwrap();
        assert_eq!(names.len(), 1, "one data blob in {partition}");
        let (blob, size) = ctx.open(&partition, &names[0]).await.unwrap();
        assert!(
            size > 4096,
            "the records span more than one page: {size} bytes"
        );
        blob.write_at(size, vec![0xff; 7], WriteOptions::default())
            .await
            .unwrap();
        blob.sync().await.unwrap();
        drop(blob);
        let reopened = Index::open(ctx.child("junk"), deployment(), hash(99))
            .await
            .unwrap();
        {
            let view = reopened.view.read().unwrap();
            assert_eq!(view.model.at, at, "an unfinished write loses nothing");
            assert_eq!(view.model.messages.len(), 16);
        }
        drop(reopened);
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let (blob, size) = ctx.open(&partition, &names[0]).await.unwrap();
        blob.resize(size - 3).await.unwrap();
        blob.sync().await.unwrap();
        drop(blob);
        let error = Index::open(ctx.child("cut"), deployment(), hash(99))
            .await
            .err()
            .expect(
                "acknowledged records lost to a cut are refused, never served as a shorter history",
            );
        assert!(
            format!("{error:#}").contains("corruption"),
            "the refusal names the corruption: {error:#}"
        );
    });
}

fn range(from: u64, through: u64, events: Vec<Event>) -> Batch {
    Batch {
        from,
        through: Checkpoint {
            block: through,
            hash: hash(through as u8),
        },
        events,
    }
}
fn post_in(block: u64, id: u64) -> Event {
    let mut event = post(id, 0, 0);
    if let Event::Posted { message } = &mut event {
        message.position.block = block;
    }
    event
}
async fn reopen<
    E: commonware_storage::Context + commonware_runtime::Spawner + commonware_runtime::Clock,
>(
    ctx: &E,
    index: Index,
    label: &'static str,
) -> Index {
    drop(index);
    // Let the owner drain and close before another handle opens the partitions.
    ctx.sleep(std::time::Duration::from_millis(1)).await;
    Index::open(ctx.child(label), deployment(), hash(99))
        .await
        .unwrap()
}

/// Empty progress is verified and served from RAM; only events and the
/// periodic checkpoint reach the journal. The batch that is journaled covers
/// the skipped blocks too, so replay stays contiguous.
#[test]
fn empty_ranges_stay_in_ram_and_the_next_durable_batch_covers_them() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        for block in 10..=19 {
            index.append(range(block, block, vec![])).await.unwrap();
        }
        assert_eq!(index.view.read().unwrap().model.next_block().unwrap(), 20);
        let posted = index
            .append(range(20, 20, vec![post_in(20, 1)]))
            .await
            .unwrap();
        for block in 21..=29 {
            index.append(range(block, block, vec![])).await.unwrap();
        }
        // Reference work between skipped ranges does not depend on them.
        index
            .update(Update::Analyze(vec![super::references::Analysis {
                hash: hash(1),
                parser: super::references::PARSER_VERSION,
                references: vec![],
            }]))
            .await
            .unwrap();
        index.update(Update::Bind(vec![1])).await.unwrap();
        let served = index.view.read().unwrap().model.at.clone();
        assert_eq!(served.checkpoint.unwrap().block, 29);
        let restored = reopen(&ctx, index, "restart").await;
        {
            let view = restored.view.read().unwrap();
            assert_eq!(
                view.model.at.checkpoint, posted.checkpoint,
                "a crash loses only unjournaled empty progress"
            );
            assert_eq!(view.model.at.sequence, served.sequence);
            assert_eq!(view.model.messages.len(), 1);
            assert!(view.model.references.analyses.contains_key(&hash(1)));
            assert_eq!(view.model.next_block().unwrap(), 21);
        }
        // The follower reads the lost blocks again from the durable checkpoint.
        restored.establish().unwrap();
        restored.append(range(21, 40, vec![])).await.unwrap();
        restored
            .append(range(41, 41, vec![post_in(41, 2)]))
            .await
            .unwrap();
        let again = reopen(&ctx, restored, "again").await;
        let view = again.view.read().unwrap();
        assert_eq!(view.model.at.checkpoint.unwrap().block, 41);
        assert_eq!(view.model.messages.len(), 2);
    });
}

#[test]
fn idle_progress_is_journaled_once_per_checkpoint_interval() {
    use crate::journal::CHECKPOINT_BLOCKS;
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        let last_skipped = 10 + CHECKPOINT_BLOCKS - 2;
        index.append(range(10, last_skipped, vec![])).await.unwrap();
        let index = reopen(&ctx, index, "short").await;
        assert_eq!(
            index.view.read().unwrap().model.at.checkpoint,
            None,
            "less than one interval of empty blocks is not journaled"
        );
        index.establish().unwrap();
        index.append(range(10, last_skipped, vec![])).await.unwrap();
        index
            .append(range(last_skipped + 1, last_skipped + 1, vec![]))
            .await
            .unwrap();
        let index = reopen(&ctx, index, "full").await;
        assert_eq!(
            index
                .view
                .read()
                .unwrap()
                .model
                .at
                .checkpoint
                .unwrap()
                .block,
            last_skipped + 1,
            "a full interval of empty blocks is journaled as one record"
        );
    });
}

#[test]
fn flush_journals_ram_progress_and_is_a_no_op_when_nothing_is_pending() {
    deterministic::Runner::default().start(|ctx| async move {
        let index = opened(ctx.child("first"), deployment(), hash(2)).await;
        index.flush().await.unwrap();
        index.append(range(10, 50, vec![])).await.unwrap();
        index.flush().await.unwrap();
        index.flush().await.unwrap();
        let index = reopen(&ctx, index, "restart").await;
        assert_eq!(
            index
                .view
                .read()
                .unwrap()
                .model
                .at
                .checkpoint
                .unwrap()
                .block,
            50
        );
    });
}

#[test]
fn a_members_clause_selects_listed_authors_in_pages_and_changes() {
    use super::{
        changes,
        query::{self, Clause, Filter, Kind},
    };
    fn apply(model: &mut Model, mut b: Batch) {
        for event in &mut b.events {
            if let Event::Posted { message } = event {
                message.position.block = b.through.block;
            }
        }
        model.validate(&b).unwrap();
        model.apply(b);
    }
    let ids = |page: &query::Page| page.items.iter().map(|r| r.message.id).collect::<Vec<_>>();
    let registry = address(90);
    let list = ListKey {
        registry,
        owner: address(1),
        list: hash(91),
    };
    let mut d = deployment();
    d.registries.push(registry);
    let mut model = Model::new(d, hash(2));
    let mut events: Vec<Event> = (1..=6).map(|id| post(id, 0, id as u32)).collect();
    // A reply to the reader's post 1, by an author who is not listed.
    events.push(post(7, 1, 7));
    for (member, log) in [(2, 20), (4, 21)] {
        events.push(Event::Membership {
            list,
            member: address(member),
            present: true,
            position: Position { block: 10, log },
        });
    }
    apply(&mut model, batch(events));
    let filter = Filter {
        policy: Default::default(),
        exclude_list: None,
        any: vec![
            Clause {
                kind: Kind::Posts,
                members_of: Some(list),
                ..Default::default()
            },
            Clause {
                parent_authors: vec![address(1)],
                ..Default::default()
            },
        ],
    };
    let page = query::feed(&model, &filter, None, 20).unwrap();
    assert_eq!(
        ids(&page),
        vec![7, 4, 2],
        "listed authors' posts, and the other clause's reply"
    );
    let mut unindexed = filter.clone();
    unindexed.any[0].members_of = Some(ListKey {
        registry: address(99),
        ..list
    });
    assert!(query::feed(&model, &unindexed, None, 20).is_err());

    // New posts are matched with the same clause.
    let after = model.at.sequence;
    apply(
        &mut model,
        Batch {
            from: 11,
            through: Checkpoint {
                block: 11,
                hash: hash(11),
            },
            events: vec![authored(8, 2, 0), post(9, 0, 1)],
        },
    );
    let mut req: changes::Request = serde_json::from_value(serde_json::json!({
        "generation": model.at.generation, "after": after.to_string(),
        "scope": {"feed": {"any": [{"kind": "posts", "members_of": list}, {"parent_authors": [address(1)]}]}}
    }))
    .unwrap();
    let answer = changes::read(&model, &req).unwrap();
    assert_eq!(
        answer
            .records
            .iter()
            .map(|r| r.message.id)
            .collect::<Vec<_>>(),
        vec![8]
    );

    // A change to the list decides which older posts belong, which only a fresh
    // read can supply: the view is told to start over, as for an excluded list.
    req.after = model.at.sequence;
    apply(
        &mut model,
        Batch {
            from: 12,
            through: Checkpoint {
                block: 12,
                hash: hash(12),
            },
            events: vec![Event::Membership {
                list,
                member: address(3),
                present: true,
                position: Position { block: 12, log: 0 },
            }],
        },
    );
    let refused = changes::read(&model, &req)
        .err()
        .expect("a list change requires a fresh read");
    assert!(refused.to_string().contains("reset_required"), "{refused}");
    // Another list's changes leave the view alone.
    req.scope.feed.as_mut().unwrap().any[0].members_of = Some(ListKey {
        list: hash(92),
        ..list
    });
    assert!(changes::read(&model, &req).is_ok());
}

#[test]
fn parent_membership_filters_replies_and_invalidates_changed_lists() {
    use super::{changes, query};
    use serde_json::json;
    let list = ListKey {
        registry: address(90),
        owner: address(1),
        list: hash(91),
    };
    let mut d = deployment();
    d.registries.push(list.registry);
    let mut model = Model::new(d, hash(2));
    let reply = |id, parent| {
        let mut event = post(id, parent, id as u32);
        if let Event::Posted { message } = &mut event {
            message.author = address(8);
        }
        event
    };
    let b = batch(vec![
        post(1, 0, 1),
        post(2, 0, 2),
        authored(3, 8, 3),
        reply(4, 1),
        reply(5, 2),
        reply(6, 1),
        Event::Membership {
            list,
            member: address(1),
            present: true,
            position: Position { block: 10, log: 7 },
        },
    ]);
    model.validate(&b).unwrap();
    model.apply(b);
    let value = json!({ "any": [{"authors": [address(8)], "parent_members_of": list}] });
    let filter: query::Filter = serde_json::from_value(value.clone()).unwrap();
    let page = query::feed(&model, &filter, None, 1).unwrap();
    assert_eq!(page.items[0].message.id, 6);
    let next = query::feed(&model, &filter, page.next.as_ref(), 20).unwrap();
    assert_eq!(
        next.items.iter().map(|r| r.message.id).collect::<Vec<_>>(),
        vec![4]
    );
    let mut other = value.clone();
    other["any"][0]["parent_members_of"]["registry"] = json!(address(99));
    let other: query::Filter = serde_json::from_value(other).unwrap();
    assert_eq!(
        query::feed(&model, &other, None, 20)
            .err()
            .unwrap()
            .to_string(),
        "registry_not_indexed"
    );
    let mut invalid = value.clone();
    invalid["any"][0]["kind"] = json!("posts");
    assert_eq!(
        serde_json::from_value::<query::Filter>(invalid)
            .unwrap()
            .key()
            .unwrap_err()
            .to_string(),
        "posts_have_no_parent_author"
    );
    let mut req: changes::Request = serde_json::from_value(json!({
        "generation": model.at.generation, "after": model.at.sequence.to_string(), "scope": { "feed": value }
    })).unwrap();
    let mut new_reply = reply(7, 1);
    if let Event::Posted { message } = &mut new_reply {
        message.position.block = 11;
    }
    let b = Batch {
        from: 11,
        through: Checkpoint {
            block: 11,
            hash: hash(11),
        },
        events: vec![new_reply],
    };
    model.validate(&b).unwrap();
    model.apply(b);
    let answer = changes::read(&model, &req).unwrap();
    assert!(answer.records.iter().any(|r| r.message.id == 7));
    req.after = model.at.sequence;
    let b = Batch {
        from: 12,
        through: Checkpoint {
            block: 12,
            hash: hash(12),
        },
        events: vec![Event::Membership {
            list,
            member: address(1),
            present: false,
            position: Position { block: 12, log: 0 },
        }],
    };
    model.validate(&b).unwrap();
    model.apply(b);
    assert_eq!(
        changes::read(&model, &req).err().unwrap().to_string(),
        "reset_required"
    );
    assert!(
        query::feed(&model, &filter, None, 20)
            .unwrap()
            .items
            .is_empty()
    );
}
