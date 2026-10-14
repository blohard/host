use super::{Index, model::*, references::*, types::*};
use crate::chain::{Address, Checkpoint, Hash, Hex, Position};
use commonware_runtime::{Clock as _, Runner as _, Supervisor as _, deterministic};

fn addr(n: u8) -> Address {
    Hex([n; 20])
}
fn hash(n: u8) -> Hash {
    Hex([n; 32])
}
fn deployment(chain: u64) -> Deployment {
    Deployment {
        chain,
        board: addr(42),
        start_block: 1,
        index_start_block: 1,
        registries: Vec::new(),
    }
}
fn body(text: &str) -> (Hash, Vec<u8>) {
    let bytes = format!("version/1 text/plain\n{text}").into_bytes();
    (Hex(crate::keccak::hash(&[&bytes]).0), bytes)
}
fn parse(text: &str) -> Analysis {
    let (hash, bytes) = body(text);
    analyse(hash, &bytes).unwrap()
}
fn message(id: u64, content_hash: Hash) -> Message {
    Message {
        id,
        author: addr(7),
        parent: 0,
        parent_author: Address::ZERO,
        timestamp: 100,
        content_hash,
        transaction: hash(3),
        position: Position {
            block: 1,
            log: id as u32,
        },
        gate: Address::ZERO,
    }
}
fn posted(m: Message) -> Update {
    Update::Chain(Batch {
        from: 1,
        through: Checkpoint {
            block: 1,
            hash: hash(4),
        },
        events: vec![Event::Posted { message: m }],
    })
}
fn apply(model: &mut Model, update: Update) {
    model.validate_update(&update).unwrap();
    model.apply_update(update);
}

#[test]
fn pinned_parser_never_downgrades_a_malformed_reference() {
    let valid = format!("post://42/7?commit={}", hash(8));
    assert_eq!(
        parse(&valid).references,
        vec![Reference::Quote {
            target: Quote {
                chain: 42,
                id: 7,
                commit: hash(8)
            }
        }]
    );
    for bad in [
        "post://42/7".to_string(),
        format!("{valid}f"),
        format!("{valid}&commit={}", hash(8)),
        format!("{valid}#fragment"),
        format!("{valid}&extra=1"),
        valid.replace("42/", "042/"),
        valid.replace("?commit=", "?other="),
        format!("post://42/7?commit=0x{}", "AB".repeat(32)),
        "post://42/7?commit=0xxyz".into(),
        "post://42/7?commit=0x01".into(),
    ] {
        assert!(parse(&bad).references.is_empty(), "accepted {bad}");
    }
    assert_eq!(parse(&format!("[quoted]({valid})")).references.len(), 1);
    assert_eq!(
        parse(&format!("[@name](account://{})", addr(9))).references,
        vec![Reference::Mention { account: addr(9) }]
    );
    assert!(
        parse(&format!("account://{}ffff", addr(9)))
            .references
            .is_empty()
    );
}

#[test]
fn occurrence_budget_is_applied_before_deduplication() {
    let same = format!("account://{} ", addr(8));
    let text = format!("{}account://{}", same.repeat(8), addr(9));
    assert_eq!(
        parse(&text).references,
        vec![Reference::Mention { account: addr(8) }]
    );
    let text = format!("{}account://{}", "post://42/7 ".repeat(8), addr(9));
    assert!(
        parse(&text).references.is_empty(),
        "plain locators still use the occurrence budget"
    );
    let (_, bytes) = body("account://0x123");
    assert!(
        analyse(hash(99), &bytes).is_err(),
        "bytes must match the anchored hash"
    );
}

#[test]
fn bodies_before_posts_and_duplicate_derived_updates_are_idempotent() {
    let analysis = parse(&format!("hello account://{}", addr(9)));
    let mut model = Model::new(deployment(42), hash(2));
    apply(&mut model, Update::Analyze(vec![analysis.clone()]));
    assert_eq!(model.at.sequence, 0);
    assert!(model.at.checkpoint.is_none());
    apply(&mut model, posted(message(1, analysis.hash)));
    assert!(model.references.pending_bodies.is_empty());
    assert!(model.references.ready_bodies.contains(&analysis.hash));
    apply(&mut model, Update::Bind(vec![1]));
    assert_eq!(model.at.sequence, 2);
    assert_eq!(
        model.references.accounts[&addr(9)]
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert!(model.references.unbound.is_empty());
    let at = model.at.clone();
    apply(&mut model, Update::Analyze(vec![analysis]));
    apply(&mut model, Update::Bind(vec![1, 1]));
    assert_eq!(
        model.at, at,
        "reconciliation must not create duplicate changes"
    );
}

#[test]
fn repeated_bind_leaves_new_posts_with_the_same_body_pending() {
    // Even an analysis with no references must be bound exactly once.
    for text in [
        "no references".to_string(),
        format!("account://{}", addr(9)),
    ] {
        let analysis = parse(&text);
        let mut model = Model::new(deployment(42), hash(2));
        apply(&mut model, Update::Analyze(vec![analysis.clone()]));
        apply(&mut model, posted(message(1, analysis.hash)));
        apply(&mut model, Update::Bind(vec![1]));
        let mut second = message(2, analysis.hash);
        second.position.block = 2;
        apply(
            &mut model,
            Update::Chain(Batch {
                from: 2,
                through: Checkpoint {
                    block: 2,
                    hash: hash(5),
                },
                events: vec![Event::Posted { message: second }],
            }),
        );
        let at = model.at.clone();
        apply(&mut model, Update::Bind(vec![1, 1]));
        assert_eq!(model.at, at);
        assert_eq!(
            model.references.unbound[&analysis.hash]
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(model.references.ready_bodies.contains(&analysis.hash));
        apply(&mut model, Update::Bind(vec![1, 2, 2]));
        // Only a body that references someone is news to a reader.
        let news = u64::from(!analysis.references.is_empty());
        assert_eq!(model.at.sequence, at.sequence + news);
        assert!(model.references.unbound.is_empty());
        assert!(model.references.ready_bodies.is_empty());
        let at = model.at.clone();
        apply(&mut model, Update::Bind(vec![2, 1]));
        assert_eq!(model.at, at);
    }
}

#[test]
fn matching_quotes_associate_and_mismatches_stay_rejected_at_accepted_depth() {
    let mut target = Model::new(deployment(43), hash(3));
    apply(&mut target, posted(message(1, hash(10))));
    let target_message = target.messages[&1].clone();
    let quote = Quote {
        chain: 43,
        id: 1,
        commit: target_message.commitment(),
    };
    let wrong = Quote {
        commit: hash(99),
        ..quote
    };
    let analysis = parse(&format!(
        "post://43/1?commit={}\npost://43/1?commit={}\naccount://{}",
        quote.commit,
        wrong.commit,
        addr(8)
    ));
    let mut source = Model::new(deployment(42), hash(2));
    apply(&mut source, posted(message(1, analysis.hash)));
    apply(&mut source, Update::Analyze(vec![analysis]));
    apply(&mut source, Update::Bind(vec![1]));
    assert_eq!(source.references.pending_quotes.len(), 2);
    assert!(
        source.references.accounts.contains_key(&addr(8)),
        "explicit mention is independent"
    );
    let resolutions: Vec<_> = [quote, wrong]
        .into_iter()
        .map(|quote| Resolution {
            key: QuoteKey { source: 1, quote },
            deployment: target.deployment.clone(),
            at: target.at.clone(),
            message: target_message.clone(),
        })
        .collect();
    let checkpoint = source.at.checkpoint;
    apply(&mut source, Update::Resolve(resolutions.clone()));
    assert!(source.references.pending_quotes.is_empty());
    assert_eq!(
        source.references.pending_quotes_on(&[43]),
        0,
        "the per-chain counts follow resolutions"
    );
    assert!(
        source.references.outcomes[&QuoteKey { source: 1, quote }]
            .author
            .is_some()
    );
    assert!(
        source.references.outcomes[&QuoteKey {
            source: 1,
            quote: wrong
        }]
            .author
            .is_none()
    );
    assert!(
        source.references.accounts[&target_message.author].contains(&1),
        "the quoted post's author is notified"
    );
    let found = super::query::reference_record(&source, 1, target_message.author).unwrap();
    assert_eq!(found.reference, Reference::Quote { target: quote });
    let mentioned = super::query::reference_record(&source, 1, addr(8)).unwrap();
    assert_eq!(mentioned.reference, Reference::Mention { account: addr(8) });
    assert!(super::query::reference_record(&source, 1, addr(99)).is_none());

    assert_eq!(
        source.at.checkpoint, checkpoint,
        "body-derived changes do not advance chain position"
    );
    let at = source.at.clone();
    apply(&mut source, Update::Resolve(resolutions));
    assert_eq!(source.at, at);
}

#[test]
fn source_journal_replays_cross_chain_associations_without_target_journal() {
    deterministic::Runner::default().start(|ctx| async move {
        let target = message(1, hash(10));
        let analysis = parse(&format!("post://43/1?commit={}", target.commitment()));
        let source = Index::open(ctx.child("source"), deployment(42), hash(2))
            .await
            .unwrap();
        // As the follower does on its first successful poll; writes wait for it.
        source.establish().unwrap();
        source
            .update(posted(message(1, analysis.hash)))
            .await
            .unwrap();
        source
            .update(Update::Analyze(vec![analysis]))
            .await
            .unwrap();
        source.update(Update::Bind(vec![1])).await.unwrap();
        source
            .update(Update::Resolve(vec![Resolution {
                key: QuoteKey {
                    source: 1,
                    quote: Quote {
                        chain: 43,
                        id: 1,
                        commit: target.commitment(),
                    },
                },
                deployment: deployment(43),
                at: At {
                    generation: hash(3),
                    sequence: 1,
                    checkpoint: Some(Checkpoint {
                        block: 1,
                        hash: hash(4),
                    }),
                },
                message: target,
            }]))
            .await
            .unwrap();
        let at = source.view.read().unwrap().model.at.clone();
        drop(source);
        ctx.sleep(std::time::Duration::from_millis(1)).await;
        let restored = Index::open(ctx.child("restart"), deployment(42), hash(99))
            .await
            .unwrap();
        let view = restored.view.read().unwrap();
        assert_eq!(view.model.at, at);
        assert_eq!(view.model.references.accounts[&addr(7)].len(), 1);
        assert!(view.model.references.pending_bodies.is_empty());
        assert!(view.model.references.pending_quotes.is_empty());
    });
}

#[test]
fn deleted_and_removed_messages_never_appear_as_references() {
    use super::query;
    use super::types::{REMOVED, TOMBSTONE};
    let mut model = Model::new(deployment(1), hash(2));
    let texts: Vec<_> = (1..=3)
        .map(|n| parse(&format!("note {n} for account://{}", addr(9))))
        .collect();
    apply(&mut model, Update::Analyze(texts.clone()));
    let reply = |id: u64, content_hash: Hash| Message {
        parent: 1,
        parent_author: addr(7),
        ..message(id, content_hash)
    };
    apply(
        &mut model,
        Update::Chain(Batch {
            from: 1,
            through: Checkpoint {
                block: 1,
                hash: hash(4),
            },
            events: vec![
                Event::Posted {
                    message: message(1, texts[0].hash),
                },
                Event::Posted {
                    message: reply(2, texts[1].hash),
                },
                Event::Posted {
                    message: reply(3, texts[2].hash),
                },
                Event::ReplyGate {
                    id: 2,
                    old: Address::ZERO,
                    new: REMOVED,
                    position: Position { block: 1, log: 4 },
                },
                Event::ReplyGate {
                    id: 3,
                    old: Address::ZERO,
                    new: TOMBSTONE,
                    position: Position { block: 1, log: 5 },
                },
            ],
        }),
    );
    apply(&mut model, Update::Bind(vec![1, 2, 3]));
    assert_eq!(
        model.references.accounts[&addr(9)].len(),
        3,
        "associations stay indexed; only the page filters"
    );
    let page = query::references(&model, addr(9), None, None, 20).unwrap();
    assert_eq!(
        page.items
            .iter()
            .map(|r| r.record.message.id)
            .collect::<Vec<_>>(),
        vec![1],
        "neither a removed nor a deleted mention notifies"
    );
}

#[test]
fn quotes_of_unserved_chains_are_not_counted_as_pending() {
    let analysis = parse(&format!(
        "see post://99/5?commit={} and post://1/1?commit={} by account://{}",
        hash(5),
        hash(6),
        addr(9)
    ));
    let mut model = Model::new(deployment(1), hash(2));
    apply(&mut model, Update::Analyze(vec![analysis.clone()]));
    apply(&mut model, posted(message(1, analysis.hash)));
    apply(&mut model, Update::Bind(vec![1]));
    assert_eq!(model.references.pending_quotes.len(), 2);
    assert_eq!(model.references.pending_quotes_on(&[1]), 1);
    assert_eq!(model.references.pending_quotes_on(&[99]), 1);
    assert_eq!(model.references.pending_quotes_on(&[1, 99]), 2);
    assert_eq!(model.references.pending_quotes_on(&[7]), 0);
    // Repeating the bind must not count the same quote twice.
    apply(&mut model, Update::Bind(vec![1]));
    assert_eq!(model.references.pending_quotes_on(&[1, 99]), 2);
}

#[test]
fn notifications_merge_replies_and_references_with_one_cursor() {
    use super::query;
    let account = addr(9);
    let analysis = parse(&format!("hello account://{account}"));
    let mut model = Model::new(deployment(42), hash(2));
    let mut root = message(1, hash(10));
    root.author = account;
    let events = std::iter::once(Event::Posted { message: root })
        .chain((2..=6).map(|id| {
            let mut m = message(
                id,
                if id == 3 || id == 4 {
                    analysis.hash
                } else {
                    hash(10)
                },
            );
            if id == 2 || id == 4 || id == 6 {
                m.parent = 1;
                m.parent_author = account;
            }
            Event::Posted { message: m }
        }))
        .collect();
    apply(
        &mut model,
        Update::Chain(Batch {
            from: 1,
            through: Checkpoint {
                block: 1,
                hash: hash(4),
            },
            events,
        }),
    );
    apply(&mut model, Update::Analyze(vec![analysis]));
    apply(&mut model, Update::Bind(vec![3, 4]));
    let first = query::notifications(&model, account, None, None, 2).unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|r| r.record.message.id)
            .collect::<Vec<_>>(),
        [6, 4]
    );
    assert!(first.items[0].reference.is_none());
    assert!(
        first.items[1].reference.is_some(),
        "a reply that mentions is one row with its reason"
    );
    let cursor = first.next.unwrap();
    let second = query::notifications(&model, account, None, Some(&cursor), 2).unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|r| r.record.message.id)
            .collect::<Vec<_>>(),
        [3, 2]
    );
    assert!(second.next.is_none());
    assert!(query::notifications(&model, addr(8), None, Some(&cursor), 2).is_err());
    assert!(query::references(&model, account, None, Some(&cursor), 2).is_err());
    assert!(query::notifications(&model, account, None, None, 51).is_err());
    // Ending a source removes the whole notification even when it matched both lists.
    model.messages.get_mut(&4).unwrap().gate = TOMBSTONE;
    model.messages.get_mut(&6).unwrap().gate = REMOVED;
    assert_eq!(
        query::notifications(&model, account, None, None, 50)
            .unwrap()
            .items
            .iter()
            .map(|r| r.record.message.id)
            .collect::<Vec<_>>(),
        [3, 2]
    );
}

#[test]
fn notifications_skip_exclusions_and_resume_after_a_bounded_empty_page() {
    use super::query;
    let account = addr(9);
    let mut deployment = deployment(42);
    deployment.registries.push(addr(8));
    let mut model = Model::new(deployment, hash(2));
    let list = ListKey {
        registry: addr(8),
        owner: account,
        list: hash(5),
    };
    let mut root = message(1, hash(10));
    root.author = account;
    let events = std::iter::once(Event::Posted { message: root })
        .chain((2..=503).map(|id| {
            let mut m = message(id, hash(10));
            m.parent = 1;
            m.parent_author = account;
            if id == 2 {
                m.author = addr(6);
            }
            Event::Posted { message: m }
        }))
        .collect();
    apply(
        &mut model,
        Update::Chain(Batch {
            from: 1,
            through: Checkpoint {
                block: 1,
                hash: hash(4),
            },
            events,
        }),
    );
    model.lists.entry(list).or_default().insert(addr(7));
    let first = query::notifications(&model, account, Some(list), None, 20).unwrap();
    assert!(first.items.is_empty());
    let cursor = first.next.unwrap();
    assert_eq!(
        cursor.before, 4,
        "inspect at most 500 candidates even when all are excluded"
    );
    let next = query::notifications(&model, account, Some(list), Some(&cursor), 20).unwrap();
    assert_eq!(
        next.items
            .iter()
            .map(|r| r.record.message.id)
            .collect::<Vec<_>>(),
        [2]
    );
    assert!(next.next.is_none());
    assert!(query::notifications(&model, account, None, Some(&cursor), 20).is_err());
    assert!(
        query::notifications(&model, Address::ZERO, None, None, 20)
            .unwrap()
            .items
            .is_empty()
    );
}
