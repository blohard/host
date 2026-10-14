//! The sweep makes a host with peers a copy: the attachments a body
//! references are fetched as the body is analysed, and the walk after a
//! restart finds any still missing.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn fixture() -> (Vec<u8>, String, Vec<u8>, String) {
    let att = record("image/png", b"\x89PNG a picture only the peer has");
    let att_hash = hx(&att);
    let body = record(
        "text/plain",
        format!("look ![a](record:{att_hash})\n").as_bytes(),
    );
    let body_hash = hx(&body);
    (att, att_hash, body, body_hash)
}

#[test]
fn the_sweep_copies_the_attachments_a_body_references_from_a_peer() {
    let (att, att_hash, body, _) = fixture();
    let peer = FakePeer::start([(att_hash.clone(), att.clone())]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "attachment-sweep-copies",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            // The walk runs past the message before its body arrives, so the
            // analysis of the body is what finds the picture.
            let mut sweep = crate::index::bodies::Sweep::default();
            sweep.run(&state, 31337, index).await.unwrap();
            let stored = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(stored.status(), 200);
            let picture = format!("{base}/api/content/{att_hash}");
            assert_eq!(
                client.get(&picture).send().await.unwrap().status(),
                404,
                "not here before the sweep"
            );
            sweep.run(&state, 31337, index).await.unwrap();
            let response = client.get(&picture).send().await.unwrap();
            assert_eq!(
                response.status(),
                200,
                "served locally, with no reader's hint"
            );
            assert_eq!(response.bytes().await.unwrap().as_ref(), att.as_slice());
            assert_eq!(sweep.pending_attachments(), 0);
            sweep.run(&state, 31337, index).await.unwrap();
            assert_eq!(peer.hits(&att_hash), 1, "fetched once");
        },
    );
}

#[test]
fn after_a_restart_the_walk_finds_attachments_still_missing() {
    let (att, att_hash, body, _) = fixture();
    // The peer lacks the picture until the test hands it over.
    let peer = FakePeer::start([]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "attachment-sweep-walk",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(stored.status(), 200);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            // The peer lacks the picture while the body is analysed: it stays pending.
            let mut before = crate::index::bodies::Sweep::default();
            before.run(&state, 31337, index).await.unwrap();
            assert_eq!(peer.hits(&att_hash), 1);
            assert_eq!(before.pending_attachments(), 1);
            assert_eq!(
                index.read_ready(|m| m.references.pending_bodies.len()),
                Some(0),
                "the body is analysed"
            );
            // A restart forgets what was pending; the walk finds it again from the stored body.
            drop(before);
            peer.serve(&att_hash, att);
            let mut after = crate::index::bodies::Sweep::default();
            after.run(&state, 31337, index).await.unwrap();
            assert_eq!(
                after.pending_attachments(),
                1,
                "found by the walk, waiting out the peer's backoff"
            );
        },
    );
}

/// A picture this host's policy refuses (here, its media type) is downloaded
/// once, then neither the sweep nor a reader downloads it again at once.
#[test]
fn a_refused_attachment_is_downloaded_once() {
    let att = record("application/zip", &vec![b'z'; 64 << 10]);
    let att_hash = hx(&att);
    let body = record("text/plain", format!("see record:{att_hash}\n").as_bytes());
    let peer = FakePeer::start([(att_hash.clone(), att)]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "attachment-sweep-refused",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            attachments: Some(AttachmentsConfig {
                media_types: Some(vec!["image/png".into()]),
                ..policy(4)
            }),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(stored.status(), 200);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..5 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            assert_eq!(sweep.pending_attachments(), 0, "given up, not kept");
            let reader = client
                .get(format!(
                    "{base}/api/content/{att_hash}?chain_id=31337&message_id=1"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(reader.status(), 404);
            assert_eq!(peer.hits(&att_hash), 1, "downloaded once in all");
        },
    );
}

/// A peer that accepts connections and never answers.
fn stalling_peer() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for s in listener.incoming() {
            held.push(s);
        }
    });
    url
}

/// While the sweep waits on a peer that never answers, readers still get a
/// peer fetch slot: the sweep takes at most two of the eight.
#[test]
fn readers_keep_peer_fetch_slots_while_the_sweep_waits_on_a_stalled_peer() {
    let junk = |n: u8| format!("record:0x{}", hex::encode([n; 32]));
    let body1 = record(
        "text/plain",
        format!("{} {} {} {}", junk(1), junk(2), junk(3), junk(4)).as_bytes(),
    );
    let body2 = record(
        "text/plain",
        format!("{} {} {} {}", junk(5), junk(6), junk(7), junk(8)).as_bytes(),
    );
    let wanted = "0x".to_string() + &hex::encode([0xf0; 32]);
    let body3 = record("text/plain", format!("record:{wanted}").as_bytes());
    let mut node = Node {
        head: 100.into(),
        messages: 3.into(),
        ..Node::new(31337, CONTRACT, body1.clone())
    };
    node.extra = vec![(2, body2.clone()), (3, body3.clone())];
    let peer_url = stalling_peer();
    with_server(
        "attachment-sweep-slots",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            for (id, b) in [(1, &body1), (2, &body2), (3, &body3)] {
                let r = publish(
                    &client,
                    &base,
                    &format!("chain_id=31337&message_id={id}"),
                    b.clone(),
                )
                .await;
                assert_eq!(r.status(), 200);
            }
            let sweeping = {
                let state = state.clone();
                tokio::spawn(async move {
                    let index = &state.gateway.as_ref().unwrap().indexes[&31337];
                    let mut sweep = crate::index::bodies::Sweep::default();
                    sweep.run(&state, 31337, index).await.unwrap();
                })
            };
            tokio::time::sleep(Duration::from_millis(1500)).await;
            let reader = client
                .get(format!(
                    "{base}/api/content/{wanted}?chain_id=31337&message_id=3"
                ))
                .send()
                .await
                .unwrap();
            assert_ne!(
                reader.status(),
                503,
                "a reader's peer fetch was refused as busy"
            );
            sweeping.await.unwrap();
        },
    );
}

/// A body the index analysed but the store has lost is copied again once a
/// peer has it, however many passes it waits.
#[test]
fn a_lost_body_is_copied_once_a_peer_has_it() {
    let (_, _, body, body_hash) = fixture();
    // No peer has the body until the test hands it over.
    let peer = FakePeer::start([]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "attachment-sweep-lost-body",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            // A replayed analysis over an empty store: the body was lost after it.
            let analysis = crate::index::analyse(body_hash.parse().unwrap(), &body).unwrap();
            index
                .update(crate::index::model::Update::Analyze(vec![analysis]))
                .await
                .unwrap();
            index
                .update(crate::index::model::Update::Bind(vec![1]))
                .await
                .unwrap();
            assert_eq!(
                index.read_ready(|m| m.references.pending_bodies.len()),
                Some(0)
            );
            let mut sweep = crate::index::bodies::Sweep::default();
            sweep.run(&state, 31337, index).await.unwrap();
            assert_eq!(peer.hits(&body_hash), 1, "asked once while no peer had it");
            assert_eq!(sweep.pending_repairs(), 1, "kept for another turn");
            peer.serve(&body_hash, body.clone());
            let peers = &state.store.as_ref().unwrap().peers;
            let digest = keccak::hash(&[&body]);
            assert!(peers.waiting(&digest, crate::store::Kind::Attachments));
            sweep.run(&state, 31337, index).await.unwrap();
            assert_eq!(peer.hits(&body_hash), 1, "backoff skips the peer");
            peers.retry_now(digest);
            sweep.run(&state, 31337, index).await.unwrap();
            let response = client
                .get(format!("{base}/api/content/{body_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200, "the lost body is back");
            assert_eq!(sweep.pending_repairs(), 0);
        },
    );
}

/// A temporary admission failure (holding full) stays an error to retry,
/// not a refusal that waits thirty minutes.
#[test]
fn a_temporary_admission_failure_is_not_a_refusal() {
    let body = record("text/plain", b"valid body waiting for confirmation");
    let body_hash = hx(&body);
    let peer = FakePeer::start([(body_hash.clone(), body.clone())]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 11.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "attachment-sweep-temporary",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 6,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            gateway: false,
            peers: vec![peer_url],
            holding: HoldingConfig {
                max_bytes: 0,
                ..HoldingConfig::default()
            },
            ..Setup::default()
        },
        move |Server { state, .. }| async move {
            let digest = crate::keccak::Digest::from_hex(&body_hash).unwrap();
            let result =
                crate::peers::pull(&state, state.store.as_ref().unwrap(), 31337, 1, digest).await;
            assert!(
                matches!(result, Err(ref e) if e.code == "holding_full"),
                "a full holding area is retried soon: {:?}",
                result.as_ref().map(|_| ()).map_err(|e| e.code)
            );
            // Backing off briefly, not for the half hour a refusal waits.
            assert!(
                state
                    .store
                    .as_ref()
                    .unwrap()
                    .peers
                    .waiting(&digest, crate::store::Kind::Attachments)
            );
        },
    );
}

/// A host without peers notes no attachments: nothing could ever fetch them.
#[test]
fn a_host_without_peers_notes_no_attachments() {
    let (_, _, body, _) = fixture();
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "attachment-sweep-no-peers",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=1", body).await;
            assert_eq!(stored.status(), 200);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            sweep.run(&state, 31337, index).await.unwrap();
            assert_eq!(sweep.pending_attachments(), 0);
        },
    );
}

/// Eight chains with bodies to analyse, every one missing and the only peer
/// stalled: the sweeps together hold two peer connections, not all eight.
#[test]
fn bodies_awaiting_analysis_share_the_background_allowance() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_url = format!("http://{}", listener.local_addr().unwrap());
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            held.push(stream);
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    let dir =
        std::env::temp_dir().join(format!("blohard-analysis-allowance-{}", std::process::id()));
    let cfg = cw_tokio::Config::default().with_storage_directory(&dir);
    cw_tokio::Runner::new(cfg).start(|context| async move {
        let mut nodes = Vec::new();
        let mut chains = Vec::new();
        for id in 31337..31345 {
            let body = record("text/plain", format!("chain {id}").as_bytes());
            let node = mock_node::spawn(Node {
                head: 100.into(), messages: 1.into(),
                ..Node::new(id, CONTRACT, body)
            }).await;
            chains.push(ChainConfig::for_test(id, &node.url, CONTRACT));
            nodes.push(node);
        }
        let client = reqwest::Client::new();
        let verifier = Verifier::from_config(&Config::for_test(chains), client.clone()).unwrap();
        let state = Arc::new(open_state(context, verifier, Options {
            store: Some(StoreConfig {
                page_cache_mib: 0, max_body_bytes: 0, attachments: Some(policy(4)),
                peers: vec![peer_url],
            }),
            gateway: Some(GatewayConfig { names: None, link_previews: None }),
            holding: HoldingConfig::default(), notice: None, policy: Default::default(), keyrings: Default::default(),
        }, client).await.unwrap());
        for (id, _) in state.verifier.chains() {
            state.verifier.check_chain(id, crate::rpc::Class::Ingestion).await.unwrap();
            state.gateway.as_ref().unwrap().indexes[&id].establish().unwrap();
        }
        crate::index::follow::once(&state).await;
        let mut tasks = Vec::new();
        for id in 31337..31345 {
            let state = state.clone();
            tasks.push(tokio::spawn(async move {
                let index = &state.gateway.as_ref().unwrap().indexes[&id];
                let mut sweep = crate::index::bodies::Sweep::default();
                sweep.run(&state, id, index).await.unwrap();
            }));
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while connections.load(Ordering::SeqCst) < 8 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await;
        let active = connections.load(Ordering::SeqCst);
        let reader = tokio::time::timeout(Duration::from_millis(100),
            state.store.as_ref().unwrap().peers.fetch(&crate::keccak::Digest([99; 32]), crate::store::Kind::Attachments)).await;
        for task in tasks { task.abort(); }
        assert!(active <= 2, "analysis bypassed the background allowance: {active} peer connections; reader: {reader:?}");
    });
    let _ = std::fs::remove_dir_all(dir);
}

/// A deleted post's body that is already here is still analysed, but the
/// sweep fetches none of its attachments from peers.
#[test]
fn the_sweep_fetches_nothing_for_a_deleted_post() {
    let (att, att_hash, body, _) = fixture();
    let peer = FakePeer::start([(att_hash.clone(), att.clone())]);
    let peer_url = peer.url.clone();
    with_server(
        "attachment-sweep-deleted",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(stored.status(), 200);
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..3 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            let pending = index
                .view
                .read()
                .unwrap()
                .model
                .references
                .pending_bodies
                .len();
            assert_eq!(pending, 0, "the stored body was analysed");
            assert_eq!(sweep.pending_attachments(), 0, "its attachment was let go");
            assert_eq!(peer.hits(&att_hash), 0, "and never fetched");
        },
    );
}

/// Two posts share a body and only the older one is deleted, as when an author
/// deletes a post and posts the same words again. The sweep fetches the body
/// for the live post rather than giving up on the hash.
#[test]
fn a_deleted_post_does_not_stop_the_sweep_fetching_a_live_posts_body() {
    let (_, _, body, body_hash) = fixture();
    let peer = FakePeer::start([(body_hash.clone(), body.clone())]);
    let peer_url = peer.url.clone();
    with_server(
        "sweep-shared-body",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body)
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server { state, .. }| async move {
            crate::index::follow::once(&state).await;
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..5 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            assert_eq!(peer.hits(&body_hash), 1, "fetched once, for post 2");
            let pending = index
                .view
                .read()
                .unwrap()
                .model
                .references
                .pending_bodies
                .len();
            assert_eq!(pending, 0);
        },
    );
}

/// The same, for an attachment of the shared body: the live post vouches for it.
#[test]
fn a_deleted_post_does_not_stop_the_sweep_fetching_a_live_posts_attachment() {
    let (image, image_hash, body, _) = fixture();
    let peer = FakePeer::start([(image_hash.clone(), image)]);
    let peer_url = peer.url.clone();
    with_server(
        "sweep-shared-attachment",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=2", body).await;
            assert_eq!(stored.status(), 200);
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..5 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            assert_eq!(peer.hits(&image_hash), 1, "fetched once, for post 2");
        },
    );
}

/// Two live posts share a body and so its attachment. When the first is
/// deleted while the attachment is on its way, the sweep asks again through
/// the second instead of letting the attachment go.
#[test]
fn a_deletion_during_a_sweep_fetch_falls_back_to_another_post() {
    let (image, image_hash, body, _) = fixture();
    let host = Arc::new(std::sync::Mutex::new(None::<Arc<AppState>>));
    let delete_on_fetch = host.clone();
    let peer = FakePeer::start_with([(image_hash.clone(), image)], move || {
        if let Some(state) = delete_on_fetch.lock().unwrap().take() {
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
        }
    });
    let peer_url = peer.url.clone();
    with_server(
        "sweep-delete-in-flight",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=2", body).await;
            assert_eq!(stored.status(), 200);
            *host.lock().unwrap() = Some(state.clone());
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..5 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            let got = client
                .get(format!("{base}/api/content/{image_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(got.status(), 200, "fetched again, for post 2");
            assert_eq!(peer.hits(&image_hash), 2);
        },
    );
}

/// Five posts share a body and so its attachment. The first four are deleted
/// while the attachment is on its way: the walk that follows finds the fifth.
#[test]
fn a_walk_finds_the_post_left_when_others_are_deleted_mid_fetch() {
    let (image, image_hash, body, _) = fixture();
    let host = Arc::new(std::sync::Mutex::new(None::<Arc<AppState>>));
    let delete_on_fetch = host.clone();
    let peer = FakePeer::start_with([(image_hash.clone(), image)], move || {
        if let Some(state) = delete_on_fetch.lock().unwrap().take() {
            for id in 1..=4 {
                set_indexed_gate(&state, 31337, id, crate::index::types::TOMBSTONE);
            }
        }
    });
    let peer_url = peer.url.clone();
    with_server(
        "sweep-fifth-post",
        Some(Node {
            head: 100.into(),
            messages: 5.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let stored = publish(&client, &base, "chain_id=31337&message_id=5", body).await;
            assert_eq!(stored.status(), 200);
            *host.lock().unwrap() = Some(state.clone());
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..8 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            let got = client
                .get(format!("{base}/api/content/{image_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(got.status(), 200, "fetched again, for post 5");
            assert_eq!(peer.hits(&image_hash), 2);
        },
    );
}

/// Two posts share a body that only a peer has. The body is fetched for the
/// first, which is then deleted while the attachment is on its way: the walk
/// that follows fetches the attachment for the second.
#[test]
fn a_fetched_bodys_attachment_passes_to_another_post_sharing_it() {
    let (image, image_hash, body, body_hash) = fixture();
    let host = Arc::new(std::sync::Mutex::new(None::<Arc<AppState>>));
    let delete_on_fetch = host.clone();
    let requests = AtomicUsize::new(0);
    let peer = FakePeer::start_with(
        [
            (body_hash.clone(), body.clone()),
            (image_hash.clone(), image),
        ],
        move || {
            // The body is asked for first, then its attachment.
            if requests.fetch_add(1, Ordering::SeqCst) == 1
                && let Some(state) = delete_on_fetch.lock().unwrap().as_ref()
            {
                set_indexed_gate(state, 31337, 1, crate::index::types::TOMBSTONE);
            }
        },
    );
    let peer_url = peer.url.clone();
    with_server(
        "sweep-body-handoff",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body)
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            *host.lock().unwrap() = Some(state.clone());
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..8 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            let got = client
                .get(format!("{base}/api/content/{image_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(got.status(), 200, "fetched for post 2");
            assert_eq!(peer.hits(&image_hash), 2);
        },
    );
}

/// A post by a blocked author vouches for nothing: the sweep fetches neither
/// its body nor its attachment, and queues nothing for it.
#[test]
fn a_blocked_authors_attachment_is_left_alone() {
    let (image, image_hash, body, _) = fixture();
    let peer = FakePeer::start([(image_hash.clone(), image)]);
    let peer_url = peer.url.clone();
    // The mock's message 1 is by 0x…01.
    let path = std::env::temp_dir().join(format!("sweep-blocked-{}.json", std::process::id()));
    let policy = format!(
        r#"{{"revision":0,"curated":[],"blocked":["0x{}01"],"bots":[]}}"#,
        "00".repeat(19)
    );
    std::fs::write(&path, policy).unwrap();
    write_no_takedowns(&path);
    let config = crate::host_policy::Config {
        file: path.clone(),
        admin: crate::chain::Hex([0xaa; 20]),
        admin_chain: 31337,
        origin: "https://host.example".into(),
    };
    with_server(
        "sweep-blocked",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body)
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            host_policy: Some(config),
            ..Setup::default()
        },
        move |Server { state, .. }| async move {
            crate::index::follow::once(&state).await;
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..5 {
                sweep.run(&state, 31337, index).await.unwrap();
            }
            assert_eq!(
                peer.total_hits(),
                0,
                "nothing fetched for the blocked author"
            );
            assert_eq!(sweep.pending_attachments(), 0);
        },
    );
    let _ = std::fs::remove_file(path);
}
