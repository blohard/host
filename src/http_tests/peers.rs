//! Peer lookup and per-hash retry behavior.

use super::*;

/// A host asks its peers for a record it lacks, but only on a reader's word
/// that a message anchors it, and only when the chain (or a body it has)
/// vouches for the hash. What comes back is checked like an upload and
/// stored, so the next reader is served locally.
#[test]
fn pulls_missing_records_from_peers() {
    let att = record("image/png", b"\x89PNG fake picture");
    let att_hash = hx(&att);
    let body = record(
        "text/plain",
        format!("# hi\n\nrecord:{att_hash}\n").as_bytes(),
    );
    let body_hash = hx(&body);
    let stranger = record("text/plain", b"# anchored nowhere\n");
    let stranger_hash = hx(&stranger);

    // The peer holds all three records.
    let peer = FakePeer::start([
        (body_hash.clone(), body.clone()),
        (att_hash.clone(), att.clone()),
        (stranger_hash.clone(), stranger.clone()),
    ]);
    let peer_url = peer.url.clone();

    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    let peer_for_setup = peer_url.clone();
    with_server(
        "http-peers",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_for_setup],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            crate::index::follow::once(&state).await;
            let before = node.as_ref().unwrap().node.counts();
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["store"]["peers"], json!([peer_url]));

            // No hint: a miss is a miss, and no peer is bothered.
            let resp = client
                .get(format!("{base}/api/content/{body_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 404);
            assert_eq!(peer.total_hits(), 0);

            // Naming the anchoring message: pulled, verified, stored, served.
            let resp = client
                .get(format!(
                    "{base}/api/content/{body_hash}?chain_id=31337&message_id=1"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            assert_eq!(resp.bytes().await.unwrap().as_ref(), body.as_slice());
            assert_eq!(peer.total_hits(), 1);

            // Now stored: served locally, no hint needed, no second pull.
            let resp = client
                .get(format!("{base}/api/content/{body_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            assert_eq!(peer.total_hits(), 1);

            // The attachment the body references follows the same way.
            let resp = client
                .get(format!(
                    "{base}/api/content/{att_hash}?chain_id=31337&message_id=1"
                ))
                .send()
                .await
                .unwrap();
            let status = resp.status();
            assert_eq!(status, 200, "{}", resp.text().await.unwrap());
            assert_eq!(peer.total_hits(), 2);

            // A hash the chain does not vouch for is never asked about.
            let resp = client
                .get(format!(
                    "{base}/api/content/{stranger_hash}?chain_id=31337&message_id=1"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 404);
            assert_eq!(peer.total_hits(), 2);

            assert_eq!(
                node.as_ref().unwrap().node.counts(),
                before,
                "accepted peer anchoring never calls RPC"
            );
            // A malformed hint is refused in the usual JSON shape.
            let resp = client
                .get(format!("{base}/api/content/{body_hash}?chain_id=abc"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 400);
            let j: Value = resp.json().await.unwrap();
            assert!(j["error"].as_str().unwrap().contains("chain_id"), "{j}");
        },
    );
}

/// Peer admission is a retryable refusal, not a false content miss. Bytes
/// already local remain readable even while all peer slots are occupied.
#[test]
fn peer_pool_overload_is_503_but_local_reads_still_work() {
    use futures::{future::join_all, poll};
    use tokio::sync::{Semaphore, mpsc};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let peer_url = format!("http://{}", listener.local_addr().unwrap());
    let release = Arc::new(Semaphore::new(0));
    let gate = release.clone();
    let (hits, mut arrived) = mpsc::unbounded_channel();
    with_server(
        "peer-overload",
        None,
        move |_| {
            tokio::spawn(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let app = axum::Router::new().route(
                    "/api/content/{hash}",
                    axum::routing::get(move || {
                        let (gate, hits) = (gate.clone(), hits.clone());
                        async move {
                            hits.send(()).unwrap();
                            gate.acquire().await.unwrap().forget();
                            axum::http::StatusCode::NOT_FOUND
                        }
                    }),
                );
                axum::serve(listener, app).await.unwrap();
            });
            Setup {
                peers: vec![peer_url],
                ..Setup::default()
            }
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            let store = state.store.as_ref().unwrap();
            let body = record("text/plain", b"already here");
            let (local, _) = store.bodies.put(body.clone()).await.unwrap();
            let hashes: Vec<_> = (0u64..8)
                .map(|i| keccak::hash(&[&i.to_be_bytes()]))
                .collect();
            let mut pending = Box::pin(join_all(
                hashes
                    .iter()
                    .map(|h| store.peers.fetch(h, crate::store::Kind::Bodies)),
            ));
            assert!(poll!(pending.as_mut()).is_pending());
            for _ in 0..8 {
                tokio::time::timeout(Duration::from_secs(2), arrived.recv())
                    .await
                    .unwrap()
                    .unwrap();
            }
            let missing = hx(b"not stored");
            let response = client
                .get(format!(
                    "{base}/api/content/{missing}?chain_id=31337&message_id=1"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 503);
            assert_eq!(
                response.json::<Value>().await.unwrap()["error"],
                "the host is busy; try again shortly"
            );
            let response = client
                .get(format!("{base}/api/content/{}", local.to_hex()))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.bytes().await.unwrap().as_ref(), body.as_slice());
            release.add_permits(8);
            assert!(pending.await.into_iter().all(|r| r.unwrap().is_none()));
        },
    );
}

/// A reader naming a deleted post gets nothing copied from peers for it, and
/// the hash stays free for a live post with the same bytes.
#[test]
fn deleted_posts_vouch_for_nothing_and_leave_the_hash_free() {
    let body = record("text/plain", b"# shared by a deleted and a live post\n");
    let body_hash = hx(&body);
    let peer = FakePeer::start([(body_hash.clone(), body.clone())]);
    let peer_url = peer.url.clone();
    with_server(
        "peers-deleted",
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
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
            let get = |id: u64| {
                let url = format!("{base}/api/content/{body_hash}?chain_id=31337&message_id={id}");
                let client = client.clone();
                async move { client.get(url).send().await.unwrap().status() }
            };
            assert_eq!(get(1).await, 404);
            assert_eq!(peer.total_hits(), 0, "nothing fetched for a deleted post");
            assert_eq!(get(2).await, 200, "no backoff left behind for the hash");
            assert_eq!(peer.total_hits(), 1);
        },
    );
}

/// A post deleted while its bytes are being fetched is refused for that
/// request only: the hash takes no backoff, so a live post sharing the bytes
/// gets them at once.
#[test]
fn a_deletion_during_a_fetch_does_not_hold_up_a_live_post() {
    let body = record("text/plain", b"# shared while one post is deleted\n");
    let hash = hx(&body);
    let host = Arc::new(std::sync::Mutex::new(None::<Arc<AppState>>));
    let delete_on_fetch = host.clone();
    let peer = FakePeer::start_with([(hash.clone(), body.clone())], move || {
        if let Some(state) = delete_on_fetch.lock().unwrap().take() {
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
        }
    });
    let peer_url = peer.url.clone();
    with_server(
        "peers-delete-in-flight",
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
            let get = |id: u64| {
                let url = format!("{base}/api/content/{hash}?chain_id=31337&message_id={id}");
                let client = client.clone();
                async move { client.get(url).send().await.unwrap().status() }
            };
            assert_eq!(get(1).await, 404, "deleted while its bytes were on the way");
            assert_eq!(get(2).await, 200);
            assert_eq!(peer.total_hits(), 2);
        },
    );
}

/// A blocked author's post vouches for nothing: no download for it, and the
/// bytes stay free for another author's post that shares them.
#[test]
fn a_blocked_authors_post_fetches_nothing() {
    let body = record("text/plain", b"# shared with a blocked author\n");
    let hash = hx(&body);
    let peer = FakePeer::start([(hash.clone(), body.clone())]);
    let peer_url = peer.url.clone();
    // The mock's message 1 is by 0x…01.
    let path = std::env::temp_dir().join(format!("peers-blocked-{}.json", std::process::id()));
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
        "peers-blocked",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body)
        }),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            host_policy: Some(config),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let get = |id: u64| {
                let url = format!("{base}/api/content/{hash}?chain_id=31337&message_id={id}");
                let client = client.clone();
                async move { client.get(url).send().await.unwrap().status() }
            };
            assert_eq!(get(1).await, 404);
            assert_eq!(
                peer.total_hits(),
                0,
                "nothing downloaded for the blocked author"
            );
            assert_eq!(get(2).await, 200);
            assert_eq!(peer.total_hits(), 1);
        },
    );
    let _ = std::fs::remove_file(path);
}

/// A peer that answers with a redirect sends the host nowhere: the target
/// gets no request, and the record stays missing.
#[test]
fn a_peers_redirect_is_not_followed() {
    let body = record("text/plain", b"behind a redirect");
    let body_hash = hx(&body);
    // The redirect's target holds the record, and counts its visitors.
    let target = FakePeer::start([(body_hash.clone(), body.clone())]);
    let location = format!("{}/api/content/{body_hash}", target.url);
    let redirecting = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let app = axum::Router::new().route(
                        "/api/content/{hash}",
                        axum::routing::get(move || {
                            let location = location.clone();
                            async move { axum::response::Redirect::temporary(&location) }
                        }),
                    );
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    axum::serve(listener, app).await.unwrap();
                });
        });
        url
    };
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "peer-redirect",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![redirecting],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let resp = client
                .get(format!(
                    "{base}/api/content/{body_hash}?chain_id=31337&message_id=1"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 404);
            assert_eq!(
                target.hits(&body_hash),
                0,
                "the redirect's target was never asked"
            );
        },
    );
}

/// A record too large to be post 1's body, but a fine attachment of post 2,
/// is fetched on post 1's word and refused as that post's body. That refusal
/// is the post's, not the bytes': the hash takes no backoff, and post 2's
/// readers get it at once.
#[test]
fn a_size_refusal_for_one_posts_body_does_not_block_the_bytes_for_another() {
    // 6,000 bytes: over the 4,000-byte body cap, under the attachment cap.
    let picture = record("image/png", &[7u8; 6_000]);
    let picture_hash = hx(&picture);
    let body = record(
        "text/plain",
        format!("a picture record:{picture_hash}").as_bytes(),
    );
    let peer = FakePeer::start([
        (picture_hash.clone(), picture.clone()),
        (hx(&body), body.clone()),
    ]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 2.into(),
        // Post 1 commits to the picture as its body; post 2 to a body referencing it.
        extra: vec![(1, picture.clone()), (2, body.clone())],
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "review-peer-size-poison",
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
            let store = state.store.as_ref().unwrap();
            store.bodies.put(body.clone()).await.unwrap();
            let fetch = |id: u64| {
                client
                    .get(format!(
                        "{base}/api/content/{picture_hash}?chain_id=31337&message_id={id}"
                    ))
                    .send()
            };
            // On post 1's word: fetched, then refused as a body too large.
            assert_eq!(fetch(1).await.unwrap().status(), 404);
            assert_eq!(peer.hits(&picture_hash), 1);
            // On post 2's word, which it is an attachment of: should be fetched and served.
            let resp = fetch(2).await.unwrap();
            assert_eq!(
                resp.status(),
                200,
                "post 2's attachment, not poisoned by post 1's refusal"
            );
            assert_eq!(peer.hits(&picture_hash), 2);
        },
    );
}

/// On a store-only host with peers, a content request naming a message for
/// a record it lacks asks the chain about that message before any peer. That
/// is a live read, under the reader's budget: a flood from one visitor is
/// turned away at its burst, another visitor still gets through, and a record
/// this host has is served free however often it is asked for.
#[test]
fn a_hinted_miss_on_a_store_only_host_spends_the_readers_live_budget() {
    let body = record("text/plain", b"a record this host has");
    let body_hash = hx(&body);
    let peer = FakePeer::start([]);
    let peer_url = peer.url.clone();
    with_server(
        "store-only-hinted-miss",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            gateway: false,
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            let node = node.unwrap();
            let chain_calls = || node.node.count("eth_call");
            let ask = |visitor: &'static str, hash: String| {
                client
                    .get(format!(
                        "{base}/api/content/{hash}?chain_id=31337&message_id=7"
                    ))
                    .header("x-forwarded-for", visitor)
                    .send()
            };
            // Each a different record, so no per-hash backoff stands in.
            let unknown = |n: u32| hx(format!("a record nobody has {n}").as_bytes());
            let before = chain_calls();
            let mut statuses = Vec::new();
            for n in 0..40 {
                statuses.push(
                    ask("203.0.113.1", unknown(n))
                        .await
                        .unwrap()
                        .status()
                        .as_u16(),
                );
            }
            let asked = chain_calls() - before;
            assert!(statuses.contains(&429), "{statuses:?}");
            assert!(asked <= 17, "{asked} chain calls for forty hinted misses");
            // Another visitor's hinted miss still asks the chain.
            let before = chain_calls();
            assert_eq!(ask("203.0.113.2", unknown(99)).await.unwrap().status(), 404);
            assert_eq!(chain_calls() - before, 1);
            // A record this host has costs the exhausted visitor nothing.
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(body.clone())
                .await
                .unwrap();
            let before = chain_calls();
            for _ in 0..40 {
                assert_eq!(
                    ask("203.0.113.1", body_hash.clone())
                        .await
                        .unwrap()
                        .status(),
                    200
                );
            }
            assert_eq!(chain_calls(), before);
        },
    );
}

/// The other way round: an attachment cap under the body cap. A record over
/// the attachment cap, asked for as post 2's attachment, is refused for post
/// 2 and fetched again as post 1's body. A type the host does not store as
/// an attachment is the same: post 2's refusal, post 1's body.
#[test]
fn an_attachment_refusal_for_one_post_does_not_block_the_bytes_as_anothers_body() {
    for (name, record_type, cap, types) in [
        ("peer-attachment-size", "text/plain", 4_000, None),
        (
            "peer-attachment-type",
            "text/plain",
            2 << 20,
            Some(vec!["image/png".to_string()]),
        ),
    ] {
        let bytes = record(record_type, &[b'z'; 6_000]);
        let bytes_hash = hx(&bytes);
        let body = record("text/plain", format!("see record:{bytes_hash}").as_bytes());
        let peer = FakePeer::start([
            (bytes_hash.clone(), bytes.clone()),
            (hx(&body), body.clone()),
        ]);
        let peer_url = peer.url.clone();
        let node = Node {
            head: 100.into(),
            messages: 2.into(),
            extra: vec![(1, bytes.clone()), (2, body.clone())],
            ..Node::new(31337, CONTRACT, body.clone())
        };
        with_server(
            name,
            Some(node),
            move |url| Setup {
                verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
                peers: vec![peer_url],
                max_body_bytes: 10_000,
                attachments: Some(AttachmentsConfig {
                    max_bytes: cap,
                    max_per_message: 4,
                    media_types: types,
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
                state
                    .store
                    .as_ref()
                    .unwrap()
                    .bodies
                    .put(body.clone())
                    .await
                    .unwrap();
                let fetch = |id: u64| {
                    client
                        .get(format!(
                            "{base}/api/content/{bytes_hash}?chain_id=31337&message_id={id}"
                        ))
                        .send()
                };
                assert_eq!(
                    fetch(2).await.unwrap().status(),
                    404,
                    "{name}: refused as post 2's attachment"
                );
                assert_eq!(
                    fetch(1).await.unwrap().status(),
                    200,
                    "{name}: post 1's body, not poisoned"
                );
                assert_eq!(peer.hits(&bytes_hash), 2, "{name}");
            },
        );
    }
}

/// A miss holds both roles. After post 1's body refusal held back body asks
/// only, the peer loses the record: post 2's attachment ask finds nothing,
/// and the next attachment ask waits out that miss instead of asking again.
#[test]
fn a_miss_after_a_role_refusal_holds_both_roles() {
    let picture = record("image/png", &[7u8; 6_000]);
    let picture_hash = hx(&picture);
    let body = record(
        "text/plain",
        format!("a picture record:{picture_hash}").as_bytes(),
    );
    let peer = FakePeer::start([
        (picture_hash.clone(), picture.clone()),
        (hx(&body), body.clone()),
    ]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 2.into(),
        extra: vec![(1, picture.clone()), (2, body.clone())],
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "peer-miss-after-role-refusal",
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
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(body.clone())
                .await
                .unwrap();
            let fetch = |id: u64| {
                client
                    .get(format!(
                        "{base}/api/content/{picture_hash}?chain_id=31337&message_id={id}"
                    ))
                    .send()
            };
            assert_eq!(
                fetch(1).await.unwrap().status(),
                404,
                "refused as post 1's body"
            );
            peer.records.lock().unwrap().remove(&picture_hash);
            assert_eq!(
                fetch(2).await.unwrap().status(),
                404,
                "the peer no longer has it"
            );
            assert_eq!(peer.hits(&picture_hash), 2);
            assert_eq!(fetch(2).await.unwrap().status(), 404);
            assert_eq!(
                peer.hits(&picture_hash),
                2,
                "the miss holds attachment asks too"
            );
        },
    );
}

/// Only the path that can reach the chain is admitted as a live read. A host
/// without peers fetches nothing, and one with an index answers from it, so
/// their hinted misses are free: forty from one visitor are all 404.
#[test]
fn hinted_misses_that_reach_no_chain_stay_free() {
    for (name, gateway, peers) in [
        ("hinted-free-no-peers", false, Vec::new()),
        ("hinted-free-indexed", true, vec![FakePeer::start([]).url]),
    ] {
        let body = record("text/plain", b"a record this host has");
        with_server(
            name,
            Some(Node {
                head: 100.into(),
                messages: 1.into(),
                ..Node::new(31337, CONTRACT, body.clone())
            }),
            move |url| Setup {
                gateway,
                verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
                peers,
                ..Setup::default()
            },
            move |Server {
                      base,
                      client,
                      state,
                      ..
                  }| async move {
                if gateway {
                    crate::index::follow::once(&state).await;
                }
                for n in 0..40u32 {
                    let hash = hx(format!("a record nobody has {n}").as_bytes());
                    let resp = client
                        .get(format!(
                            "{base}/api/content/{hash}?chain_id=31337&message_id=1"
                        ))
                        .header("x-forwarded-for", "203.0.113.1")
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(resp.status(), 404, "{name} request {n}");
                }
            },
        );
    }
}

/// Each role's refusal stands on its own: bytes refused as a body and as an
/// attachment, asked for in turn, are downloaded once per role and then
/// wait out both refusals, however the roles alternate.
#[test]
fn refusals_in_both_roles_each_stand_while_the_roles_alternate() {
    // Over the body cap as text, and not an image, so refused as both.
    let text = record("text/plain", &[b't'; 6_000]);
    let text_hash = hx(&text);
    let body = record("text/plain", format!("see record:{text_hash}").as_bytes());
    let peer = FakePeer::start([(text_hash.clone(), text.clone()), (hx(&body), body.clone())]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 2.into(),
        extra: vec![(1, text.clone()), (2, body.clone())],
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "peer-alternating-roles",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            peers: vec![peer_url],
            attachments: Some(AttachmentsConfig {
                media_types: Some(vec!["image/png".to_string()]),
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
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(body.clone())
                .await
                .unwrap();
            let fetch = |id: u64| {
                client
                    .get(format!(
                        "{base}/api/content/{text_hash}?chain_id=31337&message_id={id}"
                    ))
                    .send()
            };
            for id in [1, 2, 1, 2, 1, 2] {
                assert_eq!(fetch(id).await.unwrap().status(), 404);
            }
            assert_eq!(
                peer.hits(&text_hash),
                2,
                "once per role, then both refusals stand"
            );
        },
    );
}
