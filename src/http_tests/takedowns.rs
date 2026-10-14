//! The admin's takedown list: signed, private, durable, and enforced on every
//! path that hands content out or takes it in.
use super::*;
use crate::keccak::Digest;
use k256::ecdsa::SigningKey;

const ORIGIN: &str = "https://host.example";

fn admin() -> (SigningKey, crate::chain::Address) {
    let key = SigningKey::from_slice(&[7; 32]).unwrap();
    let point = key.verifying_key().to_encoded_point(false);
    let address = crate::chain::Hex(
        keccak::hash(&[&point.as_bytes()[1..]]).0[12..]
            .try_into()
            .unwrap(),
    );
    (key, address)
}

/// A host policy file, an empty takedown list beside it, and the config.
fn policy(name: &str) -> (crate::host_policy::Config, PathBuf) {
    let dir = std::env::temp_dir().join(format!("takedowns-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("host-policy.json");
    std::fs::write(
        &path,
        br#"{"revision":0,"curated":[],"blocked":[],"bots":[]}"#,
    )
    .unwrap();
    write_no_takedowns(&path);
    let config = crate::host_policy::Config {
        file: path.clone(),
        admin: admin().1,
        admin_chain: 31337,
        origin: ORIGIN.into(),
    };
    (config, path)
}

fn sign(key: &SigningKey, message: &str) -> String {
    let digest = alloy_primitives::eip191_hash_message(message.as_bytes());
    let (signature, recovery) = key.sign_prehash_recoverable(digest.as_slice()).unwrap();
    let mut bytes = signature.to_bytes().to_vec();
    bytes.push(recovery.to_byte() + 27);
    hex0x(&bytes)
}

fn expires() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 200
}

/// The admin's signed edit, sent.
async fn take_down(
    client: &reqwest::Client,
    base: &str,
    key: &SigningKey,
    revision: u64,
    action: &str,
    hash: &str,
) -> reqwest::Response {
    client
        .post(format!("{base}/api/host-policy/takedowns"))
        .json(&edit(key, revision, action, hash))
        .send()
        .await
        .unwrap()
}

/// A signed edit of the takedown list.
fn edit(key: &SigningKey, revision: u64, action: &str, hash: &str) -> Value {
    let expires = expires();
    let message = format!(
        "Host takedown update\nHost: {ORIGIN}\nRevision: {revision}\nExpires: {expires}\nAction: {action}\nHashes:\n{hash}"
    );
    json!({"revision": revision.to_string(), "expires": expires, "action": action,
        "hashes": [hash], "signature": sign(key, &message)})
}

/// A signed request to read the list.
fn view(key: &SigningKey) -> Value {
    let expires = expires();
    let message = format!("Host takedown list\nHost: {ORIGIN}\nExpires: {expires}");
    json!({"expires": expires, "signature": sign(key, &message)})
}

#[test]
fn the_admin_takes_content_down_privately_and_can_restore_it() {
    let (config, path) = policy("cycle");
    let body = record("text/plain", b"a post the admin takes down");
    let hash = hx(&body);
    with_server(
        "takedown-cycle",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
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
            let (key, _) = admin();
            let upload = || publish(&client, &base, "chain_id=31337&message_id=1", body.clone());
            assert_eq!(upload().await.status(), 200);
            let content = format!("{base}/api/content/{hash}");
            let etag = format!("\"{hash}\"");
            let get = |tag: Option<&str>| {
                let mut request = client.get(&content);
                if let Some(tag) = tag {
                    request = request.header(header::IF_NONE_MATCH, tag);
                }
                request.send()
            };
            let served = get(None).await.unwrap();
            assert_eq!(served.status(), 200);
            assert_eq!(served.headers()[header::ETAG], etag.as_str());
            assert_eq!(get(Some(&etag)).await.unwrap().status(), 304);
            assert_eq!(get(Some("\"0x00\"")).await.unwrap().status(), 200);

            let edits = format!("{base}/api/host-policy/takedowns");
            let refused = client
                .post(&edits)
                .json(&edit(
                    &SigningKey::from_slice(&[8; 32]).unwrap(),
                    0,
                    "add",
                    &hash,
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(refused.status(), 403, "only the admin");
            let taken = client
                .post(&edits)
                .json(&edit(&key, 0, "add", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(taken.status(), 200);
            assert_eq!(taken.headers()[header::CACHE_CONTROL], "no-store");
            let list: Value = taken.json().await.unwrap();
            assert_eq!(list, json!({"revision": "1", "hashes": [hash]}));
            let stale = client
                .post(&edits)
                .json(&edit(&key, 0, "add", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(stale.status(), 409);

            for tag in [None, Some(etag.as_str())] {
                let gone = get(tag).await.unwrap();
                assert_eq!(gone.status(), 410, "{tag:?}");
                assert_eq!(gone.headers()[header::CACHE_CONTROL], "no-store");
                assert_eq!(gone.json::<Value>().await.unwrap()["code"], "taken_down");
            }
            let again = upload().await;
            assert_eq!(again.status(), 410);
            assert_eq!(again.json::<Value>().await.unwrap()["code"], "taken_down");

            // Never published, but the admin can read it.
            for public in ["/api/host-policy", "/api/status"] {
                let text = client
                    .get(format!("{base}{public}"))
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap();
                assert!(
                    !text.contains(hash.trim_start_matches("0x")),
                    "{public} lists it"
                );
            }
            let reads = format!("{base}/api/host-policy/takedowns/read");
            let read = client.post(&reads).json(&view(&key)).send().await.unwrap();
            assert_eq!(read.status(), 200);
            assert_eq!(read.json::<Value>().await.unwrap()["hashes"], json!([hash]));
            let peek = client
                .post(&reads)
                .json(&view(&SigningKey::from_slice(&[8; 32]).unwrap()))
                .send()
                .await
                .unwrap();
            assert_eq!(peek.status(), 403);

            // Saved before it took effect, so a restart keeps it.
            let saved = crate::host_policy::takedowns::Takedowns::read(
                &crate::host_policy::takedowns::path(&path),
            )
            .unwrap();
            assert!(saved.contains(&Digest::from_hex(&hash).unwrap().0));

            let restored = client
                .post(&edits)
                .json(&edit(&key, 1, "remove", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(restored.status(), 200);
            assert_eq!(get(None).await.unwrap().status(), 200, "served again");
        },
    );
}

/// A save that fails leaves the list as it was, in memory and on disk.
#[test]
fn a_failed_save_changes_nothing() {
    let (config, path) = policy("failed-save");
    let list = crate::host_policy::takedowns::path(&path);
    // The save writes a temporary file beside the list first: a directory in
    // its place makes that fail.
    let blocker = list.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::create_dir_all(&blocker).unwrap();
    let hash = format!("0x{}", "ab".repeat(32));
    with_server(
        "takedown-failed-save",
        Some(Node::new(31337, CONTRACT, Vec::new())),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            let (key, _) = admin();
            let edits = format!("{base}/api/host-policy/takedowns");
            let failed = client
                .post(&edits)
                .json(&edit(&key, 0, "add", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(failed.status(), 500);
            assert!(!state.policy.taken_down(&[0xab; 32]), "not in effect");
            assert_eq!(
                std::fs::read_to_string(&list).unwrap(),
                r#"{"revision":0,"hashes":[]}"#
            );
            std::fs::remove_dir(&blocker).unwrap();
            let saved = client
                .post(&edits)
                .json(&edit(&key, 0, "add", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(saved.status(), 200, "the same edit, once the disk lets it");
            assert!(state.policy.taken_down(&[0xab; 32]));
        },
    );
}

/// A host with lists never starts without its takedown list, whether it is
/// missing or unreadable.
#[tokio::test]
async fn a_host_will_not_start_without_a_readable_takedown_list() {
    let (config, path) = policy("startup");
    let list = crate::host_policy::takedowns::path(&path);
    assert!(
        crate::host_policy::Policy::open(Some(config.clone()))
            .await
            .is_ok()
    );
    std::fs::write(&list, "{").unwrap();
    assert!(
        crate::host_policy::Policy::open(Some(config.clone()))
            .await
            .is_err()
    );
    std::fs::remove_file(&list).unwrap();
    assert!(
        crate::host_policy::Policy::open(Some(config))
            .await
            .is_err()
    );
}

/// Nothing taken down is fetched from a peer, and a held upload that is
/// taken down is dropped instead of promoted.
#[test]
fn taken_down_content_is_neither_fetched_nor_promoted() {
    let (config, _) = policy("paths");
    let body = record("text/plain", b"held, then taken down");
    let hash = hx(&body);
    let peer = FakePeer::start([(hash.clone(), body.clone())]);
    let peer_url = peer.url.clone();
    with_server(
        "takedown-paths",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            peers: vec![peer_url],
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            crate::index::follow::once(&state).await;
            let held = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(held.status(), 202);
            let (key, _) = admin();
            assert_eq!(
                take_down(&client, &base, &key, 0, "add", &hash)
                    .await
                    .status(),
                200
            );

            let store = state.store.as_ref().unwrap();
            let digest = Digest::from_hex(&hash).unwrap();
            let pulled = crate::peers::pull(&state, store, 31337, 1, digest)
                .await
                .unwrap();
            assert!(matches!(pulled, crate::peers::Pulled::Refused));
            assert_eq!(peer.total_hits(), 0, "nothing fetched");

            node.unwrap().node.set_head(16);
            crate::index::follow::once(&state).await;
            assert_eq!(promote_once(&state).await, 0);
            assert!(
                store.holding.get(31337, &digest).is_none(),
                "dropped from holding"
            );
        },
    );
}

/// A taken-down post's share card shows no excerpt, even one built and
/// cached before the takedown, and shows it again once restored.
#[test]
fn a_share_card_drops_a_taken_down_excerpt() {
    let (config, _) = policy("share");
    let web = std::env::temp_dir().join(format!("takedowns-share-web-{}", std::process::id()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), crate::share::APP_INDEX).unwrap();
    let dir = web.clone();
    let body = record("text/plain", b"words the admin takes down");
    let hash = hx(&body);
    with_server(
        "takedown-share",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            web_dir: Some(dir),
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
            let card = || async {
                let page = client
                    .get(format!("{base}/p/31337/1"))
                    .send()
                    .await
                    .unwrap();
                (page.status().as_u16(), page.text().await.unwrap())
            };
            let (status, page) = card().await;
            assert_eq!(status, 200);
            assert!(
                page.contains("words the admin takes down"),
                "built and cached"
            );

            let (key, _) = admin();
            let edits = format!("{base}/api/host-policy/takedowns");
            let taken = client
                .post(&edits)
                .json(&edit(&key, 0, "add", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(taken.status(), 200);
            let (status, page) = card().await;
            assert_eq!(status, 410);
            assert!(
                !page.contains("words the admin takes down"),
                "not from the cache either"
            );
            assert!(page.contains("removed by this host"));

            let restored = client
                .post(&edits)
                .json(&edit(&key, 1, "remove", &hash))
                .send()
                .await
                .unwrap();
            assert_eq!(restored.status(), 200);
            let (status, page) = card().await;
            assert_eq!(status, 200);
            assert!(page.contains("words the admin takes down"));
        },
    );
    std::fs::remove_dir_all(web).unwrap();
}

/// A cached link preview is not shown for a post whose body is taken down,
/// but the same link's preview still shows for another post.
#[test]
fn a_cached_link_preview_is_refused_only_to_the_taken_down_post() {
    let (config, _) = policy("previews");
    let body = record("text/plain", b"see https://article.example/ for more");
    let other = record("text/plain", b"also https://article.example/ again");
    let hash = hx(&body);
    with_server(
        "takedown-previews",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            extra: vec![(2, other.clone())],
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            link_previews: Some(crate::link_preview::Config {
                public_names: vec!["operator.invalid".into()],
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
            let previews = state
                .gateway
                .as_ref()
                .unwrap()
                .link_previews
                .as_ref()
                .unwrap();
            previews.seed_for_test("https://article.example/", "an article");
            let preview = |id: u64| {
                client
                    .get(format!(
                        "{base}/api/link-preview?chain=31337&id={id}&link=https://article.example/"
                    ))
                    .header("x-blohard-preview", "1")
                    .send()
            };
            assert_eq!(preview(1).await.unwrap().status(), 200);
            let (key, _) = admin();
            assert_eq!(
                take_down(&client, &base, &key, 0, "add", &hash)
                    .await
                    .status(),
                200
            );
            assert_eq!(preview(1).await.unwrap().status(), 404);
            let still = preview(2).await.unwrap();
            assert_eq!(still.status(), 200, "the link's preview stays for post 2");
            assert_eq!(
                still.json::<Value>().await.unwrap()["preview"]["title"],
                "an article"
            );
        },
    );
}

/// A held attachment taken down on its own is not stored when its body is:
/// the body goes into the store, the attachment does not.
#[test]
fn a_taken_down_attachment_is_not_promoted_with_its_body() {
    let (config, _) = policy("held-attachment");
    let attachment = record("image/png", b"an attachment taken down while held");
    let attachment_hash = hx(&attachment);
    let body = record(
        "text/plain",
        format!("a live body with record:{attachment_hash}").as_bytes(),
    );
    with_server(
        "takedown-held-attachment",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
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
            crate::index::follow::once(&state).await;
            let held =
                |bytes: Vec<u8>| publish(&client, &base, "chain_id=31337&message_id=1", bytes);
            assert_eq!(held(body.clone()).await.status(), 202);
            assert_eq!(held(attachment.clone()).await.status(), 202);
            assert_eq!(
                take_down(&client, &base, &admin().0, 0, "add", &attachment_hash)
                    .await
                    .status(),
                200
            );
            node.unwrap().node.set_head(16);
            crate::index::follow::once(&state).await;
            assert_eq!(promote_once(&state).await, 1, "the body alone");
            let store = state.store.as_ref().unwrap();
            let (attachments, _) = store.attachments.as_ref().unwrap();
            let digest = Digest::from_hex(&attachment_hash).unwrap();
            assert!(
                attachments.get(&digest).await.unwrap().is_none(),
                "not stored"
            );
            assert!(
                store.holding.get(31337, &digest).is_none(),
                "and no longer held"
            );
        },
    );
}

/// A download already waiting on the store when a takedown succeeds is
/// refused once it has the bytes, with or without a matching ETag.
#[test]
fn a_download_waiting_on_the_store_is_refused_after_a_takedown() {
    let (config, _) = policy("read-race");
    let body = record("text/plain", b"taken down while its read waits");
    let hash = hx(&body);
    with_server(
        "takedown-read-race",
        Some(Node::new(31337, CONTRACT, body.clone())),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            let bodies = &state.store.as_ref().unwrap().bodies;
            bodies.put(body).await.unwrap();
            // A held read, then a write waiting behind it: later reads wait too.
            let hold = bodies.hold_writes_for_test().await;
            let writer = bodies.put(record("text/plain", b"a write that waits"));
            tokio::pin!(writer);
            assert!(futures::poll!(&mut writer).is_pending());
            assert!(bodies.writer_waiting_for_test());
            let read = |headers| {
                crate::reads::content(
                    axum::extract::State(state.clone()),
                    crate::http::JsonPath(hash.clone()),
                    crate::http::JsonQuery(serde_json::from_value(json!({})).unwrap()),
                    headers,
                )
            };
            let plain = read(axum::http::HeaderMap::new());
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                header::IF_NONE_MATCH,
                format!("\"{hash}\"").parse().unwrap(),
            );
            let conditional = read(headers);
            tokio::pin!(plain, conditional);
            assert!(
                futures::poll!(&mut plain).is_pending(),
                "waiting on the store"
            );
            assert!(futures::poll!(&mut conditional).is_pending());
            assert_eq!(
                take_down(&client, &base, &admin().0, 0, "add", &hash)
                    .await
                    .status(),
                200
            );
            drop(hold);
            writer.await.unwrap();
            assert_eq!(plain.await.status(), 410);
            assert_eq!(conditional.await.status(), 410, "no 304 either");
        },
    );
}

/// An upload whose chain lookup is still out when a takedown succeeds is
/// refused, not stored.
#[test]
fn an_upload_waiting_on_the_chain_is_refused_after_a_takedown() {
    let (config, _) = policy("upload-race");
    let body = record("text/plain", b"taken down while its anchoring waits");
    let hash = hx(&body);
    with_server(
        "takedown-upload-race",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
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
            // A store-only host asks the chain for the post; that answer is slow.
            node.node
                .latency_ms
                .store(400, std::sync::atomic::Ordering::Relaxed);
            let upload = tokio::spawn({
                let (client, base, body) = (client.clone(), base.clone(), body.clone());
                async move {
                    publish(&client, &base, "chain_id=31337&message_id=1", body)
                        .await
                        .status()
                }
            });
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert_eq!(
                take_down(&client, &base, &admin().0, 0, "add", &hash)
                    .await
                    .status(),
                200
            );
            assert_eq!(upload.await.unwrap(), 410);
            let stored = state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .get(&Digest::from_hex(&hash).unwrap())
                .await
                .unwrap();
            assert!(stored.is_none(), "not stored");
        },
    );
}

/// An attachment upload waiting to read its stored parent body when a
/// takedown succeeds is refused, not stored: the check sits in the write.
#[test]
fn an_attachment_waiting_on_its_parent_body_is_refused_after_a_takedown() {
    let (config, _) = policy("attachment-parent-race");
    let attachment = record("image/png", b"taken down while the parent body is read");
    let hash = hx(&attachment);
    let body = record(
        "text/plain",
        format!("a live parent record:{hash}").as_bytes(),
    );
    with_server(
        "takedown-attachment-parent-race",
        Some(Node {
            head: 100.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
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
            let store = state.store.as_ref().unwrap();
            store.bodies.put(body).await.unwrap();
            // A held read, then a write waiting behind it: the upload's read
            // of its parent body waits too.
            let hold = store.bodies.hold_writes_for_test().await;
            let writer = store
                .bodies
                .put(record("text/plain", b"a write that waits"));
            tokio::pin!(writer);
            assert!(futures::poll!(&mut writer).is_pending());
            assert!(store.bodies.writer_waiting_for_test());
            let digest = Digest::from_hex(&hash).unwrap();
            let upload = crate::publish::accept(
                &state,
                store,
                crate::publish::PublishParams::by_message(31337, 1),
                digest,
                attachment.into(),
            );
            tokio::pin!(upload);
            assert!(
                futures::poll!(&mut upload).is_pending(),
                "reading the parent body"
            );
            assert_eq!(
                take_down(&client, &base, &admin().0, 0, "add", &hash)
                    .await
                    .status(),
                200
            );
            drop(hold);
            writer.await.unwrap();
            let refused = upload.await.err().expect("refused");
            assert_eq!(refused.code, "taken_down");
            let (attachments, _) = store.attachments.as_ref().unwrap();
            assert!(
                attachments.get(&digest).await.unwrap().is_none(),
                "not stored"
            );
        },
    );
}

/// A held upload taken down before its post is accepted leaves holding on the
/// next pass, rather than keeping its slot until it expires.
#[test]
fn held_content_is_discarded_on_the_next_pass_after_a_takedown() {
    let (config, _) = policy("held-discard");
    let body = record("text/plain", b"held, then taken down before confirmation");
    let hash = hx(&body);
    with_server(
        "takedown-held-discard",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
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
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let held = publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(held.status(), 202);
            assert_eq!(
                take_down(&client, &base, &admin().0, 0, "add", &hash)
                    .await
                    .status(),
                200
            );
            // The post is still not accepted: no promotion is coming.
            assert_eq!(promote_once(&state).await, 0);
            let store = state.store.as_ref().unwrap();
            let digest = Digest::from_hex(&hash).unwrap();
            assert!(store.holding.get(31337, &digest).is_none(), "discarded");
        },
    );
}
