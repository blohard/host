//! Uploads arrive before they take a slot, and local content bypasses peer
//! cooldowns.
use super::*;
use crate::publish::http::{
    FIRST_ALLOCATION, LARGE_MEMORY, MAX_SMALL_UPLOADS, MAX_UPLOADS, SMALL_MEMORY,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// An upload whose bytes are all in but whose pool has no free slot is
/// refused at once, rather than queued with its bytes, and its memory is free.
#[test]
fn a_finished_upload_without_a_free_slot_is_refused_at_once() {
    with_server(
        "upload-door",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let large = &state.store.as_ref().unwrap().uploads.large;
            let held: Vec<_> = (0..MAX_UPLOADS)
                .map(|_| large.slots.clone().try_acquire_owned().unwrap())
                .collect();
            let image = record("image/png", &[1; 6000]);
            let status = tokio::time::timeout(
                Duration::from_secs(1),
                upload_status(&base, image.len(), &image),
            )
            .await
            .expect("not queued");
            assert!(status.starts_with("HTTP/1.1 503"), "{status}");
            free_memory_is(&large.memory, LARGE_MEMORY, "its memory is free").await;
            drop(held);
        },
    );
}

/// Starts an upload declaring `declared` bytes, with any `extra` header lines.
async fn upload_request(base: &str, declared: usize, extra: &str) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(base.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    let head = format!(
        "POST /api/publish HTTP/1.1\r\nHost: localhost\r\nContent-Length: {declared}\r\n{extra}Connection: close\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
}

/// Sends an upload declaring `declared` bytes, then `body`, and returns the
/// status line; the connection stays open until the host answers.
async fn upload_status(base: &str, declared: usize, body: &[u8]) -> String {
    upload_status_from(base, declared, body, "").await
}

async fn upload_status_from(base: &str, declared: usize, body: &[u8], extra: &str) -> String {
    let mut stream = upload_request(base, declared, extra).await;
    stream.write_all(body).await.unwrap();
    status_of(stream).await
}

/// The status line of an answer, read with a five-second limit.
async fn status_of(stream: tokio::net::TcpStream) -> String {
    let mut status = String::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(stream).read_line(&mut status),
    )
    .await
    .unwrap()
    .unwrap();
    status
}

/// Waits until `memory` has exactly `free` bytes unclaimed.
async fn free_memory_is(memory: &tokio::sync::Semaphore, free: usize, what: &str) {
    wait_until(Duration::from_secs(2), what, || {
        memory.available_permits() == free
    })
    .await;
}

#[test]
fn a_message_body_gets_in_while_attachments_fill_their_pool() {
    with_server(
        "upload-lanes",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let uploads = &state.store.as_ref().unwrap().uploads;
            let held: Vec<_> = (0..MAX_UPLOADS)
                .map(|_| uploads.large.slots.clone().try_acquire_owned().unwrap())
                .collect();
            // Every attachment slot is taken: a large upload is turned away...
            let image = record("image/png", &[2; 6000]);
            let large = upload_status(&base, image.len(), &image).await;
            assert!(large.starts_with("HTTP/1.1 503"), "{large}");
            // ...while a message body, no larger than the body cap, still gets in.
            let body = record("text/plain", b"a post while images upload");
            let small = upload_status(&base, body.len(), &body).await;
            assert!(small.starts_with("HTTP/1.1 200"), "{small}");
            assert_eq!(uploads.small.slots.available_permits(), MAX_SMALL_UPLOADS);
            drop(held);
        },
    );
}

#[test]
fn two_slow_images_from_one_network_leave_its_message_bodies_free() {
    with_server(
        "visitor-lanes",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let visitor = "X-Forwarded-For: 203.0.113.7\r\n";
            // Two attachments from one network stall mid-body, holding that
            // visitor's attachment lanes...
            let mut images = Vec::new();
            for _ in 0..2 {
                let mut image = upload_request(&base, 1_000_000, visitor).await;
                image.write_all(b"i").await.unwrap();
                images.push(image);
            }
            let large = &state.store.as_ref().unwrap().uploads.large;
            free_memory_is(
                &large.memory,
                LARGE_MEMORY - 2 * FIRST_ALLOCATION,
                "both images are arriving",
            )
            .await;
            // ...while a message body from the same network gets in at once.
            let body = record("text/plain", b"a post from the same wifi");
            let text = tokio::time::timeout(
                Duration::from_secs(1),
                upload_status_from(&base, body.len(), &body, visitor),
            )
            .await
            .expect("not queued behind the images");
            assert!(text.starts_with("HTTP/1.1 200"), "{text}");
            drop(images);
        },
    );
}

/// A small body that stalls holds only its memory, never a slot, and is
/// refused for arriving too slowly, freeing that memory.
#[test]
fn a_small_body_that_stalls_is_refused_and_frees_its_memory() {
    with_server(
        "small-upload-stall",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let small = &state.store.as_ref().unwrap().uploads.small;
            let mut stalled = upload_request(&base, 100, "").await;
            stalled.write_all(b"s").await.unwrap();
            free_memory_is(
                &small.memory,
                SMALL_MEMORY - 100,
                "the stalled body is charged",
            )
            .await;
            assert_eq!(small.slots.available_permits(), MAX_SMALL_UPLOADS);
            let status = status_of(stalled).await;
            assert!(status.starts_with("HTTP/1.1 408"), "{status}");
            free_memory_is(&small.memory, SMALL_MEMORY, "its memory is free").await;
        },
    );
}

#[test]
fn reader_and_sweep_share_peer_backoff_but_a_local_upload_is_immediately_visible() {
    let body = record("text/plain", b"a body that arrives later");
    let hash = hx(&body);
    // A peer that has nothing.
    let peer = FakePeer::start([]);
    let peer_url = peer.url.clone();
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "peer-shared-backoff",
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
            let url = format!("{base}/api/content/{hash}?chain_id=31337&message_id=1");
            assert_eq!(client.get(&url).send().await.unwrap().status(), 404);
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let mut sweep = crate::index::bodies::Sweep::default();
            for _ in 0..3 {
                sweep.run(&state, 31337, index).await.unwrap();
                assert_eq!(client.get(&url).send().await.unwrap().status(), 404);
            }
            assert_eq!(peer.total_hits(), 1);
            let response =
                publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
            let response = client.get(&url).send().await.unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.bytes().await.unwrap().as_ref(), body.as_slice());
            sweep.run(&state, 31337, index).await.unwrap();
            assert_eq!(
                index.read_ready(|model| model.references.pending_bodies.len()),
                Some(0)
            );
            assert_eq!(peer.total_hits(), 1);
        },
    );
}

/// Starts an upload that declares no length; its body follows in chunks.
async fn chunked_request(base: &str) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(base.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    let head = "POST /api/publish HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
}

/// An upload of `body` that declares no length, sent in one chunk.
async fn chunked_upload(base: &str, body: &[u8]) -> tokio::net::TcpStream {
    let mut stream = chunked_request(base).await;
    stream
        .write_all(format!("{:x}\r\n", body.len()).as_bytes())
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();
    stream.write_all(b"\r\n0\r\n\r\n").await.unwrap();
    stream
}

/// An upload's bytes stay charged to its pool's memory until its write is
/// done, not only while they arrive. While one waits behind the store, an
/// upload that needs more than what is left is refused rather than buffered.
#[test]
fn an_upload_keeps_its_memory_until_its_write_is_done() {
    with_server(
        "upload-memory-held",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let store = state.store.as_ref().unwrap();
            let memory = &store.uploads.small.memory;
            let body = record("text/plain", b"waiting for the store");
            let writes = store.bodies.hold_writes_for_test().await;
            let mut waiting = upload_request(&base, body.len(), "").await;
            waiting.write_all(&body).await.unwrap();
            free_memory_is(memory, SMALL_MEMORY - body.len(), "its bytes are charged").await;
            let rest = memory
                .clone()
                .try_acquire_many_owned((SMALL_MEMORY - body.len() - 10) as u32)
                .unwrap();
            let other = record("text/plain", b"a post that does not fit");
            let refused = upload_status(&base, other.len(), &other).await;
            assert!(refused.starts_with("HTTP/1.1 503"), "{refused}");
            drop(rest);
            assert_eq!(memory.available_permits(), SMALL_MEMORY - body.len());
            drop(writes);
            let status = status_of(waiting).await;
            assert!(status.starts_with("HTTP/1.1 200"), "{status}");
            free_memory_is(memory, SMALL_MEMORY, "the memory is whole").await;
        },
    );
}

/// A client that leaves after sending its bytes does not take their charge
/// with it: the write it started still holds that memory until it is done.
#[test]
fn an_upload_whose_client_leaves_keeps_its_memory_until_its_write_is_done() {
    with_server(
        "upload-memory-detached",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let store = state.store.as_ref().unwrap();
            let memory = &store.uploads.small.memory;
            let body = record("text/plain", b"stored after its client left");
            let writes = store.bodies.hold_writes_for_test().await;
            let mut leaving = upload_request(&base, body.len(), "").await;
            leaving.write_all(&body).await.unwrap();
            free_memory_is(memory, SMALL_MEMORY - body.len(), "its bytes are charged").await;
            drop(leaving);
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(memory.available_permits(), SMALL_MEMORY - body.len());
            drop(writes);
            free_memory_is(memory, SMALL_MEMORY, "the memory is whole").await;
            let digest = keccak::hash(&[body.as_slice()]);
            assert_eq!(store.bodies.get(&digest).await.unwrap(), Some(body));
        },
    );
}

/// An upload still waiting at its deadline is told to try again, and its
/// memory is free at once, while the store is still busy.
#[test]
fn an_upload_past_its_deadline_frees_its_memory() {
    with_server(
        "upload-memory-deadline",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let store = state.store.as_ref().unwrap();
            let memory = &store.uploads.small.memory;
            let body = record("text/plain", b"never written in time");
            let writes = store.bodies.hold_writes_for_test().await;
            let late = upload_status(&base, body.len(), &body).await;
            assert!(late.starts_with("HTTP/1.1 503"), "{late}");
            free_memory_is(memory, SMALL_MEMORY, "the memory is whole").await;
            drop(writes);
        },
    );
}

/// A body sent a byte at a time is copied into one buffer that never grows
/// past the length the upload declared, and is charged exactly that.
#[test]
fn a_body_sent_in_tiny_pieces_is_charged_its_declared_length() {
    with_server(
        "upload-memory-pieces",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let store = state.store.as_ref().unwrap();
            let memory = &store.uploads.small.memory;
            let body = record("text/plain", &[b'x'; 3000]);
            let writes = store.bodies.hold_writes_for_test().await;
            let mut stream = upload_request(&base, body.len(), "").await;
            stream.set_nodelay(true).unwrap();
            for byte in &body {
                stream.write_all(std::slice::from_ref(byte)).await.unwrap();
                tokio::task::yield_now().await;
            }
            wait_until(Duration::from_secs(5), "the whole body is charged", || {
                memory.available_permits() == SMALL_MEMORY - body.len()
            })
            .await;
            drop(writes);
            let status = status_of(stream).await;
            assert!(status.starts_with("HTTP/1.1 200"), "{status}");
            free_memory_is(memory, SMALL_MEMORY, "the memory is whole").await;
        },
    );
}

/// An upload that declares no length takes the large pool and its memory,
/// whatever it turns out to be, starting with one first allocation.
#[test]
fn an_upload_without_a_declared_length_is_charged_to_the_large_pool() {
    with_server(
        "upload-memory-chunked",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let store = state.store.as_ref().unwrap();
            let uploads = &store.uploads;
            let body = record("text/plain", b"no length declared");
            let writes = store.bodies.hold_writes_for_test().await;
            let stream = chunked_upload(&base, &body).await;
            free_memory_is(
                &uploads.large.memory,
                LARGE_MEMORY - FIRST_ALLOCATION,
                "the large pool is charged",
            )
            .await;
            assert_eq!(uploads.small.memory.available_permits(), SMALL_MEMORY);
            drop(writes);
            let status = status_of(stream).await;
            assert!(status.starts_with("HTTP/1.1 200"), "{status}");
            free_memory_is(&uploads.large.memory, LARGE_MEMORY, "the memory is whole").await;
        },
    );
}

/// Too much is refused as too large, and what it was charged on the way is
/// given back: up front when the declared length is over, as it arrives when
/// no length was declared.
#[test]
fn an_oversized_upload_is_refused_and_gives_back_its_memory() {
    with_server(
        "upload-memory-oversized",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let uploads = &state.store.as_ref().unwrap().uploads;
            let limit = state.max_upload() + 1024;
            let declared = upload_status(&base, limit + 1, b"").await;
            assert!(declared.starts_with("HTTP/1.1 413"), "{declared}");
            let mut stream = chunked_request(&base).await;
            let piece = vec![b'x'; 64 << 10];
            for _ in 0..=(limit / piece.len()) {
                let chunk = [format!("{:x}\r\n", piece.len()).as_bytes(), &piece, b"\r\n"].concat();
                if stream.write_all(&chunk).await.is_err() {
                    break;
                }
            }
            let status = status_of(stream).await;
            assert!(status.starts_with("HTTP/1.1 413"), "{status}");
            wait_until(Duration::from_secs(2), "the memory is whole", || {
                uploads.large.memory.available_permits() == LARGE_MEMORY
                    && uploads.small.memory.available_permits() == SMALL_MEMORY
            })
            .await;
        },
    );
}

/// With a body cap above the default, a large body is still small: charged
/// exactly its length to the small pool, never more.
#[test]
fn a_larger_body_cap_charges_bodies_their_length() {
    with_server(
        "upload-memory-body-cap",
        None,
        |_| Setup {
            max_body_bytes: 64 << 10,
            ..Setup::default()
        },
        |Server { base, state, .. }| async move {
            let store = state.store.as_ref().unwrap();
            let memory = &store.uploads.small.memory;
            let body = record("text/plain", &vec![b'y'; 50_000]);
            let writes = store.bodies.hold_writes_for_test().await;
            let mut stream = upload_request(&base, body.len(), "").await;
            stream.write_all(&body).await.unwrap();
            free_memory_is(memory, SMALL_MEMORY - body.len(), "the body is charged").await;
            drop(writes);
            let status = status_of(stream).await;
            assert!(status.starts_with("HTTP/1.1 200"), "{status}");
        },
    );
}

/// Attachments using up their memory leave message bodies untouched.
#[test]
fn bodies_get_in_while_attachment_memory_is_used_up() {
    with_server(
        "upload-memory-separate",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let uploads = &state.store.as_ref().unwrap().uploads;
            let used = uploads
                .large
                .memory
                .clone()
                .try_acquire_many_owned((LARGE_MEMORY - 100) as u32)
                .unwrap();
            let image = record("image/png", &[7; 6000]);
            let refused = upload_status(&base, image.len(), &image).await;
            assert!(refused.starts_with("HTTP/1.1 503"), "{refused}");
            let body = record("text/plain", b"a post while images wait");
            let text = upload_status(&base, body.len(), &body).await;
            assert!(text.starts_with("HTTP/1.1 200"), "{text}");
            assert_eq!(uploads.large.memory.available_permits(), 100);
            drop(used);
        },
    );
}

/// Starts `count` uploads from each of `networks` networks, each declaring
/// `declared` bytes and sending one: senders as slow as they come.
async fn slow_senders(
    base: &str,
    networks: u8,
    count: usize,
    declared: usize,
) -> Vec<tokio::net::TcpStream> {
    let mut streams = Vec::new();
    for network in 1..=networks {
        let visitor = format!("X-Forwarded-For: 198.51.100.{network}\r\n");
        for _ in 0..count {
            let mut stream = upload_request(base, declared, &visitor).await;
            stream.write_all(b"v").await.unwrap();
            streams.push(stream);
        }
    }
    streams
}

/// Slow senders from many networks, two bodies each, hold no shared slot: an
/// ordinary post's body from elsewhere is stored at once.
#[test]
fn slow_bodies_from_many_networks_leave_room_for_an_ordinary_post() {
    with_server(
        "slow-bodies",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let small = &state.store.as_ref().unwrap().uploads.small;
            let slow = slow_senders(&base, 20, 2, 4000).await;
            free_memory_is(
                &small.memory,
                SMALL_MEMORY - 40 * 4000,
                "every slow body is charged",
            )
            .await;
            assert_eq!(small.slots.available_permits(), MAX_SMALL_UPLOADS);
            let body = record("text/plain", b"an ordinary post");
            let status = tokio::time::timeout(
                Duration::from_secs(1),
                upload_status_from(&base, body.len(), &body, "X-Forwarded-For: 192.0.2.1\r\n"),
            )
            .await
            .expect("answered at once");
            assert!(status.starts_with("HTTP/1.1 200"), "{status}");
            drop(slow);
        },
    );
}

/// The same for attachments: slow ones from many networks hold no shared
/// slot, and an ordinary attachment from elsewhere is stored at once.
#[test]
fn slow_attachments_from_many_networks_leave_room_for_an_ordinary_one() {
    with_server(
        "slow-attachments",
        None,
        |_| Setup::default(),
        |Server { base, state, .. }| async move {
            let large = &state.store.as_ref().unwrap().uploads.large;
            let slow = slow_senders(&base, 20, 2, 1_000_000).await;
            free_memory_is(
                &large.memory,
                LARGE_MEMORY - 40 * FIRST_ALLOCATION,
                "every slow attachment is charged",
            )
            .await;
            assert_eq!(large.slots.available_permits(), MAX_UPLOADS);
            let image = record("image/png", &[3; 6000]);
            let status = tokio::time::timeout(
                Duration::from_secs(1),
                upload_status_from(&base, image.len(), &image, "X-Forwarded-For: 192.0.2.1\r\n"),
            )
            .await
            .expect("answered at once");
            assert!(status.starts_with("HTTP/1.1 200"), "{status}");
            drop(slow);
        },
    );
}
