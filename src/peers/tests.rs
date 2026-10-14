use super::*;
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use futures::{future::join_all, poll};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Semaphore, mpsc};

struct Peer {
    url: String,
    hits: mpsc::UnboundedReceiver<()>,
    count: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    available: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn new(body: &'static [u8]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, hits) = mpsc::unbounded_channel();
        let count = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let available = Arc::new(AtomicBool::new(true));
        let (calls, gate, serves) = (count.clone(), release.clone(), available.clone());
        let app = Router::new().route(
            "/api/content/{hash}",
            get(move || {
                let (tx, calls, gate, serves) =
                    (tx.clone(), calls.clone(), gate.clone(), serves.clone());
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tx.send(()).unwrap();
                    gate.acquire().await.unwrap().forget();
                    if serves.load(Ordering::SeqCst) {
                        body.into_response()
                    } else {
                        StatusCode::NOT_FOUND.into_response()
                    }
                }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            url,
            hits,
            count,
            release,
            available,
            task,
        }
    }
    async fn hit(&mut self) {
        tokio::time::timeout(Duration::from_secs(2), self.hits.recv())
            .await
            .unwrap()
            .unwrap();
    }
    fn reader(&self) -> Peers {
        self.reader_capped(1024)
    }
    fn reader_capped(&self, cap: usize) -> Peers {
        Peers::new(vec![self.url.clone()], cap, reqwest::Client::new())
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn unsuccessful_lookups_back_off_to_the_cap_and_success_resets_them() {
    let mut progress = Progress::default();
    let mut now = Instant::now();
    for attempt in 0..20 {
        progress.completed(false, true, now);
        let delay = (RETRY_MIN * (1 << attempt)).min(RETRY_MAX);
        assert_eq!(progress.after, now + delay);
        assert_eq!(progress.delay, delay);
        now += delay;
    }
    progress.completed(true, false, now);
    assert_eq!(progress.delay, Duration::ZERO);
    assert_eq!(progress.after, now);
    progress.completed(false, true, now);
    assert_eq!(progress.delay, RETRY_MIN);
}

#[tokio::test]
async fn identical_reads_share_bytes_and_a_cancelled_reader_does_not_cancel_them() {
    let mut peer = Peer::new(b"one body").await;
    let peers = peer.reader();
    let hash = keccak::hash(&[b"one body"]);
    let mut first = Box::pin(peers.fetch(&hash, crate::store::Kind::Bodies));
    assert!(poll!(first.as_mut()).is_pending());
    peer.hit().await;
    drop(first);
    let mut rest = Box::pin(join_all(
        (0..15).map(|_| peers.fetch(&hash, crate::store::Kind::Bodies)),
    ));
    assert!(poll!(rest.as_mut()).is_pending());
    peer.release.add_permits(1);
    for result in rest.await {
        let (bytes, source) = result.unwrap().unwrap();
        assert_eq!(bytes.as_ref(), b"one body");
        assert_eq!(source, peer.url);
    }
    assert_eq!(peer.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn full_pool_refuses_new_hashes_but_still_shares_existing_work() {
    let mut peer = Peer::new(b"body").await;
    peer.available.store(false, Ordering::SeqCst);
    let peers = peer.reader();
    let hashes: Vec<_> = (0..MAX_ACTIVE)
        .map(|i| keccak::hash(&[&i.to_be_bytes()]))
        .collect();
    let mut requests = Box::pin(join_all(
        hashes
            .iter()
            .map(|h| peers.fetch(h, crate::store::Kind::Bodies)),
    ));
    assert!(poll!(requests.as_mut()).is_pending());
    for _ in 0..MAX_ACTIVE {
        peer.hit().await;
    }
    let new_hash = keccak::hash(&[b"another"]);
    assert!(
        peers
            .fetch(&new_hash, crate::store::Kind::Bodies)
            .await
            .is_err()
    );
    let mut duplicate = Box::pin(peers.fetch(&hashes[0], crate::store::Kind::Bodies));
    assert!(poll!(duplicate.as_mut()).is_pending());
    assert_eq!(peer.count.load(Ordering::SeqCst), MAX_ACTIVE);
    peer.release.add_permits(MAX_ACTIVE);
    assert!(duplicate.await.unwrap().is_none());
    assert!(requests.await.into_iter().all(|r| r.unwrap().is_none()));
    let mut retry = Box::pin(peers.fetch(&new_hash, crate::store::Kind::Bodies));
    assert!(poll!(retry.as_mut()).is_pending());
    peer.hit().await;
    peer.release.add_permits(1);
    assert!(retry.await.unwrap().is_none());
}

#[tokio::test]
async fn a_completed_miss_backs_off_for_every_caller() {
    let mut peer = Peer::new(b"body").await;
    let peers = peer.reader();
    let hash = keccak::hash(&[b"body"]);
    peer.available.store(false, Ordering::SeqCst);
    let mut missing = Box::pin(peers.fetch(&hash, crate::store::Kind::Bodies));
    assert!(poll!(missing.as_mut()).is_pending());
    peer.hit().await;
    peer.release.add_permits(1);
    assert!(missing.await.unwrap().is_none());
    peer.available.store(true, Ordering::SeqCst);
    for _ in 0..20 {
        assert!(
            peers
                .fetch(&hash, crate::store::Kind::Bodies)
                .await
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        peer.count.load(Ordering::SeqCst),
        1,
        "both callers share the cooldown"
    );
    peers.retry_now(hash);
    let mut retry = Box::pin(peers.fetch(&hash, crate::store::Kind::Bodies));
    assert!(poll!(retry.as_mut()).is_pending());
    peer.hit().await;
    peer.release.add_permits(1);
    assert_eq!(retry.await.unwrap().unwrap().0.as_ref(), b"body");
    assert_eq!(peer.count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_answer_over_the_byte_limit_is_ignored() {
    let mut peer = Peer::new(b"body").await;
    let peers = peer.reader_capped(3);
    let hash = keccak::hash(&[b"body"]);
    let mut fetch = Box::pin(peers.fetch(&hash, crate::store::Kind::Bodies));
    assert!(poll!(fetch.as_mut()).is_pending());
    peer.hit().await;
    peer.release.add_permits(1);
    assert!(
        fetch.await.unwrap().is_none(),
        "four bytes over a three-byte limit"
    );
}

#[tokio::test]
async fn a_timed_out_fetch_releases_its_slot_after_its_reader_leaves() {
    let mut peer = Peer::new(b"body").await;
    let peers = peer.reader();
    let hash = keccak::hash(&[b"body"]);
    let mut first = Box::pin(peers.fetch(&hash, crate::store::Kind::Bodies));
    assert!(poll!(first.as_mut()).is_pending());
    peer.hit().await;
    drop(first);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(7),
            peers.fetch(&hash, crate::store::Kind::Bodies)
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert!(
        peers
            .fetch(&hash, crate::store::Kind::Bodies)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        peer.count.load(Ordering::SeqCst),
        1,
        "a detached failure still backs off"
    );
    peers.retry_now(hash);
    let mut retry = Box::pin(peers.fetch(&hash, crate::store::Kind::Bodies));
    assert!(poll!(retry.as_mut()).is_pending());
    peer.hit().await;
    // The disconnected server handler may still be waiting too.
    peer.release.add_permits(2);
    assert_eq!(retry.await.unwrap().unwrap().0.as_ref(), b"body");
    assert_eq!(peer.count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "slow: waits for the 30-second peer fetch deadline"]
async fn retry_resumes_past_peers_that_exhausted_the_previous_budget() {
    let slow = Peer::new(b"body").await;
    let good = Peer::new(b"body").await;
    good.release.add_permits(1);
    // Six unresponsive peers use the entire budget before the healthy seventh.
    let mut urls = vec![slow.url.clone(); 6];
    urls.push(good.url.clone());
    let peers = Peers::new(urls, 1024, reqwest::Client::new());
    let hash = keccak::hash(&[b"body"]);
    assert!(
        peers
            .fetch(&hash, crate::store::Kind::Bodies)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(good.count.load(Ordering::SeqCst), 0);
    // Unrelated work must not move this hash's next peer. Let all the peers
    // answer that other lookup quickly, then make the slow prefix stall again.
    slow.available.store(false, Ordering::SeqCst);
    slow.release.add_permits(12); // Six abandoned handlers plus six new reads.
    good.release.add_permits(1);
    let other = keccak::hash(&[b"different body"]);
    assert!(
        peers
            .fetch(&other, crate::store::Kind::Bodies)
            .await
            .unwrap()
            .is_none()
    );
    slow.release
        .forget_permits(slow.release.available_permits());
    let slow_calls = slow.count.load(Ordering::SeqCst);
    peers.retry_now(hash);
    let (bytes, source) = peers
        .fetch(&hash, crate::store::Kind::Bodies)
        .await
        .unwrap()
        .expect("retry reaches the healthy peer");
    assert_eq!(bytes.as_ref(), b"body");
    assert_eq!(source, good.url);
    assert_eq!(
        slow.count.load(Ordering::SeqCst),
        slow_calls,
        "retry starts beyond the exhausted prefix"
    );
    assert_eq!(good.count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_outage_keeps_the_shortest_backoff_and_only_a_definite_miss_grows_it() {
    let mut peer = Peer::new(b"body").await;
    peer.available.store(false, Ordering::SeqCst);
    peer.release.add_permits(16);
    // A peer that refuses connections, beside one that answers 404.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let down = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let peers = Peers::new(vec![down, peer.url.clone()], 1024, reqwest::Client::new());
    let hash = keccak::hash(&[b"body"]);
    let delay = |peers: &Peers| peers.fetches.progress.get(&hash).unwrap().delay;
    for _ in 0..3 {
        assert!(
            peers
                .fetch(&hash, crate::store::Kind::Bodies)
                .await
                .unwrap()
                .is_none()
        );
        peer.hit().await;
        assert_eq!(delay(&peers), RETRY_MIN, "an outage is not a miss");
        peers.retry_now(hash);
    }
    // Once the record is there, the next lookup finds it.
    peer.available.store(true, Ordering::SeqCst);
    let (bytes, source) = peers
        .fetch(&hash, crate::store::Kind::Bodies)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes.as_ref(), b"body");
    assert_eq!(source, peer.url);

    // Every asked peer answering 404 is a miss, and the backoff grows.
    let mut missing = Peer::new(b"body").await;
    missing.available.store(false, Ordering::SeqCst);
    missing.release.add_permits(2);
    let peers = missing.reader();
    for expected in [RETRY_MIN, RETRY_MIN * 2] {
        assert!(
            peers
                .fetch(&hash, crate::store::Kind::Bodies)
                .await
                .unwrap()
                .is_none()
        );
        missing.hit().await;
        assert_eq!(delay(&peers), expected);
        peers.retry_now(hash);
    }
}
