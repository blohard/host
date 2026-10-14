//! Bounded share-card work (share.rs). Cache hits bypass client limits; misses share one build.
use crate::token_bucket::Bucket;
use axum::{
    body::{Body, Bytes, to_bytes},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use std::{
    collections::HashMap,
    future::Future,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

struct Cached {
    headers: HeaderMap,
    body: Bytes,
}
impl Cached {
    fn response(&self) -> Response {
        let mut response = Response::new(Body::from(self.body.clone()));
        *response.headers_mut() = self.headers.clone();
        response
    }
}
struct Client {
    tokens: Bucket,
    active: Arc<Semaphore>,
}

type Entry = (Instant, Arc<OnceCell<Cached>>);

/// A response extension asking for the response to be served but not cached:
/// something it shows, such as a name, could not be looked up in time.
#[derive(Clone, Copy)]
pub struct Uncached;

pub struct Cache {
    active: Arc<Semaphore>,
    clients: Mutex<HashMap<IpAddr, Client>>,
    entries: Mutex<HashMap<String, Entry>>,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            active: Arc::new(Semaphore::new(8)),
            clients: Mutex::new(HashMap::new()),
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl Cache {
    fn admit(&self, ip: IpAddr) -> Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let ip = crate::visitor::group(ip);
        let mut clients = self.clients.lock().expect("share card clients");
        if !clients.contains_key(&ip) && clients.len() >= 1024 {
            clients.retain(|_, c| {
                c.tokens.last_refill().elapsed() < Duration::from_secs(300)
                    || c.active.available_permits() != 2
            });
            if clients.len() >= 1024 {
                return None;
            }
        }
        let client = clients.entry(ip).or_insert_with(|| Client {
            tokens: Bucket::new(4.0, 0.5, Instant::now()),
            active: Arc::new(Semaphore::new(2)),
        });
        let now = Instant::now();
        client.tokens.refill(now);
        if !client.tokens.available() {
            return None;
        }
        let local = client.active.clone().try_acquire_owned().ok()?;
        let global = self.active.clone().try_acquire_owned().ok()?;
        // The same lock and instant cover the check and both slot acquisitions.
        client.tokens.take(now).then_some((local, global))
    }
    #[cfg(test)]
    pub fn exhaust(&self) -> OwnedSemaphorePermit {
        self.active.clone().try_acquire_many_owned(8).unwrap()
    }
    /// None means serve the generic app shell. Only successful cards
    /// enter the cache; overload, temporary failures and [`Uncached`]
    /// responses cannot poison it.
    pub async fn get(
        &self,
        key: String,
        ip: IpAddr,
        build: impl Future<Output = Response>,
    ) -> Option<Response> {
        let cell = {
            let mut entries = self.entries.lock().expect("share card cache");
            if entries
                .get(&key)
                .is_some_and(|(at, _)| at.elapsed() >= Duration::from_secs(60))
            {
                entries.remove(&key);
            }
            if !entries.contains_key(&key) && entries.len() >= 256 {
                entries.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(60));
                // Do not evict another client's in-flight work to admit a flood.
                if entries.len() >= 256 {
                    return None;
                }
            }
            entries
                .entry(key.clone())
                .or_insert_with(|| (Instant::now(), Arc::new(OnceCell::new())))
                .1
                .clone()
        };
        match cell
            .get_or_try_init(|| async {
                let _permits = self.admit(ip).ok_or(None)?;
                let response = build.await;
                if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                    return Err(None);
                }
                if response.status() != StatusCode::OK
                    || response.extensions().get::<Uncached>().is_some()
                {
                    return Err(Some(response));
                }
                let (parts, body) = response.into_parts();
                let body = to_bytes(body, 256 * 1024).await.map_err(|_| None)?;
                Ok(Cached {
                    headers: parts.headers,
                    body,
                })
            })
            .await
        {
            Ok(cached) => Some(cached.response()),
            Err(response) => {
                let mut entries = self.entries.lock().expect("share card cache");
                if entries
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(&current.1, &cell))
                {
                    entries.remove(&key);
                }
                response
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Addresses in one IPv6 /64 are one visitor, with one entry and one
    /// allowance, as in the host's other visitor limits.
    #[test]
    fn one_ipv6_64_is_one_visitor() {
        let cache = Cache::default();
        let held: Vec<_> = ["2001:db8::1", "2001:db8::2"]
            .map(|ip| cache.admit(ip.parse().unwrap()).expect("a build slot"))
            .into();
        assert!(
            cache.admit("2001:db8::3".parse().unwrap()).is_none(),
            "the /64's two build slots are taken"
        );
        assert_eq!(cache.clients.lock().unwrap().len(), 1);
        assert!(
            cache.admit("2001:db8:0:1::1".parse().unwrap()).is_some(),
            "another /64 is another visitor"
        );
        drop(held);
    }
    #[tokio::test]
    async fn identical_builds_are_coalesced_and_cancelled_entries_expire() {
        let cache = Cache::default();
        let ip = "192.0.2.1".parse().unwrap();
        let count = std::sync::atomic::AtomicUsize::new(0);
        let build = || async {
            count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::task::yield_now().await;
            Response::new(Body::from("card"))
        };
        let (a, b) = tokio::join!(
            cache.get("same".into(), ip, build()),
            cache.get("same".into(), ip, build())
        );
        assert!(a.is_some() && b.is_some());
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
        {
            let mut entries = cache.entries.lock().unwrap();
            entries.clear();
            for n in 0..256 {
                entries.insert(
                    n.to_string(),
                    (
                        Instant::now() - Duration::from_secs(61),
                        Arc::new(OnceCell::new()),
                    ),
                );
            }
        }
        assert!(cache.get("new".into(), ip, build()).await.is_some());
        assert_eq!(cache.entries.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn cancelled_builds_release_global_and_client_permits() {
        let cache = Arc::new(Cache::default());
        let worker = cache.clone();
        let ip = "192.0.2.1".parse().unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            worker
                .get("cancelled".into(), ip, async move {
                    started.send(()).unwrap();
                    std::future::pending::<Response>().await
                })
                .await
        });
        ready.await.unwrap();
        assert_eq!(cache.active.available_permits(), 7);
        task.abort();
        let _ = task.await;
        assert_eq!(cache.active.available_permits(), 8);
        assert_eq!(
            cache.clients.lock().unwrap()[&ip]
                .active
                .available_permits(),
            2
        );
        assert!(
            cache
                .get("cancelled".into(), ip, async {
                    Response::new(Body::from("retry"))
                })
                .await
                .is_some()
        );
    }

    #[test]
    fn refused_slots_do_not_spend_a_share_pages_tokens() {
        let cache = Cache::default();
        let ip = "192.0.2.1".parse().unwrap();
        let global = cache.exhaust();
        for _ in 0..32 {
            assert!(cache.admit(ip).is_none());
        }
        drop(global);
        let local = [cache.admit(ip).unwrap(), cache.admit(ip).unwrap()];
        for _ in 0..32 {
            assert!(cache.admit(ip).is_none());
        }
        drop(local);
        // Only the two admitted builds spent tokens; both refused paths kept them.
        assert!(cache.admit(ip).is_some());
        assert!(cache.admit(ip).is_some());
        assert!(cache.admit(ip).is_none());
    }

    #[test]
    fn one_client_is_rate_limited_without_exhausting_others() {
        let cache = Cache::default();
        let ip = "192.0.2.1".parse().unwrap();
        for _ in 0..4 {
            drop(cache.admit(ip).unwrap());
        }
        assert!(cache.admit(ip).is_none());
        assert!(cache.admit("192.0.2.2".parse().unwrap()).is_some());
    }
    #[tokio::test]
    async fn a_single_client_cannot_occupy_every_slot_and_cache_hits_need_no_slot() {
        let cache = Cache::default();
        let a = "192.0.2.1".parse().unwrap();
        let b = "192.0.2.2".parse().unwrap();
        let _one = cache.admit(a).unwrap();
        let _two = cache.admit(a).unwrap();
        assert!(cache.admit(a).is_none());
        assert!(cache.admit(b).is_some());
        assert!(
            cache
                .get("blocked".into(), a, async { panic!("must not build") })
                .await
                .is_none()
        );
        assert!(
            cache
                .get("ready".into(), b, async {
                    Response::new(Body::from("card"))
                })
                .await
                .is_some()
        );
        let _all = cache.active.clone().try_acquire_many_owned(6).unwrap();
        assert!(
            cache
                .get("ready".into(), b, async { panic!("must use cache") })
                .await
                .is_some()
        );
        assert!(
            cache
                .get("other".into(), b, async { panic!("must not build") })
                .await
                .is_none()
        );
    }
}
