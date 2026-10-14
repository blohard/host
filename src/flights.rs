//! Bounded single-flight work. Identical keys share one detached task; a
//! cancelled reader cannot cancel the work or free its slot. Callers own
//! timeouts and cache/backoff policy, completing those before returning.
use std::{
    collections::HashMap,
    future::Future,
    hash::Hash,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

type Pending<V> = watch::Receiver<Option<V>>;

#[derive(Debug, PartialEq, Eq)]
pub enum FlightError {
    Busy,
    Stopped,
}

struct Inner<K, V> {
    pending: Mutex<HashMap<K, Pending<V>>>,
    cap: usize,
}

pub struct Flights<K, V> {
    inner: Arc<Inner<K, V>>,
}

impl<K: Eq + Hash + Clone + Send + 'static, V: Clone + Send + Sync + 'static> Flights<K, V> {
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                pending: Mutex::new(HashMap::new()),
                cap,
            }),
        }
    }

    /// Check `ready` under the registration lock before starting distinct
    /// work. A completing worker has published its cache/backoff first, so
    /// a new reader cannot miss both the old flight and its remembered result.
    pub async fn run(
        &self,
        key: K,
        ready: impl FnOnce() -> Option<V>,
        work: impl Future<Output = V> + Send + 'static,
    ) -> Result<V, FlightError> {
        let mut rx = {
            let mut pending = self
                .inner
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(rx) = pending.get(&key) {
                rx.clone()
            } else {
                if let Some(value) = ready() {
                    return Ok(value);
                }
                if pending.len() >= self.inner.cap {
                    return Err(FlightError::Busy);
                }
                let (tx, rx) = watch::channel(None);
                pending.insert(key.clone(), rx.clone());
                let completion = Completion {
                    inner: self.inner.clone(),
                    key,
                };
                tokio::spawn(async move {
                    // Drop removes the key even when the worker panics. Its
                    // sender then closes and all waiters receive Stopped.
                    let completion = completion;
                    let result = work.await;
                    let _ = tx.send(Some(result));
                    // Publish before removal: a racing reader either joins
                    // this completed flight or checks the caller's cache.
                    drop(completion);
                });
                rx
            }
        };
        loop {
            if let Some(value) = rx.borrow_and_update().clone() {
                return Ok(value);
            }
            if rx.changed().await.is_err() {
                return Err(FlightError::Stopped);
            }
        }
    }
}

struct Completion<K: Eq + Hash, V> {
    inner: Arc<Inner<K, V>>,
    key: K,
}

impl<K: Eq + Hash, V> Drop for Completion<K, V> {
    fn drop(&mut self) {
        self.inner
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    #[tokio::test]
    async fn cancellation_keeps_capacity_and_identical_readers_share_completion() {
        let flights = Arc::new(Flights::new(1));
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let first = {
            let (flights, started, release) = (flights.clone(), started.clone(), release.clone());
            tokio::spawn(async move {
                flights
                    .run(1, || None, async move {
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                        42
                    })
                    .await
            })
        };
        started.acquire().await.unwrap().forget();
        first.abort();
        assert_eq!(
            flights.run(2, || None, async { 0 }).await,
            Err(FlightError::Busy)
        );
        let second = flights.run(1, || None, async { panic!("duplicate worker") });
        tokio::pin!(second);
        assert!(futures::poll!(&mut second).is_pending());
        release.add_permits(1);
        assert_eq!(second.await, Ok(42));
        assert_eq!(flights.run(2, || None, async { 7 }).await, Ok(7));
    }

    #[tokio::test]
    async fn completion_publishes_policy_before_releasing_the_key_and_panics_remove_it() {
        let flights = Flights::new(1);
        let cache = Arc::new(AtomicUsize::new(0));
        let publish = cache.clone();
        assert_eq!(
            flights
                .run(1, || None, async move {
                    publish.store(9, Ordering::SeqCst);
                    9
                })
                .await,
            Ok(9)
        );
        assert_eq!(
            flights
                .run(1, || Some(cache.load(Ordering::SeqCst)), async {
                    panic!("cached result must win")
                })
                .await,
            Ok(9)
        );
        assert_eq!(
            flights
                .run(2, || None, async { panic!("worker failed") })
                .await,
            Err(FlightError::Stopped)
        );
        assert_eq!(flights.run(2, || None, async { 10 }).await, Ok(10));
    }
}
