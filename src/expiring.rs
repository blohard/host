//! Bounded, expiring answers with Clock2Q+ eviction.
use commonware_utils::cache::Cache;
use std::time::Duration;

/// How long an answer from outside this host is kept. A successful answer
/// lasts a day: fewer upstream calls matter more than seeing a change
/// quickly. A negative one (a failure, a partial answer or a refusal) lasts
/// an hour at most, so a passing failure never stands in for a day-long
/// answer. Names and link previews both follow this rule.
pub const SUCCESS_LIFETIME: Duration = Duration::from_secs(24 * 3600);
/// A negative answer's longest lifetime; see [`SUCCESS_LIFETIME`].
pub const NEGATIVE_LIFETIME: Duration = Duration::from_secs(3600);

/// Expiring answers in a fixed-capacity Clock2Q+ cache behind a mutex. A new
/// key starts on probation in a small queue and joins the main ring only once
/// it is asked for again. When full, one entry makes room, so a flood of
/// one-off keys pushes out other one-off keys and leaves alone what readers
/// keep asking for. Nothing is ever cleared wholesale. A poisoned lock yields
/// its data for reuse.
pub struct Expiring<K, V> {
    map: std::sync::Mutex<Cache<K, (std::time::Instant, V)>>,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> Expiring<K, V> {
    pub fn new(cap: usize) -> Self {
        let cap = std::num::NonZeroUsize::new(cap.max(1)).expect("at least one entry");
        Self {
            map: std::sync::Mutex::new(Cache::new(cap)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Cache<K, (std::time::Instant, V)>> {
        // A panic while holding the lock leaves plain data behind; keep serving.
        self.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The value under `key`, if it has not expired. An expired entry is
    /// dropped on the spot rather than left holding a slot.
    pub fn get(&self, key: &K) -> Option<V> {
        let now = std::time::Instant::now();
        let mut m = self.lock();
        let live = m
            .get(key)
            .map(|(until, value)| (now < *until).then(|| value.clone()));
        match live {
            Some(Some(value)) => Some(value),
            Some(None) => {
                m.remove(key);
                None
            }
            None => None,
        }
    }

    /// Remember `value` under `key` for `ttl`.
    pub fn put(&self, key: K, value: V, ttl: Duration) {
        self.lock()
            .put(key, (std::time::Instant::now() + ttl, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Callers put an answer back after a miss, as the name, peer and link
    /// preview caches do. In a full cache, keys that readers keep asking for
    /// stop missing after the first rounds, and a flood of one-off keys after
    /// that, many times the capacity, pushes none of them out.
    #[test]
    fn a_flood_of_one_off_keys_leaves_what_readers_keep_asking_for() {
        let cache: Expiring<u32, u32> = Expiring::new(1024);
        let hour = Duration::from_secs(3600);
        let hot = 0..200;
        let mut one_off = 1_000_000..;
        for k in one_off.by_ref().take(5_000) {
            cache.put(k, k, hour);
        }
        let mut late_misses = 0;
        for round in 0..5 {
            for k in hot.clone() {
                if cache.get(&k).is_none() {
                    if round >= 2 {
                        late_misses += 1;
                    }
                    cache.put(k, k, hour);
                }
            }
            for k in one_off.by_ref().take(200) {
                cache.put(k, k, hour);
            }
        }
        assert_eq!(late_misses, 0, "asked for again, so kept");
        for k in one_off.by_ref().take(50_000) {
            cache.put(k, k, hour);
        }
        for k in hot {
            assert_eq!(
                cache.get(&k),
                Some(k),
                "the flood never pushed out a hot key"
            );
        }
        let last = one_off.next().unwrap() - 1;
        assert_eq!(cache.get(&last), Some(last), "one-off keys still get in");
    }

    #[test]
    fn an_expired_answer_is_a_miss() {
        let cache: Expiring<u8, u8> = Expiring::new(4);
        cache.put(1, 1, Duration::from_secs(3600));
        cache.put(2, 2, Duration::ZERO);
        assert_eq!(cache.get(&2), None, "expired on arrival");
        assert_eq!(cache.get(&1), Some(1));
    }
}
