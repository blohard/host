//! Bounded, expiring answers with CLOCK eviction.
use commonware_utils::cache::Clock;
use std::time::Duration;

/// How long an answer from outside this host is kept. A successful answer
/// lasts a day: fewer upstream calls matter more than seeing a change
/// quickly. A negative one (a failure, a partial answer or a refusal) lasts
/// an hour at most, so a passing failure never stands in for a day-long
/// answer. Names and link previews both follow this rule.
pub const SUCCESS_LIFETIME: Duration = Duration::from_secs(24 * 3600);
/// A negative answer's longest lifetime; see [`SUCCESS_LIFETIME`].
pub const NEGATIVE_LIFETIME: Duration = Duration::from_secs(3600);

/// Expiring answers in a fixed-capacity CLOCK cache behind a mutex. When
/// full, one entry makes room: the first the hand finds that nobody has read
/// since its last pass, so a flood of one-off keys evicts itself before it
/// touches what readers keep asking for. Nothing is ever cleared wholesale.
/// A poisoned lock yields its data for reuse.
pub struct Expiring<K, V> {
    map: std::sync::Mutex<Clock<K, (std::time::Instant, V)>>,
}

impl<K: std::hash::Hash + Eq + Clone, V: Clone> Expiring<K, V> {
    pub fn new(cap: usize) -> Self {
        let cap = std::num::NonZeroUsize::new(cap.max(1)).expect("at least one entry");
        Self {
            map: std::sync::Mutex::new(Clock::new(cap)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Clock<K, (std::time::Instant, V)>> {
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
    #[test]
    fn expiring_evicts_the_unread_and_never_clears() {
        let cache: Expiring<u8, u8> = Expiring::new(4);
        let hour = Duration::from_secs(3600);
        for k in 1..=4 {
            cache.put(k, k, hour);
        }
        cache.put(5, 5, hour);
        assert_eq!(cache.get(&1), None, "the oldest unread entry went first");
        assert_eq!(cache.get(&2), Some(2), "read, so referenced");
        cache.put(6, 6, hour);
        assert_eq!(cache.get(&2), Some(2), "a second chance for what was read");
        assert_eq!(cache.get(&3), None, "the next unread entry went instead");
        for k in [4, 5, 6] {
            assert_eq!(cache.get(&k), Some(k), "everything else is still there");
        }
        cache.put(7, 7, Duration::ZERO);
        assert_eq!(cache.get(&7), None, "expired on arrival");
        assert_eq!(cache.get(&2), Some(2));
    }
}
