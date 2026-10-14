//! Token accounting only. Callers own locking, slots, queues and admission order.
use std::time::{Duration, Instant};

pub struct Bucket {
    tokens: f64,
    updated: Instant,
    capacity: f64,
    per_second: f64,
}

impl Bucket {
    pub fn new(capacity: f64, per_second: f64, now: Instant) -> Self {
        Self {
            tokens: capacity,
            updated: now,
            capacity,
            per_second,
        }
    }

    fn accrued(&self, now: Instant) -> f64 {
        (self.tokens + now.saturating_duration_since(self.updated).as_secs_f64() * self.per_second)
            .min(self.capacity)
    }

    pub fn refill(&mut self, now: Instant) {
        self.tokens = self.accrued(now);
        self.updated = now;
    }

    /// Availability after the caller's last refill, without spending a token.
    pub fn available(&self) -> bool {
        self.tokens >= 1.0
    }

    pub fn take(&mut self, now: Instant) -> bool {
        self.take_keeping(now, 0.0).is_ok()
    }

    /// Take a token if `keep` would still remain, or say how long until one
    /// accrues. A reserve lets one kind of caller keep the last tokens.
    pub fn take_keeping(&mut self, now: Instant, keep: f64) -> Result<(), Duration> {
        self.refill(now);
        if self.tokens - 1.0 >= keep {
            self.tokens -= 1.0;
            return Ok(());
        }
        Err(Duration::from_secs_f64(
            (keep + 1.0 - self.tokens) / self.per_second,
        ))
    }

    pub fn give_back(&mut self) {
        self.tokens = (self.tokens + 1.0).min(self.capacity);
    }

    /// Whether all tokens have replenished, without renewing the last-refill time.
    pub fn full(&self, now: Instant) -> bool {
        self.accrued(now) >= self.capacity
    }

    pub fn last_refill(&self) -> Instant {
        self.updated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn fractional_refills_and_refunds_never_enlarge_the_burst() {
        let now = Instant::now();
        let mut bucket = Bucket::new(2.0, 0.5, now);
        assert!(bucket.take(now));
        assert!(bucket.take(now));
        assert!(!bucket.take(now));
        let halfway = now + Duration::from_secs(1);
        bucket.refill(halfway);
        assert!(!bucket.available());
        assert!(!bucket.full(halfway));
        bucket.give_back();
        assert!(bucket.take(halfway));
        assert!(!bucket.take(halfway));
        assert!(bucket.take(now + Duration::from_secs(2)));
        for _ in 0..10 {
            bucket.give_back();
        }
        assert!(bucket.full(now + Duration::from_secs(2)));
        let later = now + Duration::from_secs(1000);
        assert!(bucket.take(later));
        assert!(bucket.take(later));
        assert!(!bucket.take(later));
    }

    #[test]
    fn checking_fullness_does_not_change_the_refill_clock_or_spend_tokens() {
        let now = Instant::now();
        let mut bucket = Bucket::new(4.0, 1.0 / 15.0, now);
        assert!(bucket.take(now));
        assert!(!bucket.full(now + Duration::from_secs(14)));
        assert!(bucket.full(now + Duration::from_secs(15)));
        assert_eq!(bucket.last_refill(), now);
        for _ in 0..4 {
            assert!(bucket.take(now + Duration::from_secs(15)));
        }
        assert!(!bucket.take(now + Duration::from_secs(15)));
    }
}
