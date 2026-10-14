//! Per-visitor bounds for HTTP routes that can spend upstream calls.
use crate::token_bucket::Bucket;
use axum::{
    extract::{ConnectInfo, Request},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const CAPACITY: usize = 4096;
/// How many of a kind's requests may wait for a slot.
const WAITING: usize = 16;
#[derive(Clone, Copy)]
pub enum Kind {
    Read,
    Name,
    Preview,
    DraftPreview,
    /// An attachment, or any upload beyond the body cap.
    Publish,
    /// A message body: slots of its own, spending the same tokens as `Publish`.
    SmallPublish,
}

/// A kind's allowance: a burst of tokens, refilled at a rate and spent one
/// per request, and how many of its requests may run at once.
struct Allowance {
    burst: f64,
    per_second: f64,
    slots: usize,
}

impl Kind {
    fn allowance(self) -> Allowance {
        // Burst, tokens per second, slots. Message bodies spend the
        // uploads' tokens, in slots of their own.
        let (burst, per_second, slots) = match self {
            Self::Read | Self::Publish | Self::SmallPublish => (16.0, 1.0, 2),
            Self::Name => (32.0, 2.0, 4),
            Self::Preview => (32.0, 2.0, 2),
            Self::DraftPreview => (4.0, 1.0 / 15.0, 2),
        };
        Allowance {
            burst,
            per_second,
            slots,
        }
    }
}
struct Budget {
    tokens: Bucket,
    active: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
}
struct Client {
    reads: Budget,
    names: Budget,
    previews: Budget,
    publishes: Budget,
    /// Slots for message bodies, so slow attachments from one network cannot
    /// hold up its posts; they spend `publishes`' tokens.
    bodies: Lane,
}
/// Active slots and a bounded queue, without tokens of their own.
struct Lane {
    active: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
}
impl Lane {
    fn new() -> Self {
        Self {
            active: Arc::new(Semaphore::new(Kind::SmallPublish.allowance().slots)),
            waiting: Arc::new(Semaphore::new(WAITING)),
        }
    }
    fn idle(&self) -> bool {
        self.active.available_permits() == Kind::SmallPublish.allowance().slots
            && self.waiting.available_permits() == WAITING
    }
}
impl Client {
    fn budget(&mut self, kind: Kind) -> &mut Budget {
        match kind {
            Kind::Read => &mut self.reads,
            Kind::Name => &mut self.names,
            Kind::Preview => &mut self.previews,
            Kind::DraftPreview => unreachable!("separate draft visitor table"),
            Kind::Publish | Kind::SmallPublish => &mut self.publishes,
        }
    }
}
impl Budget {
    fn take(&mut self, now: Instant) -> Option<(Arc<Semaphore>, Arc<Semaphore>)> {
        self.tokens
            .take(now)
            .then(|| (self.active.clone(), self.waiting.clone()))
    }
    fn idle(&self, now: Instant, kind: Kind) -> bool {
        self.active.available_permits() == kind.allowance().slots
            && self.waiting.available_permits() == WAITING
            && self.tokens.full(now)
    }
    fn new(now: Instant, kind: Kind) -> Self {
        Self {
            tokens: Bucket::new(kind.allowance().burst, kind.allowance().per_second, now),
            active: Arc::new(Semaphore::new(kind.allowance().slots)),
            waiting: Arc::new(Semaphore::new(WAITING)),
        }
    }
}
#[derive(Default)]
pub struct Limits {
    clients: Mutex<HashMap<IpAddr, Client>>,
    // Slow draft refills must not retain entries in posting/read admission.
    drafts: Mutex<HashMap<IpAddr, Budget>>,
}

/// The visitor an address belongs to: an IPv6 address counts as its /64,
/// which one home or phone is normally given, and an IPv4-mapped address as
/// the IPv4 address it carries.
pub(crate) fn group(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or_else(|| IpAddr::V6((u128::from(v6) & (u128::MAX << 64)).into())),
        v4 => v4,
    }
}
impl Limits {
    fn budget(
        &self,
        ip: IpAddr,
        now: Instant,
        kind: Kind,
    ) -> Option<(Arc<Semaphore>, Arc<Semaphore>)> {
        let ip = group(ip);
        if matches!(kind, Kind::DraftPreview) {
            let mut drafts = self.drafts.lock().expect("draft visitor limits");
            if !drafts.contains_key(&ip) && drafts.len() >= CAPACITY {
                drafts.retain(|_, b| !b.idle(now, kind));
                if drafts.len() >= CAPACITY {
                    return None;
                }
            }
            return drafts
                .entry(ip)
                .or_insert_with(|| Budget::new(now, kind))
                .take(now);
        }
        let mut clients = self.clients.lock().expect("visitor limits");
        if !clients.contains_key(&ip) && clients.len() >= CAPACITY {
            clients.retain(|_, c| {
                // Reclaim fully refilled, idle entries only. Evicting an active
                // or depleted entry would reset that visitor's allowance.
                !c.reads.idle(now, Kind::Read)
                    || !c.names.idle(now, Kind::Name)
                    || !c.previews.idle(now, Kind::Preview)
                    || !c.publishes.idle(now, Kind::Publish)
                    || !c.bodies.idle()
            });
            if clients.len() >= CAPACITY {
                return None;
            }
        }
        let client = clients.entry(ip).or_insert_with(|| Client {
            reads: Budget::new(now, Kind::Read),
            names: Budget::new(now, Kind::Name),
            previews: Budget::new(now, Kind::Preview),
            publishes: Budget::new(now, Kind::Publish),
            bodies: Lane::new(),
        });
        let lanes = client.budget(kind).take(now)?;
        Some(match kind {
            Kind::SmallPublish => (client.bodies.active.clone(), client.bodies.waiting.clone()),
            _ => lanes,
        })
    }

    /// Give back the token a request took when it is refused a slot: only
    /// admitted work spends the burst.
    fn refund(&self, ip: IpAddr, kind: Kind) {
        let ip = group(ip);
        let give = |b: &mut Budget| b.tokens.give_back();
        if matches!(kind, Kind::DraftPreview) {
            if let Some(b) = self
                .drafts
                .lock()
                .expect("draft visitor limits")
                .get_mut(&ip)
            {
                give(b);
            }
        } else if let Some(c) = self.clients.lock().expect("visitor limits").get_mut(&ip) {
            give(c.budget(kind));
        }
    }

    fn admit(&self, ip: IpAddr, now: Instant) -> Option<OwnedSemaphorePermit> {
        let permit = self.budget(ip, now, Kind::Read)?.0.try_acquire_owned().ok();
        if permit.is_none() {
            self.refund(ip, Kind::Read);
        }
        permit
    }

    async fn queued(&self, ip: IpAddr, kind: Kind) -> Option<OwnedSemaphorePermit> {
        let (active, waiting) = self.budget(ip, Instant::now(), kind)?;
        if let Ok(permit) = active.clone().try_acquire_owned() {
            return Some(permit);
        }
        // Separate bounded queues absorb names and uploads from tabs sharing a
        // home network without taking reply/receipt slots or unbounded waiters.
        let permit = match waiting.try_acquire_owned() {
            Ok(_waiting) => tokio::time::timeout(Duration::from_secs(3), active.acquire_owned())
                .await
                .ok()
                .and_then(Result::ok),
            Err(_) => None,
        };
        if permit.is_none() {
            self.refund(ip, kind);
        }
        permit
    }
}

/// Hold the visitor permit through the handler; detached work takes its own clone.
pub async fn guard(mut request: Request, next: Next, limits: Arc<Limits>, kind: Kind) -> Response {
    if request.method() == axum::http::Method::OPTIONS {
        return next.run(request).await;
    }
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|p| p.0);
    let ip = crate::http::client(peer, request.headers());
    let permit = if matches!(kind, Kind::Read) {
        limits.admit(ip, Instant::now())
    } else {
        limits.queued(ip, kind).await
    };
    let Some(permit) = permit else {
        let mut response =
            crate::http::json_error(StatusCode::TOO_MANY_REQUESTS, "visitor_read_limit");
        response.headers_mut().insert(
            "retry-after",
            if matches!(kind, Kind::DraftPreview) {
                "15"
            } else {
                "2"
            }
            .parse()
            .unwrap(),
        );
        return response;
    };
    let permit = Arc::new(permit);
    request.extensions_mut().insert(permit.clone());
    let response = next.run(request).await;
    drop(permit);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn draft_previews_refill_slowly_and_cannot_reset_their_budget_by_eviction() {
        let limits = Limits::default();
        let ip = "192.0.2.1".parse().unwrap();
        let now = Instant::now();
        for _ in 0..4 {
            assert!(limits.budget(ip, now, Kind::DraftPreview).is_some());
        }
        assert!(
            limits
                .budget(ip, now + Duration::from_secs(14), Kind::DraftPreview)
                .is_none()
        );
        assert!(
            !limits.drafts.lock().unwrap()[&ip]
                .idle(now + Duration::from_secs(14), Kind::DraftPreview)
        );
        assert!(
            limits
                .budget(ip, now + Duration::from_secs(15), Kind::DraftPreview)
                .is_some()
        );
        assert!(
            limits
                .budget(ip, now + Duration::from_secs(15), Kind::DraftPreview)
                .is_none()
        );
        assert!(
            limits
                .budget(ip, now + Duration::from_secs(15), Kind::Preview)
                .is_some()
        );
    }

    #[test]
    fn draft_address_churn_cannot_fill_the_posting_visitor_table() {
        let limits = Limits::default();
        let now = Instant::now();
        for n in 1..=CAPACITY {
            assert!(
                limits
                    .budget(IpAddr::V4((n as u32).into()), now, Kind::DraftPreview)
                    .is_some()
            );
        }
        let next = IpAddr::V4(((CAPACITY + 1) as u32).into());
        assert!(limits.budget(next, now, Kind::DraftPreview).is_none());
        assert!(limits.admit(next, now).is_some());
        assert!(limits.budget(next, now, Kind::Publish).is_some());
        assert!(
            limits
                .budget(next, now + Duration::from_secs(15), Kind::DraftPreview)
                .is_some()
        );
    }

    #[test]
    fn full_draft_table_keeps_live_work_even_after_refill() {
        let limits = Limits::default();
        let now = Instant::now();
        let mut permits = Vec::new();
        for n in 1..=CAPACITY {
            let (slots, _) = limits
                .budget(IpAddr::V4((n as u32).into()), now, Kind::DraftPreview)
                .unwrap();
            permits.push(slots.try_acquire_owned().unwrap());
        }
        let next = IpAddr::V4(((CAPACITY + 1) as u32).into());
        let later = now + Duration::from_secs(60);
        assert!(limits.budget(next, later, Kind::DraftPreview).is_none());
        drop(permits.pop());
        assert!(limits.budget(next, later, Kind::DraftPreview).is_some());
        assert_eq!(limits.drafts.lock().unwrap().len(), CAPACITY);
    }

    #[test]
    fn one_visitor_cannot_hold_all_slots_or_spend_another_visitors_budget() {
        let limits = Limits::default();
        let now = Instant::now();
        let a = "192.0.2.1".parse().unwrap();
        let b = "192.0.2.2".parse().unwrap();
        let one = limits.admit(a, now).unwrap();
        let two = limits.admit(a, now).unwrap();
        assert!(limits.admit(a, now).is_none());
        assert!(limits.admit(b, now).is_some());
        drop((one, two));
        for _ in 2..16 {
            drop(limits.admit(a, now).unwrap());
        }
        assert!(limits.admit(a, now).is_none());
        assert!(limits.admit(a, now + Duration::from_secs(1)).is_some());
        assert!(limits.admit(b, now).is_some());
    }

    /// A request turned away for full slots or a queue that timed out gets
    /// its token back: only admitted work spends the burst.
    #[tokio::test]
    async fn a_refused_request_spends_no_token() {
        let limits = Limits::default();
        let ip = "192.0.2.1".parse().unwrap();
        let now = Instant::now();
        let held = [
            limits.admit(ip, now).unwrap(),
            limits.admit(ip, now).unwrap(),
        ];
        for _ in 0..5 {
            assert!(limits.admit(ip, now).is_none());
        }
        drop(held);
        for _ in 2..16 {
            assert!(limits.budget(ip, now, Kind::Read).is_some());
        }
        assert!(limits.budget(ip, now, Kind::Read).is_none());

        // Draft previews refill slowly enough to outlast the queue's wait.
        let held = [
            limits.queued(ip, Kind::DraftPreview).await.unwrap(),
            limits.queued(ip, Kind::DraftPreview).await.unwrap(),
        ];
        let refused = limits.queued(ip, Kind::DraftPreview).await;
        assert!(refused.is_none(), "timed out");
        drop(held);
        let now = Instant::now();
        for _ in 2..4 {
            assert!(limits.budget(ip, now, Kind::DraftPreview).is_some());
        }
        assert!(limits.budget(ip, now, Kind::DraftPreview).is_none());
    }

    #[tokio::test]
    async fn previews_have_their_own_budget_and_slots() {
        let limits = Limits::default();
        let ip = "192.0.2.1".parse().unwrap();
        let mut permits = Vec::new();
        for _ in 0..2 {
            permits.push(limits.queued(ip, Kind::Preview).await.unwrap());
        }
        assert!(limits.admit(ip, Instant::now()).is_some());
        assert!(limits.queued(ip, Kind::Name).await.is_some());
        assert!(limits.queued(ip, Kind::SmallPublish).await.is_some());
        assert_eq!(
            limits
                .budget(ip, Instant::now(), Kind::Preview)
                .unwrap()
                .0
                .available_permits(),
            0
        );
    }

    #[tokio::test]
    async fn name_bursts_wait_without_taking_transaction_slots() {
        let limits = Arc::new(Limits::default());
        let ip = "192.0.2.1".parse().unwrap();
        let mut names = Vec::new();
        for _ in 0..4 {
            names.push(limits.queued(ip, Kind::Name).await.unwrap());
        }
        let waiting = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.queued(ip, Kind::Name).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        let one = limits.admit(ip, Instant::now()).unwrap();
        let two = limits.admit(ip, Instant::now()).unwrap();
        drop(names.pop());
        assert!(waiting.await.unwrap().is_some());
        drop((one, two));
    }

    #[tokio::test]
    async fn uploads_queue_independently_of_reads_and_names() {
        let limits = Arc::new(Limits::default());
        let ip = "192.0.2.1".parse().unwrap();
        let reads = [
            limits.admit(ip, Instant::now()).unwrap(),
            limits.admit(ip, Instant::now()).unwrap(),
        ];
        let first = limits.queued(ip, Kind::Publish).await.unwrap();
        let second = limits.queued(ip, Kind::Publish).await.unwrap();
        let waiting = {
            let limits = limits.clone();
            tokio::spawn(async move { limits.queued(ip, Kind::Publish).await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert!(limits.queued(ip, Kind::Name).await.is_some());
        drop(first);
        assert!(waiting.await.unwrap().is_some());
        drop((reads, second));
    }

    #[tokio::test]
    async fn a_visitors_message_bodies_have_slots_its_attachments_cannot_take() {
        let limits = Arc::new(Limits::default());
        let ip = "192.0.2.1".parse().unwrap();
        // Two slow images from one network hold its attachment slots...
        let images = [
            limits.queued(ip, Kind::Publish).await.unwrap(),
            limits.queued(ip, Kind::Publish).await.unwrap(),
        ];
        // ...while its message bodies still start at once, in slots of their own.
        let quick = Duration::from_millis(100);
        let texts = [
            tokio::time::timeout(quick, limits.queued(ip, Kind::SmallPublish))
                .await
                .unwrap()
                .unwrap(),
            tokio::time::timeout(quick, limits.queued(ip, Kind::SmallPublish))
                .await
                .unwrap()
                .unwrap(),
        ];
        drop((images, texts));
        // Both spend one rate budget: four so far, twelve left.
        let now = Instant::now();
        for _ in 0..12 {
            assert!(limits.budget(ip, now, Kind::SmallPublish).is_some());
        }
        assert!(limits.budget(ip, now, Kind::SmallPublish).is_none());
        assert!(limits.budget(ip, now, Kind::Publish).is_none());
    }

    #[test]
    fn publishing_budget_is_independent_and_refills_at_a_bounded_rate() {
        let limits = Limits::default();
        let ip = "192.0.2.1".parse().unwrap();
        let now = Instant::now();
        for _ in 0..16 {
            assert!(limits.budget(ip, now, Kind::Publish).is_some());
        }
        assert!(limits.budget(ip, now, Kind::Publish).is_none());
        assert!(limits.admit(ip, now).is_some());
        assert!(
            limits
                .budget(ip, now + Duration::from_secs(1), Kind::Publish)
                .is_some()
        );
    }

    #[test]
    fn ipv6_rotations_and_mapped_ipv4_share_limits() {
        assert_eq!(
            group("2001:db8::1".parse().unwrap()),
            group("2001:db8::ffff".parse().unwrap())
        );
        assert_eq!(
            group("::ffff:192.0.2.1".parse().unwrap()),
            group("192.0.2.1".parse().unwrap())
        );
    }
    #[test]
    fn new_visitors_cannot_evict_active_limits() {
        let limits = Limits::default();
        let now = Instant::now();
        for n in 1..=CAPACITY {
            drop(limits.admit(IpAddr::V4((n as u32).into()), now).unwrap());
        }
        let next = IpAddr::V4(((CAPACITY + 1) as u32).into());
        assert!(limits.admit(next, now).is_none());
        assert!(limits.admit(next, now + Duration::from_secs(1)).is_some());
    }
    #[test]
    fn refilled_entries_with_live_work_or_depleted_budgets_stay() {
        let limits = Limits::default();
        let now = Instant::now();
        let ip = "192.0.2.1".parse().unwrap();
        let permit = limits.admit(ip, now).unwrap();
        let clients = limits.clients.lock().unwrap();
        let client = &clients[&ip];
        assert!(!client.reads.idle(now + Duration::from_secs(60), Kind::Read));
        drop(permit);
        assert!(!client.reads.idle(now, Kind::Read));
        assert!(client.reads.idle(now + Duration::from_secs(1), Kind::Read));
    }
}
