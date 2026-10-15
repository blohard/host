//! Shared RPC transport, budgets, limits and response envelopes.
//!
//! Every module that talks to a chain (the verifier, index ingestion and
//! verified names) goes through [`call`], so transport errors are worded the
//! same everywhere and never carry the endpoint URL: it may embed an API key,
//! and these messages end up in logs and HTTP error bodies.

use crate::abi::decode_hex;
use crate::token_bucket::Bucket;
use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{Semaphore, SemaphorePermit};

/// Limit concurrent upstream calls host-wide, across every configured node,
/// split into 28 client slots and four ingestion slots, with a bounded wait
/// before refusing as busy. The node behind this host is
/// one process: past a point more concurrent calls only queue there, and a
/// queue that grows without bound under a slow node is what turns a slow
/// minute into a stuck one. Cache hits and followers of a shared fetch take
/// no slot, so the limit applies to distinct upstream work.
pub const MAX_UPSTREAM_CALLS: usize = 32;
/// Separate slots prevent slow client calls from occupying ingestion's capacity.
const INGESTION_SLOTS: usize = 4;

pub const UPSTREAM_WAIT: Duration = Duration::from_secs(2);

/// A bounded number of slots with a bounded wait for one.
pub struct Limiter {
    slots: Semaphore,
    wait: Duration,
}

impl Limiter {
    pub fn new(slots: usize, wait: Duration) -> Self {
        Self {
            slots: Semaphore::new(slots),
            wait,
        }
    }

    /// A slot, or `None` when none came free within the wait: the caller
    /// answers busy rather than joining a queue that only grows.
    pub async fn slot(&self) -> Option<SemaphorePermit<'_>> {
        tokio::time::timeout(self.wait, self.slots.acquire())
            .await
            .ok()
            .and_then(Result::ok)
    }
}

/// Gas ceiling for contract-wallet verification and name resolution.
/// Board admission and post estimation use their own smaller bound.
pub const SIM_GAS_CAP: u64 = 1_000_000;

/// Whose work an upstream call is. Ingestion keeps the index moving and may
/// spend the whole budget, waiting for tokens to accrue; a request from a
/// client may not dip into the reserve and is answered busy at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Client,
    /// Lookups of display names and transfer recipients: client work that
    /// takes client slots and is answered busy at once, but stops earlier,
    /// leaving headroom that a flood of lookups cannot spend.
    Names,
    Ingestion,
}

/// How long ingestion waits for a token before giving the attempt up. The
/// follower retries with its own backoff.
const INGESTION_WAIT: Duration = Duration::from_secs(10);

/// Calls per minute this host may make to every upstream together. Chains and
/// name lookups share one provider subscription as a rule, so one bucket
/// meters them all; a metered plan is what the bucket protects. Tokens accrue
/// continuously up to a minute's worth. The last `reserve` tokens are
/// ingestion's alone, so client traffic cannot stall the follower: under a
/// flood the bucket hovers at the reserve and every accrued token goes to
/// whichever follower asks first. Name lookups stop at `names_line`, halfway
/// between the reserve and a full bucket, so a flood of them leaves the
/// headroom below to other client work, such as checking uploads and
/// receipts, and to ingestion. A budget of zero is no limit.
pub struct Budget {
    /// A minute's worth of calls, refilled continuously; none when unlimited.
    bucket: Option<Mutex<Bucket>>,
    reserve: f64,
    names_line: f64,
}

impl Budget {
    pub fn new(calls_per_minute: u32, reserved_for_ingestion: u32) -> Self {
        let capacity = f64::from(calls_per_minute);
        let reserve = reserved_for_ingestion.min(calls_per_minute.saturating_sub(1));
        Self {
            bucket: (calls_per_minute > 0)
                .then(|| Mutex::new(Bucket::new(capacity, capacity / 60.0, Instant::now()))),
            reserve: f64::from(reserve),
            // In whole tokens, so it stays at least one under a full bucket
            // and a lookup can always make a call, however small the budget.
            names_line: f64::from(reserve + (calls_per_minute - reserve) / 2),
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0, 0)
    }

    /// One token at `now`, or how long until one accrues for `class`.
    fn take_at(&self, now: Instant, class: Class) -> Result<(), Duration> {
        let Some(bucket) = &self.bucket else {
            return Ok(());
        };
        let keep = match class {
            Class::Client => self.reserve,
            Class::Names => self.names_line,
            Class::Ingestion => 0.0,
        };
        bucket
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take_keeping(now, keep)
    }

    /// Spend one token. Client work, names included, is answered busy at once
    /// when none is to be had above its line; ingestion waits for one to
    /// accrue, within reason.
    pub async fn take(&self, class: Class) -> Result<(), RpcError> {
        let deadline = Instant::now() + INGESTION_WAIT;
        loop {
            match self.take_at(Instant::now(), class) {
                Ok(()) => return Ok(()),
                Err(_) if matches!(class, Class::Client | Class::Names) => {
                    return Err(RpcError::Busy);
                }
                Err(wait) => {
                    if Instant::now() + wait > deadline {
                        return Err(RpcError::Busy);
                    }
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
}

/// Calls sent and calls refused as busy since the host started, per class.
/// Published by the status so a rise in busy answers can be read against
/// what the budget and the limiter were doing.
/// Calls sent and calls refused as busy, for one class.
#[derive(Default)]
struct ClassCounters {
    calls: AtomicU64,
    busy: AtomicU64,
}

impl ClassCounters {
    fn count(&self, busy: bool) {
        let counter = if busy { &self.busy } else { &self.calls };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ClassCounts {
        ClassCounts {
            calls: self.calls.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct Counters {
    client: ClassCounters,
    names: ClassCounters,
    ingestion: ClassCounters,
}

impl Counters {
    fn of(&self, class: Class) -> &ClassCounters {
        match class {
            Class::Client => &self.client,
            Class::Names => &self.names,
            Class::Ingestion => &self.ingestion,
        }
    }
}

/// A snapshot of [`Counters`] as the status reports it.
#[derive(Serialize)]
pub struct UpstreamCounts {
    pub client: ClassCounts,
    pub names: ClassCounts,
    pub ingestion: ClassCounts,
}

#[derive(Serialize)]
pub struct ClassCounts {
    pub calls: u64,
    pub busy: u64,
}

/// The host's one transport to its upstreams: the HTTP client, the budget and
/// the concurrency limiter, shared by every chain and the names service.
#[derive(Clone)]
pub struct Upstream {
    client: reqwest::Client,
    budget: Arc<Budget>,
    client_limiter: Arc<Limiter>,
    ingestion_limiter: Arc<Limiter>,
    counters: Arc<Counters>,
    // Keys are configured RPC URLs, never URLs supplied by a caller or resolver.
    cooldowns: Arc<Mutex<HashMap<String, Instant>>>,
}

impl Upstream {
    pub fn new(client: reqwest::Client, budget: Budget) -> Self {
        Self {
            client,
            budget: Arc::new(budget),
            client_limiter: Arc::new(Limiter::new(
                MAX_UPSTREAM_CALLS - INGESTION_SLOTS,
                UPSTREAM_WAIT,
            )),
            ingestion_limiter: Arc::new(Limiter::new(INGESTION_SLOTS, UPSTREAM_WAIT)),
            counters: Arc::default(),
            cooldowns: Arc::default(),
        }
    }

    /// No budget: development mode and tests that meter nothing.
    pub fn unlimited(client: reqwest::Client) -> Self {
        Self::new(client, Budget::unlimited())
    }

    fn check_cooldown(&self, url: &str) -> Result<(), RpcError> {
        let now = Instant::now();
        let mut cooldowns = self
            .cooldowns
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        cooldowns.retain(|_, until| *until > now);
        match cooldowns.get(url) {
            Some(until) => Err(RpcError::RateLimited(*until - now)),
            None => Ok(()),
        }
    }

    fn pause(&self, url: &str, delay: Duration) -> RpcError {
        let now = Instant::now();
        let mut cooldowns = self
            .cooldowns
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        cooldowns.retain(|_, until| *until > now);
        let until = cooldowns.entry(url.to_owned()).or_insert(now);
        // A late response may extend a pause, but must never shorten it.
        *until = (*until).max(now + delay);
        RpcError::RateLimited(*until - now)
    }

    #[cfg(test)]
    pub(crate) fn expire_cooldown(&self, url: &str) {
        let mut cooldowns = self.cooldowns.lock().unwrap();
        let until = cooldowns
            .get_mut(url)
            .expect("endpoint must be cooling down");
        *until = Instant::now();
    }

    pub fn counts(&self) -> UpstreamCounts {
        UpstreamCounts {
            client: self.counters.client.snapshot(),
            names: self.counters.names.snapshot(),
            ingestion: self.counters.ingestion.snapshot(),
        }
    }
}

/// Why a round trip to an RPC endpoint produced no usable answer.
#[derive(Debug)]
pub enum RpcError {
    /// The endpoint could not be reached, or closed the connection.
    Unreachable(String),
    /// The endpoint answered with something other than JSON.
    NotJson(String),
    /// The response is JSON but has no `result` of the expected shape.
    BadResult(String),
    /// The answer exceeded the caller's response bound. Asking the same
    /// question again brings the same answer.
    TooLarge(usize),
    /// Too many calls to the node are out already; the caller should answer
    /// busy and the client try again shortly.
    Busy,
    /// This endpoint asked us to wait. No upstream slot is held during the pause.
    RateLimited(Duration),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable(e) => write!(f, "rpc unreachable: {e}"),
            Self::NotJson(e) => write!(f, "rpc returned non-json: {e}"),
            Self::BadResult(e) => f.write_str(e),
            Self::TooLarge(bound) => write!(f, "upstream response over {bound} bytes"),
            Self::Busy => f.write_str("the host is busy; try again shortly"),
            Self::RateLimited(wait) => write!(
                f,
                "upstream rate limited; try again in {} seconds",
                wait.as_secs_f64().ceil() as u64
            ),
        }
    }
}

impl RpcError {
    /// Nothing about the request itself failed: the endpoint could not be
    /// reached, answered with something other than JSON, or this host had no
    /// slot free, or asked for a rate-limit pause. The same request may succeed
    /// a moment later.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Unreachable(_) | Self::NotJson(_) | Self::Busy | Self::RateLimited(_)
        )
    }
}

impl std::error::Error for RpcError {}

/// Maximum assembled upstream response, checked before appending chunks.
/// Ingestion splits a block range when its logs do not fit this bound, and
/// reads a single block whose logs still do not fit from its receipts
/// (`eth_getBlockReceipts`), under a larger bound.
pub const MAX_RESPONSE: usize = 8 << 20;

/// One JSON-RPC round trip on a client's behalf. Returns the whole response
/// object; callers look at `result` / `error` themselves. Transport errors
/// are reported without the URL.
pub async fn call(
    upstream: &Upstream,
    url: &str,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    call_as(Class::Client, upstream, url, method, params, MAX_RESPONSE).await
}

/// The round trip itself, metered as `class`: a budget token first, then one
/// of the host's slots for the whole exchange, body included.
pub async fn call_as(
    class: Class,
    upstream: &Upstream,
    url: &str,
    method: &str,
    params: Value,
    max_response: usize,
) -> Result<Value, RpcError> {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let admitted = async {
        upstream.check_cooldown(url)?;
        upstream.budget.take(class).await?;
        let limiter = match class {
            Class::Client | Class::Names => &upstream.client_limiter,
            Class::Ingestion => &upstream.ingestion_limiter,
        };
        let slot = limiter.slot().await.ok_or(RpcError::Busy)?;
        // Another request may have been throttled while this one waited.
        upstream.check_cooldown(url)?;
        Ok(slot)
    }
    .await;
    upstream.counters.of(class).count(admitted.is_err());
    let _slot = admitted?;
    let mut resp = upstream
        .client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| RpcError::Unreachable(e.without_url().to_string()))?;
    let retry_after = retry_after(resp.headers(), SystemTime::now());
    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        || (resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE && retry_after.is_some())
    {
        // Do not read a possibly large or non-JSON error body.
        return Err(upstream.pause(url, retry_after.unwrap_or(DEFAULT_COOLDOWN)));
    }
    if resp
        .content_length()
        .is_some_and(|n| n > max_response as u64)
    {
        return Err(RpcError::TooLarge(max_response));
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| RpcError::Unreachable(e.without_url().to_string()))?
    {
        if buf.len() + chunk.len() > max_response {
            return Err(RpcError::TooLarge(max_response));
        }
        buf.extend_from_slice(&chunk);
    }
    let answer: Value =
        serde_json::from_slice(&buf).map_err(|e| RpcError::NotJson(e.to_string()))?;
    if answer.get("error").is_some_and(rate_throttle) {
        return Err(upstream.pause(url, retry_after.unwrap_or(DEFAULT_COOLDOWN)));
    }
    Ok(answer)
}

const DEFAULT_COOLDOWN: Duration = Duration::from_secs(2);
const MAX_COOLDOWN: Duration = Duration::from_secs(300);

/// Retry-After permits seconds or an HTTP date. Bound provider-controlled
/// delays so a mistaken date cannot disable this endpoint indefinitely.
fn retry_after(headers: &reqwest::header::HeaderMap, now: SystemTime) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    let delay = if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        Duration::from_secs(value.parse::<u64>().unwrap_or(u64::MAX))
    } else {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default()
    };
    Some(delay.clamp(Duration::from_secs(1), MAX_COOLDOWN))
}

/// -32005 also means query/result limits: the code alone is not a throttle.
/// Revert errors are left alone even when a contract chooses these words.
fn rate_throttle(error: &Value) -> bool {
    match error["code"].as_i64() {
        Some(429) => true,
        Some(-32005) => error["message"].as_str().is_some_and(|message| {
            let message = message.to_ascii_lowercase();
            [
                "rate limit",
                "rate-limit",
                "too many requests",
                "requests per second",
                "request rate",
            ]
            .iter()
            .any(|phrase| message.contains(phrase))
        }),
        _ => false,
    }
}

/// A nullable RPC result. Only an explicit null means absence; errors and
/// malformed envelopes must not become evidence that a transaction is missing.
pub fn optional_result(resp: &Value) -> Result<Option<&Value>, RpcError> {
    if let Some(error) = resp.get("error") {
        return Err(RpcError::Unreachable(format!("RPC failed: {error}")));
    }
    let result = resp
        .get("result")
        .ok_or_else(|| RpcError::Unreachable("RPC response missing result".into()))?;
    Ok((!result.is_null()).then_some(result))
}

/// The `result` of a JSON-RPC response. A provider that refuses, as a rate
/// limit does, answers with an `error` instead, which comes back in the
/// provider's own words rather than as a missing field.
pub fn result(resp: &Value) -> Result<&Value, RpcError> {
    if let Some(error) = resp.get("error") {
        return Err(RpcError::BadResult(format!("provider error: {error}")));
    }
    resp.get("result")
        .filter(|result| !result.is_null())
        .ok_or_else(|| RpcError::BadResult("RPC response has no result".into()))
}

/// The newest block number the endpoint at `url` reports, read under
/// `class`'s share of the budget. Each caller subtracts its own confirmation
/// depth.
pub async fn head(client: &Upstream, url: &str, class: Class) -> Result<u64, RpcError> {
    let resp = call_as(
        class,
        client,
        url,
        "eth_blockNumber",
        json!([]),
        MAX_RESPONSE,
    )
    .await?;
    let quantity = result(&resp)?
        .as_str()
        .ok_or_else(|| RpcError::BadResult("eth_blockNumber result is not a quantity".into()))?;
    crate::abi::parse_quantity(quantity).map_err(|e| RpcError::BadResult(e.to_string()))
}

/// The `result` of a JSON-RPC response as decoded hex bytes.
pub fn result_bytes(resp: &Value) -> Result<Vec<u8>, RpcError> {
    let result = resp
        .get("result")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::BadResult("rpc response missing result".into()))?;
    decode_hex(result).ok_or_else(|| RpcError::BadResult("bad rpc result hex".into()))
}

/// A structured RPC error that reports EVM rejection, rather than a provider outage.
pub fn is_revert(error: &Value) -> bool {
    error.get("code").and_then(Value::as_i64) == Some(3)
        || error
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.to_ascii_lowercase().contains("revert"))
}

/// Provider revert data, allowing three wrappers inside `error.data`.
/// Callers retain their own byte bounds, hex checks and error policy.
pub fn error_data(error: &Value) -> Option<&str> {
    let mut data = &error["data"];
    for _ in 0..3 {
        if data.is_string() {
            break;
        }
        data = &data["data"];
    }
    data.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    mod throttle;

    #[test]
    fn revert_data_allows_only_three_nested_wrappers() {
        let mut data = json!("0x1234");
        for _ in 0..=3 {
            assert_eq!(error_data(&json!({"data": data})), Some("0x1234"));
            data = json!({"data": data});
        }
        assert_eq!(error_data(&json!({"data": data})), None);
        assert_eq!(error_data(&json!({"message": "refused"})), None);
        assert_eq!(error_data(&json!({"data": 42})), None);
    }

    #[tokio::test]
    async fn ingestion_has_slots_even_while_every_client_slot_is_held() {
        let node = crate::mock_node::spawn(crate::mock_node::Node::new(
            1,
            "0x0000000000000000000000000000000000000001",
            Vec::new(),
        ))
        .await;
        let upstream = Upstream::unlimited(reqwest::Client::new());
        // These permits model client calls whose upstream responses are still out.
        let capacity = upstream.client_limiter.slots.available_permits();
        assert_eq!(
            capacity + upstream.ingestion_limiter.slots.available_permits(),
            MAX_UPSTREAM_CALLS
        );
        let held = upstream
            .client_limiter
            .slots
            .acquire_many(capacity as u32)
            .await
            .unwrap();
        let answer = call_as(
            Class::Ingestion,
            &upstream,
            &node.url,
            "eth_chainId",
            json!([]),
            MAX_RESPONSE,
        )
        .await
        .unwrap();
        assert_eq!(answer["result"], "0x1");
        assert!(matches!(
            call(&upstream, &node.url, "eth_chainId", json!([])).await,
            Err(RpcError::Busy)
        ));
        // Releasing cancelled client work makes its slots usable again.
        drop(held);
        assert!(
            call(&upstream, &node.url, "eth_chainId", json!([]))
                .await
                .is_ok()
        );
    }

    /// Clients spend down to the reserve and no further; ingestion spends the
    /// reserve too; tokens accrue with time; a zero budget meters nothing.
    #[test]
    fn budget_keeps_the_reserve_for_ingestion() {
        let budget = Budget::new(60, 50);
        let start = Instant::now();
        for _ in 0..10 {
            assert_eq!(budget.take_at(start, Class::Client), Ok(()));
        }
        let wait = budget.take_at(start, Class::Client).unwrap_err();
        assert!(
            (Duration::from_millis(900)..=Duration::from_millis(1100)).contains(&wait),
            "one token a second: {wait:?}"
        );
        for _ in 0..50 {
            assert_eq!(budget.take_at(start, Class::Ingestion), Ok(()));
        }
        assert!(budget.take_at(start, Class::Ingestion).is_err(), "empty");
        let later = start + Duration::from_secs(2);
        assert!(
            budget.take_at(later, Class::Client).is_err(),
            "two tokens are below the reserve"
        );
        assert_eq!(budget.take_at(later, Class::Ingestion), Ok(()));
        assert_eq!(budget.take_at(later, Class::Ingestion), Ok(()));
        assert!(budget.take_at(later, Class::Ingestion).is_err());
        let unlimited = Budget::unlimited();
        for _ in 0..1000 {
            assert_eq!(unlimited.take_at(start, Class::Client), Ok(()));
        }
    }

    /// Name lookups stop halfway between the ingestion reserve and the full
    /// budget. Other client work still spends down to the reserve, and once
    /// the balance is under the names' line, the tokens that accrue go to it
    /// first: lookups wait until the balance is back over their line.
    #[test]
    fn names_leave_the_lower_half_of_the_client_allowance_to_other_work() {
        // A token a second, ten kept for ingestion: names stop at 35.
        let budget = Budget::new(60, 10);
        let start = Instant::now();
        for _ in 0..25 {
            assert_eq!(budget.take_at(start, Class::Names), Ok(()));
        }
        assert!(budget.take_at(start, Class::Names).is_err(), "35 left");
        for _ in 0..25 {
            assert_eq!(budget.take_at(start, Class::Client), Ok(()));
        }
        assert!(budget.take_at(start, Class::Client).is_err(), "10 left");
        let later = start + Duration::from_secs(2);
        assert!(budget.take_at(later, Class::Names).is_err(), "12 left");
        assert_eq!(budget.take_at(later, Class::Client), Ok(()));
        let much_later = later + Duration::from_secs(30);
        assert_eq!(budget.take_at(much_later, Class::Names), Ok(()), "41 left");
    }

    /// The names' line is a whole number of tokens under a full budget, so
    /// however small the budget, a lookup can always make a call.
    #[test]
    fn names_can_always_call_from_a_full_budget() {
        for (per_minute, reserve) in [(1, 0), (1, 5), (2, 1), (120, 119), (3000, 300)] {
            let budget = Budget::new(per_minute, reserve);
            assert_eq!(
                budget.take_at(Instant::now(), Class::Names),
                Ok(()),
                "{per_minute} a minute, {reserve} reserved"
            );
        }
    }

    /// Like other client work, a lookup over its line is refused at once,
    /// before it waits for tokens or for a slot, and counted as names' own.
    #[tokio::test]
    async fn a_lookup_over_its_line_is_refused_at_once() {
        let node = crate::mock_node::spawn(crate::mock_node::Node::new(
            1,
            "0x0000000000000000000000000000000000000001",
            Vec::new(),
        ))
        .await;
        let upstream = Upstream::new(reqwest::Client::new(), Budget::new(60, 10));
        for _ in 0..25 {
            upstream.budget.take(Class::Names).await.unwrap();
        }
        let capacity = upstream.client_limiter.slots.available_permits();
        let held = upstream
            .client_limiter
            .slots
            .acquire_many(capacity as u32)
            .await
            .unwrap();
        let started = Instant::now();
        let refused = call_as(
            Class::Names,
            &upstream,
            &node.url,
            "eth_chainId",
            json!([]),
            MAX_RESPONSE,
        )
        .await;
        assert!(matches!(refused, Err(RpcError::Busy)));
        assert!(started.elapsed() < Duration::from_millis(500), "at once");
        let counts = upstream.counts();
        assert_eq!((counts.names.busy, counts.client.busy), (1, 0));
        drop(held);
        assert!(
            call(&upstream, &node.url, "eth_chainId", json!([]))
                .await
                .is_ok(),
            "other client work still has tokens"
        );
        // With tokens to spend, a lookup takes a client slot, never one of
        // ingestion's: it gets through while every ingestion slot is held.
        let upstream = Upstream::unlimited(reqwest::Client::new());
        let _ingesting = upstream
            .ingestion_limiter
            .slots
            .acquire_many(INGESTION_SLOTS as u32)
            .await
            .unwrap();
        let started = Instant::now();
        let answer = call_as(
            Class::Names,
            &upstream,
            &node.url,
            "eth_chainId",
            json!([]),
            MAX_RESPONSE,
        )
        .await;
        assert!(answer.is_ok() && started.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn a_providers_error_comes_back_in_its_own_words() {
        let refused = json!({"jsonrpc": "2.0", "id": 1, "error": {"code": -32005, "message": "rate limited"}});
        let error = result(&refused).unwrap_err().to_string();
        assert!(error.contains("rate limited"), "{error}");
        assert_eq!(result(&json!({ "result": "0x1" })).unwrap(), "0x1");
        assert!(result(&json!({ "result": null })).is_err());
        assert!(result(&json!({})).is_err());
    }

    #[test]
    fn result_bytes_wants_a_hex_result() {
        assert_eq!(
            result_bytes(&json!({ "result": "0x0102" })).unwrap(),
            vec![1, 2]
        );
        assert_eq!(
            result_bytes(&json!({ "result": "0102" })).unwrap(),
            vec![1, 2]
        );
        let err = result_bytes(&json!({ "error": {} }))
            .unwrap_err()
            .to_string();
        assert_eq!(err, "rpc response missing result");
        let err = result_bytes(&json!({ "result": "0xzz" }))
            .unwrap_err()
            .to_string();
        assert!(err.starts_with("bad rpc result hex"), "{err}");
    }

    #[tokio::test]
    async fn transport_errors_never_name_the_endpoint() {
        // Nothing listens here; the error must say so without echoing the URL.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/secret-key", listener.local_addr().unwrap());
        drop(listener);
        let err = call(
            &Upstream::unlimited(reqwest::Client::new()),
            &url,
            "eth_chainId",
            json!([]),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, RpcError::Unreachable(_)), "{err}");
        let text = err.to_string();
        assert!(text.starts_with("rpc unreachable"), "{text}");
        assert!(!text.contains("secret-key"), "endpoint leaked: {text}");
    }

    #[tokio::test]
    async fn a_full_limiter_refuses_after_the_wait_and_frees_on_release() {
        let l = Limiter::new(2, Duration::from_millis(50));
        let a = l.slot().await.expect("first slot");
        let _b = l.slot().await.expect("second slot");
        let started = std::time::Instant::now();
        assert!(l.slot().await.is_none(), "no third slot");
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "waited for one first"
        );
        drop(a);
        assert!(l.slot().await.is_some(), "a released slot is taken again");
    }
}
