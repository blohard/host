//! HTTP uploads: `POST /api/publish`.
//!
//! Read the bounded body first, charging its buffer to the pool's memory as
//! it grows, and take one of the pool's slots only once every byte is in, so
//! a slow sender holds no slot. Detached verification and storage keep both
//! until completion or the upload deadline, even if the client disconnects.
//! Shared content checks live in [`super::accept`]; this module owns request
//! extraction, upload deadlines and response rendering.

use super::{Outcome, PublishParams, accept};
use crate::http::{ApiError, JsonQuery, json_ok, json_said};
use crate::keccak;
use crate::server::{AppState, NO_STORE};
use axum::{
    extract::{FromRequestParts, State},
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use futures::StreamExt as _;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Large uploads being checked and written at once, including after their
/// client has gone. A slot is taken once the upload's bytes are all in.
pub const MAX_UPLOADS: usize = 32;
/// Uploads no larger than the body cap, the message bodies every post has,
/// have slots of their own, so attachments cannot hold them up.
pub const MAX_SMALL_UPLOADS: usize = 16;
/// The upload memory each pool has, in bytes: the capacity of its uploads'
/// buffers, charged before a buffer grows and kept from an upload's first
/// byte until its checks and write are done or its deadline passes. Small
/// uploads have enough for every visitor this host tracks (4,096) to send two
/// 4,000-byte bodies at once. Large ones have what their 32 slots held at the
/// example's 2 MiB attachment cap. Not counted: the HTTP server's own read
/// buffer for each connection, a parent body read to check an attachment (one
/// per running check), and a write the store has begun when its upload's
/// deadline passes (one per store).
pub const SMALL_MEMORY: usize = 32 << 20;
pub const LARGE_MEMORY: usize = 64 << 20;
/// A buffer's first allocation, unless its upload declares less, so a sender
/// that has sent a single byte holds at most this much.
pub const FIRST_ALLOCATION: usize = 8 << 10;
/// How long a small upload's body may take to arrive: a few kilobytes that
/// trickle in longer are refused, freeing their memory.
#[cfg(not(test))]
const SMALL_ARRIVAL: Duration = Duration::from_secs(10);
#[cfg(test)]
const SMALL_ARRIVAL: Duration = Duration::from_millis(500);
/// How long an upload's checks and write may hold its slot once its bytes
/// are in: under the app's minute-long wait for an answer.
#[cfg(not(test))]
const UPLOAD_DEADLINE: Duration = Duration::from_secs(50);
#[cfg(test)]
const UPLOAD_DEADLINE: Duration = Duration::from_secs(2);

/// Upload admission: two pools, chosen by the length an upload declares.
pub struct Uploads {
    /// Uploads that declare a length no larger than the body cap.
    pub small: Pool,
    /// Every other upload, including one that does not declare its length.
    pub large: Pool,
}

/// One pool's slots, taken once an upload's bytes are in, and its memory.
#[derive(Clone)]
pub struct Pool {
    pub slots: Arc<Semaphore>,
    /// Bytes, see [`SMALL_MEMORY`].
    pub memory: Arc<Semaphore>,
}

impl Pool {
    fn new(slots: usize, memory: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(slots)),
            memory: Arc::new(Semaphore::new(memory)),
        }
    }
}

impl Uploads {
    pub fn new() -> Self {
        Self {
            small: Pool::new(MAX_SMALL_UPLOADS, SMALL_MEMORY),
            large: Pool::new(MAX_UPLOADS, LARGE_MEMORY),
        }
    }

    /// Wait until every admitted upload has finished: each holds a slot of
    /// its pool until its write is done, even after its client has gone. The
    /// slots are free again on return, so this keeps no new upload out: a
    /// stopping host calls it once its server, the only way in, has stopped.
    pub async fn drain(&self) {
        let _ = tokio::join!(
            self.small.slots.acquire_many(MAX_SMALL_UPLOADS as u32),
            self.large.slots.acquire_many(MAX_UPLOADS as u32)
        );
    }
}

/// An upload's declared length, and whether it takes the small pool: no
/// larger than the body cap. The host's pools and each visitor's slots both
/// use this one rule. The server reads no more than the declared length, so
/// a small upload cannot grow into an attachment after admission.
pub fn declared_size(headers: &axum::http::HeaderMap, state: &AppState) -> (Option<usize>, bool) {
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok());
    let small = match (declared, state.store.as_ref()) {
        (Some(n), Some(store)) => n <= store.bodies.max_size(),
        _ => false,
    };
    (declared, small)
}

/// Extracted before the body: the pool the declared length picks.
pub struct Upload {
    small: bool,
    declared: Option<usize>,
    pool: Pool,
}

impl FromRequestParts<Arc<AppState>> for Upload {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Response> {
        let store = state
            .store
            .as_ref()
            .ok_or_else(|| json_said(StatusCode::NOT_FOUND, "no_store", NO_STORE))?;
        let (declared, small) = declared_size(&parts.headers, state);
        let pool = if small {
            &store.uploads.small
        } else {
            &store.uploads.large
        };
        Ok(Self {
            small,
            declared,
            pool: pool.clone(),
        })
    }
}

impl Upload {
    /// The upload's bytes, with the memory their buffer is charged: at most
    /// about a kilobyte past the largest upload this host takes, so an
    /// oversized one gets the exact refusal. A small upload's must arrive
    /// within [`SMALL_ARRIVAL`]. Each piece is copied into one buffer as it
    /// arrives, never kept with the read buffer it came in.
    async fn read(
        &self,
        state: &AppState,
        body: axum::body::Body,
    ) -> Result<(bytes::Bytes, Option<OwnedSemaphorePermit>), ApiError> {
        let limit = state.max_upload() + 1024;
        let too_large = || ApiError::too_large("content", state.max_upload());
        if self.declared.is_some_and(|n| n > limit) {
            return Err(too_large());
        }
        // The server delivers no more than a declared length.
        let ceiling = self.declared.unwrap_or(limit);
        let read = async {
            let mut stream = body.into_data_stream();
            let mut buffer = Buffer::new(self.pool.memory.clone());
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| {
                    ApiError::said(
                        StatusCode::BAD_REQUEST,
                        "upload_incomplete",
                        "the upload did not arrive in full; send it again",
                    )
                })?;
                if buffer.bytes.len() + chunk.len() > limit {
                    return Err(too_large());
                }
                buffer.push(&chunk, ceiling)?;
            }
            Ok(buffer.into_parts())
        };
        if !self.small {
            return read.await;
        }
        tokio::time::timeout(SMALL_ARRIVAL, read)
            .await
            .unwrap_or_else(|_| {
                Err(ApiError::said(
                    StatusCode::REQUEST_TIMEOUT,
                    "upload_too_slow",
                    "the upload arrived too slowly; send it again",
                ))
            })
    }
}

/// Run `work` detached, holding `slot` and `charge`, the upload's memory,
/// until it finishes or [`UPLOAD_DEADLINE`] passes. Dropping the HTTP waiter
/// cannot release them early. Stopping at the deadline is safe: the store
/// finishes a write it began in a task of its own. That write keeps its
/// bytes past their charge, but the store runs one write at a time, so that
/// is at most one record per store.
async fn finish(
    slot: OwnedSemaphorePermit,
    charge: Option<OwnedSemaphorePermit>,
    work: impl std::future::Future<Output = Response> + Send + 'static,
) -> Response {
    {
        tokio::spawn(async move {
            let _held = (slot, charge);
            tokio::time::timeout(UPLOAD_DEADLINE, work)
                .await
                .unwrap_or_else(|_| {
                    let mut response = ApiError::busy().respond();
                    response
                        .headers_mut()
                        .insert(header::RETRY_AFTER, "5".parse().unwrap());
                    response
                })
        })
        .await
        .unwrap_or_else(|error| {
            tracing::error!(?error, "upload task failed");
            json_said(
                StatusCode::INTERNAL_SERVER_ERROR,
                "upload_failed",
                "upload failed; retry the same bytes",
            )
        })
    }
}

/// An upload's buffer, charged to its pool's memory for exactly its
/// capacity: nothing can grow the bytes without charging them first.
struct Buffer {
    bytes: Vec<u8>,
    memory: Arc<Semaphore>,
    charge: Option<OwnedSemaphorePermit>,
}

impl Buffer {
    fn new(memory: Arc<Semaphore>) -> Self {
        Self {
            bytes: Vec::new(),
            memory,
            charge: None,
        }
    }

    /// Append `chunk`. Capacity at least doubles, from [`FIRST_ALLOCATION`],
    /// so a body sent in tiny pieces costs few copies, but never passes
    /// `ceiling`, the most the upload can hold, and any new capacity is
    /// charged first. Without room, the host is busy.
    fn push(&mut self, chunk: &[u8], ceiling: usize) -> Result<(), ApiError> {
        let needed = self.bytes.len() + chunk.len();
        let capacity = self.bytes.capacity();
        if needed > capacity {
            let doubled = (capacity * 2).max(FIRST_ALLOCATION).min(ceiling);
            let target = needed.max(doubled);
            let added = u32::try_from(target - capacity).map_err(|_| ApiError::busy())?;
            let permit = self
                .memory
                .clone()
                .try_acquire_many_owned(added)
                .map_err(|_| ApiError::busy())?;
            match &mut self.charge {
                Some(held) => held.merge(permit),
                None => self.charge = Some(permit),
            }
            self.bytes.reserve_exact(target - self.bytes.len());
            debug_assert_eq!(
                self.bytes.capacity(),
                target,
                "charged exactly the capacity"
            );
        }
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    #[cfg(test)]
    fn charged(&self) -> usize {
        self.charge.as_ref().map_or(0, |c| c.num_permits())
    }

    /// The bytes without a copy, so the store takes this allocation back as
    /// it is, and the charge that pays for them.
    fn into_parts(self) -> (bytes::Bytes, Option<OwnedSemaphorePermit>) {
        (bytes::Bytes::from(self.bytes), self.charge)
    }
}

pub async fn publish(
    State(state): State<Arc<AppState>>,
    upload: Upload,
    JsonQuery(params): JsonQuery<PublishParams>,
    body: axum::body::Body,
) -> Response {
    let (body, charge) = match upload.read(&state, body).await {
        Ok(read) => read,
        Err(refusal) => return refusal.respond(),
    };
    // Every byte is in, so how slowly it came no longer matters. A slot now,
    // or try again: finished uploads do not queue with their bytes.
    let Ok(slot) = upload.pool.slots.clone().try_acquire_owned() else {
        return ApiError::busy().respond();
    };
    finish(slot, charge, publish_buffered(state, params, body)).await
}

async fn publish_buffered(
    state: Arc<AppState>,
    params: PublishParams,
    body: bytes::Bytes,
) -> Response {
    let (chain_id, message_id, bytes) = (params.chain_id, params.message_id, body.len());
    let tx_hash = params.tx_hash.clone();
    let digest = keccak::hash(&[body.as_ref()]);
    let hash = digest.to_hex();
    let Some(store) = state.store.as_ref() else {
        return json_said(StatusCode::NOT_FOUND, "no_store", NO_STORE);
    };
    match accept(&state, store, params, digest, body).await {
        Ok(Outcome::Stored { kind, newly_stored }) => {
            tracing::info!(chain_id, message_id, tx_hash, bytes, hash = %hash, kind = kind.label(), stored = newly_stored, "publish accepted");
            json_ok(
                json!({ "hash": hash, "stored": newly_stored, "held": false, "bytes": bytes, "kind": kind.label() }),
            )
        }
        Ok(Outcome::Held {
            kind,
            message_id: known,
        }) => {
            tracing::info!(chain_id, message_id = known, tx_hash, bytes, hash = %hash, kind = kind.label(), "publish held until the message is final");
            (
                StatusCode::ACCEPTED,
                axum::Json(json!({ "hash": hash, "stored": false, "held": true, "bytes": bytes, "kind": kind.label(), "message_id": known })),
            )
                .into_response()
        }
        Err(error) => {
            // Logged here with the chain, message and hash; the plain reply logs nothing more.
            tracing::warn!(chain_id, message_id, tx_hash, bytes, hash = %hash, status = error.status.as_u16(), "publish refused: {}", error.message);
            error.into_response()
        }
    }
}

#[cfg(test)]
mod upload_tests {
    use super::*;
    use futures::poll;
    use tokio::sync::{Semaphore, oneshot};

    #[tokio::test]
    async fn cancelled_upload_waiter_keeps_its_slot_until_work_finishes() {
        let pool = Arc::new(Semaphore::new(1));
        let slot = pool.clone().try_acquire_owned().unwrap();
        let (started, running) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        let mut request = Box::pin(finish(slot, None, async move {
            started.send(()).unwrap();
            wait.await.unwrap();
            done.send(()).unwrap();
            json_ok(json!({"stored": true}))
        }));
        assert!(poll!(request.as_mut()).is_pending());
        running.await.unwrap();
        drop(request);
        assert!(pool.clone().try_acquire_owned().is_err());
        release.send(()).unwrap();
        finished.await.unwrap();
        let _recovered = tokio::time::timeout(std::time::Duration::from_secs(1), pool.acquire())
            .await
            .unwrap()
            .unwrap();
    }

    /// Checks that never answer, as behind a hanging upstream, cannot hold
    /// the slot past the deadline: the uploader is told to try again.
    #[tokio::test]
    async fn an_upload_past_its_deadline_frees_its_slot_and_can_be_retried() {
        let pool = Arc::new(Semaphore::new(1));
        let slot = pool.clone().try_acquire_owned().unwrap();
        let response = tokio::time::timeout(
            UPLOAD_DEADLINE + Duration::from_secs(1),
            finish(slot, None, std::future::pending()),
        )
        .await
        .expect("answered by the deadline");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
        assert!(pool.try_acquire().is_ok(), "the slot is free");
    }

    /// A buffer filled a byte at a time is always charged exactly its
    /// capacity, which stays within twice what it holds (or the first
    /// allocation) and within its ceiling, and grows only a few times.
    #[test]
    fn a_buffer_filled_in_tiny_pieces_is_charged_its_capacity() {
        for (ceiling, length) in [(100_000, 100_000), (1 << 22, 100_000), (100, 100)] {
            let memory = Arc::new(Semaphore::new(1 << 22));
            let mut buffer = Buffer::new(memory.clone());
            let mut growths = 0;
            for _ in 0..length {
                let before = buffer.bytes.capacity();
                buffer.push(b"x", ceiling).unwrap();
                growths += usize::from(buffer.bytes.capacity() != before);
                assert_eq!(buffer.charged(), buffer.bytes.capacity());
                assert_eq!(memory.available_permits(), (1 << 22) - buffer.charged());
                assert!(buffer.bytes.capacity() <= ceiling);
                assert!(buffer.bytes.capacity() <= (2 * buffer.bytes.len()).max(FIRST_ALLOCATION));
            }
            assert!(growths <= 6, "{growths} growths for {length} bytes");
            drop(buffer);
            assert_eq!(memory.available_permits(), 1 << 22);
        }
    }

    /// Without room, nothing grows and nothing more is charged.
    #[test]
    fn a_buffer_without_room_is_refused_unchanged() {
        let memory = Arc::new(Semaphore::new(FIRST_ALLOCATION + 10));
        let mut buffer = Buffer::new(memory.clone());
        buffer.push(&[0; FIRST_ALLOCATION], 1 << 20).unwrap();
        let refused = buffer.push(b"x", 1 << 20).unwrap_err();
        assert_eq!(refused.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(buffer.bytes.capacity(), FIRST_ALLOCATION);
        assert_eq!(buffer.charged(), FIRST_ALLOCATION);
        assert_eq!(memory.available_permits(), 10);
    }
}
