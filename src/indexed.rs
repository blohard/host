//! The indexed queries over HTTP: each request is answered from one chain's
//! index (crate::index) under one short RAM read lock. The index knows
//! nothing of HTTP; this module owns the routes' requests and responses.

use crate::chain::{Address, decimal};
use crate::index::{
    changes,
    model::Model,
    query::{self, Cursor, Filter, Refusal, ReplyCursor, page_size},
    types::*,
};
use crate::{
    http::{JsonPath, json_error, json_ok},
    server::AppState,
};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

fn read(
    state: &AppState,
    chain: u64,
    query: impl FnOnce(&Model) -> anyhow::Result<Value>,
) -> Response {
    let Some(index) = state.index(chain) else {
        return json_error(StatusCode::NOT_FOUND, "chain_not_served");
    };
    let Some(result) = index.read_ready(query) else {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "index_unavailable");
    };
    respond(chain, result)
}

/// Deliberate refusals carry their code; anything else is an internal
/// failure, logged here and answered without its text.
fn respond(chain: u64, result: anyhow::Result<Value>) -> Response {
    match result {
        Ok(value) => json_ok(value),
        Err(error) => match error.downcast_ref::<Refusal>() {
            Some(Refusal(code)) => {
                let status = match *code {
                    "reset_required" => StatusCode::CONFLICT,
                    "message_not_found" => StatusCode::NOT_FOUND,
                    _ => StatusCode::BAD_REQUEST,
                };
                json_error(status, code)
            }
            None => {
                tracing::error!(chain, ?error, "index query failed");
                json_error(StatusCode::INTERNAL_SERVER_ERROR, "index_query_failed")
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refusals_keep_their_code_and_other_errors_become_a_fixed_500() {
        let cases = [
            (
                Err(anyhow::anyhow!(Refusal("reset_required"))),
                409,
                "reset_required",
            ),
            (
                Err(anyhow::anyhow!(Refusal("message_not_found"))),
                404,
                "message_not_found",
            ),
            (
                Err(anyhow::anyhow!(Refusal("page_limit_out_of_range"))),
                400,
                "page_limit_out_of_range",
            ),
            (
                Err(anyhow::anyhow!("serialising secret detail failed")),
                500,
                "index_query_failed",
            ),
            (Ok(serde_json::json!({"ok": true})), 200, ""),
        ];
        for (result, status, code) in cases {
            let response = respond(1, result);
            assert_eq!(response.status().as_u16(), status);
            let bytes = axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap();
            let text = String::from_utf8_lossy(&bytes);
            if status != 200 {
                assert_eq!(
                    serde_json::from_slice::<Value>(&bytes).unwrap()["error"],
                    code
                );
            }
            assert!(!text.contains("secret"), "{text}");
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceRequest {
    account: Address,
    exclude_list: Option<ListKey>,
    cursor: Option<Cursor>,
    #[serde(default = "page_size")]
    limit: usize,
}

pub async fn references(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(req): Json<ReferenceRequest>,
) -> Response {
    read(&state, chain, |model| {
        Ok(serde_json::to_value(query::references(
            model,
            req.account,
            req.exclude_list,
            req.cursor.as_ref(),
            req.limit,
        )?)?)
    })
}

pub async fn notifications(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(req): Json<ReferenceRequest>,
) -> Response {
    read(&state, chain, |model| {
        Ok(serde_json::to_value(query::notifications(
            model,
            req.account,
            req.exclude_list,
            req.cursor.as_ref(),
            req.limit,
        )?)?)
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedRequest {
    #[serde(default)]
    filter: Filter,
    cursor: Option<Cursor>,
    #[serde(default = "page_size")]
    limit: usize,
}

pub async fn feed(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(mut req): Json<FeedRequest>,
) -> Response {
    read(&state, chain, |model| {
        req.filter.policy = state.policy.snapshot();
        Ok(serde_json::to_value(query::feed(
            model,
            &req.filter,
            req.cursor.as_ref(),
            req.limit,
        )?)?)
    })
}

pub async fn message(
    State(state): State<Arc<AppState>>,
    JsonPath((chain, id)): JsonPath<(u64, u64)>,
    Query(req): Query<query::MessageRequest>,
) -> Response {
    read(&state, chain, |model| query::message(model, id, &req))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepliesRequest {
    #[serde(with = "decimal")]
    parent: u64,
    cursor: Option<ReplyCursor>,
    #[serde(default = "page_size")]
    limit: usize,
}

pub async fn replies(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(req): Json<RepliesRequest>,
) -> Response {
    read(&state, chain, |model| {
        Ok(serde_json::to_value(query::replies(
            model,
            req.parent,
            req.cursor.as_ref(),
            req.limit,
        )?)?)
    })
}

pub async fn changes(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(mut req): Json<changes::Request>,
) -> Response {
    read(&state, chain, |model| {
        if let Some(filter) = &mut req.scope.feed {
            filter.policy = state.policy.snapshot();
        }
        Ok(serde_json::to_value(changes::read(model, &req)?)?)
    })
}

pub async fn snapshots(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(req): Json<query::StateRequest>,
) -> Response {
    let policy = state.policy.snapshot();
    read(&state, chain, |model| {
        query::snapshots(
            model,
            state.policy.enabled().then_some(policy.as_ref()),
            &req,
        )
    })
}

pub async fn list(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(req): Json<query::ListRequest>,
) -> Response {
    read(&state, chain, |model| query::list(model, &req))
}

pub async fn lookup(
    State(state): State<Arc<AppState>>,
    JsonPath(chain): JsonPath<u64>,
    Json(req): Json<query::LookupRequest>,
) -> Response {
    read(&state, chain, |model| query::lookup(model, &req))
}
