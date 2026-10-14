//! Host-policy requests: validation, signatures, author checks and responses.
//! Durable writes and the checks repeated under each writer live in [`super`].

use super::{Config, Edit, Policy, now, refused, takedowns};
use crate::{
    bot_declarations::{self, Statement},
    chain::Address,
    http::{ApiError, json_ok},
    server::AppState,
};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

impl Policy {
    pub fn allow(&self, author: &[u8; 20]) -> Result<(), ApiError> {
        if self.blocked(author) {
            Err(ApiError::said(
                StatusCode::FORBIDDEN,
                "host_blocked",
                "posting blocked by host",
            ))
        } else {
            Ok(())
        }
    }
    /// Every policy answer (status, reads, edits and declarations) is built
    /// here, so none can miss a list. Declarations carry their own counter.
    pub fn public(&self) -> Value {
        self.config().map_or(Value::Null, |c| {
            let lists = self.snapshot();
            let declarations = self.declarations();
            json!({ "admin": c.admin, "admin_chain": c.admin_chain, "origin": c.origin,
                "revision": lists.revision.to_string(), "curated": lists.curated, "blocked": lists.blocked,
                "bots": lists.bots, "declared_bots": declarations.declared(),
                "declarations": declarations.counter.to_string() })
        })
    }
}

pub async fn read(State(state): State<Arc<AppState>>) -> Response {
    response(&state.policy)
}
fn response(policy: &Policy) -> Response {
    let mut response = json_ok(policy.public());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}
pub async fn write(
    State(state): State<Arc<AppState>>,
    edit: Result<Json<Edit>, JsonRejection>,
) -> Response {
    let edit = match edit {
        Ok(Json(edit)) => edit,
        Err(_) => {
            return ApiError::new(StatusCode::BAD_REQUEST, "invalid_policy_edit").into_response();
        }
    };
    match update(&state, edit).await {
        Ok(()) => response(&state.policy),
        Err(error) => error.into_response(),
    }
}
async fn update(state: &AppState, edit: Edit) -> Result<(), ApiError> {
    let config = configured(state)?;
    if edit.revision != state.policy.snapshot().revision {
        return Err(ApiError::new(StatusCode::CONFLICT, "policy_changed"));
    }
    let message = edit.message(&config.origin, now())?;
    admin_signed(state, config, &message, &edit.signature).await?;
    state.policy.apply(edit).await
}

/// The host policy settings, or the refusal of a host that has none.
fn configured(state: &AppState) -> Result<&Config, ApiError> {
    state
        .policy
        .config()
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "host_policy_disabled"))
}

/// Refuse `message` unless the configured admin signed it.
async fn admin_signed(
    state: &AppState,
    config: &Config,
    message: &str,
    signature: &str,
) -> Result<(), ApiError> {
    if signed_by(state, config, &config.admin, message, signature).await? {
        Ok(())
    } else {
        Err(ApiError::new(StatusCode::FORBIDDEN, "not_host_admin"))
    }
}

/// Whether `signer` signed `message` (EIP-191): recovered directly for an
/// ordinary wallet, otherwise approved through ERC-1271 on the configured
/// `admin_chain`. Labels apply on every chain, and one contract address can
/// have different owners on different chains, so no request picks the chain.
async fn signed_by(
    state: &AppState,
    config: &Config,
    signer: &Address,
    message: &str,
    signature: &str,
) -> Result<bool, ApiError> {
    let signature = crate::abi::decode_hex(signature)
        .filter(|s| s.len() <= 4096)
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "invalid_signature"))?;
    let digest = alloy_primitives::eip191_hash_message(message.as_bytes()).0;
    if crate::verifier::recover_signer(&digest, &signature) == Some(signer.0) {
        return Ok(true);
    }
    let (client, chain) = state
        .verifier
        .chain(config.admin_chain)
        .map_err(ApiError::from)?;
    let result = crate::rpc::call(client, &chain.url, "eth_call", json!([{
        "to": signer, "data": crate::abi::hex0x(&crate::abi::is_valid_signature_calldata(digest, &signature)),
        "gas": format!("0x{:x}", crate::rpc::SIM_GAS_CAP)
    }, "latest"])).await.map_err(ApiError::from)?;
    Ok(result.get("error").is_none()
        && crate::rpc::result_bytes(&result).is_ok_and(|bytes| {
            bytes.len() >= 32
                && bytes[..4] == [0x16, 0x26, 0xba, 0x7e]
                && bytes[4..32].iter().all(|b| *b == 0)
        }))
}

/// The takedown list as only the admin sees it. Never cached.
fn takedown_list(policy: &Policy) -> Response {
    let list = policy.takedowns();
    let mut response = json_ok(json!({
        "revision": list.revision.to_string(),
        "hashes": list.hashes,
    }));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

/// The admin adds hashes to the takedown list or removes them.
pub async fn write_takedowns(
    State(state): State<Arc<AppState>>,
    edit: Result<Json<takedowns::Edit>, JsonRejection>,
) -> Response {
    let Ok(Json(edit)) = edit else {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_takedown_edit").into_response();
    };
    let checked = async {
        let config = configured(&state)?;
        if edit.revision != state.policy.takedowns().revision {
            return Err(ApiError::new(StatusCode::CONFLICT, "takedowns_changed"));
        }
        let message = edit.message(&config.origin, now())?;
        admin_signed(&state, config, &message, &edit.signature).await?;
        state.policy.write_takedowns(edit).await
    };
    match checked.await {
        Ok(()) => takedown_list(&state.policy),
        Err(error) => error.into_response(),
    }
}

/// The admin reads the takedown list, which is never published.
pub async fn takedowns(
    State(state): State<Arc<AppState>>,
    read: Result<Json<takedowns::View>, JsonRejection>,
) -> Response {
    let Ok(Json(read)) = read else {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_takedown_read").into_response();
    };
    let checked = async {
        let config = configured(&state)?;
        let message = read.message(&config.origin, now())?;
        admin_signed(&state, config, &message, &read.signature).await
    };
    match checked.await {
        Ok(()) => takedown_list(&state.policy),
        Err(error) => error.into_response(),
    }
}

/// An account's own statement that it is, or is no longer, a bot.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    account: Address,
    bot: bool,
    issued: u64,
    signature: String,
}

pub async fn declare(
    State(state): State<Arc<AppState>>,
    declaration: Result<Json<Declaration>, JsonRejection>,
) -> Response {
    let Ok(Json(declaration)) = declaration else {
        return ApiError::new(StatusCode::BAD_REQUEST, "invalid_declaration").into_response();
    };
    match record(&state, declaration).await {
        Ok(()) => response(&state.policy),
        Err(error) => error.into_response(),
    }
}

/// Cheap checks and authorship first, then the signature, then the write.
async fn record(state: &AppState, declaration: Declaration) -> Result<(), ApiError> {
    let config = configured(state)?;
    if declaration.account == Address::ZERO {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_declaration",
        ));
    }
    let account = alloy_primitives::Address::from(declaration.account.0);
    let statement = Statement {
        bot: declaration.bot,
        issued: declaration.issued,
    };
    state
        .policy
        .declarations()
        .check(&account, statement, now())
        .map_err(refused)?;
    let message = bot_declarations::message(
        &config.origin,
        &declaration.account.0,
        declaration.bot,
        declaration.issued,
    );
    // The free check first: an account that never posted is turned away
    // before any signature check, which may ask the chain about a contract
    // wallet. Naming an author that has posted still reaches that check.
    if !has_posted(state, &declaration.account)? {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "not_an_author"));
    }
    if !signed_by(
        state,
        config,
        &declaration.account,
        &message,
        &declaration.signature,
    )
    .await?
    {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "wrong_signer"));
    }
    state.policy.declare(account, statement).await
}

/// Declaring is free, so only an account with a message on a chain this host
/// indexes may declare. An index still starting cannot say no.
fn has_posted(state: &AppState, account: &Address) -> Result<bool, ApiError> {
    let unavailable = || ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "index_unavailable");
    let indexes = state
        .gateway
        .as_ref()
        .map(|g| &g.indexes)
        .ok_or_else(unavailable)?;
    let mut answered = true;
    for index in indexes.values() {
        match index.read_ready(|m| m.authors.contains_key(account)) {
            Some(true) => return Ok(true),
            Some(false) => {}
            None => answered = false,
        }
    }
    if answered {
        Ok(false)
    } else {
        Err(unavailable())
    }
}
