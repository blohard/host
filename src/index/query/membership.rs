//! Membership snapshots and ordered list paging.

use super::page_size;
use crate::chain::{Address, Hash};
use crate::index::{model::Model, types::*};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRequest {
    list: ListKey,
    member: Address,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRequest {
    #[serde(default)]
    ids: Vec<Id>,
    #[serde(default)]
    accounts: Vec<Address>,
    #[serde(default)]
    memberships: Vec<MembershipRequest>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    list: ListKey,
    generation: Option<Hash>,
    after: Option<Address>,
    #[serde(default = "page_size")]
    limit: usize,
}

pub fn snapshots(
    model: &Model,
    policy: Option<&blohard_policy::Lists>,
    req: &StateRequest,
) -> anyhow::Result<Value> {
    anyhow::ensure!(
        req.ids.len() + req.accounts.len() + req.memberships.len() <= 50,
        super::Refusal("too_many_lookup_subjects")
    );
    let items: Vec<_> = req
        .ids
        .iter()
        .map(|id| {
            model
                .messages
                .contains_key(&id.0)
                .then(|| model.record(id.0))
        })
        .collect();
    let accounts: Vec<_> = req
        .accounts
        .iter()
        .map(|a| {
            json!({
                "address": a, "gate": model.author_gate(*a),
                "member": policy.map(|p| p.curated(&a.0)),
            })
        })
        .collect();
    let memberships: Vec<bool> = req
        .memberships
        .iter()
        .map(|q| {
            super::require_registry(model, q.list.registry)?;
            Ok(model
                .lists
                .get(&q.list)
                .is_some_and(|members| members.contains(&q.member)))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(json!({"at": model.at, "items": items, "accounts": accounts, "memberships": memberships}))
}

pub fn list(model: &Model, req: &ListRequest) -> anyhow::Result<Value> {
    super::check_limit(req.limit)?;
    super::require_registry(model, req.list.registry)?;
    anyhow::ensure!(
        req.generation.is_none_or(|g| g == model.at.generation),
        super::Refusal("reset_required")
    );
    let empty = std::collections::BTreeSet::new();
    let members = model.lists.get(&req.list).unwrap_or(&empty);
    let start = req
        .after
        .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    let mut candidates = members
        .range((start, std::ops::Bound::Unbounded))
        .peekable();
    let items: Vec<_> = candidates.by_ref().take(req.limit).copied().collect();
    let next = candidates.peek().and_then(|_| items.last().copied());
    Ok(json!({"at": model.at, "items": items, "next": next}))
}
