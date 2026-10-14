//! Message records, ancestry and bounded publication recovery.

use super::{Cursor, page_size};
use crate::chain::{Address, Hash, Hex};
use crate::index::{model::Model, types::*};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageRequest {
    #[serde(default)]
    ancestors: usize,
    commit: Option<Hash>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupRequest {
    author: Address,
    content_hash: Hash,
    parent: Option<Id>,
    from_block: Option<Id>,
    cursor: Option<Cursor>,
    #[serde(default = "page_size")]
    limit: usize,
}

pub fn message(model: &Model, id: u64, req: &MessageRequest) -> anyhow::Result<Value> {
    anyhow::ensure!(
        req.ancestors <= 32,
        super::Refusal("ancestor_limit_out_of_range")
    );
    let m = model
        .messages
        .get(&id)
        .ok_or_else(|| anyhow::anyhow!(super::Refusal("message_not_found")))?;
    anyhow::ensure!(
        req.commit.is_none_or(|pin| pin == m.commitment()),
        super::Refusal("commitment_mismatch")
    );
    let mut ancestors = Vec::new();
    let mut parent = m.parent;
    while parent != 0 && ancestors.len() < req.ancestors {
        let m = &model.messages[&parent];
        ancestors.push(model.record(m.id));
        parent = m.parent;
    }
    Ok(json!({ "at": model.at, "message": model.record(m.id), "ancestors": ancestors }))
}

pub fn lookup(model: &Model, req: &LookupRequest) -> anyhow::Result<Value> {
    super::check_limit(req.limit)?;
    let filter = serde_json::to_vec(&json!([
        req.author,
        req.content_hash,
        req.parent,
        req.from_block
    ]))?;
    let key = Hex(crate::keccak::hash(&[b"lookup", &filter]).0);
    super::check_cursor(req.cursor.as_ref(), model, key)?;
    let ids = model
        .authors
        .get(&req.author)
        .map_or(&[][..], Vec::as_slice);
    let ceiling = req
        .cursor
        .as_ref()
        .map_or_else(|| ids.last().copied().unwrap_or(0), |c| c.ceiling);
    let start = ids.partition_point(|id| {
        model.messages[id].position.block < req.from_block.map_or(0, |n| n.0)
    });
    let end = ids
        .partition_point(|id| *id <= ceiling && req.cursor.as_ref().is_none_or(|c| *id < c.before));
    let candidates = ids[start.min(end)..end].iter().copied().rev();
    let (items, resume) = super::scan(candidates, req.limit, |id| {
        let m = &model.messages[&id];
        (m.content_hash == req.content_hash && req.parent.is_none_or(|p| p.0 == m.parent))
            .then(|| model.record(id))
    });
    let next = resume.map(|before| Cursor {
        generation: model.at.generation,
        filter: key,
        ceiling,
        before,
    });
    Ok(json!({"at": model.at, "items": items, "next": next}))
}
