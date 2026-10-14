//! Replies and body references share one bounded, deduplicated timeline.
use super::*;

#[derive(Serialize)]
pub struct NotificationRecord<'a> {
    #[serde(flatten)]
    pub record: Record<'a>,
    pub reference: Option<super::super::references::Reference>,
}

#[derive(Serialize)]
pub struct NotificationPage<'a> {
    pub at: &'a At,
    pub items: Vec<NotificationRecord<'a>>,
    pub next: Option<Cursor>,
}

pub fn notifications<'a>(
    model: &'a Model,
    account: Address,
    exclude_list: Option<ListKey>,
    cursor: Option<&Cursor>,
    limit: usize,
) -> Result<NotificationPage<'a>> {
    check_limit(limit)?;
    let excluded = Exclusion::new(model, exclude_list)?;
    let key = Hex(crate::keccak::hash(&[
        b"notifications",
        &serde_json::to_vec(&(account, exclude_list))?,
    ])
    .0);
    check_cursor(cursor, model, key)?;
    let ceiling = cursor.map_or_else(
        || model.messages.last_key_value().map_or(0, |(id, _)| *id),
        |c| c.ceiling,
    );
    let upper = cursor.map_or(ceiling, |c| ceiling.min(c.before.saturating_sub(1)));
    let empty = std::collections::BTreeSet::new();
    let sources = model.references.accounts.get(&account).unwrap_or(&empty);
    let replies = model
        .parent_authors
        .get(&account)
        .map_or(&[][..], Vec::as_slice);
    let end = replies.partition_point(|id| *id <= upper);
    let candidates = Merge::iterators(vec![
        Box::new(replies[..end].iter().rev().copied()),
        Box::new(sources.range(..=upper).rev().copied()),
    ]);
    let (items, resume) = scan(candidates, limit, |id| {
        let m = &model.messages[&id];
        (m.stands_alone() && !excluded.contains(m.author)).then(|| NotificationRecord {
            record: model.record(id),
            reference: reference_record(model, id, account).map(|r| r.reference),
        })
    });
    Ok(NotificationPage {
        at: &model.at,
        items,
        next: resume.map(|before| Cursor {
            generation: model.at.generation,
            filter: key,
            ceiling,
            before,
        }),
    })
}
