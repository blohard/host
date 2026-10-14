//! Bounded refreshes. `until` is the first page's captured boundary; `at`
//! describes the current records. A filtered page still advances its scan.
//! Of a gate's changes within one page's scan, only the newest is returned:
//! a client applies each gate's latest value and needs no earlier one, so a
//! gate flipped many times costs it one change rather than a page a flip.

use super::{
    model::{At, Change, ChangeEvent, Event, Model},
    query::{Exclusion, Filter, Record, ReferenceRecord, Refusal, reference_record},
    types::*,
};
use crate::chain::{Address, Hash, decimal};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Entries one page looks through. Inspecting an entry is a few map lookups,
/// so a filtered view passes far more unrelated activity per request than it
/// returns, and its one-page poll keeps up with a busy chain.
pub const INSPECTED: usize = 2_000;
/// What one page returns at most, as the client checks it: changes and
/// records. References are a subset of the records' ids.
pub const MAX_CHANGES: usize = 50;
const MAX_RECORDS: usize = 100;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub feed: Option<Filter>,
    #[serde(default)]
    pub ids: Vec<Id>,
    #[serde(default)]
    pub parents: Vec<Id>,
    pub account: Option<Address>,
    pub exclude_list: Option<ListKey>,
    #[serde(default = "yes")]
    pub gates: bool,
}
fn yes() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub generation: Hash,
    #[serde(with = "decimal")]
    pub after: u64,
    pub until: Option<At>,
    pub scope: Scope,
}

#[derive(Serialize)]
pub struct Page<'a> {
    pub at: &'a At,
    pub until: At,
    pub gates: bool,
    pub changes: Vec<&'a Change>,
    pub records: Vec<Record<'a>>,
    pub references: Vec<ReferenceRecord<'a>>,
    #[serde(with = "decimal")]
    pub through: u64,
    pub more: bool,
}

/// What a gate change is about: the post, or the author.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Subject {
    Post(u64),
    Author(Address),
}

fn subject(event: &ChangeEvent) -> Option<Subject> {
    match event {
        ChangeEvent::Chain {
            event: Event::ReplyGate { id, .. },
        } => Some(Subject::Post(*id)),
        ChangeEvent::Chain {
            event: Event::AuthorGate { author, .. },
        } => Some(Subject::Author(*author)),
        _ => None,
    }
}

pub fn read<'a>(model: &'a Model, req: &Request) -> Result<Page<'a>> {
    let first = model
        .changes
        .front()
        .map_or(model.at.sequence, |c| c.sequence - 1);
    let until = req.until.as_ref().unwrap_or(&model.at);
    ensure!(
        req.generation == model.at.generation
            && until.generation == req.generation
            && req.after >= first
            && req.after <= until.sequence
            && until.sequence <= model.at.sequence,
        Refusal("reset_required")
    );
    // A cursor bounds this caller's scan; it never changes the index's state.
    // The current snapshot is reported independently as `at`.
    ensure!(
        until.checkpoint.is_none_or(|p| model
            .at
            .checkpoint
            .is_some_and(|head| p.block < head.block || p == head)),
        Refusal("invalid_change_boundary")
    );
    ensure!(
        req.scope.ids.len() + req.scope.parents.len() <= 4096,
        Refusal("too_many_change_subjects")
    );
    if let Some(filter) = &req.scope.feed {
        filter.key()?;
        filter.check_lists(model)?;
    }
    let feed_excluded =
        Exclusion::new(model, req.scope.feed.as_ref().and_then(|f| f.exclude_list))?;
    let refs_excluded = Exclusion::new(model, req.scope.exclude_list)?;
    let held: BTreeSet<_> = req.scope.ids.iter().map(|id| id.0).collect();
    let parents: BTreeSet<_> = req.scope.parents.iter().map(|id| id.0).collect();
    let matches = |id: u64| {
        model.messages.get(&id).is_some_and(|m| {
            held.contains(&id)
                || parents.contains(&m.parent)
                || req
                    .scope
                    .feed
                    .as_ref()
                    .is_some_and(|f| f.selects(model, &feed_excluded, m))
                || req.scope.account.is_some_and(|a| {
                    !refs_excluded.contains(m.author)
                        && model
                            .references
                            .accounts
                            .get(&a)
                            .is_some_and(|ids| ids.contains(&id))
                })
        })
    };
    let mut changes: Vec<&Change> = Vec::new();
    // Where each gate subject's newest change sits in `changes`.
    let mut gated: BTreeMap<Subject, usize> = BTreeMap::new();
    let mut ids = BTreeSet::new();
    let mut through = req.after;
    for change in model
        .changes
        .iter()
        .skip((req.after - first) as usize)
        .take_while(|c| c.sequence <= until.sequence)
        .take(INSPECTED)
    {
        let mut include = false;
        let mut add = [None, None];
        match &change.event {
            ChangeEvent::Chain {
                event: Event::Posted { message },
            } => {
                if matches(message.id) {
                    add[0] = Some(message.id);
                    include = true;
                }
                if message.parent != 0
                    && (matches(message.parent) || parents.contains(&message.parent))
                {
                    add[1] = Some(message.parent);
                }
            }
            ChangeEvent::Chain {
                event: Event::ReplyGate { id, .. },
            } => {
                include = req.scope.gates;
                if matches(*id) {
                    add[0] = Some(*id);
                }
                let parent = model.messages[id].parent;
                if parent != 0 && (matches(parent) || parents.contains(&parent)) {
                    add[1] = Some(parent);
                }
            }
            ChangeEvent::Chain {
                event: Event::AuthorGate { .. },
            } => include = req.scope.gates,
            ChangeEvent::References { id } if matches(*id) => {
                add[0] = Some(*id);
                include = true;
            }
            ChangeEvent::Chain {
                event: Event::Membership { list, .. },
            } if Some(*list) == req.scope.exclude_list
                || req.scope.feed.as_ref().is_some_and(|f| {
                    Some(*list) == f.exclude_list
                        || f.any.iter().any(|c| {
                            c.members_of == Some(*list) || c.parent_members_of == Some(*list)
                        })
                }) =>
            {
                // Membership decides which older messages belong to the view too,
                // whether a list excludes authors or selects them. The caller
                // reloads this view; other readers and the shared index keep their state.
                anyhow::bail!(Refusal("reset_required"));
            }
            _ => {}
        }
        // A gate change replaces its subject's earlier one on this page.
        let subject = if include {
            subject(&change.event)
        } else {
            None
        };
        let replaces = subject.as_ref().and_then(|s| gated.get(s).copied());
        // The entry that would pass what the client accepts starts the next
        // page. The first always fits, so every page makes progress.
        let fresh = add.iter().flatten().filter(|id| !ids.contains(*id)).count();
        let more_changes = usize::from(include && replaces.is_none());
        if changes.len() + more_changes > MAX_CHANGES || ids.len() + fresh > MAX_RECORDS {
            break;
        }
        through = change.sequence;
        ids.extend(add.into_iter().flatten());
        if !include {
            continue;
        }
        if let Some(at) = replaces {
            // In the older one's place for now; sorted into sequence order below.
            changes[at] = change;
            continue;
        }
        if let Some(subject) = subject {
            gated.insert(subject, changes.len());
        }
        changes.push(change);
    }
    changes.sort_by_key(|change| change.sequence);
    let references = req
        .scope
        .account
        .into_iter()
        .flat_map(|a| {
            ids.iter()
                .filter(|id| !refs_excluded.contains(model.messages[id].author))
                .filter_map(move |id| reference_record(model, *id, a))
        })
        .collect();
    Ok(Page {
        at: &model.at,
        until: until.clone(),
        gates: req.scope.gates,
        changes,
        records: ids.into_iter().map(|id| model.record(id)).collect(),
        references,
        through,
        more: through < until.sequence,
    })
}
