//! Bounded queries over RAM posting lists. Cursors bind the filter, generation
//! and original upper bound; continuation follows the last examined candidate.

use super::{
    model::{At, Model},
    types::*,
};
use crate::chain::{Address, Hash, Hex, decimal};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;

fn require_registry(model: &Model, registry: Address) -> Result<()> {
    ensure!(
        model.deployment.registries.contains(&registry),
        Refusal("registry_not_indexed")
    );
    Ok(())
}

pub const MAX_PAGE: usize = 50;
// 256 followed authors, the reader, and replies to the reader.
const MAX_AUTHORS: usize = 258;
const SCAN_BUDGET: usize = 500;

/// A page size the client may ask for.
fn check_limit(limit: usize) -> Result<()> {
    ensure!(
        (1..=MAX_PAGE).contains(&limit),
        Refusal("page_limit_out_of_range")
    );
    Ok(())
}

/// A cursor continues only the query and the generation that issued it.
fn check_cursor(cursor: Option<&Cursor>, model: &Model, key: Hash) -> Result<()> {
    if let Some(c) = cursor {
        ensure!(
            c.generation == model.at.generation && c.filter == key,
            Refusal("reset_required")
        );
    }
    Ok(())
}

/// Look at up to `SCAN_BUDGET` candidates in order, keeping at most `limit`.
/// Returns what was kept and, when candidates remain, the last id looked at:
/// the next page resumes after it, so a page that keeps nothing still moves on.
fn scan<T>(
    candidates: impl Iterator<Item = u64>,
    limit: usize,
    mut keep: impl FnMut(u64) -> Option<T>,
) -> (Vec<T>, Option<u64>) {
    let mut candidates = candidates.peekable();
    let mut items = Vec::new();
    let mut last = None;
    for _ in 0..SCAN_BUDGET {
        let Some(id) = candidates.next() else {
            break;
        };
        last = Some(id);
        if let Some(item) = keep(id) {
            items.push(item);
        }
        if items.len() == limit {
            break;
        }
    }
    (items, candidates.peek().and(last))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    All,
    Posts,
    Replies,
}

/// A clause intersects its author, parent-author, list-membership and
/// message-kind predicates. A feed is a bounded union of clauses, so timelines
/// and following views share one cursor rather than merging independently
/// paginated requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Clause {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub curated: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub parent_curated: bool,
    #[serde(default)]
    pub authors: Vec<Address>,
    #[serde(default)]
    pub parent_authors: Vec<Address>,
    /// Only authors currently on this list: a predicate read while scanning,
    /// not a source of candidates, so it adds no index state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members_of: Option<ListKey>,
    /// Only replies whose parent author is currently on this list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_members_of: Option<ListKey>,
    #[serde(default)]
    pub kind: Kind,
}

impl Clause {
    fn unrestricted(&self) -> bool {
        self.authors.is_empty() && self.parent_authors.is_empty() && self.kind != Kind::Posts
    }

    fn lists<'a>(&self, model: &'a Model) -> Vec<&'a [u64]> {
        if !self.authors.is_empty() {
            self.authors
                .iter()
                .filter_map(|a| model.authors.get(a))
                .map(Vec::as_slice)
                .collect()
        } else if !self.parent_authors.is_empty() {
            self.parent_authors
                .iter()
                .filter_map(|a| model.parent_authors.get(a))
                .map(Vec::as_slice)
                .collect()
        } else {
            model
                .children
                .get(&0)
                .map(Vec::as_slice)
                .into_iter()
                .collect()
        }
    }

    pub(super) fn selects(&self, model: &Model, message: &Message) -> bool {
        (self.authors.is_empty() || self.authors.contains(&message.author))
            && (self.parent_authors.is_empty()
                || self.parent_authors.contains(&message.parent_author))
            && self.members_of.is_none_or(|list| {
                model
                    .lists
                    .get(&list)
                    .is_some_and(|members| members.contains(&message.author))
            })
            && self.parent_members_of.is_none_or(|list| {
                message.parent != 0
                    && model
                        .lists
                        .get(&list)
                        .is_some_and(|members| members.contains(&message.parent_author))
            })
            && match self.kind {
                Kind::All => true,
                Kind::Posts => message.parent == 0,
                Kind::Replies => message.parent != 0,
            }
    }
}

/// A deliberate refusal of a query, answered as a 4xx carrying this code.
/// Any other error inside a query is unexpected: logged, answered as a
/// fixed 500, never echoed to the client.
#[derive(Debug)]
pub struct Refusal(pub &'static str);

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Refusal {}

fn all_clauses() -> Vec<Clause> {
    vec![Clause::default()]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    #[serde(skip)]
    pub policy: std::sync::Arc<blohard_policy::Lists>,
    #[serde(default = "all_clauses")]
    pub any: Vec<Clause>,
    pub exclude_list: Option<ListKey>,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            policy: Default::default(),
            any: all_clauses(),
            exclude_list: None,
        }
    }
}

impl Filter {
    /// Whether this feed selects `m`: some clause chooses it and its author is
    /// not excluded. Whether it still stands is the caller's question: the
    /// changes scan reports ended posts too.
    pub(super) fn selects(&self, model: &Model, excluded: &Exclusion, m: &Message) -> bool {
        !self.policy.blocked(&m.author.0)
            && !excluded.contains(m.author)
            && self.any.iter().any(|c| {
                c.selects(model, m)
                    && (!c.curated || self.policy.curated(&m.author.0))
                    && (!c.parent_curated
                        || (m.parent != 0 && self.policy.curated(&m.parent_author.0)))
            })
    }

    /// Lists named by clauses must be on an indexed registry, as for exclusion.
    pub(super) fn check_lists(&self, model: &Model) -> Result<()> {
        for list in self
            .any
            .iter()
            .flat_map(|c| [c.members_of, c.parent_members_of])
            .flatten()
        {
            require_registry(model, list.registry)?;
        }
        Ok(())
    }

    pub(super) fn key(&self) -> Result<Hash> {
        ensure!(self.any.len() <= 8, Refusal("too_many_feed_clauses"));
        ensure!(
            self.any
                .iter()
                .map(|c| c.authors.len() + c.parent_authors.len())
                .sum::<usize>()
                <= MAX_AUTHORS,
            Refusal("too_many_authors")
        );
        for clause in &self.any {
            // Top-level posts record the zero address as parent author, so a
            // parent-author restriction can never select one; refusing it
            // here saves every page a full empty scan.
            ensure!(
                clause.kind != Kind::Posts
                    || (clause.parent_authors.is_empty()
                        && clause.parent_members_of.is_none()
                        && !clause.parent_curated),
                Refusal("posts_have_no_parent_author")
            );
        }
        let mut filter = self.clone();
        for clause in &mut filter.any {
            clause.authors.sort_unstable();
            clause.authors.dedup();
            clause.parent_authors.sort_unstable();
            clause.parent_authors.dedup();
        }
        Ok(Hex(crate::keccak::hash(&[
            &serde_json::to_vec(&filter)?,
            &self.policy.revision.to_be_bytes(),
        ])
        .0))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub generation: Hash,
    pub filter: Hash,
    #[serde(with = "decimal")]
    pub ceiling: u64,
    #[serde(with = "decimal")]
    pub before: u64,
}

#[derive(Serialize)]
pub struct Record<'a> {
    #[serde(flatten)]
    pub message: &'a Message,
    pub author_gate: Address,
    pub effective_gate: Address,
    pub commitment: Hash,
    #[serde(with = "decimal")]
    pub replies: u64,
}

impl Model {
    pub fn record(&self, id: u64) -> Record<'_> {
        let message = &self.messages[&id];
        Record {
            message,
            author_gate: self.author_gate(message.author),
            effective_gate: self.effective_gate(message),
            commitment: message.commitment(),
            replies: self.counts.get(&id).copied().unwrap_or(0),
        }
    }
}

#[derive(Serialize)]
pub struct Page<'a> {
    pub at: &'a At,
    pub items: Vec<Record<'a>>,
    pub next: Option<Cursor>,
}

/// Merge decreasing posting lists without constructing a union of their history.
struct Merge<'a> {
    lists: Vec<Box<dyn Iterator<Item = u64> + 'a>>,
    heap: BinaryHeap<(u64, usize)>,
    last: Option<u64>,
}

impl<'a> Merge<'a> {
    fn new(lists: Vec<&'a [u64]>, ceiling: u64, before: Option<u64>) -> Self {
        Self::iterators(
            lists
                .into_iter()
                .map(|list| {
                    let end =
                        list.partition_point(|id| *id <= ceiling && before.is_none_or(|b| *id < b));
                    Box::new(list[..end].iter().rev().copied()) as Box<dyn Iterator<Item = u64>>
                })
                .collect(),
        )
    }

    fn iterators(mut lists: Vec<Box<dyn Iterator<Item = u64> + 'a>>) -> Self {
        let heap = lists
            .iter_mut()
            .enumerate()
            .filter_map(|(n, list)| list.next().map(|id| (id, n)))
            .collect();
        Self {
            lists,
            heap,
            last: None,
        }
    }
}

impl Iterator for Merge<'_> {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        while let Some((id, list)) = self.heap.pop() {
            if let Some(next) = self.lists[list].next() {
                self.heap.push((next, list));
            }
            if self.last != Some(id) {
                self.last = Some(id);
                return Some(id);
            }
        }
        None
    }
}

pub fn feed<'a>(
    model: &'a Model,
    filter: &Filter,
    cursor: Option<&Cursor>,
    limit: usize,
) -> Result<Page<'a>> {
    check_limit(limit)?;
    let excluded = Exclusion::new(model, filter.exclude_list)?;
    filter.check_lists(model)?;
    let key = filter.key()?;
    let ceiling = cursor.map_or_else(
        || model.messages.last_key_value().map_or(0, |(id, _)| *id),
        |c| c.ceiling,
    );
    let before = cursor.map(|c| c.before);
    check_cursor(cursor, model, key)?;
    let candidates: Box<dyn Iterator<Item = u64>> = if filter.any.iter().any(Clause::unrestricted) {
        Box::new(
            model
                .messages
                .range(..=before.map_or(ceiling, |b| ceiling.min(b.saturating_sub(1))))
                .rev()
                .map(|(id, _)| *id),
        )
    } else {
        Box::new(Merge::new(
            filter.any.iter().flat_map(|c| c.lists(model)).collect(),
            ceiling,
            before,
        ))
    };
    let (items, resume) = scan(candidates, limit, |id| {
        let m = &model.messages[&id];
        (m.stands_alone() && filter.selects(model, &excluded, m)).then(|| model.record(id))
    });
    Ok(Page {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyCursor {
    pub generation: Hash,
    #[serde(with = "decimal")]
    pub parent: u64,
    #[serde(with = "decimal")]
    pub ceiling: u64,
    #[serde(with = "decimal")]
    pub after: u64,
}

#[derive(Serialize)]
pub struct ReplyPage<'a> {
    pub at: &'a At,
    pub parent: Record<'a>,
    pub items: Vec<Record<'a>>,
    pub next: Option<ReplyCursor>,
}

pub fn replies<'a>(
    model: &'a Model,
    parent: u64,
    cursor: Option<&ReplyCursor>,
    limit: usize,
) -> Result<ReplyPage<'a>> {
    check_limit(limit)?;
    ensure!(
        model.messages.contains_key(&parent),
        Refusal("message_not_found")
    );
    if let Some(c) = cursor {
        ensure!(
            c.generation == model.at.generation && c.parent == parent,
            Refusal("reset_required")
        );
    }
    let list = model.children.get(&parent).map_or(&[][..], Vec::as_slice);
    let ceiling = cursor.map_or_else(|| list.last().copied().unwrap_or(0), |c| c.ceiling);
    let after = cursor.map_or(0, |c| c.after);
    let start = list.partition_point(|id| *id <= after);
    let candidates = list[start..]
        .iter()
        .copied()
        .take_while(|id| *id <= ceiling);
    // Ended children leave the parent's discussion with their branches.
    // Point reads still retain them as context for surviving descendants.
    let (items, resume) = scan(candidates, limit, |id| {
        model.messages[&id].stands_alone().then(|| model.record(id))
    });
    Ok(ReplyPage {
        at: &model.at,
        parent: model.record(parent),
        items,
        next: resume.map(|after| ReplyCursor {
            generation: model.at.generation,
            parent,
            ceiling,
            after,
        }),
    })
}

#[derive(Serialize)]
pub struct ReferenceRecord<'a> {
    #[serde(flatten)]
    pub record: Record<'a>,
    pub reference: super::references::Reference,
}

#[derive(Serialize)]
pub struct ReferencePage<'a> {
    pub at: &'a At,
    pub items: Vec<ReferenceRecord<'a>>,
    pub next: Option<Cursor>,
}

/// Explain a match from durable body facts and accepted quote outcomes.
/// At most eight references are inspected, independent of total history.
pub fn reference_record(model: &Model, id: u64, account: Address) -> Option<ReferenceRecord<'_>> {
    use super::references::{QuoteKey, Reference};
    let refs = &model.references;
    if !refs.accounts.get(&account)?.contains(&id) {
        return None;
    }
    let message = model.messages.get(&id)?;
    let analysis = refs.analyses.get(&message.content_hash)?;
    let mention = Reference::Mention { account };
    let reference = if analysis.references.contains(&mention) {
        mention
    } else {
        *analysis.references.iter().find(|r| match r {
            Reference::Quote { target } => refs
                .outcomes
                .get(&QuoteKey {
                    source: id,
                    quote: *target,
                })
                .is_some_and(|outcome| outcome.author == Some(account)),
            _ => false,
        })?
    };
    Some(ReferenceRecord {
        record: model.record(id),
        reference,
    })
}

pub fn references<'a>(
    model: &'a Model,
    account: Address,
    exclude_list: Option<ListKey>,
    cursor: Option<&Cursor>,
    limit: usize,
) -> Result<ReferencePage<'a>> {
    check_limit(limit)?;
    let excluded = Exclusion::new(model, exclude_list)?;
    let key = Hex(crate::keccak::hash(&[
        b"references",
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
    let (items, resume) = scan(sources.range(..=upper).rev().copied(), limit, |id| {
        let m = &model.messages[&id];
        (m.stands_alone() && !excluded.contains(m.author))
            .then(|| reference_record(model, id, account))
            .flatten()
    });
    Ok(ReferencePage {
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

mod notifications;
pub use notifications::notifications;
mod exclusion;
pub(super) use exclusion::Exclusion;
mod membership;
mod records;
pub use membership::{ListRequest, StateRequest, list, snapshots};
pub use records::{LookupRequest, MessageRequest, lookup, message};

pub fn page_size() -> usize {
    20
}
