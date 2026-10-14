//! Deterministic application of accepted chain facts. No IO or RPC lives here.

use super::references::{Analysis, References, Resolution};
use super::types::*;
use crate::chain::{Address, Checkpoint, Hash, Position, decimal};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Events one batch, and so one block, may carry. Gas bounds a block's events
/// only by the cost of the log itself, roughly fifteen thousand per 30M gas
/// when a contract toggles a gate or a membership in a loop, so this must sit
/// well above what any served chain's block gas limit allows. A block beyond
/// it stops that chain with the block named; raising the limit and restarting
/// resumes from the last committed checkpoint.
pub const MAX_EVENTS: usize = 1 << 17;
/// Changes kept for incremental refresh. An entry is 176 bytes inline, so this
/// is about 44 MB per chain: days of history on a busy host. A view matching
/// few of them catches up in one refresh (200 pages looking through 2,000
/// each); one matching many returns at most 50 per page, runs out of pages
/// sooner, and starts over from the head instead.
pub const CHANGE_RETENTION: usize = 250_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Event {
    Posted {
        message: Message,
    },
    ReplyGate {
        #[serde(with = "decimal")]
        id: u64,
        old: Address,
        new: Address,
        position: Position,
    },
    AuthorGate {
        author: Address,
        old: Address,
        new: Address,
        position: Position,
    },
    Membership {
        list: ListKey,
        member: Address,
        present: bool,
        position: Position,
    },
}

impl Event {
    pub fn position(&self) -> Position {
        match self {
            Self::Posted { message } => message.position,
            Self::ReplyGate { position, .. }
            | Self::AuthorGate { position, .. }
            | Self::Membership { position, .. } => *position,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    #[serde(with = "decimal")]
    pub from: u64,
    pub through: Checkpoint,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Update {
    Chain(Batch),
    Analyze(Vec<Analysis>),
    Bind(Vec<u64>),
    Resolve(Vec<Resolution>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChangeEvent {
    Chain {
        event: Event,
    },
    References {
        #[serde(with = "decimal")]
        id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Change {
    #[serde(with = "decimal")]
    pub sequence: u64,
    pub event: ChangeEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct At {
    pub generation: Hash,
    #[serde(with = "decimal")]
    pub sequence: u64,
    pub checkpoint: Option<Checkpoint>,
}

pub struct Model {
    pub deployment: Deployment,
    pub at: At,
    pub messages: BTreeMap<u64, Message>,
    pub authors: BTreeMap<Address, Vec<u64>>,
    pub parent_authors: BTreeMap<Address, Vec<u64>>,
    pub children: BTreeMap<u64, Vec<u64>>,
    pub author_gates: BTreeMap<Address, Address>,
    pub counts: BTreeMap<u64, u64>,
    pub changes: VecDeque<Change>,
    pub references: References,
    pub lists: BTreeMap<ListKey, BTreeSet<Address>>,
}

impl Model {
    pub fn new(deployment: Deployment, generation: Hash) -> Self {
        Self {
            deployment,
            at: At {
                generation,
                sequence: 0,
                checkpoint: None,
            },
            messages: BTreeMap::new(),
            authors: BTreeMap::new(),
            parent_authors: BTreeMap::new(),
            children: BTreeMap::new(),
            author_gates: BTreeMap::new(),
            counts: BTreeMap::new(),
            changes: VecDeque::new(),
            references: References::default(),
            lists: BTreeMap::new(),
        }
    }

    pub fn next_block(&self) -> Result<u64> {
        match self.at.checkpoint {
            Some(c) => c
                .block
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("block overflow")),
            None => Ok(self.deployment.index_start_block),
        }
    }

    pub fn author_gate(&self, author: Address) -> Address {
        self.author_gates
            .get(&author)
            .copied()
            .unwrap_or(Address::ZERO)
    }

    pub fn effective_gate(&self, message: &Message) -> Address {
        if message.gate == Address::ZERO {
            self.author_gate(message.author)
        } else {
            message.gate
        }
    }

    /// Validate the whole batch before journaling. The temporary maps hold only
    /// this batch's changes, so validation cost does not grow with history.
    pub fn validate(&self, batch: &Batch) -> Result<()> {
        ensure!(
            batch.events.len() <= MAX_EVENTS,
            "too many events in one batch"
        );
        ensure!(batch.from == self.next_block()?, "noncontiguous checkpoint");
        ensure!(batch.through.block >= batch.from, "backwards checkpoint");
        ensure!(batch.through.hash != Hash::ZERO, "missing checkpoint hash");
        self.at
            .sequence
            .checked_add(batch.events.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("sequence overflow"))?;
        let mut posted = BTreeMap::<u64, &Message>::new();
        let mut gates = BTreeMap::<u64, Address>::new();
        let mut authors = BTreeMap::<Address, Address>::new();
        let mut memberships = BTreeMap::<(ListKey, Address), bool>::new();
        let mut next_id = self.messages.last_key_value().map_or(0, |(id, _)| *id);
        let mut previous = None;
        for event in &batch.events {
            let p = event.position();
            ensure!(
                p.block >= batch.from && p.block <= batch.through.block,
                "event outside batch"
            );
            ensure!(
                previous.is_none_or(|prev| p > prev),
                "unordered or duplicate event"
            );
            previous = Some(p);
            match event {
                Event::Posted { message: m } => {
                    ensure!(
                        p.block >= self.deployment.start_block,
                        "post before Board deployment"
                    );
                    next_id = next_id
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("id overflow"))?;
                    ensure!(m.id == next_id, "missing or duplicate post");
                    ensure!(m.author != Address::ZERO && m.parent < m.id, "invalid post");
                    ensure!(m.gate != REMOVED, "post cannot start removed");
                    let parent = posted
                        .get(&m.parent)
                        .copied()
                        .or_else(|| self.messages.get(&m.parent));
                    if m.parent == 0 {
                        ensure!(m.parent_author == Address::ZERO, "root has a parent author");
                    } else {
                        let parent = parent.ok_or_else(|| anyhow::anyhow!("missing parent"))?;
                        ensure!(m.parent_author == parent.author, "wrong parent author");
                        ensure!(m.timestamp >= parent.timestamp, "reply predates parent");
                    }
                    gates.insert(m.id, m.gate);
                    posted.insert(m.id, m);
                }
                Event::ReplyGate { id, old, new, .. } => {
                    let m = posted
                        .get(id)
                        .copied()
                        .or_else(|| self.messages.get(id))
                        .ok_or_else(|| anyhow::anyhow!("gate for missing post"))?;
                    let current = gates.get(id).copied().unwrap_or(m.gate);
                    ensure!(current == *old, "gate history disagrees");
                    ensure!(
                        current != TOMBSTONE && current != REMOVED,
                        "permanent gate changed"
                    );
                    ensure!(*new != REMOVED || m.parent != 0, "root removed");
                    gates.insert(*id, *new);
                }
                Event::AuthorGate {
                    author, old, new, ..
                } => {
                    let current = authors
                        .get(author)
                        .copied()
                        .unwrap_or_else(|| self.author_gate(*author));
                    ensure!(
                        *author != Address::ZERO && *new != REMOVED,
                        "invalid author gate"
                    );
                    ensure!(current == *old, "author gate history disagrees");
                    authors.insert(*author, *new);
                }
                Event::Membership {
                    list,
                    member,
                    present,
                    ..
                } => {
                    ensure!(
                        self.deployment.registries.contains(&list.registry),
                        "unconfigured registry"
                    );
                    let current =
                        memberships
                            .get(&(*list, *member))
                            .copied()
                            .unwrap_or_else(|| {
                                self.lists
                                    .get(list)
                                    .is_some_and(|members| members.contains(member))
                            });
                    ensure!(current != *present, "registry history disagrees");
                    memberships.insert((*list, *member), *present);
                }
            }
        }
        Ok(())
    }

    /// Caller validated this exact batch and journaled it, unless it is an
    /// empty range that RAM serves alone until the next checkpoint interval
    /// (`record_for` in the journal). This operation runs under one write
    /// lock; readers never see half a batch.
    pub fn apply(&mut self, batch: Batch) {
        for event in batch.events {
            match &event {
                Event::Posted { message: m } => {
                    self.authors.entry(m.author).or_default().push(m.id);
                    self.children.entry(m.parent).or_default().push(m.id);
                    if m.parent != 0 {
                        self.parent_authors
                            .entry(m.parent_author)
                            .or_default()
                            .push(m.id);
                        // Only live direct replies appear in the parent's list.
                        if m.stands_alone() {
                            *self.counts.entry(m.parent).or_default() += 1;
                        }
                    }
                    self.messages.insert(m.id, m.clone());
                    self.references.posted(m);
                }
                Event::ReplyGate { id, new, .. } => {
                    let m = self.messages.get_mut(id).expect("validated message");
                    if (*new == TOMBSTONE || *new == REMOVED) && m.parent != 0 {
                        *self.counts.get_mut(&m.parent).expect("counted child") -= 1;
                    }
                    m.gate = *new;
                }
                Event::AuthorGate { author, new, .. } => {
                    self.author_gates.insert(*author, *new);
                }
                Event::Membership {
                    list,
                    member,
                    present,
                    ..
                } => {
                    let members = self.lists.entry(*list).or_default();
                    if *present {
                        members.insert(*member);
                    } else {
                        members.remove(member);
                    }
                }
            }
            self.changed(ChangeEvent::Chain { event });
        }
        self.at.checkpoint = Some(batch.through);
    }

    fn changed(&mut self, event: ChangeEvent) {
        self.at.sequence += 1;
        self.changes.push_back(Change {
            sequence: self.at.sequence,
            event,
        });
        if self.changes.len() > CHANGE_RETENTION {
            self.changes.pop_front();
        }
    }

    pub fn validate_update(&self, update: &Update) -> Result<()> {
        let count = match update {
            Update::Chain(batch) => return self.validate(batch),
            Update::Analyze(analyses) => return self.references.validate_analyses(analyses),
            Update::Bind(ids) => {
                self.references.validate_bindings(ids, &self.messages)?;
                ids.len()
            }
            Update::Resolve(resolutions) => {
                self.references.validate_resolutions(resolutions)?;
                resolutions.len()
            }
        };
        ensure!(
            self.at.sequence.checked_add(count as u64).is_some(),
            "sequence overflow"
        );
        Ok(())
    }

    pub fn apply_update(&mut self, update: Update) {
        let changed = match update {
            Update::Chain(batch) => {
                self.apply(batch);
                return;
            }
            Update::Analyze(analyses) => {
                self.references.analyzed(analyses);
                return;
            }
            Update::Bind(ids) => self.references.bind(ids, &self.messages),
            Update::Resolve(resolutions) => self.references.resolve(resolutions),
        };
        for id in changed {
            self.changed(ChangeEvent::References { id });
        }
    }
}
