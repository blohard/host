//! Body facts are journaled once per hash. Linking them to posts is separate,
//! bounded work: a popular identical body must not hold the index lock for its
//! entire posting history. Quote outcomes belong to the source chain's journal.

use super::{model::At, types::*};
use crate::chain::{Address, Hash, Hex, decimal};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// Reference extraction rules recorded with each saved analysis.
pub const PARSER_VERSION: u32 = 1;
pub const MAX_REFERENCES: usize = 8;
pub const WORK_BATCH: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Quote {
    #[serde(with = "decimal")]
    pub chain: u64,
    #[serde(with = "decimal")]
    pub id: u64,
    pub commit: Hash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reference {
    Mention { account: Address },
    Quote { target: Quote },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Analysis {
    pub hash: Hash,
    pub parser: u32,
    pub references: Vec<Reference>,
}

fn positive_decimal(s: &str) -> Option<u64> {
    if s.starts_with('0') || s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn reference(uri: &str) -> Option<Reference> {
    if let Some(address) = uri.strip_prefix("account://") {
        return Some(Reference::Mention {
            account: address.parse().ok()?,
        });
    }
    let post = uri.strip_prefix("post://")?;
    let (path, pin) = post.split_once("?commit=")?;
    // Validate the complete suffix. Never recognize the unpinned prefix of a
    // malformed pin, duplicate parameter or unknown extension.
    if !pin.starts_with("0x")
        || pin.len() != 66
        || !pin[2..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let (chain, id) = path.split_once('/')?;
    Some(Reference::Quote {
        target: Quote {
            chain: positive_decimal(chain)?,
            id: positive_decimal(id)?,
            commit: pin.parse().ok()?,
        },
    })
}

/// Count occurrences before deduplication. Unpinned locators and malformed
/// reference tokens consume their place but never create an association.
pub fn analyse(expected: Hash, bytes: &[u8]) -> Result<Analysis> {
    ensure!(
        Hex(crate::keccak::hash(&[bytes]).0) == expected,
        "body hash mismatch"
    );
    let mut out = Analysis {
        hash: expected,
        parser: PARSER_VERSION,
        references: Vec::new(),
    };
    let Ok(header) = crate::envelope::parse(bytes) else {
        return Ok(out);
    };
    if header.media_type != "text/plain" {
        return Ok(out);
    }
    let Ok(mut rest) = std::str::from_utf8(&bytes[header.body_start..]) else {
        return Ok(out);
    };
    for _ in 0..MAX_REFERENCES {
        let start = [rest.find("post://"), rest.find("account://")]
            .into_iter()
            .flatten()
            .min();
        let Some(start) = start else {
            break;
        };
        rest = &rest[start..];
        let end = rest
            .find(|c: char| c.is_whitespace() || "<>\"'`()[]{}\\".contains(c))
            .unwrap_or(rest.len());
        if let Some(r) = reference(&rest[..end])
            && !out.references.contains(&r)
        {
            out.references.push(r);
        }
        rest = &rest[end..];
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteKey {
    #[serde(with = "decimal")]
    pub source: u64,
    pub quote: Quote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resolution {
    pub key: QuoteKey,
    pub deployment: Deployment,
    pub at: At,
    pub message: Message,
}

impl Resolution {
    fn outcome(&self) -> Outcome {
        let observed = self.message.commitment();
        Outcome {
            observed,
            author: (observed == self.key.quote.commit).then_some(self.message.author),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Outcome {
    pub observed: Hash,
    /// None means rejected, not pending. There is no finality-dependent state.
    pub author: Option<Address>,
}

#[derive(Default)]
pub struct References {
    pub analyses: BTreeMap<Hash, Analysis>,
    pub pending_bodies: BTreeSet<Hash>,
    pub ready_bodies: BTreeSet<Hash>,
    pub unbound: BTreeMap<Hash, BTreeSet<u64>>,
    pub pending_quotes: BTreeSet<QuoteKey>,
    /// Pending quotes per target chain, kept with `pending_quotes` so the
    /// status count costs nothing per request.
    pub pending_by_chain: BTreeMap<u64, usize>,
    pub outcomes: BTreeMap<QuoteKey, Outcome>,
    pub accounts: BTreeMap<Address, BTreeSet<u64>>,
}

impl References {
    pub fn posted(&mut self, message: &Message) {
        self.unbound
            .entry(message.content_hash)
            .or_default()
            .insert(message.id);
        if self.analyses.contains_key(&message.content_hash) {
            self.ready_bodies.insert(message.content_hash);
        } else {
            self.pending_bodies.insert(message.content_hash);
        }
    }

    pub fn validate_analyses(&self, analyses: &[Analysis]) -> Result<()> {
        ensure!(analyses.len() <= WORK_BATCH, "too many body analyses");
        let mut seen = BTreeSet::new();
        for a in analyses {
            ensure!(seen.insert(a.hash), "duplicate body in batch");
            ensure!(
                a.parser == PARSER_VERSION && a.references.len() <= MAX_REFERENCES,
                "invalid body analysis"
            );
            ensure!(
                a.references.iter().collect::<BTreeSet<_>>().len() == a.references.len(),
                "duplicate analyzed reference"
            );
            if let Some(old) = self.analyses.get(&a.hash) {
                ensure!(old == a, "body analysis changed");
            }
        }
        Ok(())
    }

    pub fn analyzed(&mut self, analyses: Vec<Analysis>) {
        for a in analyses {
            self.pending_bodies.remove(&a.hash);
            if self.unbound.contains_key(&a.hash) {
                self.ready_bodies.insert(a.hash);
            }
            self.analyses.insert(a.hash, a);
        }
    }

    pub fn validate_bindings(&self, ids: &[u64], messages: &BTreeMap<u64, Message>) -> Result<()> {
        ensure!(ids.len() <= WORK_BATCH, "too many body bindings");
        for id in ids {
            let m = messages
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("binding for unknown post"))?;
            ensure!(
                self.analyses.contains_key(&m.content_hash),
                "body not analyzed"
            );
        }
        Ok(())
    }

    /// Unresolved quotes whose target is one of the host's configured
    /// chains. A quote of any other chain is not a backlog: it waits unseen
    /// and resolves if that chain is added later, with no rebuild of the
    /// source. Walks the configured chains, never the map, since bodies can
    /// name a new chain id forever and must not make status slower.
    pub fn pending_quotes_on<'a>(&self, served: impl IntoIterator<Item = &'a u64>) -> usize {
        served
            .into_iter()
            .map(|chain| self.pending_by_chain.get(chain).copied().unwrap_or(0))
            .sum()
    }

    /// Returns the newly bound ids whose bodies mention or quote someone, so a
    /// retry emits no second change and a body without references none at
    /// all: binding it changes no association a reader could learn from.
    pub fn bind(&mut self, ids: Vec<u64>, messages: &BTreeMap<u64, Message>) -> Vec<u64> {
        let mut changed = Vec::new();
        for id in ids {
            let m = &messages[&id];
            let Some(pending) = self.unbound.get_mut(&m.content_hash) else {
                continue;
            };
            if !pending.remove(&id) {
                continue;
            }
            if pending.is_empty() {
                self.unbound.remove(&m.content_hash);
                self.ready_bodies.remove(&m.content_hash);
            }
            for reference in &self.analyses[&m.content_hash].references {
                match reference {
                    Reference::Mention { account } => {
                        self.accounts.entry(*account).or_default().insert(id);
                    }
                    Reference::Quote { target } => {
                        let key = QuoteKey {
                            source: id,
                            quote: *target,
                        };
                        if self.pending_quotes.insert(key) {
                            *self.pending_by_chain.entry(target.chain).or_default() += 1;
                        }
                    }
                }
            }
            if !self.analyses[&m.content_hash].references.is_empty() {
                changed.push(id);
            }
        }
        changed
    }

    pub fn validate_resolutions(&self, resolutions: &[Resolution]) -> Result<()> {
        ensure!(
            resolutions.len() <= WORK_BATCH,
            "too many quote resolutions"
        );
        let mut seen = BTreeSet::new();
        for r in resolutions {
            ensure!(seen.insert(&r.key), "duplicate quote resolution");
            ensure!(
                r.key.quote.chain == r.deployment.chain && r.key.quote.id == r.message.id,
                "wrong quote target"
            );
            ensure!(
                r.at.checkpoint
                    .is_some_and(|c| c.block >= r.message.position.block),
                "target beyond accepted checkpoint"
            );
            ensure!(r.message.author != Address::ZERO, "missing quote author");
            if let Some(old) = self.outcomes.get(&r.key) {
                ensure!(*old == r.outcome(), "accepted quote target changed");
            } else {
                ensure!(
                    self.pending_quotes.contains(&r.key),
                    "unsolicited quote resolution"
                );
            }
        }
        Ok(())
    }

    pub fn resolve(&mut self, resolutions: Vec<Resolution>) -> Vec<u64> {
        let mut changed = Vec::new();
        for r in resolutions {
            if self.outcomes.contains_key(&r.key) {
                continue;
            }
            let outcome = r.outcome();
            if let Some(author) = outcome.author {
                self.accounts
                    .entry(author)
                    .or_default()
                    .insert(r.key.source);
            }
            if self.pending_quotes.remove(&r.key) {
                let chain = r.key.quote.chain;
                if let Some(count) = self.pending_by_chain.get_mut(&chain) {
                    *count -= 1;
                    if *count == 0 {
                        self.pending_by_chain.remove(&chain);
                    }
                }
            }
            changed.push(r.key.source);
            self.outcomes.insert(r.key, outcome);
        }
        changed
    }
}
