//! The keyring's events and its in-memory model: balances, supplies and each
//! account's recent activity, with the rules every range is checked against
//! before it is applied.
use super::{BalanceView, Config, Fatal};
use crate::chain::{Address, Checkpoint, Hash, Position};
use anyhow::{Result, anyhow, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::Bound::{Excluded, Unbounded},
};

/// One accepted block range. `from` is always the block after the previous
/// record's `through`, so a record also covers the empty ranges RAM applied
/// since the last one.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Range {
    pub(super) from: u64,
    pub(super) through: Checkpoint,
    pub(super) events: Vec<Logged>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Logged {
    pub(super) position: Position,
    pub(super) transaction: Hash,
    pub(super) timestamp: u32,
    pub(super) event: Event,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Event {
    Activated { creator: Address },
    Trade(Trade),
    Transfer(Transfer),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Trade {
    pub(super) creator: Address,
    pub(super) holder: Address,
    pub(super) buy: bool,
    pub(super) amount: u64,
    pub(super) supply: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Transfer {
    pub(super) creator: Address,
    pub(super) from: Address,
    pub(super) to: Address,
    pub(super) amount: u64,
}

/// An account's recent key activity is a bounded notification inbox, not a
/// market-history archive: trades of its own market by others, and keys
/// someone moved to it.
const TRADE_HISTORY_LIMIT: usize = 100;
#[derive(Clone, Debug, Serialize)]
pub(super) struct TradeNotice {
    pub(super) trader: Address,
    pub(super) buy: bool,
    pub(super) amount: u64,
    pub(super) position: Position,
    pub(super) transaction: Hash,
    pub(super) timestamp: u32,
}
/// Keys to `creator` that `from` moved to the inbox's account.
#[derive(Clone, Debug, Serialize)]
pub(super) struct ReceivedNotice {
    creator: Address,
    from: Address,
    amount: u64,
    position: Position,
    transaction: Hash,
    timestamp: u32,
}
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Notice {
    Trade(TradeNotice),
    Received(ReceivedNotice),
}
impl Notice {
    pub(super) fn position(&self) -> Position {
        match self {
            Self::Trade(n) => n.position,
            Self::Received(n) => n.position,
        }
    }
    /// The other account: the inbox owner's block of it hides the notice.
    fn party(&self) -> Address {
        match self {
            Self::Trade(n) => n.trader,
            Self::Received(n) => n.from,
        }
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct TradeHistory {
    pub(super) items: VecDeque<Notice>,
    pub(super) truncated: bool,
}
impl TradeHistory {
    pub(super) fn push(&mut self, notice: Notice) {
        self.items.push_back(notice);
        if self.items.len() > TRADE_HISTORY_LIMIT {
            self.items.pop_front();
            self.truncated = true;
        }
    }
    pub(super) fn page(
        &self,
        before: Option<Position>,
        limit: usize,
        blocked: Option<&BTreeSet<Address>>,
    ) -> Value {
        let mut items: Vec<_> = self
            .items
            .iter()
            .rev()
            .filter(|n| {
                before.is_none_or(|p| n.position() < p)
                    && !blocked.is_some_and(|b| b.contains(&n.party()))
            })
            .take(limit + 1)
            .collect();
        let more = items.len() > limit;
        items.truncate(limit);
        let next = if more {
            items.last().map(|n| n.position())
        } else {
            None
        };
        json!({"items":items,"next":next,"truncated":self.truncated})
    }
}

/// What replay and ingestion build. Only the writer changes it.
#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct Model {
    pub(super) checkpoint: Option<Checkpoint>,
    pub(super) holdings: BTreeMap<Address, BTreeMap<Address, u64>>,
    pub(super) supplies: BTreeMap<Address, u64>,
    pub(super) activity: BTreeMap<Address, TradeHistory>,
    /// Reverse of `holdings`, maintained with it; queries never scan unrelated markets.
    #[serde(skip)]
    pub(super) held_by: BTreeMap<Address, BTreeSet<Address>>,
}

/// Balance and supply reads and writes, shared by validation and application,
/// so both follow exactly the same rules.
pub(super) trait Ledger {
    fn supply(&self, creator: Address) -> Option<u64>;
    fn balance(&self, creator: Address, holder: Address) -> u64;
    fn set_supply(&mut self, creator: Address, supply: u64);
    fn set_balance(&mut self, creator: Address, holder: Address, balance: u64);
}

/// One event against a ledger. Every contradiction is fatal: accepted history
/// that disagrees with itself needs an operator, not a retry.
pub(super) fn step(ledger: &mut impl Ledger, event: &Event) -> Result<()> {
    match event {
        Event::Activated { creator } => {
            ensure!(
                ledger.supply(*creator).is_none(),
                Fatal("duplicate activation")
            );
            ledger.set_supply(*creator, 0);
        }
        Event::Trade(trade) => {
            let old_supply = ledger
                .supply(trade.creator)
                .ok_or(Fatal("trade before activation"))?;
            let old = ledger.balance(trade.creator, trade.holder);
            let change = |n: u64| {
                if trade.buy {
                    n.checked_add(trade.amount)
                } else {
                    n.checked_sub(trade.amount)
                }
            };
            let supply = change(old_supply).ok_or(Fatal("invalid supply change"))?;
            let balance = change(old).ok_or(Fatal("invalid balance change"))?;
            ensure!(
                supply == trade.supply,
                Fatal("trade supply disagrees with history")
            );
            ledger.set_supply(trade.creator, supply);
            ledger.set_balance(trade.creator, trade.holder, balance);
        }
        Event::Transfer(event) => {
            let supply = ledger
                .supply(event.creator)
                .ok_or(Fatal("transfer before activation"))?;
            let from = ledger
                .balance(event.creator, event.from)
                .checked_sub(event.amount)
                .ok_or(Fatal("transfer exceeds balance"))?;
            let to = ledger
                .balance(event.creator, event.to)
                .checked_add(event.amount)
                .ok_or(Fatal("transfer balance overflow"))?;
            ensure!(
                event.amount > 0 && event.from != event.to && to <= supply,
                Fatal("invalid transfer balances")
            );
            // Supply is unchanged.
            ledger.set_balance(event.creator, event.from, from);
            ledger.set_balance(event.creator, event.to, to);
        }
    }
    Ok(())
}

impl Ledger for Model {
    fn supply(&self, creator: Address) -> Option<u64> {
        self.supplies.get(&creator).copied()
    }
    fn balance(&self, creator: Address, holder: Address) -> u64 {
        self.holdings
            .get(&creator)
            .and_then(|h| h.get(&holder))
            .copied()
            .unwrap_or(0)
    }
    fn set_supply(&mut self, creator: Address, supply: u64) {
        self.supplies.insert(creator, supply);
    }
    fn set_balance(&mut self, creator: Address, holder: Address, balance: u64) {
        let held = self.holdings.entry(creator).or_default();
        if balance == 0 {
            held.remove(&holder);
        } else {
            held.insert(holder, balance);
        }
        if held.is_empty() {
            self.holdings.remove(&creator);
        }
        let markets = self.held_by.entry(holder).or_default();
        if balance == 0 {
            markets.remove(&creator);
        } else {
            markets.insert(creator);
        }
        if markets.is_empty() {
            self.held_by.remove(&holder);
        }
    }
}

/// The model plus the changes one range would make, touching only what the
/// range touches. Validation runs here so the model itself never changes
/// until the range is durable.
struct Overlay<'a> {
    base: &'a Model,
    supplies: BTreeMap<Address, u64>,
    balances: BTreeMap<(Address, Address), u64>,
}
impl Ledger for Overlay<'_> {
    fn supply(&self, creator: Address) -> Option<u64> {
        self.supplies
            .get(&creator)
            .copied()
            .or_else(|| self.base.supply(creator))
    }
    fn balance(&self, creator: Address, holder: Address) -> u64 {
        self.balances
            .get(&(creator, holder))
            .copied()
            .unwrap_or_else(|| self.base.balance(creator, holder))
    }
    fn set_supply(&mut self, creator: Address, supply: u64) {
        self.supplies.insert(creator, supply);
    }
    fn set_balance(&mut self, creator: Address, holder: Address, balance: u64) {
        self.balances.insert((creator, holder), balance);
    }
}

impl Model {
    pub(super) fn next_block(&self, config: &Config) -> Result<u64> {
        match self.checkpoint {
            Some(c) => c
                .block
                .checked_add(1)
                .ok_or(anyhow!(Fatal("block overflow"))),
            None => Ok(config.start_block),
        }
    }

    /// Everything that could make `apply` fail, checked without changing anything.
    pub(super) fn check(&self, config: &Config, range: &Range) -> Result<()> {
        ensure!(
            range.from == self.next_block(config)?,
            "noncontiguous keyring range"
        );
        ensure!(range.through.block >= range.from, "backwards keyring range");
        let mut previous = None;
        let mut overlay = Overlay {
            base: self,
            supplies: BTreeMap::new(),
            balances: BTreeMap::new(),
        };
        for logged in &range.events {
            let p = logged.position;
            ensure!(
                p.block >= range.from && p.block <= range.through.block,
                "keyring event outside its range"
            );
            ensure!(
                previous.is_none_or(|prev| p > prev),
                "unordered or duplicate keyring event"
            );
            previous = Some(p);
            step(&mut overlay, &logged.event)?;
        }
        Ok(())
    }

    /// Apply a range `check` accepted. It cannot fail.
    pub(super) fn apply(&mut self, range: Range) {
        for logged in range.events {
            step(self, &logged.event).expect("validated keyring event");
            let (position, transaction, timestamp) =
                (logged.position, logged.transaction, logged.timestamp);
            let notice = match logged.event {
                Event::Trade(t) if t.creator != t.holder => Some((
                    t.creator,
                    Notice::Trade(TradeNotice {
                        trader: t.holder,
                        buy: t.buy,
                        amount: t.amount,
                        position,
                        transaction,
                        timestamp,
                    }),
                )),
                // The contract never moves keys to their own holder, so `from` is someone else.
                Event::Transfer(t) => Some((
                    t.to,
                    Notice::Received(ReceivedNotice {
                        creator: t.creator,
                        from: t.from,
                        amount: t.amount,
                        position,
                        transaction,
                        timestamp,
                    }),
                )),
                _ => None,
            };
            if let Some((account, notice)) = notice {
                self.activity.entry(account).or_default().push(notice);
            }
        }
        self.checkpoint = Some(range.through);
    }

    pub(super) fn authors(&self, holder: Address) -> (Vec<Address>, Vec<Address>) {
        let holders: Vec<_> = self
            .holdings
            .get(&holder)
            .into_iter()
            .flat_map(|h| h.keys())
            .filter(|a| **a != holder)
            .copied()
            .take(257)
            .collect();
        let holding = self
            .held_by
            .get(&holder)
            .into_iter()
            .flat_map(|s| s.iter())
            .filter(|a| **a != holder)
            .copied()
            .take(257)
            .collect();
        (holders, holding)
    }

    pub(super) fn balance_page(
        &self,
        market: Address,
        account: Address,
        view: BalanceView,
        after: Option<Address>,
        limit: usize,
    ) -> Value {
        let own = self.balance(account, account);
        let start = after.map_or(Unbounded, Excluded);
        let (mut entries, total): (Vec<(Address, u64)>, usize) = match view {
            BalanceView::Holders => {
                let holders = self.holdings.get(&account);
                (
                    holders
                        .into_iter()
                        .flat_map(|h| h.range((start, Unbounded)))
                        .filter(|(a, _)| **a != account)
                        .take(limit + 1)
                        .map(|(a, n)| (*a, *n))
                        .collect(),
                    holders.map_or(0, BTreeMap::len),
                )
            }
            BalanceView::Holding => {
                let markets = self.held_by.get(&account);
                (
                    markets
                        .into_iter()
                        .flat_map(|h| h.range((start, Unbounded)))
                        .filter(|a| **a != account)
                        .take(limit + 1)
                        .map(|a| (*a, self.holdings[a][&account]))
                        .collect(),
                    markets.map_or(0, BTreeSet::len),
                )
            }
        };
        let more = entries.len() > limit;
        entries.truncate(limit);
        let next = if more {
            entries.last().map(|(a, _)| *a)
        } else {
            None
        };
        let entries: Vec<_> = entries
            .into_iter()
            .map(|(address, balance)| json!({"address":address,"balance":balance}))
            .collect();
        json!({"market":market,"checkpoint":self.checkpoint,
            "active":self.supplies.contains_key(&account),"view":view,
            "own_balance":own,"total":total - usize::from(own > 0),"entries":entries,"next":next})
    }
}
