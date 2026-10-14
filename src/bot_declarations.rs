//! Accounts' own signed statements that they are bots (GATEWAY_API.md, "Host policy").
//! They cost nothing, live beside the host policy file and never move the
//! policy revision: a counter of their own orders them for browsers.

use alloy_primitives::Address;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io::Read, path::Path};

/// Accounts kept, declared and withdrawn alike.
pub const MAX_ACCOUNTS: usize = 4096;
/// How far a statement's time may be from the host's clock, either way.
pub const WINDOW_SECS: u64 = 300;
const MAX_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    pub bot: bool,
    /// Unix seconds, as signed.
    pub issued: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declarations {
    /// Raised by every accepted statement.
    pub counter: u64,
    /// Each account's newest statement. Withdrawals are kept so an older
    /// "yes" cannot be replayed over them.
    pub accounts: BTreeMap<Address, Statement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// Outside the window, or not newer than the account's last statement.
    Stale,
    /// No room for another account.
    Full,
}

impl Declarations {
    /// Accounts whose newest statement says they are bots.
    pub fn declared(&self) -> Vec<Address> {
        self.accounts
            .iter()
            .filter(|(_, s)| s.bot)
            .map(|(a, _)| *a)
            .collect()
    }

    /// Whether `statement` is timely, ignoring who signed it and the cap.
    pub fn check(&self, account: &Address, statement: Statement, now: u64) -> Result<(), Refusal> {
        if statement.issued.abs_diff(now) > WINDOW_SECS
            || self
                .accounts
                .get(account)
                .is_some_and(|s| s.issued >= statement.issued)
        {
            return Err(Refusal::Stale);
        }
        Ok(())
    }

    /// The declarations once `statement` is accepted. An account already kept
    /// can always change its statement. A new one fits if room remains after
    /// dropping withdrawals older than the window, which no replay can beat.
    pub fn accept(
        &self,
        account: Address,
        statement: Statement,
        now: u64,
    ) -> Result<Self, Refusal> {
        self.check(&account, statement, now)?;
        let mut next = self.clone();
        if !next.accounts.contains_key(&account) && next.accounts.len() >= MAX_ACCOUNTS {
            next.accounts
                .retain(|_, s| s.bot || s.issued.saturating_add(WINDOW_SECS) >= now);
            if next.accounts.len() >= MAX_ACCOUNTS {
                return Err(Refusal::Full);
            }
        }
        next.accounts.insert(account, statement);
        next.counter = next.counter.checked_add(1).ok_or(Refusal::Full)?;
        Ok(next)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.accounts.len() <= MAX_ACCOUNTS,
            "too many bot declarations"
        );
        ensure!(
            !self.accounts.contains_key(&Address::ZERO),
            "zero address is not an account"
        );
        Ok(())
    }

    /// A missing file is no declarations yet; it is created on first write.
    /// A present file must be readable: a damaged one stops startup.
    pub fn read(path: &Path) -> Result<Self> {
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("opening bot declarations {}", path.display()));
            }
        };
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "bot declarations file is too large"
        );
        let declarations: Self =
            serde_json::from_slice(&bytes).context("reading bot declarations")?;
        declarations.validate()?;
        Ok(declarations)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(self)?;
        ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "bot declarations file is too large"
        );
        Ok(bytes)
    }
}

/// The exact text an account signs (EIP-191) to declare or withdraw. No
/// codename: the host is named by its origin.
pub fn message(origin: &str, account: &[u8; 20], bot: bool, issued: u64) -> String {
    format!(
        "Bot declaration\nHost: {origin}\nAccount: 0x{}\nBot: {}\nIssued: {issued}",
        hex::encode(account),
        if bot { "yes" } else { "no" }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_800_000_000;
    fn account(n: u64) -> Address {
        let mut a = [0u8; 20];
        a[12..].copy_from_slice(&n.to_be_bytes());
        Address::from(a)
    }
    fn says(bot: bool, issued: u64) -> Statement {
        Statement { bot, issued }
    }

    #[test]
    fn statements_must_be_timely_and_newer_than_the_last() {
        let empty = Declarations::default();
        assert_eq!(
            empty.check(&account(1), says(true, NOW - WINDOW_SECS - 1), NOW),
            Err(Refusal::Stale)
        );
        assert_eq!(
            empty.check(&account(1), says(true, NOW + WINDOW_SECS + 1), NOW),
            Err(Refusal::Stale)
        );
        let declared = empty.accept(account(1), says(true, NOW - 10), NOW).unwrap();
        assert_eq!(declared.declared(), vec![account(1)]);
        assert_eq!(declared.counter, 1);
        let withdrawn = declared
            .accept(account(1), says(false, NOW - 5), NOW)
            .unwrap();
        assert!(withdrawn.declared().is_empty());
        assert_eq!(withdrawn.counter, 2);
        // The earlier "yes" is still inside the window, but it is not newer.
        assert_eq!(
            withdrawn.accept(account(1), says(true, NOW - 10), NOW),
            Err(Refusal::Stale)
        );
        assert_eq!(
            withdrawn.accept(account(1), says(true, NOW - 5), NOW),
            Err(Refusal::Stale)
        );
        assert_eq!(
            withdrawn
                .accept(account(1), says(true, NOW - 4), NOW)
                .unwrap()
                .counter,
            3
        );
    }

    #[test]
    fn the_cap_covers_withdrawals_and_prunes_only_expired_ones_when_full() {
        let mut full = Declarations::default();
        for n in 1..=MAX_ACCOUNTS as u64 {
            // Half declared, half withdrawn long ago.
            full.accounts
                .insert(account(n), says(n % 2 == 0, NOW - 10 * WINDOW_SECS));
        }
        // Accounts already kept can always change their statement.
        let changed = full.accept(account(2), says(false, NOW), NOW).unwrap();
        assert_eq!(changed.accounts.len(), MAX_ACCOUNTS);
        // A new account fits once expired withdrawals are dropped, and only then.
        let joined = full
            .accept(account(MAX_ACCOUNTS as u64 + 1), says(true, NOW), NOW)
            .unwrap();
        assert_eq!(joined.accounts.len(), MAX_ACCOUNTS / 2 + 1);
        assert!(joined.accounts.values().all(|s| s.bot));
        // Recent withdrawals are kept, so a full file of them refuses newcomers.
        let mut recent = Declarations::default();
        for n in 1..=MAX_ACCOUNTS as u64 {
            recent.accounts.insert(account(n), says(false, NOW - 1));
        }
        assert_eq!(
            recent.accept(account(MAX_ACCOUNTS as u64 + 1), says(true, NOW), NOW),
            Err(Refusal::Full)
        );
    }

    #[test]
    fn files_round_trip_and_a_missing_one_is_empty() {
        let path =
            std::env::temp_dir().join(format!("bot-declarations-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert_eq!(Declarations::read(&path).unwrap(), Declarations::default());
        let declared = Declarations::default()
            .accept(account(9), says(true, NOW), NOW)
            .unwrap();
        std::fs::write(&path, declared.encode().unwrap()).unwrap();
        assert_eq!(Declarations::read(&path).unwrap(), declared);
        std::fs::write(&path, b"{").unwrap();
        assert!(
            Declarations::read(&path).is_err(),
            "a damaged file stops startup"
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn the_signed_text_names_the_host_account_answer_and_time() {
        assert_eq!(
            message("https://host.example", &[0xab; 20], true, 1_759_650_000),
            format!(
                "Bot declaration\nHost: https://host.example\nAccount: 0x{}\nBot: yes\nIssued: 1759650000",
                "ab".repeat(20)
            )
        );
    }
}
