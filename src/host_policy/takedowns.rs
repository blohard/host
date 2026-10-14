//! The takedown list: content hashes this host refuses to serve or accept,
//! whichever post they come from. Unlike the other host lists it is never
//! published: only the admin reads or changes it, with a signed request.
//! Anyone holding a hash can still learn that it was taken down, by asking
//! for the content.
//!
//! It lives beside the host policy file: `host-policy.json` keeps it in
//! `host-policy.takedowns.json`. The file must exist, and one that cannot be
//! read stops the host: taken-down content must never be served because the
//! list went missing.

use super::{MAX_PER_EDIT, fresh};
use crate::{chain::Hash, http::ApiError};
use anyhow::{Context, Result, ensure};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    io::Read as _,
    path::{Path, PathBuf},
};

/// Hashes the list holds at most. A full list refuses new takedowns; it never
/// drops old ones to make room.
pub const MAX_HASHES: usize = 4096;
/// A full list comes to about 300 KiB.
const MAX_BYTES: u64 = 1 << 20;
/// The refusal for a record on the list: this host neither serves nor
/// accepts it, whichever post it came from.
pub fn refused() -> ApiError {
    ApiError::said(
        StatusCode::GONE,
        "taken_down",
        "this host has taken this content down",
    )
}

/// The list as the file holds it.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Takedowns {
    pub revision: u64,
    pub hashes: BTreeSet<Hash>,
}

/// Where the takedown list of the host policy file `policy_file` lives.
pub fn path(policy_file: &Path) -> PathBuf {
    policy_file.with_extension("takedowns.json")
}

impl Takedowns {
    pub fn read(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path).with_context(|| {
            format!(
                "opening the takedown list {}; a host with none starts it as {{\"revision\":0,\"hashes\":[]}}",
                path.display()
            )
        })?;
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "the takedown list {} is too large",
            path.display()
        );
        let list: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("reading the takedown list {}", path.display()))?;
        ensure!(
            list.hashes.len() <= MAX_HASHES,
            "the takedown list {} holds more than {MAX_HASHES} hashes",
            path.display()
        );
        Ok(list)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec_pretty(self)?;
        ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "the takedown list is too large"
        );
        Ok(bytes)
    }

    pub fn contains(&self, digest: &[u8; 32]) -> bool {
        self.hashes.contains(&crate::chain::Hex(*digest))
    }

    /// The list after `edit`, with the next revision. Adding past the limit
    /// is refused rather than dropping anything.
    pub(super) fn after(&self, edit: &Edit) -> Result<Self, ApiError> {
        let mut next = self.clone();
        for hash in &edit.hashes {
            match edit.action {
                Action::Add => {
                    next.hashes.insert(*hash);
                }
                Action::Remove => {
                    next.hashes.remove(hash);
                }
            }
        }
        if next.hashes.len() > MAX_HASHES {
            return Err(ApiError::said(
                StatusCode::CONFLICT,
                "takedowns_full",
                format!("the takedown list holds at most {MAX_HASHES} hashes; remove some first"),
            ));
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| ApiError::new(StatusCode::CONFLICT, "takedowns_revision_exhausted"))?;
        Ok(next)
    }
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Add,
    Remove,
}

/// The admin's signed change to the list.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Edit {
    #[serde(with = "crate::chain::decimal")]
    pub(super) revision: u64,
    expires: u64,
    action: Action,
    hashes: Vec<Hash>,
    pub(super) signature: String,
}

impl Edit {
    /// The text the admin signs, after the request's own checks.
    pub fn message(&self, origin: &str, now: u64) -> Result<String, ApiError> {
        if !fresh(self.expires, now)
            || self.hashes.is_empty()
            || self.hashes.len() > MAX_PER_EDIT
            || self.hashes.contains(&crate::chain::Hex([0; 32]))
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_takedown_edit",
            ));
        }
        let action = match self.action {
            Action::Add => "add",
            Action::Remove => "remove",
        };
        Ok(format!(
            "Host takedown update\nHost: {origin}\nRevision: {}\nExpires: {}\nAction: {action}\nHashes:\n{}",
            self.revision,
            self.expires,
            self.hashes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        ))
    }
}

/// The admin's signed request to view the list.
#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct View {
    expires: u64,
    pub(super) signature: String,
}

impl View {
    pub fn message(&self, origin: &str, now: u64) -> Result<String, ApiError> {
        if !fresh(self.expires, now) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_takedown_read",
            ));
        }
        Ok(format!(
            "Host takedown list\nHost: {origin}\nExpires: {}",
            self.expires
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_policy::MAX_EXPIRY;

    fn hash(b: u8) -> Hash {
        crate::chain::Hex([b; 32])
    }
    fn edit(action: Action, hashes: Vec<Hash>, expires: u64) -> Edit {
        Edit {
            revision: 0,
            expires,
            action,
            hashes,
            signature: String::new(),
        }
    }

    #[test]
    fn the_list_round_trips_and_names_its_file() {
        let mut list = Takedowns::default();
        list.hashes.insert(hash(7));
        let back: Takedowns = serde_json::from_slice(&list.encode().unwrap()).unwrap();
        assert_eq!(back, list);
        assert!(back.contains(&[7; 32]));
        assert_eq!(
            path(Path::new("/x/host-policy.json")),
            Path::new("/x/host-policy.takedowns.json")
        );
    }

    #[test]
    fn a_missing_or_broken_file_is_an_error_never_an_empty_list() {
        let dir = std::env::temp_dir().join(format!("takedowns-read-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("host-policy.takedowns.json");
        let _ = std::fs::remove_file(&file);
        assert!(Takedowns::read(&file).is_err(), "missing");
        for broken in [
            "",
            "{",
            r#"{"revision":0}"#,
            r#"{"revision":0,"hashes":["0x12"]}"#,
        ] {
            std::fs::write(&file, broken).unwrap();
            assert!(Takedowns::read(&file).is_err(), "{broken:?}");
        }
        std::fs::write(&file, r#"{"revision":3,"hashes":[]}"#).unwrap();
        assert_eq!(Takedowns::read(&file).unwrap().revision, 3);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn edits_add_and_remove_and_a_full_list_refuses_more() {
        let list = Takedowns::default();
        let added = list
            .after(&edit(Action::Add, vec![hash(1), hash(2)], 0))
            .unwrap();
        assert_eq!(added.revision, 1);
        assert!(added.contains(&[1; 32]) && added.contains(&[2; 32]));
        let removed = added
            .after(&edit(Action::Remove, vec![hash(1)], 0))
            .unwrap();
        assert!(!removed.contains(&[1; 32]) && removed.contains(&[2; 32]));

        let mut full = Takedowns::default();
        for n in 0..MAX_HASHES as u32 {
            let mut h = [0; 32];
            h[..4].copy_from_slice(&n.to_be_bytes());
            h[31] = 1;
            full.hashes.insert(crate::chain::Hex(h));
        }
        let error = full
            .after(&edit(Action::Add, vec![hash(9)], 0))
            .unwrap_err();
        assert_eq!(error.code, "takedowns_full");
        assert_eq!(full.hashes.len(), MAX_HASHES, "nothing was dropped");
        assert!(full.after(&edit(Action::Remove, vec![hash(9)], 0)).is_ok());
    }

    #[test]
    fn signed_texts_name_the_host_and_expire_within_five_minutes() {
        let now = 1_000_000;
        let add = edit(Action::Add, vec![hash(5)], now + 60);
        let text = add.message("https://host.example", now).unwrap();
        assert!(text.starts_with("Host takedown update\nHost: https://host.example\n"));
        assert!(text.ends_with(&format!("Hashes:\n{}", hash(5))));
        for bad in [
            edit(Action::Add, vec![hash(5)], now - 1),
            edit(Action::Add, vec![hash(5)], now + MAX_EXPIRY + 1),
            edit(Action::Add, vec![], now + 60),
            edit(Action::Add, vec![hash(0)], now + 60),
            edit(Action::Add, vec![hash(5); MAX_PER_EDIT + 1], now + 60),
        ] {
            assert!(bad.message("https://host.example", now).is_err(), "{bad:?}");
        }
        let read = View {
            expires: now + 60,
            signature: String::new(),
        };
        assert_eq!(
            read.message("https://host.example", now).unwrap(),
            format!(
                "Host takedown list\nHost: https://host.example\nExpires: {}",
                now + 60
            )
        );
    }
}
