//! Shared chain identifiers, block positions and uncached header reads.
//! Fixed-width identifiers stay compact in RAM and use hex on disk and HTTP.

use crate::{
    abi::{quantity, text},
    rpc,
    verifier::Chain,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::json;
use std::{fmt, str::FromStr};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hex<const N: usize>(pub [u8; N]);

pub type Address = Hex<20>;
pub type Hash = Hex<32>;

impl<const N: usize> Hex<N> {
    pub const ZERO: Self = Self([0; N]);
}

impl<const N: usize> fmt::Display for Hex<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl<const N: usize> fmt::Debug for Hex<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<const N: usize> FromStr for Hex<N> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        ensure!(
            s.starts_with("0x") && s.len() == 2 + 2 * N,
            "invalid hex identifier"
        );
        let mut out = [0; N];
        hex::decode_to_slice(&s[2..], &mut out)?;
        Ok(Self(out))
    }
}

impl<const N: usize> Serialize for Hex<N> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de, const N: usize> Deserialize<'de> for Hex<N> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?.parse().map_err(de::Error::custom)
    }
}

/// Message ids and sequence/block numbers must survive JavaScript's number limit.
pub mod decimal {
    use super::*;

    pub fn serialize<S: Serializer>(value: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let value = String::deserialize(d)?;
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(de::Error::custom("expected a decimal string"));
        }
        value.parse().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    #[serde(with = "decimal")]
    pub block: u64,
    pub log: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    #[serde(with = "decimal")]
    pub block: u64,
    pub hash: Hash,
}

pub struct Header {
    pub checkpoint: Checkpoint,
    pub timestamp: u32,
}

pub async fn header(
    client: &rpc::Upstream,
    chain: &Chain,
    block: u64,
    class: rpc::Class,
) -> Result<Header> {
    // Checkpoint verification always reads the node directly, without caching.
    let value = rpc::call_as(
        class,
        client,
        &chain.url,
        "eth_getBlockByNumber",
        json!([format!("0x{block:x}"), false]),
        rpc::MAX_RESPONSE,
    )
    .await?;
    let value = rpc::result(&value)?;
    ensure!(quantity(&value["number"])? == block, "wrong header number");
    Ok(Header {
        checkpoint: Checkpoint {
            block,
            hash: text(&value["hash"])?.parse()?,
        },
        timestamp: quantity(&value["timestamp"])?.try_into()?,
    })
}
