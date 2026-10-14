//! Board records and identifiers used by the post index and its queries.

use crate::chain::{Address, Hash, Hex, Position, decimal};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id(#[serde(with = "decimal")] pub u64);

pub const TOMBSTONE: Address = Hex([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
pub const REMOVED: Address = Hex([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    #[serde(with = "decimal")]
    pub chain: u64,
    pub board: Address,
    #[serde(with = "decimal")]
    pub start_block: u64,
    #[serde(with = "decimal")]
    pub index_start_block: u64,
    pub registries: Vec<Address>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListKey {
    pub registry: Address,
    pub owner: Address,
    pub list: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    #[serde(with = "decimal")]
    pub id: u64,
    pub author: Address,
    #[serde(with = "decimal")]
    pub parent: u64,
    pub parent_author: Address,
    pub timestamp: u32,
    pub content_hash: Hash,
    pub transaction: Hash,
    pub position: Position,
    pub gate: Address,
}

impl Message {
    /// ABI encoding of getMessage's four immutable fields, including the author.
    pub fn commitment(&self) -> Hash {
        let mut words = [0; 128];
        words[12..32].copy_from_slice(&self.author.0);
        words[56..64].copy_from_slice(&self.parent.to_be_bytes());
        words[92..96].copy_from_slice(&self.timestamp.to_be_bytes());
        words[96..128].copy_from_slice(&self.content_hash.0);
        Hex(crate::keccak::hash(&[&words]).0)
    }

    /// Deleted and removed messages leave lists and counts. Their records
    /// remain available for direct visits and surviving descendants' context.
    pub fn stands_alone(&self) -> bool {
        !withdrawn(self.gate.0)
    }
}

/// Whether a message's own gate says it was deleted or removed.
pub fn withdrawn(gate: [u8; 20]) -> bool {
    gate == TOMBSTONE.0 || gate == REMOVED.0
}
