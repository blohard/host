//! Keccak-256, the EVM's native hash (the `KECCAK256` opcode). Content is
//! addressed by it, so every content key is the very digest a message
//! commits to on the chain, checkable by an EVM contract at native-opcode
//! cost.

use bytes::{Buf, BufMut};
use commonware_codec::{Error as CodecError, FixedSize, Read, ReadExt, Write};
use commonware_utils::{Array, Span};
use core::{
    fmt::{Debug, Display},
    ops::Deref,
};

const DIGEST_LENGTH: usize = 32;

/// The Keccak-256 digest of `parts`, hashed as one string of bytes.
pub fn hash(parts: &[&[u8]]) -> Digest {
    let mut hasher = alloy_primitives::Keccak256::new();
    for part in parts {
        hasher.update(part);
    }
    Digest(hasher.finalize().0)
}

/// A Keccak-256 digest.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Digest(pub [u8; DIGEST_LENGTH]);

impl Digest {
    /// Parse from a hex string, with or without a `0x` prefix.
    pub fn from_hex(s: &str) -> Option<Self> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        let bytes = hex::decode(s).ok()?;
        let array: [u8; DIGEST_LENGTH] = bytes.try_into().ok()?;
        Some(Self(array))
    }

    /// Render as a `0x`-prefixed hex string.
    pub fn to_hex(self) -> String {
        format!("0x{}", hex::encode(self.0))
    }
}

// The content store's Freezer is keyed by Digest, so it is a commonware
// `Array`: a fixed-size byte string that encodes, orders and prints.

impl Write for Digest {
    fn write(&self, buf: &mut impl BufMut) {
        self.0.write(buf);
    }
}

impl Read for Digest {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, CodecError> {
        let array = <[u8; DIGEST_LENGTH]>::read(buf)?;
        Ok(Self(array))
    }
}

impl FixedSize for Digest {
    const SIZE: usize = DIGEST_LENGTH;
}

impl Span for Digest {}

impl Array for Digest {}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for Digest {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for Digest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl Display for Digest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known EVM keccak256 vectors: these must match what `keccak256(...)`
    /// returns in Solidity, or the whole content-addressing scheme is wrong.
    #[test]
    fn test_evm_vectors() {
        let cases: &[(&[u8], &str)] = &[
            (
                b"",
                "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
            ),
            (
                b"abc",
                "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45",
            ),
            (
                b"hello world",
                "47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad",
            ),
        ];
        for (input, expected) in cases {
            let digest = hash(&[input]);
            assert_eq!(hex::encode(digest.0), *expected);
            assert_eq!(digest.to_string(), *expected, "printed without a prefix");
        }
    }

    /// Several parts hash as their bytes joined.
    #[test]
    fn test_parts_hash_as_one_string() {
        assert_eq!(hash(&[b"hello", b" ", b"world"]), hash(&[b"hello world"]));
        assert_eq!(hash(&[b"", b"abc", b""]), hash(&[b"abc"]));
    }

    #[test]
    fn test_hex_roundtrip() {
        let digest = hash(&[b"roundtrip"]);
        assert_eq!(Digest::from_hex(&digest.to_hex()), Some(digest));
        let bare = digest.to_hex().trim_start_matches("0x").to_string();
        assert_eq!(Digest::from_hex(&bare), Some(digest));
    }
}
