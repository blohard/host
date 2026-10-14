//! Pure Ethereum encodings: hex, ABI words, Board calldata and indexed events.
//! Authentication and RPC work stay with their callers.
use crate::chain::{Address, Hash, Hex, Position};
use crate::{
    index::{model::Event, types::*},
    keccak,
};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Result, anyhow, ensure};
use serde_json::Value;

sol! {
    // The calls the host makes or simulates, declared as the contracts
    // declare them (the protocol's src/IBoard.sol and src/IReplyGate.sol) and as
    // ERC-1271 does. Their encodings are the bytes Solidity's abi.encodeCall
    // produces.
    interface IBoard {
        function post(bytes32 contentHash, uint64 parentId, address replyGate, bytes gateData) external returns (uint64 id);
        function checkReply(uint64 parentId, address replier, bytes gateData) external view returns (address gate);
        function replyGate(uint64 id) external view returns (address);
    }
    interface IReplyGate {
        function canReply(uint64 parentId, address replier, bytes data) external view returns (bool allowed);
    }
    interface IERC1271 {
        function isValidSignature(bytes32 hash, bytes signature) external view returns (bytes4 magicValue);
    }
}

/// The board's `Posted` event as the ABI spells it: the topics are the author,
/// the parent and the parent's author; the id, hash and gate ride in the data.
/// Shared by ingestion, receipt decoding and the mock node.
pub const POSTED_SIGNATURE: &[u8] = b"Posted(address,uint64,address,uint64,bytes32,address)";

pub const POST_SIGNATURE: &str = "post(bytes32,uint64,address,bytes)";
pub const POST_BY_SIG_SIGNATURE: &str =
    "postBySig(address,bytes32,uint64,address,uint256,uint256,bytes,bytes)";

/// Hex bytes as the RPC spells them, `0x`-prefixed or bare; `None` when not hex.
pub fn decode_hex(s: &str) -> Option<Vec<u8>> {
    hex::decode(s.strip_prefix("0x").unwrap_or(s)).ok()
}

/// Parse a JSON-RPC `QUANTITY` (`0x`-prefixed hex) as u64.
pub fn parse_quantity(s: &str) -> Result<u64> {
    let digits = s
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("quantity {s:?} lacks 0x prefix"))?;
    ensure!(!digits.is_empty(), "empty quantity");
    u64::from_str_radix(digits, 16).map_err(|e| anyhow!("bad quantity {s:?}: {e}"))
}

/// Parse a `0x`-prefixed (or bare) hex EVM address.
pub fn parse_address(hex_addr: &str) -> Result<[u8; 20]> {
    let bytes = decode_hex(hex_addr).ok_or_else(|| anyhow!("bad address: {hex_addr:?}"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("address must be 20 bytes"))
}

/// Bytes as `0x`-prefixed lowercase hex, the way the RPC spells them.
pub fn hex0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// The 4-byte ABI selector of a function signature such as `getMessage(uint64)`.
pub fn selector(sig: &str) -> [u8; 4] {
    keccak::hash(&[sig.as_bytes()]).0[..4]
        .try_into()
        .expect("4 bytes")
}

/// Word `post_word` of a `post(...)` call, or `by_sig_word` of a
/// `postBySig(...)` call; `None` for any other calldata, or truncated
/// calldata.
fn posted_word(input: &[u8], post_word: usize, by_sig_word: usize) -> Option<&[u8]> {
    let selector_bytes = input.get(..4)?;
    let word = if selector_bytes == selector(POST_SIGNATURE) {
        post_word
    } else if selector_bytes == selector(POST_BY_SIG_SIGNATURE) {
        by_sig_word
    } else {
        return None;
    };
    input.get(4 + 32 * word..4 + 32 * (word + 1))
}

/// The content hash a `post(...)` or `postBySig(...)` call commits to, or
/// `None` if the calldata is neither.
pub fn posted_content_hash(input: &[u8]) -> Option<[u8; 32]> {
    posted_word(input, 0, 1)?.try_into().ok()
}

/// The reply gate a `post` or `postBySig` call creates its message with, from
/// the same calldata as [`posted_content_hash`]. `None` for anything else.
pub fn posted_reply_gate(input: &[u8]) -> Option<[u8; 20]> {
    posted_word(input, 2, 3)?[12..].try_into().ok()
}

/// ABI calldata for the Board's `replyGate(uint64 id)`: the message's own gate,
/// `TOMBSTONE` once its author deleted it and `REMOVED` once its parent's author
/// removed it.
pub fn reply_gate_calldata(message_id: u64) -> Vec<u8> {
    IBoard::replyGateCall { id: message_id }.abi_encode()
}

/// The pieces of a `postBySig` call the Board signs over and checks: the six
/// static words (author, contentHash, parentId, replyGate,
/// nonce, deadline) and the signature bytes. `None` for anything else, or
/// malformed calldata.
pub fn post_by_sig_parts(input: &[u8]) -> Option<([[u8; 32]; 6], &[u8])> {
    if input.len() < 4 + 8 * 32 || input[..4] != selector(POST_BY_SIG_SIGNATURE) {
        return None;
    }
    let args = &input[4..];
    let word = |i: usize| -> Option<[u8; 32]> { args.get(32 * i..32 * (i + 1))?.try_into().ok() };
    let mut words = [[0u8; 32]; 6];
    for (i, w) in words.iter_mut().enumerate() {
        *w = word(i)?;
    }
    // `sig` is dynamic: its word holds the offset of its length, then the bytes.
    // Offsets and lengths are untrusted: an unrepresentable one is malformed
    // calldata, never an arithmetic overflow.
    let offset = usize::try_from(u64::from_be_bytes(word(6)?[24..].try_into().ok()?)).ok()?;
    let sig_start = offset.checked_add(32)?;
    let len = usize::try_from(u64::from_be_bytes(
        args.get(offset..sig_start)?[24..].try_into().ok()?,
    ))
    .ok()?;
    let sig = args.get(sig_start..sig_start.checked_add(len)?)?;
    Some((words, sig))
}

/// The EIP-712 digest a `postBySig` call signs, exactly as the Board computes
/// it: domain `("Board", "1", chainId, contract)`, struct `Post(author, ...)`.
pub fn post_by_sig_digest(chain_id: u64, contract: &[u8; 20], words: &[[u8; 32]; 6]) -> [u8; 32] {
    let domain_typehash = keccak::hash(&[
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ]);
    let mut chain_word = [0u8; 32];
    chain_word[24..].copy_from_slice(&chain_id.to_be_bytes());
    let mut contract_word = [0u8; 32];
    contract_word[12..].copy_from_slice(contract);
    let domain = keccak::hash(&[
        &domain_typehash.0,
        &keccak::hash(&[b"Board"]).0,
        &keccak::hash(&[b"1"]).0,
        &chain_word,
        &contract_word,
    ]);
    let post_typehash = keccak::hash(&[
        b"Post(address author,bytes32 contentHash,uint64 parentId,address replyGate,uint256 nonce,uint256 deadline)",
    ]);
    let mut struct_parts: Vec<&[u8]> = vec![&post_typehash.0];
    struct_parts.extend(words.iter().map(|w| w.as_slice()));
    let struct_hash = keccak::hash(&struct_parts);
    keccak::hash(&[b"\x19\x01", &domain.0, &struct_hash.0]).0
}

/// ABI calldata for `getMessage(uint64 id)`: 4-byte selector followed by the
/// id as a right-aligned 32-byte word.
pub fn get_message_calldata(message_id: u64) -> Vec<u8> {
    let mut calldata = Vec::with_capacity(36);
    calldata.extend_from_slice(&selector("getMessage(uint64)"));
    calldata.extend_from_slice(&number_word(message_id));
    calldata
}

/// ABI calldata for the Board's `post`, without gate data: the call the host
/// estimates gas for.
pub fn post_calldata(content_hash: Hash, parent: u64, reply_gate: Address) -> Vec<u8> {
    IBoard::postCall {
        contentHash: content_hash.0.into(),
        parentId: parent,
        replyGate: reply_gate.0.into(),
        gateData: Default::default(),
    }
    .abi_encode()
}

/// ABI calldata for the Board's `checkReply`, which answers the gate a reply
/// from `replier` would face.
pub fn check_reply_calldata(parent: u64, replier: Address, gate_data: &[u8]) -> Vec<u8> {
    IBoard::checkReplyCall {
        parentId: parent,
        replier: replier.0.into(),
        gateData: gate_data.to_vec().into(),
    }
    .abi_encode()
}

/// ABI calldata for ERC-1271's `isValidSignature`, the question the Board
/// asks a contract account that signed a `postBySig`.
pub fn is_valid_signature_calldata(digest: [u8; 32], signature: &[u8]) -> Vec<u8> {
    IERC1271::isValidSignatureCall {
        hash: digest.into(),
        signature: signature.to_vec().into(),
    }
    .abi_encode()
}

/// ABI calldata for a reply gate's own `canReply`.
pub fn can_reply_calldata(parent: u64, replier: Address, data: &[u8]) -> Vec<u8> {
    IReplyGate::canReplyCall {
        parentId: parent,
        replier: replier.0.into(),
        data: data.to_vec().into(),
    }
    .abi_encode()
}

/// Extract `contentHash` from the ABI-encoded return of `getMessage`:
/// `(address author, uint64 parentId, uint32 timestamp, bytes32 contentHash)`
/// is four static 32-byte words, with the hash in the fourth.
pub fn content_hash_from_return(raw: &[u8]) -> Result<[u8; 32]> {
    ensure!(
        raw.len() >= 128,
        "unexpected getMessage return size: {} bytes",
        raw.len()
    );
    Ok(raw[96..128].try_into().expect("32-byte slice"))
}

const REPLY_GATE: &[u8] = b"ReplyGateChanged(uint64,address,address)";
const AUTHOR_GATE: &[u8] = b"AuthorGateChanged(address,address,address)";
const ADDED: &[u8] = b"Added(address,bytes32,address)";
const REMOVED: &[u8] = b"Removed(address,bytes32,address)";

/// The event signatures the index follows, for the log filter and for
/// picking the same events out of receipts.
pub fn topics() -> [Hash; 5] {
    [
        signature(POSTED_SIGNATURE),
        signature(REPLY_GATE),
        signature(AUTHOR_GATE),
        signature(ADDED),
        signature(REMOVED),
    ]
}

fn signature(bytes: &[u8]) -> Hash {
    Hex(keccak::hash(&[bytes]).0)
}

fn address(word: &[u8]) -> Result<Address> {
    ensure!(
        word.len() == 32 && word[..12].iter().all(|&b| b == 0),
        "invalid address word"
    );
    Ok(Hex(word[12..].try_into()?))
}
fn number(word: &[u8]) -> Result<u64> {
    ensure!(
        word.len() == 32 && word[..24].iter().all(|&b| b == 0),
        "invalid id word"
    );
    Ok(u64::from_be_bytes(word[24..].try_into()?))
}

pub fn decode_event(
    log: &Value,
    board: Address,
    registries: &[Address],
    timestamp: u32,
) -> Result<Event> {
    let emitter = text(&log["address"])?.parse::<Address>()?;
    ensure!(
        emitter == board || registries.contains(&emitter),
        "log from an unconfigured contract"
    );
    ensure!(
        log["removed"].as_bool() == Some(false),
        "removed or malformed log"
    );
    let topics = log["topics"]
        .as_array()
        .ok_or_else(|| anyhow!("missing topics"))?
        .iter()
        .map(|v| text(v)?.parse::<Hash>())
        .collect::<Result<Vec<_>>>()?;
    let data = decode_hex(text(&log["data"])?).ok_or_else(|| anyhow!("invalid log data"))?;
    let position = Position {
        block: quantity(&log["blockNumber"])?,
        log: quantity(&log["logIndex"])?.try_into()?,
    };
    if emitter != board {
        ensure!(
            topics.len() == 4 && data.is_empty(),
            "malformed registry event"
        );
        let present = if topics[0] == signature(ADDED) {
            true
        } else if topics[0] == signature(REMOVED) {
            false
        } else {
            return Err(anyhow!("unexpected registry event"));
        };
        return Ok(Event::Membership {
            list: ListKey {
                registry: emitter,
                owner: address(&topics[1].0)?,
                list: topics[2],
            },
            member: address(&topics[3].0)?,
            present,
            position,
        });
    }
    match topics.first().copied() {
        Some(topic) if topic == signature(POSTED_SIGNATURE) => {
            ensure!(topics.len() == 4 && data.len() == 96, "malformed Posted");
            Ok(Event::Posted {
                message: Message {
                    id: number(&data[..32])?,
                    author: address(&topics[1].0)?,
                    parent: number(&topics[2].0)?,
                    parent_author: address(&topics[3].0)?,
                    content_hash: Hex(data[32..64].try_into()?),
                    gate: address(&data[64..96])?,
                    timestamp,
                    position,
                    transaction: text(&log["transactionHash"])?.parse()?,
                },
            })
        }
        Some(topic) if topic == signature(REPLY_GATE) => {
            ensure!(
                topics.len() == 2 && data.len() == 64,
                "malformed ReplyGateChanged"
            );
            Ok(Event::ReplyGate {
                id: number(&topics[1].0)?,
                old: address(&data[..32])?,
                new: address(&data[32..])?,
                position,
            })
        }
        Some(topic) if topic == signature(AUTHOR_GATE) => {
            ensure!(
                topics.len() == 2 && data.len() == 64,
                "malformed AuthorGateChanged"
            );
            Ok(Event::AuthorGate {
                author: address(&topics[1].0)?,
                old: address(&data[..32])?,
                new: address(&data[32..])?,
                position,
            })
        }
        _ => Err(anyhow!("unexpected Board event")),
    }
}
pub fn number_word(n: u64) -> [u8; 32] {
    let mut word = [0; 32];
    word[24..].copy_from_slice(&n.to_be_bytes());
    word
}
pub fn address_word(address: Address) -> [u8; 32] {
    let mut word = [0; 32];
    word[12..].copy_from_slice(&address.0);
    word
}

/// Receipt fields shared by upload verification and the public recovery
/// route. Parsing is pure; the caller decides whether to verify a canonical
/// header as well. Malformed known events fail rather than looking absent.
pub struct Receipt<'a> {
    pub hash: Hash,
    pub block: u64,
    pub block_hash: Hash,
    pub succeeded: bool,
    logs: &'a [Value],
}

impl<'a> Receipt<'a> {
    pub fn parse(value: &'a Value, hash: Hash) -> Result<Self> {
        ensure!(
            text(&value["transactionHash"])?.parse::<Hash>()? == hash,
            "wrong receipt"
        );
        let block = quantity(&value["blockNumber"])?;
        let block_hash = text(&value["blockHash"])?.parse::<Hash>()?;
        let status = quantity(&value["status"])?;
        ensure!(status <= 1, "invalid receipt status");
        let logs = value["logs"]
            .as_array()
            .ok_or_else(|| anyhow!("missing receipt logs"))?;
        ensure!(logs.len() <= 4096, "receipt exceeds log limit");
        ensure!(status == 1 || logs.is_empty(), "reverted receipt has logs");
        Ok(Self {
            hash,
            block,
            block_hash,
            succeeded: status == 1,
            logs,
        })
    }

    pub fn events(
        &self,
        board: Address,
        registries: &[Address],
        timestamp: u32,
    ) -> Result<Vec<Event>> {
        let known = topics();
        let mut events = Vec::new();
        for log in self.logs {
            let emitter = text(&log["address"])?.parse::<Address>()?;
            if emitter != board && !registries.contains(&emitter) {
                continue;
            }
            let topics = log["topics"]
                .as_array()
                .ok_or_else(|| anyhow!("missing receipt topics"))?;
            // Anonymous logs and other events (nonce invalidation, etc.) do
            // not describe a post or moderation result.
            let Some(topic) = topics.first() else {
                continue;
            };
            let topic = text(topic)?.parse::<Hash>()?;
            let relevant = if emitter == board {
                &known[..3]
            } else {
                &known[3..]
            };
            if !relevant.contains(&topic) {
                continue;
            }
            let event = decode_event(log, board, registries, timestamp)?;
            ensure!(
                event.position().block == self.block
                    && text(&log["blockHash"])?.parse::<Hash>()? == self.block_hash
                    && text(&log["transactionHash"])?.parse::<Hash>()? == self.hash,
                "inconsistent receipt log"
            );
            events.push(event);
        }
        Ok(events)
    }
}

pub(crate) fn text(value: &Value) -> Result<&str> {
    value.as_str().ok_or_else(|| anyhow!("missing RPC field"))
}
pub(crate) fn quantity(value: &Value) -> Result<u64> {
    parse_quantity(text(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The calls the host builds are the bytes Foundry's `cast calldata`
    /// gives for the same arguments, as Solidity's abi.encodeCall would: the
    /// selector, then one 32-byte word per line.
    #[test]
    fn contract_calls_match_an_independent_encoder() {
        assert_eq!(
            hex::encode(can_reply_calldata(1, Hex([0; 20]), &[])),
            concat!(
                "ff7804fa",
                "0000000000000000000000000000000000000000000000000000000000000001",
                "0000000000000000000000000000000000000000000000000000000000000000",
                "0000000000000000000000000000000000000000000000000000000000000060",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
        );
        assert_eq!(
            hex::encode(check_reply_calldata(1, Hex([0x11; 20]), &[1, 8, 15])),
            concat!(
                "26a5eb56",
                "0000000000000000000000000000000000000000000000000000000000000001",
                "0000000000000000000000001111111111111111111111111111111111111111",
                "0000000000000000000000000000000000000000000000000000000000000060",
                "0000000000000000000000000000000000000000000000000000000000000003",
                "01080f0000000000000000000000000000000000000000000000000000000000",
            )
        );
        assert_eq!(
            hex::encode(post_calldata(Hex([0x5a; 32]), 7, Hex([0x22; 20]))),
            concat!(
                "934f0227",
                "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
                "0000000000000000000000000000000000000000000000000000000000000007",
                "0000000000000000000000002222222222222222222222222222222222222222",
                "0000000000000000000000000000000000000000000000000000000000000080",
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
        );
        let signature: Vec<u8> = (0..33).map(|i| i * 7 + 1).collect();
        assert_eq!(
            hex::encode(is_valid_signature_calldata([0xab; 32], &signature)),
            concat!(
                "1626ba7e",
                "abababababababababababababababababababababababababababababababab",
                "0000000000000000000000000000000000000000000000000000000000000040",
                "0000000000000000000000000000000000000000000000000000000000000021",
                "01080f161d242b323940474e555c636a71787f868d949ba2a9b0b7bec5ccd3da",
                "e100000000000000000000000000000000000000000000000000000000000000",
            )
        );
    }

    #[test]
    fn parses_addresses_and_quantities() {
        let bare = "00000000000000000000000000000000000000ab";
        assert_eq!(parse_address(bare).unwrap()[19], 0xab);
        assert_eq!(parse_address(&format!("0x{bare}")).unwrap()[19], 0xab);
        let err = parse_address("0x1234").unwrap_err().to_string();
        assert_eq!(err, "address must be 20 bytes");
        assert!(
            parse_address("0xzz")
                .unwrap_err()
                .to_string()
                .starts_with("bad address"),
            "not hex"
        );

        assert_eq!(parse_quantity("0x1").unwrap(), 1);
        assert_eq!(parse_quantity("0x7a69").unwrap(), 31337);
        assert_eq!(parse_quantity("0x2105").unwrap(), 8453);
        assert!(parse_quantity("7a69").is_err(), "no prefix");
        assert!(parse_quantity("0x").is_err(), "empty");
        assert!(parse_quantity("0xzz").is_err(), "not hex");
    }
    #[test]
    fn malformed_post_by_sig_offsets_are_rejected_without_arithmetic() {
        let word = |n: u64| {
            let mut w = [0u8; 32];
            w[24..].copy_from_slice(&n.to_be_bytes());
            w
        };
        let calldata = |offset: u64, len: u64, sig: &[u8]| {
            let mut v = selector(POST_BY_SIG_SIGNATURE).to_vec();
            for _ in 0..6 {
                v.extend_from_slice(&[0u8; 32]);
            }
            v.extend_from_slice(&word(offset));
            v.extend_from_slice(&word(len));
            v.extend_from_slice(sig);
            v
        };
        assert!(post_by_sig_parts(&calldata(224, 3, b"abc")).is_some_and(|(_, sig)| sig == b"abc"));
        for (offset, len) in [
            (u64::MAX, 3),
            (u64::MAX - 31, 3),
            (224, u64::MAX),
            (224, u64::MAX - 300),
            (300, 3),
        ] {
            assert!(
                post_by_sig_parts(&calldata(offset, len, b"abc")).is_none(),
                "offset {offset} len {len}"
            );
        }
    }

    #[test]
    fn selectors_and_hex() {
        // keccak256("getMessage(uint64)")[..4], computed independently with
        // js-sha3 — pins our keccak and the ABI selector together.
        assert_eq!(hex::encode(selector("getMessage(uint64)")), "539ca17a");
        assert_eq!(hex0x(&[0xde, 0xad]), "0xdead");
        assert_eq!(hex0x(&[]), "0x");
    }

    #[test]
    fn calldata_matches_abi() {
        let data = get_message_calldata(42);
        assert_eq!(data.len(), 36);
        // keccak256("getMessage(uint64)")[..4], computed independently with
        // js-sha3 — pins our keccak and the ABI selector together.
        assert_eq!(hex::encode(&data[..4]), "539ca17a");
        // uint64 is right-aligned in a zero-padded 32-byte word.
        assert!(data[4..35].iter().all(|&b| b == 0));
        assert_eq!(data[35], 42);

        let max = get_message_calldata(u64::MAX);
        assert!(max[4..28].iter().all(|&b| b == 0));
        assert!(max[28..36].iter().all(|&b| b == 0xff));
    }

    #[test]
    fn extracts_fourth_word_as_content_hash() {
        let hash = keccak::hash(&[b"blohard"]);
        let mut ret = vec![0u8; 128];
        ret[12..32].copy_from_slice(&[0xaa; 20]); // word 0: author
        ret[63] = 9; // word 1: parentId
        ret[95] = 1; // word 2: timestamp
        ret[96..128].copy_from_slice(&hash.0); // word 3: contentHash
        assert_eq!(content_hash_from_return(&ret).unwrap(), hash.0);

        // Extra trailing bytes are tolerated; short returns are not.
        ret.extend_from_slice(&[0u8; 32]);
        assert_eq!(content_hash_from_return(&ret).unwrap(), hash.0);
        let err = content_hash_from_return(&ret[..127]).unwrap_err();
        assert!(err.to_string().contains("return size"), "{err}");
        assert!(content_hash_from_return(&[]).is_err());
    }

    #[test]
    fn recognises_post_calldata() {
        let hash = keccak::hash(&[b"body"]).0;
        let mut post = selector(POST_SIGNATURE).to_vec();
        post.extend_from_slice(&hash);
        assert_eq!(posted_content_hash(&post), Some(hash));
        let mut by_sig = selector(POST_BY_SIG_SIGNATURE).to_vec();
        by_sig.extend_from_slice(&[0u8; 32]); // author word
        by_sig.extend_from_slice(&hash);
        assert_eq!(posted_content_hash(&by_sig), Some(hash));
        assert_eq!(posted_content_hash(&post[..20]), None, "truncated");
        assert_eq!(posted_content_hash(&selector("retract(uint64)")), None);
        assert_eq!(posted_content_hash(&[]), None);
    }

    #[test]
    fn reads_the_gate_a_post_is_created_with() {
        let gate = [0x77; 20];
        let post = post_calldata(Hex([1; 32]), 5, Hex(gate));
        assert_eq!(posted_reply_gate(&post), Some(gate));
        let mut by_sig = selector(POST_BY_SIG_SIGNATURE).to_vec();
        for w in [[0u8; 32], [1; 32], [0; 32], address_word(Hex(gate))] {
            by_sig.extend_from_slice(&w); // author, contentHash, parentId, replyGate
        }
        assert_eq!(posted_reply_gate(&by_sig), Some(gate));
        assert_eq!(posted_reply_gate(&post[..4 + 64]), None, "truncated");
        assert_eq!(posted_reply_gate(&selector("retract(uint64)")), None);
        assert_eq!(posted_reply_gate(&[]), None);
    }

    fn word(v: &[u8]) -> [u8; 32] {
        let mut w = [0u8; 32];
        w[32 - v.len()..].copy_from_slice(v);
        w
    }

    /// Vectors computed with `cast` against the Board's typehash strings.
    #[test]
    fn digest_matches_the_boards_eip712() {
        let contract = [0x11u8; 20];
        let words = [
            word(&[0x22u8; 20]),
            [0x33u8; 32],
            word(&5u64.to_be_bytes()),
            [0u8; 32],
            word(&7u64.to_be_bytes()),
            word(&1_700_000_000u64.to_be_bytes()),
        ];
        let digest = post_by_sig_digest(31337, &contract, &words);
        assert_eq!(
            hex::encode(digest),
            "930a1316956d5730b26f714a271f757f782333b131040b7b3d9005f694332478"
        );
    }

    #[test]
    fn calldata_parts_are_read_back() {
        let words = [
            [1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32], [5u8; 32], [6u8; 32],
        ];
        let sig = vec![9u8; 65];
        let mut input = selector(POST_BY_SIG_SIGNATURE).to_vec();
        for w in &words {
            input.extend_from_slice(w);
        }
        input.extend_from_slice(&word(&(8 * 32u64).to_be_bytes())); // sig offset
        input.extend_from_slice(&word(&((8 * 32 + 32 + 96) as u64).to_be_bytes())); // gateData offset
        input.extend_from_slice(&word(&65u64.to_be_bytes()));
        input.extend_from_slice(&sig);
        input.resize(input.len() + 31, 0);
        input.extend_from_slice(&[0u8; 32]);
        let (w, s) = post_by_sig_parts(&input).unwrap();
        assert_eq!(w, words);
        assert_eq!(s, &sig[..]);
        assert!(post_by_sig_parts(&input[..100]).is_none());
        assert!(post_by_sig_parts(b"nope").is_none());
    }
}
