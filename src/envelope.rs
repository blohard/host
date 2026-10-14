//! The content envelope: one header line, then the body (protocol SPEC.md §6).
//!
//! ```text
//! version/1 text/plain
//! <body bytes>
//! ```
//!
//! The header ends at the first newline. `version/N` is the envelope version;
//! the second token is the record's media type. The on-chain hash covers the
//! whole thing, header included, so the type is committed to. Hosts refuse
//! bytes without a valid header, and readers never render bytes whose header
//! they cannot parse.

use std::{collections::HashSet, fmt};

/// The only envelope version defined so far.
pub const VERSION: u32 = 1;
/// The header line's hard size cap: a media type never needs more.
pub const MAX_HEADER_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header<'a> {
    /// e.g. `text/plain`.
    pub media_type: &'a str,
    /// Byte offset where the body starts (just after the newline).
    pub body_start: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvelopeError {
    /// No newline within the first `MAX_HEADER_LEN` bytes.
    NoHeaderLine,
    /// The header is not `version/N <media-type>`.
    Malformed,
    /// A `version/N` this implementation does not know.
    UnsupportedVersion(u32),
    /// The media type is not `type/subtype` in the allowed alphabet.
    BadMediaType,
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoHeaderLine => write!(
                f,
                "content must start with a header line `version/{VERSION} <media-type>` ending in a newline"
            ),
            Self::Malformed => write!(f, "header line must be `version/{VERSION} <media-type>`"),
            Self::UnsupportedVersion(v) => write!(
                f,
                "unsupported envelope version {v} (this host knows {VERSION})"
            ),
            Self::BadMediaType => write!(
                f,
                "media type must be `type/subtype` (letters, digits, `.`, `-`, `+`)"
            ),
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// Parse and validate the header of `bytes`.
pub fn parse(bytes: &[u8]) -> Result<Header<'_>, EnvelopeError> {
    let limit = bytes.len().min(MAX_HEADER_LEN + 1);
    let nl = bytes[..limit]
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(EnvelopeError::NoHeaderLine)?;
    let line = std::str::from_utf8(&bytes[..nl]).map_err(|_| EnvelopeError::Malformed)?;
    let line = line.strip_suffix('\r').unwrap_or(line);

    let mut parts = line.split(' ');
    let (Some(version), Some(media_type), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(EnvelopeError::Malformed);
    };
    let version: u32 = version
        .strip_prefix("version/")
        .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|v| v.parse().ok())
        .ok_or(EnvelopeError::Malformed)?;
    if version != VERSION {
        return Err(EnvelopeError::UnsupportedVersion(version));
    }
    if !is_media_type(media_type) {
        return Err(EnvelopeError::BadMediaType);
    }
    Ok(Header {
        media_type,
        body_start: nl + 1,
    })
}

/// `type/subtype`, RFC 6838 restricted-name alphabet, lowercase, no parameters.
fn is_media_type(s: &str) -> bool {
    let Some((t, sub)) = s.split_once('/') else {
        return false;
    };
    let ok = |part: &str| {
        !part.is_empty()
            && part.len() <= 64
            && part
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_alphanumeric())
            && part.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+')
            })
    };
    ok(t) && ok(sub)
}

/// The `record:0x<keccak>` references in a body's text (protocol SPEC.md §7),
/// in order of first appearance, without duplicates. Lazy: a caller's limit
/// stops both scanning and deduplication, rather than trimming a full scan.
fn references(text: &str) -> impl Iterator<Item = [u8; 32]> + '_ {
    let mut seen = HashSet::new();
    text.match_indices("record:0x").filter_map(move |(i, _)| {
        let candidate = text.as_bytes().get(i + "record:0x".len()..)?.get(..64)?;
        let mut hash = [0; 32];
        hex::decode_to_slice(candidate, &mut hash).ok()?;
        seen.insert(hash).then_some(hash)
    })
}

/// The attachments a body references: its first `limit` distinct `record:`
/// references, the ones a host stores for it. Only text/plain records carry them.
pub fn attachments(body: &[u8], limit: usize) -> Vec<[u8; 32]> {
    let Ok(hdr) = parse(body) else {
        return Vec::new();
    };
    if hdr.media_type != "text/plain" {
        return Vec::new();
    }
    match std::str::from_utf8(&body[hdr.body_start..]) {
        Ok(text) => references(text).take(limit).collect(),
        Err(_) => Vec::new(),
    }
}

/// Does `parent` (a body) reference `digest` among its first `limit`
/// `record:` references? Only text/plain records carry references.
pub fn references_within(parent: &[u8], digest: &[u8; 32], limit: usize) -> bool {
    let Ok(hdr) = parse(parent) else {
        return false;
    };
    if hdr.media_type != "text/plain" {
        return false;
    }
    let Ok(text) = std::str::from_utf8(&parent[hdr.body_start..]) else {
        return false;
    };
    references(text).take(limit).any(|r| r == *digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_within_honours_the_cap_and_the_type() {
        let a = [0xaa; 32];
        let b = [0xbb; 32];
        let body = format!(
            "version/1 text/plain\n![a](record:0x{})\n[b](record:0x{})\n",
            hex::encode(a),
            hex::encode(b)
        );
        assert!(references_within(body.as_bytes(), &a, 2));
        assert!(references_within(body.as_bytes(), &b, 2));
        assert!(!references_within(body.as_bytes(), &b, 1), "beyond the cap");
        assert!(!references_within(body.as_bytes(), &[0xcc; 32], 2));
        // Only message text carries references; headerless bytes carry none.
        let other = body.replacen("text/plain", "application/octet-stream", 1);
        assert!(!references_within(other.as_bytes(), &a, 2));
        assert!(!references_within(&body.as_bytes()[21..], &a, 2));
    }

    #[test]
    fn reference_limit_counts_distinct_valid_hashes() {
        let a = "ab".repeat(32);
        let b = "cd".repeat(32);
        let body = format!(
            "version/1 text/plain\nrecord:0x1234 record:0x{}é \
             record:0x{a} record:0x{} record:0x{b}",
            "a".repeat(63),
            a.to_uppercase(),
        );
        assert!(!references_within(body.as_bytes(), &[0xab; 32], 0));
        assert!(references_within(body.as_bytes(), &[0xab; 32], 1));
        assert!(!references_within(body.as_bytes(), &[0xcd; 32], 1));
        assert!(references_within(body.as_bytes(), &[0xcd; 32], 2));
    }

    #[test]
    fn attachment_limit_holds_for_a_body_with_fourteen_thousand_references() {
        let mut body = String::from("version/1 text/plain\n");
        for i in 0u64..14_000 {
            body.push_str(&format!("record:0x{i:064x}\n"));
        }
        assert!(body.len() <= crate::store::MAX_CONTENT_SIZE);
        let mut hash = [0; 32];
        hash[31] = 3;
        assert!(references_within(body.as_bytes(), &hash, 4));
        hash[31] = 4;
        assert!(!references_within(body.as_bytes(), &hash, 4));
        assert!(!references_within(body.as_bytes(), &[0xff; 32], 4));
    }

    #[test]
    fn extracts_record_references_in_order_without_duplicates() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let text = format!(
            "![x](record:0x{a}) and [y](record:0x{b}) then again record:0x{a}\nshort record:0x1234 upper record:0x{}",
            "C".repeat(64)
        );
        let refs: Vec<_> = references(&text).collect();
        assert_eq!(refs.len(), 3);
        assert_eq!(hex::encode(refs[0]), a);
        assert_eq!(hex::encode(refs[1]), b);
        assert_eq!(hex::encode(refs[2]), "c".repeat(64));
        assert!(references("no references here").next().is_none());
        // 65 hex digits: only the first 64 count, which is deliberate — a
        // reference is exactly 32 bytes and trailing hex-looking text is text.
        assert_eq!(references(&format!("record:0x{a}f")).count(), 1);
    }

    #[test]
    fn parses_a_text_record() {
        let h = parse(b"version/1 text/plain\n# hi\n").unwrap();
        assert_eq!(h.media_type, "text/plain");
        assert_eq!(h.body_start, 21);
        // CRLF is tolerated in the header line.
        assert_eq!(parse(b"version/1 text/plain\r\nx").unwrap().body_start, 22);
        // An empty body is a valid record.
        assert_eq!(parse(b"version/1 text/plain\n").unwrap().body_start, 21);
        // Vendor and structured-syntax suffixes are fine.
        assert!(parse(b"version/1 application/vnd.example.poll+json\n{}").is_ok());
    }

    #[test]
    fn rejects_everything_else() {
        assert_eq!(
            parse(b"# no header\n").unwrap_err(),
            EnvelopeError::Malformed
        );
        assert_eq!(
            parse(b"just text without a newline"),
            Err(EnvelopeError::NoHeaderLine)
        );
        assert_eq!(parse(b""), Err(EnvelopeError::NoHeaderLine));
        assert_eq!(
            parse(b"version/2 text/plain\n"),
            Err(EnvelopeError::UnsupportedVersion(2))
        );
        assert_eq!(
            parse(b"version/ text/plain\n"),
            Err(EnvelopeError::Malformed)
        );
        assert_eq!(
            parse(b"version/01x text/plain\n"),
            Err(EnvelopeError::Malformed)
        );
        assert_eq!(parse(b"version/1\n"), Err(EnvelopeError::Malformed));
        assert_eq!(
            parse(b"version/1 text/plain extra\n"),
            Err(EnvelopeError::Malformed)
        );
        assert_eq!(
            parse(b"version/1  text/plain\n"),
            Err(EnvelopeError::Malformed),
            "double space"
        );
        assert_eq!(
            parse(b"version/1 plain\n"),
            Err(EnvelopeError::BadMediaType)
        );
        assert_eq!(
            parse(b"version/1 Text/Plain\n"),
            Err(EnvelopeError::BadMediaType),
            "lowercase only"
        );
        assert_eq!(
            parse(b"version/1 text/plain; charset=utf-8\n"),
            Err(EnvelopeError::Malformed),
            "no parameters"
        );
        assert_eq!(
            parse(b"version/1 text/pl ain\n"),
            Err(EnvelopeError::Malformed)
        );
        assert_eq!(
            parse(b"version/1 \xff/x\n"),
            Err(EnvelopeError::Malformed),
            "non-utf8 header"
        );
        // A newline only beyond the cap does not count as a header.
        let mut long = b"version/1 text/".to_vec();
        long.extend(std::iter::repeat_n(b'a', MAX_HEADER_LEN));
        long.push(b'\n');
        assert_eq!(parse(&long), Err(EnvelopeError::NoHeaderLine));
    }
}
