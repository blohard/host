//! Extract bounded head metadata without building or retaining a document tree.
use super::{Metadata, outbound, text};
use html5ever::tokenizer::{
    BufferQueue, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer, states::RawKind,
};
use reqwest::Url;
use std::{
    cell::{Cell, RefCell},
    time::Instant,
};

const CHUNK_BYTES: usize = 256;
// Bound incomplete tags too: the tokenizer checks duplicate attributes by
// scanning the attributes already seen. A page-sized tag is not cheap, but an
// inline icon, a build banner or an app's config often runs past 8 KiB.
// The cost of a page of such floods grows with this bound, so it stays as low
// as those heads allow.
const TOKEN_BYTES: usize = 16 << 10;

#[derive(Default)]
struct Head {
    title: Option<String>,
    fallback_title: Option<String>,
    description: Option<String>,
    fallback_description: Option<String>,
    image: Option<String>,
    in_title: bool,
    in_raw: bool,
}

struct Sink<'a> {
    head: RefCell<Head>,
    base: &'a Url,
    emitted: Cell<bool>,
    done: Cell<bool>,
}
impl TokenSink for Sink<'_> {
    type Handle = ();

    fn process_token(&self, token: Token, _: u64) -> TokenSinkResult<()> {
        if self.done.get() {
            return TokenSinkResult::Continue;
        }
        // Parse errors do not finish the token being accumulated.
        if !matches!(token, Token::ParseError(_)) {
            self.emitted.set(true);
        }
        let mut head = self.head.borrow_mut();
        match token {
            Token::TagToken(tag) => {
                let name = tag.name.as_ref();
                if tag.kind == TagKind::EndTag {
                    if name == "title" {
                        head.in_title = false;
                    }
                    if matches!(name, "title" | "script" | "style" | "noscript") {
                        head.in_raw = false;
                    }
                    if matches!(name, "head" | "body" | "html") {
                        self.done.set(true);
                        return TokenSinkResult::Continue;
                    }
                    return TokenSinkResult::Continue;
                }
                match name {
                    "html" | "head" | "base" | "link" => {}
                    "title" => {
                        head.in_raw = true;
                        head.in_title = head.fallback_title.is_none();
                        if head.in_title {
                            head.fallback_title = Some(String::new());
                        }
                        return TokenSinkResult::RawData(RawKind::Rcdata);
                    }
                    "script" => {
                        head.in_raw = true;
                        return TokenSinkResult::RawData(RawKind::ScriptData);
                    }
                    "style" | "noscript" => {
                        head.in_raw = true;
                        return TokenSinkResult::RawData(RawKind::Rawtext);
                    }
                    "meta" => {
                        let attr = |name: &str| {
                            tag.attrs
                                .iter()
                                .find(|a| a.name.local.as_ref() == name)
                                .map(|a| a.value.as_ref())
                        };
                        let Some(content) = attr("content") else {
                            return TokenSinkResult::Continue;
                        };
                        match attr("property")
                            .or_else(|| attr("name"))
                            .unwrap_or("")
                            .to_ascii_lowercase()
                            .as_str()
                        {
                            "og:title" if head.title.is_none() => {
                                head.title = Some(text(content, 256))
                            }
                            "og:description" if head.description.is_none() => {
                                head.description = Some(text(content, 512))
                            }
                            "description" if head.fallback_description.is_none() => {
                                head.fallback_description = Some(text(content, 512))
                            }
                            "og:image"
                                if head.image.is_none() && content.len() <= outbound::URL_BYTES =>
                            {
                                head.image = self
                                    .base
                                    .join(content.trim())
                                    .ok()
                                    .and_then(|u| outbound::url(u.as_str()).ok())
                                    .filter(|u| u.scheme() == "https")
                                    .map(|u| u.to_string());
                            }
                            _ => {}
                        }
                    }
                    // An implicit body ends the head just as an explicit one does.
                    // Template contents are not page metadata either.
                    _ => self.done.set(true),
                }
            }
            Token::CharacterTokens(chars) if head.in_title => {
                let title = head.fallback_title.as_mut().unwrap();
                for ch in chars.chars() {
                    // Leading space must not use up the room for the words.
                    if title.is_empty() && ch.is_whitespace() {
                        continue;
                    }
                    if title.len() + ch.len_utf8() > 1024 {
                        break;
                    }
                    title.push(ch);
                }
            }
            Token::CharacterTokens(chars)
                // A byte-order mark left mid-head by an include is not body text.
                if !head.in_raw
                    && chars
                        .chars()
                        .any(|c| !c.is_ascii_whitespace() && c != '\u{feff}') =>
            {
                self.done.set(true);
                return TokenSinkResult::Continue;
            }
            _ => {}
        }
        TokenSinkResult::Continue
    }
}

/// A prefix may end before the head does. A finished scan cannot benefit
/// from more bytes, whether the head ended or a safety bound rejected it.
#[derive(Debug, PartialEq)]
pub(super) struct Parsed {
    pub metadata: Option<Metadata>,
    pub finished: bool,
}

pub(super) fn parse(
    bytes: &[u8],
    base: &Url,
    deadline: Instant,
) -> Result<Parsed, outbound::Failure> {
    if Instant::now() >= deadline {
        return Err(outbound::Failure::timeout());
    }
    if bytes.len() > outbound::HTML_BYTES {
        return Ok(Parsed {
            metadata: None,
            finished: true,
        });
    }
    let html = String::from_utf8_lossy(bytes);
    let input = BufferQueue::default();
    let tokenizer = Tokenizer::new(
        Sink {
            head: RefCell::new(Head::default()),
            base,
            emitted: Cell::new(false),
            done: Cell::new(false),
        },
        Default::default(),
    );
    let mut offset = 0;
    let mut unfinished = 0;
    while offset < html.len() {
        if Instant::now() >= deadline {
            return Err(outbound::Failure::timeout());
        }
        let mut end = (offset + CHUNK_BYTES).min(html.len());
        while !html.is_char_boundary(end) {
            end -= 1;
        }
        input.push_back(html[offset..end].into());
        tokenizer.sink.emitted.set(false);
        let _ = tokenizer.feed(&input);
        if tokenizer.sink.done.get() {
            break;
        }
        unfinished = if tokenizer.sink.emitted.get() {
            0
        } else {
            unfinished + end - offset
        };
        if unfinished >= TOKEN_BYTES {
            return Ok(Parsed {
                metadata: None,
                finished: true,
            });
        }
        offset = end;
    }
    if offset == html.len() {
        tokenizer.end();
    }
    if Instant::now() >= deadline {
        return Err(outbound::Failure::timeout());
    }
    let finished = tokenizer.sink.done.get();
    let head = tokenizer.sink.head.into_inner();
    // Bytes that were not UTF-8 decode to U+FFFD. Such text is left out, so
    // the card falls back to the page's other text, or its domain, rather
    // than showing it garbled.
    let readable = |s: &String| !s.is_empty() && !s.contains('\u{fffd}');
    let title = head
        .title
        .filter(readable)
        .or_else(|| Some(text(&head.fallback_title.unwrap_or_default(), 256)).filter(readable))
        .unwrap_or_default();
    let description = head
        .description
        .filter(readable)
        .or(head.fallback_description.filter(readable))
        .unwrap_or_default();
    Ok(Parsed {
        metadata: (!title.is_empty() || !description.is_empty()).then_some(Metadata {
            title,
            description,
            image: head.image,
        }),
        finished,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn read(html: &str) -> Option<Metadata> {
        parse(
            html.as_bytes(),
            &Url::parse("https://example.org/page").unwrap(),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap()
        .metadata
    }

    #[test]
    fn ignores_comments_scripts_styles_and_body_metadata() {
        let head = r#"<!doctype html><html><head>
          <!-- <meta property=og:title content=comment> -->
          <script>let x = '</head><meta property=og:title content=script>';</script>
          <style>x::after {content:'</head><meta property=og:title content=style>'}</style>
          <noscript><meta property=og:title content=noscript></noscript>
          <TITLE>fallback &amp; &#x1f600;</TITLE>
          <META PROPERTY="og:title" CONTENT="A &quot;title&quot; > B">
          <meta name=description content='first'><meta name=description content='second'>
          <meta property=og:image content='/picture?a=1&amp;b=2'>
        "#;
        for ending in ["</head>", "<body>", "<div>", "body text", "<template>"] {
            let metadata = read(&format!(
                "{head}{ending}<meta property=og:title content=wrong>"
            ))
            .unwrap();
            assert_eq!(metadata.title, "A \"title\" > B");
            assert_eq!(metadata.description, "first");
            assert_eq!(
                metadata.image.as_deref(),
                Some("https://example.org/picture?a=1&b=2")
            );
        }
        assert_eq!(
            read("<title>fallback &amp; &#x1f600;</title>")
                .unwrap()
                .title,
            "fallback & 😀"
        );
        assert!(read("body text <meta property=og:title content=wrong>").is_none());
    }

    #[test]
    fn chunk_boundaries_preserve_unicode_entities_and_raw_text() {
        for offset in 0..CHUNK_BYTES {
            let html = format!(
                "{}<title>雪 &amp; &#x1f600;</title><script>'<meta property=og:title content=wrong>'</script><meta name=description content='a > b &amp; c'>",
                " ".repeat(offset)
            );
            let metadata = read(&html).unwrap();
            assert_eq!(metadata.title, "雪 & 😀");
            assert_eq!(metadata.description, "a > b & c");
        }
    }

    #[test]
    fn refuses_unfinished_tokens_and_attribute_floods_before_reading_the_whole_page() {
        let attributes: String = (0..30_000).map(|i| format!(" a{i}=x")).collect();
        for html in [
            format!("<meta {attributes} property=og:title content=wrong>"),
            format!(
                "<meta content='{}' property=og:title>",
                "x".repeat(outbound::HTML_BYTES / 2)
            ),
            format!(
                "<!--{}--><title>wrong</title>",
                "x".repeat(outbound::HTML_BYTES / 2)
            ),
        ] {
            let start = Instant::now();
            assert!(read(&html).is_none());
            assert!(start.elapsed() < Duration::from_secs(1));
        }
    }

    #[test]
    fn bounds_title_and_rejects_expired_deadlines_and_oversized_input() {
        let base = Url::parse("https://example.org/").unwrap();
        assert_eq!(
            parse(b"<title>x</title>", &base, Instant::now()),
            Err(outbound::Failure::timeout())
        );
        assert!(
            parse(
                &vec![b'x'; outbound::HTML_BYTES + 1],
                &base,
                Instant::now() + Duration::from_secs(1)
            )
            .unwrap()
            .metadata
            .is_none()
        );
        let metadata = read(&format!("<title>{}</title>", "雪".repeat(100_000))).unwrap();
        assert!(metadata.title.len() <= 256);
        assert!(!metadata.title.is_empty());
        for tag in ["ul", "dl", "div"] {
            assert!(read(&format!("<{tag}>").repeat(50_000)).is_none());
        }
    }

    fn read_bytes(html: &[u8]) -> Option<Metadata> {
        parse(
            html,
            &Url::parse("https://example.org/page").unwrap(),
            Instant::now() + Duration::from_secs(2),
        )
        .unwrap()
        .metadata
    }

    #[test]
    fn ordinary_heads_keep_their_metadata_past_marks_inline_icons_and_long_comments() {
        let og =
            "<meta property=og:title content=found><meta property=og:description content=words>";
        for head in [
            // A byte-order mark left in the middle of a head by an include.
            format!("<head><title>x</title>\u{feff}{og}"),
            // An inline icon or a build banner longer than one chunk of the scan.
            format!(
                "<head><link rel=icon href=\"data:image/png;base64,{}\">{og}",
                "A".repeat(12_000)
            ),
            format!("<head><!-- {} -->{og}", "build ".repeat(2_000)),
            format!(
                "<head><meta name=app-config content=\"{}\">{og}",
                "%7B".repeat(4_000)
            ),
        ] {
            let metadata = read(&head).unwrap();
            assert_eq!(
                (metadata.title.as_str(), metadata.description.as_str()),
                ("found", "words")
            );
        }
    }

    #[test]
    fn an_empty_open_graph_description_leaves_the_page_description() {
        let metadata = read("<meta property=og:description content=''><meta name=description content='real words'><title>t</title>").unwrap();
        assert_eq!(metadata.description, "real words");
    }

    #[test]
    fn a_title_after_long_leading_space_is_kept() {
        let metadata = read(&format!("<title>{}Real title</title>", "\n ".repeat(600))).unwrap();
        assert_eq!(metadata.title, "Real title");
    }

    #[test]
    fn text_that_did_not_decode_as_utf8_is_left_out_rather_than_shown_garbled() {
        let metadata =
            read_bytes(b"<title>Caf\xe9 news</title><meta name=description content='plain words'>")
                .unwrap();
        assert_eq!(metadata.title, "", "the card shows the domain instead");
        assert_eq!(metadata.description, "plain words");
        assert!(
            read_bytes(b"<title>Caf\xe9</title><meta name=description content='\x93quoted\x94'>")
                .is_none()
        );
    }

    #[test]
    fn garbled_open_graph_text_gives_way_to_the_readable_title_and_description() {
        let metadata = read_bytes(
            b"<meta property=og:title content='Caf\xe9'><title>Cafe news</title>\
              <meta property=og:description content='Caf\xe9 x'><meta name=description content='clean words'>",
        )
        .unwrap();
        assert_eq!(metadata.title, "Cafe news");
        assert_eq!(metadata.description, "clean words");
    }

    #[test]
    fn a_page_of_attribute_heavy_tags_each_within_the_token_limit_parses_to_its_end() {
        // Every tag is just under the bound, so none is refused early: the
        // whole page goes through the tokenizer's duplicate-attribute checks.
        let mut tag = String::from("<meta");
        let mut i = 0;
        while tag.len() < TOKEN_BYTES - 512 {
            tag.push_str(&format!(" a{i}=x"));
            i += 1;
        }
        tag.push('>');
        let fill = tag.repeat((outbound::HTML_BYTES - 1024) / tag.len());
        let page = format!("<head>{fill}<meta property=og:title content=found>");
        assert!(page.len() > 400 << 10, "{}", page.len());
        let start = Instant::now();
        let metadata = parse(
            page.as_bytes(),
            &Url::parse("https://example.org/").unwrap(),
            Instant::now() + Duration::from_secs(10),
        );
        assert_eq!(
            metadata.unwrap().metadata.unwrap().title,
            "found",
            "parsed within the fetch deadline"
        );
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{:?}",
            start.elapsed()
        );
    }

    fn read_at(bytes: &[u8], base: &Url) -> Option<Metadata> {
        parse(bytes, base, Instant::now() + Duration::from_secs(10))
            .unwrap()
            .metadata
    }

    #[test]
    fn metadata_does_not_build_a_tree_for_the_body() {
        let mut html = b"<head><title>preview</title></head><body>".to_vec();
        while html.len() + 4 <= outbound::HTML_BYTES {
            html.extend_from_slice(b"<ul>");
        }
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let metadata = read_at(&html, &Url::parse("https://example.org/").unwrap());
            let _ = send.send(metadata);
        });
        let metadata = receive
            .recv_timeout(Duration::from_secs(2))
            .expect("metadata parser spent over two seconds on the body")
            .unwrap();
        assert_eq!(metadata.title, "preview");
    }

    #[test]
    fn metadata_is_plain_bounded_text_with_relative_https_image_and_no_canonical_override() {
        let base = Url::parse("https://redirect.example/final/page").unwrap();
        let m = read_at(br#"<title>fallback</title><meta property='og:title' content='A &amp; B'><meta name='description' content='  words   here '><meta property='og:image' content='../pic.jpg'><meta property='og:url' content='https://spoof.example/'><script>document.title='evil'</script>"#, &base).unwrap();
        assert_eq!(
            m,
            Metadata {
                title: "A & B".into(),
                description: "words here".into(),
                image: Some("https://redirect.example/pic.jpg".into())
            }
        );
        assert_eq!(
            read_at(b"<title>&lt;img src=x onerror=alert(1)&gt;</title>", &base)
                .unwrap()
                .title,
            "<img src=x onerror=alert(1)>"
        );
        assert!(read_at(b"<body>no metadata", &base).is_none());
        for image in [
            "http://example.org/i",
            "https://127.1/i",
            "data:image/png,x",
            "javascript:alert(1)",
        ] {
            assert!(
                read_at(
                    format!("<title>x</title><meta property=og:image content='{image}'>")
                        .as_bytes(),
                    &base
                )
                .unwrap()
                .image
                .is_none()
            );
        }
        assert!(text(&"𐀀".repeat(1000), 255).len() <= 255);
    }
}
