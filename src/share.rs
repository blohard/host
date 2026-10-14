//! Server-rendered metadata for portable post and profile links. The built app handles navigation.
//!
//! Each page fills in one [`Card`]: what a link preview shows and what a
//! reader without JavaScript reads. [`render_card`] writes it into the
//! deployed `index.html`, escaping every value.
use crate::http::client;
use crate::share_cache::{Cache, Uncached};
use crate::{
    chain::{Address, Hash},
    envelope,
    http::JsonPath,
    index::types::Message,
    keccak::{self, Digest},
    server::{AppState, GatewayState},
};
use axum::{
    Extension,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
};
use reqwest::Url;
use serde::Deserialize;
use std::net::SocketAddr;
use std::{path::PathBuf, sync::Arc, time::Duration};

pub struct Pages {
    dir: PathBuf,
    cache: Cache,
}
impl Pages {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            cache: Cache::default(),
        }
    }
}
#[derive(Default, Deserialize)]
pub struct Options {
    commit: Option<Hash>,
    reply: Option<String>,
}

/// A picture a card shows: its path on the site, its height at 1200 wide,
/// and what it shows, for readers who cannot see it.
#[derive(Clone, Copy)]
struct Picture {
    path: &'static str,
    height: u32,
    alt: &'static str,
}
const WHALE: Picture = Picture {
    path: "/card.jpg",
    height: 630,
    alt: "a cartoon whale spouting water among the waves",
};
const GOLDEN_KEY: Picture = Picture {
    path: "/account-keys-card.jpg",
    height: 654,
    alt: "the blohard whale holding a golden key",
};
/// The square picture beside a post's title and words in X's small card,
/// which its large card would leave out.
const THUMBNAIL: &str = "/whale-avatar.jpg";

/// Everything a link preview shows for one page, and what a reader without
/// JavaScript reads.
struct Card {
    title: String,
    description: String,
    /// The page's canonical address.
    url: Url,
    /// `og:type`: an article, a profile or a website.
    kind: &'static str,
    picture: Picture,
    /// X's small card with the square thumbnail, rather than the large picture.
    small: bool,
    chain: Option<u64>,
    /// A post's author label and time.
    author: Option<(String, u32)>,
    /// What a reader without JavaScript is invited to do: "enable JavaScript to …".
    noscript: &'static str,
}

/// The card's tags, between the template's metadata markers, and its words for
/// readers without JavaScript. A template without the markers is returned as it is.
fn render_card(template: &str, origin: &Url, card: &Card) -> String {
    const START: &str = "<!-- blohard-meta:start -->";
    const END: &str = "<!-- blohard-meta:end -->";
    let Some(start) = template.find(START).map(|at| at + START.len()) else {
        return template.to_owned();
    };
    let Some(end) = template[start..].find(END).map(|at| start + at) else {
        return template.to_owned();
    };
    let at = |path: &str| escape(origin.join(path).expect("static path").as_str());
    let title = escape(&card.title);
    let description = escape(&card.description);
    let url = escape(card.url.as_str());
    let image = at(card.picture.path);
    let (twitter_card, twitter_image) = if card.small {
        ("summary", at(THUMBNAIL))
    } else {
        ("summary_large_image", image.clone())
    };
    let Card {
        kind,
        picture: Picture { height, alt, .. },
        noscript,
        ..
    } = card;
    let mut tags = format!(
        r#"<title>{title}</title>
<meta name="description" content="{description}" />
<link rel="canonical" href="{url}" />
<meta property="og:type" content="{kind}" />
<meta property="og:site_name" content="blohard" />
<meta property="og:title" content="{title}" />
<meta property="og:description" content="{description}" />
<meta property="og:url" content="{url}" />
<meta property="og:image" content="{image}" />
<meta property="og:image:width" content="1200" />
<meta property="og:image:height" content="{height}" />
<meta property="og:image:alt" content="{alt}" />
<meta name="twitter:card" content="{twitter_card}" />
<meta name="twitter:title" content="{title}" />
<meta name="twitter:description" content="{description}" />
<meta name="twitter:image" content="{twitter_image}" />
<meta name="twitter:image:alt" content="{alt}" />
"#
    );
    if let Some(chain) = card.chain {
        tags.push_str(&format!(
            "<meta name=\"blohard:chain\" content=\"{chain}\" />\n"
        ));
    }
    if let Some((author, timestamp)) = &card.author {
        tags.push_str(&format!(
            "<meta name=\"author\" content=\"{}\" />\n<meta name=\"blohard:timestamp\" content=\"{timestamp}\" />\n",
            escape(author)
        ));
    }
    let mut html = template.to_owned();
    html.replace_range(start..end, &format!("\n{tags}"));
    html.replacen("</body>", &format!("<noscript><main><h1>{title}</h1><p>{description}</p><p>enable JavaScript to {noscript}.</p></main></noscript></body>"), 1)
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// The app as it is, with its own generic card: what a page serves when its
/// card cannot be built now.
fn plain(template: String) -> Response {
    html_page(StatusCode::OK, template)
}

/// An HTML page browsers must neither keep nor reinterpret.
fn html_page(status: StatusCode, html: String) -> Response {
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        Html(html),
    )
        .into_response()
}

fn chain_not_served() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CACHE_CONTROL, "no-store")],
        "chain not served by this host.",
    )
        .into_response()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::RETRY_AFTER, "30"),
        ],
        "preview temporarily unavailable. please retry.",
    )
        .into_response()
}

async fn template(pages: &Pages) -> Option<String> {
    tokio::time::timeout(
        Duration::from_secs(2),
        tokio::fs::read_to_string(pages.dir.join("index.html")),
    )
    .await
    .ok()?
    .ok()
}

fn hex_template(template: &str) -> Hash {
    crate::chain::Hex(keccak::hash(&[template.as_bytes()]).0)
}

/// The deployment's own address, from its HTML: never an untrusted Host header.
fn public_origin(template: &str) -> Option<Url> {
    template
        .split("<meta property=\"og:url\" content=\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .and_then(|s| Url::parse(s).ok())
        .filter(|u| matches!(u.scheme(), "http" | "https") && u.host_str().is_some())
}

/// The cached card, or the plain app when the card cannot be built in time:
/// the host is busy, a read failed, or eight seconds passed.
async fn cached(
    pages: &Pages,
    key: String,
    ip: std::net::IpAddr,
    build: impl std::future::Future<Output = Response>,
    template: &str,
) -> Response {
    match tokio::time::timeout(Duration::from_secs(8), pages.cache.get(key, ip, build)).await {
        Ok(Some(response)) => response,
        _ => plain(template.to_owned()),
    }
}

/// What a post link points at, as this host's index has it.
enum Post {
    /// This host does not index the chain.
    NotServed,
    /// The index is not ready to say.
    NotReady,
    Missing,
    /// The link pins a different version of the post.
    Conflict,
    /// Deleted by its author, or removed by the author of its parent.
    Gone,
    Blocked,
    /// Its body has been taken down by this host.
    TakenDown,
    Live(Message),
}

fn post_at(state: &AppState, chain: u64, id: u64, commit: Option<Hash>) -> Post {
    let Some(index) = state.index(chain) else {
        return Post::NotServed;
    };
    match index.read_ready(|m| m.messages.get(&id).cloned()) {
        None => Post::NotReady,
        Some(None) => Post::Missing,
        Some(Some(m)) if commit.is_some_and(|pin| pin != m.commitment()) => Post::Conflict,
        Some(Some(m)) if state.policy.blocked(&m.author.0) => Post::Blocked,
        Some(Some(m)) if !m.stands_alone() => Post::Gone,
        Some(Some(m)) if state.policy.taken_down(&m.content_hash.0) => Post::TakenDown,
        Some(Some(m)) => Post::Live(m),
    }
}

pub async fn page(
    State(state): State<Arc<AppState>>,
    Extension(pages): Extension<Arc<Pages>>,
    JsonPath((chain, id)): JsonPath<(u64, u64)>,
    Query(options): Query<Options>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response {
    let Some(template) = template(&pages).await else {
        return unavailable();
    };
    if options.reply.as_ref().is_some_and(|v| !v.is_empty()) {
        return (StatusCode::BAD_REQUEST, "invalid reply parameter.").into_response();
    }
    let Some(origin) = public_origin(&template) else {
        return plain(template);
    };
    let mut url = origin
        .join(&format!("/p/{chain}/{id}"))
        .expect("numeric path");
    if let Some(commit) = options.commit {
        url.query_pairs_mut()
            .append_pair("commit", &commit.to_string());
    }
    // Missing, mismatched and ended posts need nothing from outside this host,
    // so they keep their status even when the card builders are busy.
    let (status, description) = match post_at(&state, chain, id, options.commit) {
        Post::NotServed => return chain_not_served(),
        Post::NotReady => return plain(template),
        Post::Missing => (StatusCode::NOT_FOUND, "this post is not available."),
        Post::Conflict => (
            StatusCode::CONFLICT,
            "this link does not match the post currently recorded on this chain.",
        ),
        Post::Gone => (StatusCode::GONE, "this post has been deleted or removed."),
        Post::Blocked => (StatusCode::FORBIDDEN, "this post is blocked by this host."),
        Post::TakenDown => (
            StatusCode::GONE,
            "this post's content was removed by this host.",
        ),
        Post::Live(message) => {
            let key = format!(
                "post/{chain}/{id}/{:?}/{message:?}/{}",
                options.commit,
                hex_template(&template)
            );
            let response = cached(
                &pages,
                key,
                client(peer.map(|p| p.0.0), &headers),
                live_post(
                    &state,
                    &template,
                    &origin,
                    url,
                    chain,
                    &message,
                    name_wait(&headers),
                ),
                &template,
            )
            .await;
            // The post may have changed while its card was built or cached.
            return match post_at(&state, chain, id, options.commit) {
                Post::Live(current) if current == message => response,
                _ => plain(template),
            };
        }
    };
    let card = Card {
        title: format!("post #{id} · blohard"),
        description: description.into(),
        url,
        kind: "article",
        picture: WHALE,
        small: true,
        chain: Some(chain),
        author: None,
        noscript: "open the conversation",
    };
    html_page(status, render_card(&template, &origin, &card))
}

/// A live post's card: its words, once its complete envelope matches the
/// indexed hash, and its author's name. A 503 here serves the plain app.
async fn live_post(
    state: &AppState,
    template: &str,
    origin: &Url,
    url: Url,
    chain: u64,
    m: &Message,
    wait: Duration,
) -> Response {
    let Some(gateway) = &state.gateway else {
        return unavailable();
    };
    let digest = Digest(m.content_hash.0);
    let Ok(Some(bytes)) = state.body_from_store_or_peer(chain, m.id, digest).await else {
        return unavailable();
    };
    let Some(text) = excerpt(&bytes, m) else {
        return unavailable();
    };
    let (name, known) = display_name(gateway, m.author, wait).await;
    // The content and name reads may have crossed an accepted reorg or deletion.
    if !matches!(post_at(state, chain, m.id, None), Post::Live(current) if current == *m) {
        return unavailable();
    }
    let card = Card {
        title: post_title(
            m.timestamp,
            name.as_deref().unwrap_or(&short_address(m.author)),
        ),
        description: if text.is_empty() {
            "a post on blohard.".into()
        } else {
            text
        },
        url,
        kind: "article",
        picture: WHALE,
        small: true,
        chain: Some(chain),
        author: Some((author_label(name.as_deref(), m.author), m.timestamp)),
        noscript: "open the conversation",
    };
    let mut response = html_page(StatusCode::OK, render_card(template, origin, &card));
    if !known {
        response.extensions_mut().insert(Uncached);
    }
    response
}

#[derive(Clone, Copy, Debug)]
pub enum ProfileTab {
    Posts,
    Replies,
    Keys,
    Keyring,
    Holders,
}
impl ProfileTab {
    fn suffix(self) -> &'static str {
        match self {
            Self::Posts => "",
            Self::Replies => "/replies",
            Self::Keys => "/keys",
            Self::Keyring => "/keyring",
            Self::Holders => "/keyring/holders",
        }
    }
}

/// A profile tab; each route names its tab (routes.rs).
pub async fn profile(
    State(state): State<Arc<AppState>>,
    Extension(pages): Extension<Arc<Pages>>,
    Extension(tab): Extension<ProfileTab>,
    JsonPath((chain, address)): JsonPath<(u64, Address)>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
) -> Response {
    let Some(template) = template(&pages).await else {
        return unavailable();
    };
    let Some(gateway) = &state.gateway else {
        return unavailable();
    };
    if !gateway.indexes.contains_key(&chain) {
        return chain_not_served();
    }
    let Some(origin) = public_origin(&template) else {
        return plain(template);
    };
    let key = format!(
        "profile/{chain}/{address}/{tab:?}/{}",
        hex_template(&template)
    );
    let build = async {
        // A profile exists for any address, including one that has not posted yet.
        let (name, known) = display_name(gateway, address, name_wait(&headers)).await;
        let name = name.unwrap_or_else(|| address.to_string());
        let card = profile_card(&origin, tab, chain, address, &name);
        let mut response = html_page(StatusCode::OK, render_card(&template, &origin, &card));
        if !known {
            response.extensions_mut().insert(Uncached);
        }
        response
    };
    cached(
        &pages,
        key,
        client(peer.map(|p| p.0.0), &headers),
        build,
        &template,
    )
    .await
}

fn profile_card(origin: &Url, tab: ProfileTab, chain: u64, address: Address, name: &str) -> Card {
    let network = match chain {
        8453 => "Base",
        84532 => "Base Sepolia",
        _ => "another network",
    };
    let (title, description, picture) = match tab {
        ProfileTab::Posts => (
            format!("{name} · blohard"),
            format!("posts and replies by {name} on blohard. wallet {address} · on {network}."),
            WHALE,
        ),
        ProfileTab::Replies => (
            format!("{name} replies · blohard"),
            format!("replies by {name} on blohard. wallet {address} · on {network}."),
            WHALE,
        ),
        ProfileTab::Keys => (
            format!("{name} account keys · blohard"),
            format!(
                "explore account keys for {name} on blohard. view the market, buy or sell keys, and see your holdings."
            ),
            GOLDEN_KEY,
        ),
        ProfileTab::Keyring => (
            format!("{name} keyring · blohard"),
            format!("explore the account keys held by {name} on blohard."),
            WHALE,
        ),
        ProfileTab::Holders => (
            format!("{name} key holders · blohard"),
            format!("see who holds keys to {name} on blohard."),
            WHALE,
        ),
    };
    Card {
        title,
        description,
        url: origin
            .join(&format!("/u/{chain}/{address}{}", tab.suffix()))
            .expect("hex address path"),
        kind: "profile",
        picture,
        small: false,
        chain: Some(chain),
        author: None,
        noscript: "open this profile",
    }
}

pub async fn about(Extension(pages): Extension<Arc<Pages>>) -> Response {
    information_page(&pages, None).await
}

pub async fn keys_about(
    State(state): State<Arc<AppState>>,
    Extension(pages): Extension<Arc<Pages>>,
    JsonPath(chain): JsonPath<u64>,
) -> Response {
    if !state
        .gateway
        .as_ref()
        .is_some_and(|g| g.indexes.contains_key(&chain))
    {
        return chain_not_served();
    }
    information_page(&pages, Some(chain)).await
}

/// The about page, or the account keys' about page on `chain`.
async fn information_page(pages: &Pages, chain: Option<u64>) -> Response {
    let Some(template) = template(pages).await else {
        return unavailable();
    };
    let Some(origin) = public_origin(&template) else {
        return plain(template);
    };
    let card = information_card(&origin, chain);
    html_page(StatusCode::OK, render_card(&template, &origin, &card))
}

fn information_card(origin: &Url, chain: Option<u64>) -> Card {
    let (path, title, description, picture) = match chain {
        Some(chain) => (
            format!("/keys/{chain}/about"),
            "about account keys · blohard",
            "an optional market for your blohard account. learn how account keys, the bonding curve, and creator fees work.",
            GOLDEN_KEY,
        ),
        None => (
            "/about".to_owned(),
            "about blohard",
            "a public message board built on blockchains. your wallet is your identity, and you decide who may reply. learn how the blohard protocol works.",
            WHALE,
        ),
    };
    Card {
        title: title.into(),
        description: description.into(),
        url: origin.join(&path).expect("static or numeric path"),
        kind: "website",
        picture,
        small: false,
        chain,
        author: None,
        noscript: "read more",
    }
}

/// How long a card waits for its author's name. A link-preview crawler
/// fetches a card once and keeps it for days, so it is worth waiting longer
/// for; a reader opening the page is not kept waiting.
fn name_wait(headers: &HeaderMap) -> Duration {
    Duration::from_secs(if crawler(headers) { 5 } else { 2 })
}

/// A link-preview crawler (X, Facebook, Slack, Discord, Telegram, LinkedIn…), by its User-Agent.
fn crawler(headers: &HeaderMap) -> bool {
    let Some(agent) = headers
        .get(header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let agent = agent.to_ascii_lowercase();
    [
        "bot",
        "facebookexternalhit",
        "embedly",
        "whatsapp",
        "skypeuripreview",
        "vkshare",
        "pinterest",
    ]
    .iter()
    .any(|mark| agent.contains(mark))
}

/// The verified name to show for `address`, if it has one, and whether the
/// lookup answered. A lookup that timed out or failed leaves the card out of
/// the cache, so the next request shows the name once it is known.
async fn display_name(
    gateway: &GatewayState,
    address: Address,
    wait: Duration,
) -> (Option<String>, bool) {
    let Some(names) = &gateway.names else {
        return (None, true);
    };
    match tokio::time::timeout(wait, names.lookup(address)).await {
        Ok(Ok(entry)) => (entry.name, true),
        _ => (None, false),
    }
}

/// The address as the app shortens it: `0x1234…abcd`.
fn short_address(address: Address) -> String {
    let wallet = address.to_string();
    format!("{}…{}", &wallet[..6], &wallet[wallet.len() - 4..])
}

fn author_label(name: Option<&str>, address: Address) -> String {
    match name {
        Some(name) => format!("{name} · {}", short_address(address)),
        None => short_address(address),
    }
}

/// The post's date in UTC, independent of the crawler's locale.
fn post_title(timestamp: u32, who: &str) -> String {
    let date =
        httpdate::fmt_http_date(std::time::UNIX_EPOCH + Duration::from_secs(u64::from(timestamp)));
    // IMF-fixdate is fixed-width ASCII: "Thu, 01 Oct 2026 00:00:00 GMT".
    format!("blohard - {} - {who}", &date[5..16])
}

fn excerpt(bytes: &[u8], message: &Message) -> Option<String> {
    if keccak::hash(&[bytes]).0 != message.content_hash.0 {
        return None;
    }
    let header = envelope::parse(bytes).ok()?;
    if header.media_type != "text/plain" {
        return None;
    }
    let text = std::str::from_utf8(&bytes[header.body_start..]).ok()?;
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = normalized.chars();
    let mut result: String = chars.by_ref().take(240).collect();
    if chars.next().is_some() {
        result.push('…');
    }
    Some(result)
}

/// A copy of the web app's `index.html`, whose metadata block the host rewrites. Keep it in
/// step with the app's page.
#[cfg(test)]
pub(crate) const APP_INDEX: &str = include_str!("../testdata/app-index.html");

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_authors_include_an_address_cue_and_unnamed_ones_a_short_address() {
        let address = "0x1212121212121212121212121212121212121212"
            .parse::<Address>()
            .unwrap();
        assert_eq!(
            author_label(Some("alice.eth"), address),
            "alice.eth · 0x1212…1212"
        );
        assert_eq!(author_label(None, address), "0x1212…1212");
    }

    #[test]
    fn a_post_card_title_names_the_date_and_author() {
        for (timestamp, date) in [
            (0, "01 Jan 1970"),
            (1_709_164_800, "29 Feb 2024"),
            (1_735_689_599, "31 Dec 2024"),
            (1_735_689_600, "01 Jan 2025"),
            (1_790_812_800, "01 Oct 2026"),
            (u32::MAX, "07 Feb 2106"),
        ] {
            for who in ["alice.eth", "0x1212…1212"] {
                assert_eq!(
                    post_title(timestamp, who),
                    format!("blohard - {date} - {who}")
                );
            }
        }
    }

    #[test]
    fn link_preview_crawlers_are_told_apart_from_readers() {
        let headers = |agent: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::USER_AGENT, agent.parse().unwrap());
            h
        };
        for agent in [
            "Twitterbot/1.0",
            "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)",
            "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)",
            "Mozilla/5.0 (compatible; Discordbot/2.0; +https://discordapp.com)",
            "TelegramBot (like TwitterBot)",
        ] {
            assert!(crawler(&headers(agent)), "{agent}");
        }
        assert!(!crawler(&headers(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 Safari/605.1.15"
        )));
        assert!(!crawler(&HeaderMap::new()));
    }

    fn origin() -> Url {
        Url::parse("https://blohard.social").unwrap()
    }

    #[test]
    fn missing_metadata_region_preserves_the_app() {
        let template = "<html><head><title>blohard</title></head><body>app</body></html>";
        let card = information_card(&origin(), None);
        assert_eq!(render_card(template, &origin(), &card), template);
    }

    /// What each kind of card says it is, which picture it shows, its X card
    /// and the line for readers without JavaScript.
    #[test]
    fn each_card_kind_says_what_it_is() {
        let template = APP_INDEX;
        let o = origin();
        let post = Card {
            title: "post #1 · blohard".into(),
            description: "this post is not available.".into(),
            url: o.join("/p/84532/1").unwrap(),
            kind: "article",
            picture: WHALE,
            small: true,
            chain: Some(84532),
            author: None,
            noscript: "open the conversation",
        };
        let profile = |tab| profile_card(&o, tab, 84532, Address::ZERO, "alice.eth");
        let cards = [
            (
                "post",
                post,
                "article",
                "630",
                "summary",
                "open the conversation",
            ),
            (
                "profile",
                profile(ProfileTab::Posts),
                "profile",
                "630",
                "summary_large_image",
                "open this profile",
            ),
            (
                "replies",
                profile(ProfileTab::Replies),
                "profile",
                "630",
                "summary_large_image",
                "open this profile",
            ),
            (
                "keys",
                profile(ProfileTab::Keys),
                "profile",
                "654",
                "summary_large_image",
                "open this profile",
            ),
            (
                "keyring",
                profile(ProfileTab::Keyring),
                "profile",
                "630",
                "summary_large_image",
                "open this profile",
            ),
            (
                "holders",
                profile(ProfileTab::Holders),
                "profile",
                "630",
                "summary_large_image",
                "open this profile",
            ),
            (
                "about",
                information_card(&o, None),
                "website",
                "630",
                "summary_large_image",
                "read more",
            ),
            (
                "keys about",
                information_card(&o, Some(84532)),
                "website",
                "654",
                "summary_large_image",
                "read more",
            ),
        ];
        for (name, card, kind, height, twitter, noscript) in cards {
            let html = render_card(template, &o, &card);
            let has = |tag: String| assert!(html.contains(&tag), "{name}: {tag}\n{html}");
            has(format!("property=\"og:type\" content=\"{kind}\""));
            has(format!("property=\"og:image:height\" content=\"{height}\""));
            has(format!("name=\"twitter:card\" content=\"{twitter}\""));
            has(format!("<p>enable JavaScript to {noscript}.</p>"));
            let alt = if height == "654" {
                GOLDEN_KEY.alt
            } else {
                WHALE.alt
            };
            has(format!("property=\"og:image:alt\" content=\"{alt}\""));
            let thumbnail = if twitter == "summary" {
                THUMBNAIL
            } else {
                card.picture.path
            };
            has(format!(
                "name=\"twitter:image\" content=\"https://blohard.social{thumbnail}\""
            ));
            assert_eq!(html.contains("blohard:chain"), name != "about", "{name}");
            assert_eq!(html.matches("property=\"og:title\"").count(), 1, "{name}");
        }
    }

    #[tokio::test]
    async fn saturation_serves_the_app_and_does_not_block_about_pages() {
        let dir =
            std::env::temp_dir().join(format!("blohard-preview-fallback-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let html = APP_INDEX;
        std::fs::write(dir.join("index.html"), html).unwrap();
        let pages = Pages::new(dir.clone());
        let _all = pages.cache.exhaust();
        let response = cached(
            &pages,
            "post".into(),
            "192.0.2.1".parse().unwrap(),
            async { panic!("no capacity for enrichment") },
            html,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1_000_000)
                .await
                .unwrap()
                .as_ref(),
            html.as_bytes()
        );
        let about = information_page(&pages, None).await;
        assert_eq!(about.status(), StatusCode::OK);
        let body = axum::body::to_bytes(about.into_body(), 1_000_000)
            .await
            .unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("about blohard")
        );
        assert_ne!(
            hex_template(html),
            hex_template(&format!("{html}<!-- new assets -->"))
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn metadata_replacement_preserves_unrelated_tags_on_one_line() {
        let compact = APP_INDEX.lines().map(str::trim).collect::<String>();
        let o = origin();
        let html = render_card(
            &compact,
            &o,
            &profile_card(&o, ProfileTab::Posts, 84532, Address::ZERO, "alice.eth"),
        );
        assert!(html.contains("<meta charset=\"UTF-8\""));
        assert!(html.contains("rel=\"icon\""));
        assert!(html.contains("/src/main.ts"));
        assert_eq!(html.matches("<title>").count(), 1);
        assert_eq!(html.matches("property=\"og:title\"").count(), 1);
    }

    #[test]
    fn profile_names_are_escaped_and_metadata_is_unique() {
        let template = APP_INDEX;
        let o = origin();
        let html = render_card(
            template,
            &o,
            &profile_card(
                &o,
                ProfileTab::Posts,
                84532,
                Address::ZERO,
                "<script>\" & whale",
            ),
        );
        assert!(html.contains("&lt;script&gt;&quot; &amp; whale"));
        assert!(!html.contains("<script>\""));
        assert_eq!(html.matches("property=\"og:title\"").count(), 1);
        assert!(html.contains("property=\"og:type\" content=\"profile\""));
        assert!(html.contains("name=\"twitter:card\" content=\"summary_large_image\""));
        assert!(html.contains("/src/main.ts"));
        assert!(!html.contains("blohard:timestamp"));
    }

    #[test]
    fn excerpts_require_the_exact_envelope_hash() {
        use crate::chain::{Hex, Position};
        let bytes = "version/1 text/plain\nhello 🌊".as_bytes();
        let message = Message {
            id: 1,
            author: Hex([1; 20]),
            parent: 0,
            parent_author: Hex([0; 20]),
            timestamp: 1,
            content_hash: Hex(keccak::hash(&[bytes]).0),
            transaction: Hex([0; 32]),
            position: Position { block: 1, log: 0 },
            gate: Hex([0; 20]),
        };
        assert_eq!(excerpt(bytes, &message).as_deref(), Some("hello 🌊"));
        for text in ["wait…", "…"] {
            let bytes = format!("version/1 text/plain\n{text}");
            let mut current = message.clone();
            current.content_hash = Hex(keccak::hash(&[bytes.as_bytes()]).0);
            assert_eq!(excerpt(bytes.as_bytes(), &current).as_deref(), Some(text));
        }
        assert_eq!(excerpt(b"version/1 text/plain\nforged", &message), None);
    }
}
