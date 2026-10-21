//! Static pair metadata from DEX Screener's public API. Prices are omitted
//! because successful link previews can remain cached for a day.
use super::{Metadata, outbound, text};
use reqwest::Url;

pub(super) struct Pair {
    chain: String,
    id: String,
}

impl Pair {
    pub(super) fn from_url(url: &Url) -> Option<Self> {
        if !matches!(
            url.host_str(),
            Some("dexscreener.com" | "www.dexscreener.com")
        ) || !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
        {
            return None;
        }
        let mut segments = url.path().strip_prefix('/')?.split('/');
        let chain = segments.next()?;
        let id = segments.next()?;
        if chain.is_empty()
            || chain.len() > 32
            || !chain
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
            || id.is_empty()
            || id.len() > 128
            || !id.bytes().all(|c| c.is_ascii_alphanumeric())
            || segments.next().is_some_and(|s| !s.is_empty())
            || segments.next().is_some()
        {
            return None;
        }
        Some(Self {
            chain: chain.into(),
            id: id.into(),
        })
    }

    pub(super) fn endpoint(&self) -> Url {
        Url::parse(&format!(
            "https://api.dexscreener.com/latest/dex/pairs/{}/{}",
            self.chain, self.id
        ))
        .unwrap()
    }

    pub(super) fn parse(&self, bytes: &[u8]) -> Result<Option<Metadata>, outbound::Failure> {
        let answer: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| outbound::Failure::NoPreview)?;
        let Some(pair) = answer["pairs"].as_array().and_then(|pairs| {
            pairs.iter().find(|pair| {
                pair["chainId"].as_str() == Some(&self.chain)
                    && pair["pairAddress"].as_str().is_some_and(|id| {
                        id == self.id
                            || (self.id.starts_with("0x") && id.eq_ignore_ascii_case(&self.id))
                    })
            })
        }) else {
            return Ok(None);
        };
        let name = text(pair["baseToken"]["name"].as_str().unwrap_or(""), 256);
        let symbol = text(pair["baseToken"]["symbol"].as_str().unwrap_or(""), 256);
        let title = match (name.is_empty(), symbol.is_empty()) {
            (true, true) => return Ok(None),
            (true, false) => symbol.clone(),
            (false, true) => name,
            (false, false) => format!("{name} ({symbol})"),
        };
        let quote = text(pair["quoteToken"]["symbol"].as_str().unwrap_or(""), 256);
        let dex = text(pair["dexId"].as_str().unwrap_or(""), 256);
        let mut description = self.chain.clone();
        if !symbol.is_empty() && !quote.is_empty() {
            description = format!("{symbol} / {quote} · {description}");
        }
        if !dex.is_empty() {
            description.push_str(&format!(" · {dex}"));
        }
        // Prefer static artwork over the API's price-bearing openGraph image.
        // The common image check still excludes operator addresses and checks DNS.
        let image = ["header", "imageUrl"].into_iter().find_map(|field| {
            let url = outbound::url(pair["info"][field].as_str()?).ok()?;
            (url.scheme() == "https").then(|| url.to_string())
        });
        Ok(Some(Metadata {
            title: text(&title, 256),
            description: text(&description, 512),
            image,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const LINK: &str = "https://dexscreener.com/base/0x87c274948a3bbb7848217944b21f6243c0de87a435ff7b1be22252ac8c35009b";

    fn pair() -> Pair {
        Pair::from_url(&Url::parse(LINK).unwrap()).unwrap()
    }

    fn answer() -> serde_json::Value {
        json!({"pairs": [{
            "chainId": "base", "pairAddress": pair().id,
            "baseToken": {"name": "AiBubble", "symbol": "AIPOP"},
            "quoteToken": {"symbol": "WETH"}, "dexId": "uniswap",
            "priceUsd": "0.123", "marketCap": 123,
            "info": {"header": "https://cdn.dexscreener.com/banner.png",
                     "imageUrl": "https://cdn.dexscreener.com/icon.png"}
        }]})
    }

    #[test]
    fn pair_links_use_a_fixed_api_origin_and_discard_tracking() {
        for link in [
            LINK.to_owned(),
            format!("{LINK}/?utm_source=test#chart"),
            LINK.replace("https://dexscreener.com", "http://www.dexscreener.com"),
        ] {
            let p = Pair::from_url(&Url::parse(&link).unwrap()).unwrap();
            assert_eq!(
                p.endpoint().as_str(),
                LINK.replace(
                    "https://dexscreener.com/",
                    "https://api.dexscreener.com/latest/dex/pairs/"
                )
            );
        }
        assert!(
            Pair::from_url(&Url::parse("https://dexscreener.com/solana/AbCd1234").unwrap())
                .is_some()
        );
        for link in [
            "https://dexscreener.com/",
            "https://dexscreener.com/base",
            "https://dexscreener.com/base/one/two",
            "https://dexscreener.com/base/a,b",
            "https://dexscreener.com/base/a%2fb",
            "https://dexscreener.com/base/a%3fb",
            "https://dexscreener.com/base//",
            "https://dexscreener.com/base/..",
            "https://dexscreener.com/base/abc%00",
            "https://dexscreener.com/base/abc%20def",
            "https://dexscreener.com.attacker.example/base/abc",
            "https://attacker.example/base/abc",
            "https://api.dexscreener.com/base/abc",
            "https://user@dexscreener.com/base/abc",
            "https://dexscreener.com:444/base/abc",
            "ftp://dexscreener.com/base/abc",
        ] {
            assert!(
                Pair::from_url(&Url::parse(link).unwrap()).is_none(),
                "{link}"
            );
        }
        assert!(
            Pair::from_url(
                &Url::parse(&format!("https://dexscreener.com/base/{}", "a".repeat(129))).unwrap()
            )
            .is_none()
        );
    }

    #[test]
    fn cards_use_matching_pair_metadata_and_static_artwork() {
        let mut value = answer();
        value["pairs"].as_array_mut().unwrap().insert(
            0,
            json!({
                "chainId":"base", "pairAddress":"0xother", "baseToken":{"name":"wrong"}
            }),
        );
        let card = pair()
            .parse(&serde_json::to_vec(&value).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(card.title, "AiBubble (AIPOP)");
        assert_eq!(card.description, "AIPOP / WETH · base · uniswap");
        assert_eq!(
            card.image.as_deref(),
            Some("https://cdn.dexscreener.com/banner.png")
        );
        assert!(!card.description.contains("0.123"));
        value["pairs"][1]["info"]["header"] = json!("http://127.0.0.1/private");
        let card = pair()
            .parse(&serde_json::to_vec(&value).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            card.image.as_deref(),
            Some("https://cdn.dexscreener.com/icon.png")
        );
    }

    #[test]
    fn missing_wrong_or_malformed_pairs_cannot_supply_a_card() {
        for value in [
            json!({}),
            json!({"pairs":null}),
            json!({"pairs":[]}),
            json!({"pairs":[{"chainId":"solana", "pairAddress":pair().id, "baseToken":{"name":"wrong"}}]}),
            json!({"pairs":[{"chainId":"base", "pairAddress":"other", "baseToken":{"name":"wrong"}}]}),
        ] {
            assert_eq!(
                pair().parse(&serde_json::to_vec(&value).unwrap()).unwrap(),
                None
            );
        }
        assert!(pair().parse(b"<html>challenge</html>").is_err());
        let sol = Pair::from_url(&Url::parse("https://dexscreener.com/solana/AbCd1234").unwrap())
            .unwrap();
        assert_eq!(sol.parse(br#"{"pairs":[{"chainId":"solana","pairAddress":"abcd1234","baseToken":{"name":"wrong"}}]}"#).unwrap(), None);
        let mut value = answer();
        value["pairs"][0]["pairAddress"] = json!(pair().id.to_uppercase().replacen("0X", "0x", 1));
        assert!(
            pair()
                .parse(&serde_json::to_vec(&value).unwrap())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn fields_are_bounded_and_images_must_be_public_https_urls() {
        let mut value = answer();
        value["pairs"][0]["baseToken"]["name"] =
            json!(format!("  A\u{202e}  {}", "雪".repeat(400)));
        value["pairs"][0]["baseToken"]["symbol"] = json!("x".repeat(600));
        value["pairs"][0]["quoteToken"]["symbol"] = json!("q".repeat(600));
        value["pairs"][0]["dexId"] = json!("d".repeat(600));
        for bad in [
            "http://example.org/i",
            "https://127.1/i",
            "https://user:pass@example.org/i",
            "https://example.org:444/i",
            "data:image/png,x",
        ] {
            value["pairs"][0]["info"] = json!({"header":bad,"imageUrl":bad});
            let card = pair()
                .parse(&serde_json::to_vec(&value).unwrap())
                .unwrap()
                .unwrap();
            assert!(card.title.starts_with("A "));
            assert!(card.title.len() <= 256 && card.description.len() <= 512);
            assert!(!card.title.contains('\u{202e}'));
            assert_eq!(card.image, None, "{bad}");
        }
    }
}
