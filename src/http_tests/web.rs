//! HTTP routing, role isolation and web asset policies.

use super::*;
use crate::config::DEFAULT_BODY_BYTES;

/// Domain reads answer any origin, preflight included, so a client on
/// another origin can use the indexed API. Error responses carry CORS too.
#[test]
fn domain_routes_answer_any_origin() {
    with_server(
        "http-cors",
        Some(Node::new(31337, CONTRACT, record("text/plain", b"# hi\n"))),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        |Server { base, client, .. }| async move {
            let origin = "https://another-client.invalid";
            for path in ["/api/chains/31337/state", "/api/chains/31337/feed"] {
                let preflight = client
                    .request(reqwest::Method::OPTIONS, format!("{base}{path}"))
                    .header("origin", origin)
                    .header("access-control-request-method", "POST")
                    .header("access-control-request-headers", "content-type")
                    .send()
                    .await
                    .unwrap();
                assert!(
                    preflight.status().is_success(),
                    "{path}: {}",
                    preflight.status()
                );
                assert_eq!(
                    preflight.headers()["access-control-allow-origin"],
                    "*",
                    "{path}"
                );
                let methods = preflight.headers()["access-control-allow-methods"]
                    .to_str()
                    .unwrap();
                assert!(methods.contains("POST"), "{path}: {methods}");
                let resp = client
                    .post(format!("{base}{path}"))
                    .header("origin", origin)
                    .json(&json!({}))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 503, "the test has not backfilled yet");
                assert_eq!(resp.headers()["access-control-allow-origin"], "*", "{path}");
                let j: Value = resp.json().await.unwrap();
                assert_eq!(j["error"], "index_unavailable", "{path}: {j}");
            }
        },
    );
}

/// A host plays the roles its config enables. The routes of a role it lacks
/// answer 404 naming the gap, and the status reports that role as null, so a
/// client knows what a host is before asking it for anything.
#[test]
fn roles_can_run_alone() {
    with_server(
        "http-gateway-only",
        None,
        |_| Setup {
            store: false,
            ..Setup::default()
        },
        |Server { base, client, .. }| async move {
            let resp = client
                .get(format!("{base}/api/status"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
            let status: Value = resp.json().await.unwrap();
            assert!(status["store"].is_null(), "{status}");
            assert!(status["gateway"].is_object(), "{status}");
            let resp = publish(&client, &base, "", record("text/plain", b"# x\n")).await;
            assert_eq!(resp.status(), 404);
            let j: Value = resp.json().await.unwrap();
            assert_eq!(j["error"], "this host stores no content");
            let resp = client
                .get(format!("{base}/api/content/0x{}", "00".repeat(32)))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 404);
            let j: Value = resp.json().await.unwrap();
            assert_eq!(j["error"], "this host stores no content");
        },
    );
    with_server(
        "http-store-only",
        None,
        |_| Setup {
            gateway: false,
            ..Setup::default()
        },
        |Server { base, client, .. }| async move {
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert!(status["gateway"].is_null(), "{status}");
            assert_eq!(status["store"]["max_content_size"], DEFAULT_BODY_BYTES);
            // Storing works (dev mode: no proof needed)...
            let resp = publish(&client, &base, "", record("text/plain", b"# store only\n")).await;
            assert_eq!(resp.status(), 200);
            // ...and the gateway's routes are not there.
            let call =
                json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}).to_string();
            for path in [
                "/api/chains/31337/rpc",
                "/api/chains/31337/wallet",
                "/api/names/ethereum/rpc",
            ] {
                let resp = client
                    .post(format!("{base}{path}"))
                    .body(call.clone())
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 404, "{path}");
            }
        },
    );
}

#[test]
fn http_serves_web_dir_with_spa_fallback() {
    let web = std::env::temp_dir().join(format!("blohard-web-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&web);
    std::fs::create_dir_all(web.join("assets")).unwrap();
    std::fs::write(web.join("index.html"), "<html>blohard app</html>").unwrap();
    std::fs::write(web.join("assets/app.js"), "console.log(1)").unwrap();
    let dir = web.clone();
    with_server(
        "http-web-dir",
        None,
        move |_| Setup {
            web_dir: Some(dir),
            ..Setup::default()
        },
        |Server { base, client, .. }| async move {
            let cache = |resp: &reqwest::Response| {
                resp.headers()
                    .get("cache-control")
                    .map(|v| v.to_str().unwrap().to_string())
            };
            let resp = client.get(&base).send().await.unwrap();
            assert_eq!(resp.status(), 200);
            // An entry file: kept, but checked with the server before reuse.
            assert_eq!(cache(&resp).as_deref(), Some("no-cache"));
            assert_eq!(resp.text().await.unwrap(), "<html>blohard app</html>");

            let resp = client
                .get(format!("{base}/assets/app.js"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            // A hashed asset never changes under its name.
            assert_eq!(
                cache(&resp).as_deref(),
                Some("public, max-age=31536000, immutable")
            );
            assert_eq!(resp.text().await.unwrap(), "console.log(1)");

            // A client-side route: index.html, so the app can route it.
            let resp = client
                .get(format!("{base}/m/31337/1"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            assert_eq!(cache(&resp).as_deref(), Some("no-cache"));
            assert_eq!(resp.text().await.unwrap(), "<html>blohard app</html>");

            // A missing asset is a 404 nobody stores, never index.html kept for a year.
            let resp = client
                .get(format!("{base}/assets/missing.js"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 404);
            assert_eq!(cache(&resp).as_deref(), Some("no-store"));

            // Revalidation: a 304 carries the policy too.
            let first = client
                .get(format!("{base}/assets/app.js"))
                .send()
                .await
                .unwrap();
            let modified = first.headers().get("last-modified").unwrap().clone();
            let resp = client
                .get(format!("{base}/assets/app.js"))
                .header("if-modified-since", modified)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 304);
            assert_eq!(
                cache(&resp).as_deref(),
                Some("public, max-age=31536000, immutable")
            );

            // API answers are not stored unless a handler says so.
            let resp = client
                .get(format!("{base}/api/status"))
                .send()
                .await
                .unwrap();
            assert_eq!(cache(&resp).as_deref(), Some("no-store"));
            let resp = client
                .get(format!("{base}/api/content/0x{}", "00".repeat(32)))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 404);
            assert_eq!(cache(&resp).as_deref(), Some("no-store"));

            // The API still wins over the fallback.
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["verifier"], "allow-all (dev)");
            assert_eq!(
                client
                    .get(format!("{base}/api/nope"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );
        },
    );
    let _ = std::fs::remove_dir_all(&web);
}

#[test]
fn share_pages_verify_content_and_handle_missing_deleted_and_pinned_posts() {
    let web = std::env::temp_dir().join(format!("blohard-share-{}", std::process::id()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), crate::share::APP_INDEX).unwrap();
    let dir = web.clone();
    let bytes = record(
        "text/plain",
        b"Hello <script>alert(1)</script> & \"friends\"!",
    );
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, bytes.clone())
    };
    with_server(
        "http-share",
        Some(node),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            web_dir: Some(dir),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            let url = format!("{base}/p/31337/1");
            let warming = client.get(&url).send().await.unwrap();
            assert_eq!(
                warming.status(),
                200,
                "index warmup still serves the app shell"
            );
            assert!(warming.text().await.unwrap().contains("/src/main.ts"));
            crate::index::follow::once(&state).await;
            assert_eq!(
                client.get(&url).send().await.unwrap().status(),
                200,
                "missing preview content still serves the app shell"
            );
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(bytes.clone())
                .await
                .unwrap();
            let response = client
                .get(&url)
                .header("host", "attacker.invalid")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["cache-control"], "no-store");
            let html = response.text().await.unwrap();
            assert!(
                html.contains(
                    "Hello &lt;script&gt;alert(1)&lt;/script&gt; &amp; &quot;friends&quot;!"
                ),
                "{html}"
            );
            assert!(!html.contains("<script>alert"));
            assert!(!html.contains("attacker.invalid"));
            assert!(html.contains("https://blohard.social/p/31337/1"));
            assert!(html.contains("/src/main.ts"));
            assert_eq!(html.matches("property=\"og:title\"").count(), 1);
            assert_eq!(client.head(&url).send().await.unwrap().status(), 200);
            for query in [
                "fbclid=tracking",
                "utm_source=newsletter&li_fat_id=tracking",
                "unknown=a&unknown=b",
            ] {
                let response = client.get(format!("{url}?{query}")).send().await.unwrap();
                assert_eq!(response.status(), 200);
                assert_eq!(response.text().await.unwrap(), html);
            }
            assert_eq!(
                client
                    .get(format!("{url}?commit=invalid&fbclid=tracking"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );

            for (path, status) in [("/p/1/1", 404), ("/p/31337/999", 404), ("/p/no/1", 400)] {
                assert_eq!(
                    client
                        .get(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    status
                );
            }
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            let message = index.read_ready(|m| m.messages[&1].clone()).unwrap();
            assert_eq!(
                client
                    .get(format!("{url}?commit={}", message.commitment()))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            assert_eq!(
                client
                    .get(format!("{url}?commit=0x{}", "11".repeat(32)))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                409
            );
            // A tombstone must never expose the old excerpt even while those bytes remain stored.
            index
                .view
                .write()
                .unwrap()
                .model
                .messages
                .get_mut(&1)
                .unwrap()
                .gate = crate::index::types::TOMBSTONE;
            let deleted = client.get(&url).send().await.unwrap();
            assert_eq!(deleted.status(), 410);
            assert!(!deleted.text().await.unwrap().contains("friends"));
        },
    );
    std::fs::remove_dir_all(web).unwrap();
}

/// A card whose name lookup ran out of time shows the address but is not
/// cached: once the name is known, the next card shows it.
#[test]
fn a_card_built_before_its_name_arrived_is_not_cached() {
    use alloy_sol_types::SolCall as _;
    let web = std::env::temp_dir().join(format!("blohard-slow-name-{}", std::process::id()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), crate::share::APP_INDEX).unwrap();
    let dir = web.clone();
    with_server(
        "http-slow-name",
        Some(Node::new(31337, CONTRACT, record("text/plain", b"hello"))),
        move |url| {
            // An ENS upstream slower than the card waits for its first answer.
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let primary = hex0x(&crate::names::reverseWithGatewaysCall::abi_encode_returns(
                &crate::names::reverseWithGatewaysReturn {
                    primary: "alice.eth".into(),
                    resolver: Default::default(),
                    reverseResolver: Default::default(),
                },
            ));
            let names = axum::Router::new().route(
                "/",
                axum::routing::post(move || {
                    let (calls, primary) = (calls.clone(), primary.clone());
                    async move {
                        if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                            // No avatar record.
                            return axum::Json(
                                json!({"error": {"code": 3, "message": "execution reverted"}}),
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(2500)).await;
                        axum::Json(json!({"result": primary}))
                    }
                }),
            );
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let names_url = format!("http://{}", listener.local_addr().unwrap());
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(axum::serve(listener, names).into_future());
            Setup {
                verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
                names: Some(NamesConfig {
                    ethereum_upstream: Some(names_url),
                    base_upstream: None,
                }),
                web_dir: Some(dir),
                ..Setup::default()
            }
        },
        |Server {
             base,
             client,
             state,
             ..
         }| async move {
            let address = format!("0x{}", "ab".repeat(20));
            let url = format!("{base}/u/31337/{address}");
            let first = client.get(&url).send().await.unwrap();
            assert_eq!(first.status(), 200);
            assert!(
                first
                    .text()
                    .await
                    .unwrap()
                    .contains(&format!("posts and replies by {address}"))
            );
            let names = state.gateway.as_ref().unwrap().names.clone().unwrap();
            wait_until(Duration::from_secs(5), "the name arrives", || {
                names.cached(address.parse().unwrap()).is_some()
            })
            .await;
            let second = client.get(&url).send().await.unwrap().text().await.unwrap();
            assert!(
                second.contains("posts and replies by alice.eth"),
                "{second}"
            );
        },
    );
    std::fs::remove_dir_all(web).unwrap();
}

#[test]
fn a_crawlers_post_card_has_a_dated_title_and_waits_longer_for_the_name() {
    use alloy_sol_types::SolCall as _;
    let web = std::env::temp_dir().join(format!("blohard-crawler-card-{}", std::process::id()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), crate::share::APP_INDEX).unwrap();
    let dir = web.clone();
    let bytes = record("text/plain", b"the tide is high and so are we");
    let node = Node {
        head: 100.into(),
        messages: 1.into(),
        ..Node::new(31337, CONTRACT, bytes.clone())
    };
    with_server(
        "http-crawler-card",
        Some(node),
        move |url| {
            // An ENS upstream slower than a reader's wait, but not a crawler's.
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let primary = hex0x(&crate::names::reverseWithGatewaysCall::abi_encode_returns(
                &crate::names::reverseWithGatewaysReturn {
                    primary: "alice.eth".into(),
                    resolver: Default::default(),
                    reverseResolver: Default::default(),
                },
            ));
            let names = axum::Router::new().route(
                "/",
                axum::routing::post(move || {
                    let (calls, primary) = (calls.clone(), primary.clone());
                    async move {
                        if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                            return axum::Json(
                                json!({"error": {"code": 3, "message": "execution reverted"}}),
                            );
                        }
                        tokio::time::sleep(Duration::from_millis(2500)).await;
                        axum::Json(json!({"result": primary}))
                    }
                }),
            );
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let names_url = format!("http://{}", listener.local_addr().unwrap());
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            tokio::spawn(axum::serve(listener, names).into_future());
            Setup {
                verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
                names: Some(NamesConfig {
                    ethereum_upstream: Some(names_url),
                    base_upstream: None,
                }),
                web_dir: Some(dir),
                ..Setup::default()
            }
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            state
                .store
                .as_ref()
                .unwrap()
                .bodies
                .put(bytes.clone())
                .await
                .unwrap();
            let html = client
                .get(format!("{base}/p/31337/1"))
                .header("user-agent", "Twitterbot/1.0")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            assert!(
                html.contains("<title>blohard - 14 Nov 2023 - alice.eth</title>"),
                "{html}"
            );
            assert!(
                html.contains(
                    "name=\"twitter:title\" content=\"blohard - 14 Nov 2023 - alice.eth\""
                )
            );
            // X's small card: a square thumbnail beside the title and the post's words.
            assert!(
                html.contains("name=\"twitter:card\" content=\"summary\""),
                "{html}"
            );
            assert!(html.contains(
                "name=\"twitter:image\" content=\"https://blohard.social/whale-avatar.jpg\""
            ));
            assert!(
                html.contains("property=\"og:image\" content=\"https://blohard.social/card.jpg\"")
            );
            assert!(html.contains("property=\"og:image:width\" content=\"1200\""));
            assert!(html.contains("property=\"og:image:height\" content=\"630\""));
            for tag in [
                "property=\"og:description\"",
                "name=\"twitter:description\"",
            ] {
                assert!(
                    html.contains(&format!("{tag} content=\"the tide is high and so are we\""))
                );
            }
        },
    );
    std::fs::remove_dir_all(web).unwrap();
}

#[test]
fn profile_share_pages_work_without_posts_and_reject_invalid_targets() {
    let web = std::env::temp_dir().join(format!("blohard-profile-share-{}", std::process::id()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), crate::share::APP_INDEX).unwrap();
    let dir = web.clone();
    with_server(
        "http-profile-share",
        Some(Node::new(31337, CONTRACT, record("text/plain", b"hello"))),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            web_dir: Some(dir),
            ..Setup::default()
        },
        |Server { base, client, .. }| async move {
            for (path, title, image) in [
                ("/about", "about blohard", "/card.jpg"),
                (
                    "/keys/31337/about",
                    "about account keys · blohard",
                    "/account-keys-card.jpg",
                ),
            ] {
                let response = client
                    .get(format!("{base}{path}"))
                    .header("host", "attacker.invalid")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 200);
                let html = response.text().await.unwrap();
                assert!(html.contains(&format!("<title>{title}</title>")));
                assert!(html.contains(&format!("https://blohard.social{path}")));
                assert!(html.contains(&format!("https://blohard.social{image}")));
                assert!(!html.contains("attacker.invalid"));
                assert_eq!(html.matches("property=\"og:title\"").count(), 1);
                assert_eq!(
                    client
                        .head(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    200
                );
            }
            assert_eq!(
                client
                    .get(format!("{base}/keys/1/about"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );
            let address = format!("0x{}", "ab".repeat(20));
            let url = format!("{base}/u/31337/{address}");
            let response = client
                .get(&url)
                .header("host", "attacker.invalid")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["cache-control"], "no-store");
            let html = response.text().await.unwrap();
            assert!(html.contains(&format!("https://blohard.social/u/31337/{address}")));
            assert!(html.contains(&format!("posts and replies by {address}")));
            assert!(html.contains("https://blohard.social/card.jpg"));
            assert!(!html.contains("attacker.invalid"));
            assert_eq!(client.head(&url).send().await.unwrap().status(), 200);
            let replies_url = format!("{url}/replies");
            let response = client.get(&replies_url).send().await.unwrap();
            assert_eq!(response.status(), 200);
            let html = response.text().await.unwrap();
            assert!(html.contains(&format!("<title>{address} replies · blohard</title>")));
            assert!(html.contains(&format!(
                "content=\"https://blohard.social/u/31337/{address}/replies\""
            )));
            assert!(html.contains(&format!("content=\"replies by {address}")));
            assert!(html.contains("/src/main.ts"));
            assert_eq!(
                client.head(&replies_url).send().await.unwrap().status(),
                200
            );
            assert!(
                client
                    .get(&url)
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap()
                    .contains(&format!("<title>{address} · blohard</title>")),
                "tab metadata is cached separately"
            );
            let market = client.get(format!("{url}/keys")).send().await.unwrap();
            assert_eq!(market.status(), 200);
            let html = market.text().await.unwrap();
            assert!(html.contains(&format!("https://blohard.social/u/31337/{address}/keys")));
            assert!(html.contains("account keys · blohard"));
            assert!(html.contains("/account-keys-card.jpg"));

            for (path, status) in [
                (format!("/u/1/{address}"), 404),
                (format!("/u/1/{address}/replies"), 404),
                (format!("/u/1/{address}/keyring"), 404),
                ("/u/31337/nope/keyring/holders".into(), 400),
                ("/u/31337/nope/replies".into(), 400),
                ("/u/31337/nope".into(), 400),
                (format!("/u/no/{address}"), 400),
            ] {
                assert_eq!(
                    client
                        .get(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    status
                );
            }
        },
    );
    std::fs::remove_dir_all(web).unwrap();
}

#[test]
fn keyring_share_tabs_have_distinct_metadata_and_canonical_paths() {
    let web = std::env::temp_dir().join(format!("blohard-keyring-share-{}", std::process::id()));
    std::fs::create_dir_all(&web).unwrap();
    std::fs::write(web.join("index.html"), crate::share::APP_INDEX).unwrap();
    let dir = web.clone();
    with_server(
        "http-keyring-share",
        Some(Node::new(31337, CONTRACT, record("text/plain", b"hello"))),
        move |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            web_dir: Some(dir),
            ..Setup::default()
        },
        |Server { base, client, .. }| async move {
            let address = format!("0x{}", "ab".repeat(20));
            for (suffix, title) in [("keyring", "keyring"), ("keyring/holders", "key holders")] {
                let path = format!("/u/31337/{address}/{suffix}");
                let response = client.get(format!("{base}{path}")).send().await.unwrap();
                assert_eq!(response.status(), 200);
                let html = response.text().await.unwrap();
                assert!(
                    html.contains(&format!("<title>{address} {title} · blohard</title>")),
                    "{suffix}: {:?}",
                    html.split("<title>")
                        .nth(1)
                        .and_then(|s| s.split("</title>").next())
                );
                assert!(html.contains(&format!("content=\"https://blohard.social{path}\"")));
                assert!(html.contains("/src/main.ts"));
                assert_eq!(
                    client
                        .head(format!("{base}{path}"))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    200
                );
            }
        },
    );
    std::fs::remove_dir_all(web).unwrap();
}
