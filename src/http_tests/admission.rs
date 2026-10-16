//! Upload anchoring, holding and content policies.

use super::*;
use crate::config::DEFAULT_BODY_BYTES;

#[test]
fn contract_wallet_provider_failures_are_retryable_without_holding_bytes() {
    let account = [0xaau8; 20];
    let body = record("text/plain", b"contract wallet provider failures");
    let words = post_words(account, keccak::hash(&[&body]).0, 1);
    with_server(
        "contract-wallet-errors",
        Some(Node {
            contracts: vec![account].into(),
            txs: vec![MockTx {
                hash: [7; 32],
                from: [9; 20],
                to: CONTRACT.into(),
                input: post_by_sig_input(&words, &[0x99; 65]),
                block: None,
                message_id: None,
                reverted: false,
            }]
            .into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            let node = node.unwrap();
            let outage = json!({"error": {"code": -32005, "message": "rate limited"}});
            let cases = [
                ("eth_getCode", outage.clone(), 503),
                ("eth_getCode", json!({"result": null}), 502),
                ("eth_getCode", json!({"result": "0xzz"}), 502),
                ("eth_getCode", json!({"result": "0x"}), 403),
                ("eth_call", outage, 503),
                ("eth_call", json!({}), 502),
                ("eth_call", json!({"result": "0xzz"}), 502),
                (
                    "eth_call",
                    json!({"error": {"code": 3, "message": "execution reverted"}}),
                    403,
                ),
                ("eth_call", json!({"result": "0x"}), 403),
                ("eth_call", json!({"result": hex0x(&[0; 32])}), 403),
            ];
            let query = format!("chain_id=31337&tx_hash={}", hex0x(&[7; 32]));
            for (method, answer, expected) in cases {
                node.node
                    .response_overrides
                    .lock()
                    .unwrap()
                    .insert(method.into(), answer.clone());
                let response = publish(&client, &base, &query, body.clone()).await;
                assert_eq!(response.status().as_u16(), expected, "{method}: {answer}");
                assert_eq!(state.store.as_ref().unwrap().holding.stats(), (0, 0));
                if answer.get("error").is_some_and(|e| e["code"] == -32005) {
                    assert_eq!(response.headers()["retry-after"], "2");
                    state
                        .verifier
                        .chain(31337)
                        .unwrap()
                        .0
                        .expire_cooldown(&node.url);
                }
                node.node.response_overrides.lock().unwrap().clear();
            }
            assert_eq!(
                publish(&client, &base, &query, body)
                    .await
                    .status()
                    .as_u16(),
                202
            );
            assert_eq!(state.store.as_ref().unwrap().holding.stats().0, 1);
        },
    );
}

/// A body uploaded on the strength of a pending `postBySig` transaction
/// belongs to the author the calldata names, not to the relayer that sent
/// it, once that author's signature verifies the way the Board will verify
/// it: the record is held in that author's bucket. A signature that does
/// not verify is refused, so nobody can fill another author's slots by
/// naming them. A contract author is asked through ERC-1271.
#[test]
fn http_relayed_pending_upload_is_the_authors() {
    use k256::ecdsa::SigningKey;
    let key = SigningKey::from_slice(&[7u8; 32]).unwrap();
    let point = key.verifying_key().to_encoded_point(false);
    let author: [u8; 20] = keccak::hash(&[&point.as_bytes()[1..]]).0[12..]
        .try_into()
        .unwrap();
    let relayer = [9u8; 20];
    let account = [0xAAu8; 20]; // a contract account that accepts every signature
    let contract = crate::abi::parse_address(CONTRACT).unwrap();

    let body = record("text/plain", b"# relayed, not yet mined\n");
    let words = post_words(author, keccak::hash(&[body.as_slice()]).0, 1);
    let digest = crate::abi::post_by_sig_digest(31337, &contract, &words);
    let (sig, rec) = key.sign_prehash_recoverable(&digest).unwrap();
    let mut good = sig.to_bytes().to_vec();
    good.push(27 + rec.to_byte());

    let forged_body = record("text/plain", b"# forged in the member's name\n");
    let forged_words = post_words(author, keccak::hash(&[forged_body.as_slice()]).0, 2);
    let account_body = record("text/plain", b"# from a contract account\n");
    let account_words = post_words(account, keccak::hash(&[account_body.as_slice()]).0, 3);

    let pending = |hash: u8, input: Vec<u8>| MockTx {
        hash: [hash; 32],
        from: relayer,
        to: CONTRACT.to_string(),
        input,
        block: None,
        message_id: None,
        reverted: false,
    };
    let node = Node {
        head: 100.into(),
        txs: vec![
            pending(0x71, post_by_sig_input(&words, &good)),
            pending(0x72, post_by_sig_input(&forged_words, &[0x99u8; 65])),
            pending(0x73, post_by_sig_input(&account_words, &[0x99u8; 65])),
        ]
        .into(),
        contracts: vec![account].into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "http-relayed-pending",
        Some(node),
        |url| Setup {
            gateway: false,
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            let upload = |hash: u8, body: Vec<u8>| {
                let client = client.clone();
                let base = base.clone();
                async move {
                    let resp = publish(
                        &client,
                        &base,
                        &format!("chain_id=31337&tx_hash=0x{}", hex::encode([hash; 32])),
                        body,
                    )
                    .await;
                    (resp.status().as_u16(), resp.json::<Value>().await.unwrap())
                }
            };
            // The author's own signature: held in that author's bucket.
            let (status, j) = upload(0x71, body).await;
            assert_eq!(
                status, 202,
                "held for the member author, not refused for the relayer: {j}"
            );
            assert_eq!(j["held"], true);
            // A forged post naming the author: refused before any slot is taken.
            let (status, j) = upload(0x72, forged_body).await;
            assert_eq!(status, 403, "{j}");
            assert!(
                j["error"].as_str().unwrap().contains("does not verify"),
                "{j}"
            );
            // A contract account that stands behind the signature: held under it.
            let (status, j) = upload(0x73, account_body).await;
            assert_eq!(status, 202, "{j}");
            let held = state.store.as_ref().unwrap().holding.snapshot();
            let mut signers: Vec<[u8; 20]> = held.iter().map(|h| h.signer).collect();
            signers.sort();
            let mut expected = vec![author, account];
            expected.sort();
            assert_eq!(
                signers, expected,
                "one record per authenticated author, none for the forgery"
            );
        },
    );
}

/// Authenticating a contract account runs code the uploader chose, so the
/// call carries the host's simulation ceiling like every call it forwards.
#[test]
fn contract_signers_are_checked_within_the_gas_ceiling() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let account = [0xccu8; 20];
            let node = mock_node::spawn(Node {
                contracts: vec![account].into(),
                ..Node::new(31337, CONTRACT, record("text/plain", b"# hi\n"))
            })
            .await;
            let verifier = chain_verifier(&node.url, ChainConfig::for_test(31337, "", CONTRACT));
            let words = post_words(account, [0u8; 32], 1);
            let who = verifier
                .signed_author(31337, &post_by_sig_input(&words, &[0x99u8; 65]))
                .await
                .unwrap();
            assert!(matches!(who, SignedAuthor::Author(a) if a == account));
            assert_eq!(
                node.node.last("eth_call")[0]["gas"],
                format!("0x{SIM_GAS_CAP:x}"),
                "bounded like every simulation the host forwards"
            );
        });
}

#[test]
fn indexed_uploads_wait_for_acceptance_and_promotion_reads_no_rpc() {
    let body = record("text/plain", b"# indexed upload\n");
    with_server(
        "indexed-upload",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            let upload = || publish(&client, &base, "chain_id=31337&message_id=1", body.clone());
            assert_eq!(
                upload().await.status(),
                503,
                "warming index is not bypassed"
            );
            crate::index::follow::once(&state).await;
            assert_eq!(
                upload().await.status(),
                202,
                "mined but not accepted, so held"
            );
            let node = node.unwrap();
            let before = node.node.counts();
            assert_eq!(crate::watcher::promote_once(&state).await, 0);
            assert_eq!(node.node.counts(), before);
            node.node.set_head(16);
            crate::index::follow::once(&state).await;
            let before = node.node.counts();
            assert_eq!(crate::watcher::promote_once(&state).await, 1);
            assert_eq!(upload().await.status(), 200);
            assert_eq!(
                node.node.counts(),
                before,
                "accepted uploads and promotion use RAM facts"
            );
            let index = &state.gateway.as_ref().unwrap().indexes[&31337];
            index.stop("test disagreement");
            assert_eq!(
                upload().await.status(),
                503,
                "stopped index is not bypassed either"
            );
            assert_eq!(node.node.counts(), before);
        },
    );
}

/// End-to-end over a real listener in dev mode: publish, dedup, fetch
/// (with and without the 0x prefix), and every client-error path.
#[test]
fn http_publish_and_fetch_roundtrip() {
    with_server(
        "http-allow-all",
        None,
        |_| Setup::default(),
        |Server { base, client, .. }| async move {
            // The UI is served at the root.
            let page = client.get(&base).send().await.unwrap();
            assert_eq!(page.status(), 200);
            assert!(page.text().await.unwrap().contains("blohard CDN"));

            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["verifier"], "allow-all (dev)");
            assert_eq!(status["chains"], json!([]));
            assert_eq!(status["store"]["max_content_size"], DEFAULT_BODY_BYTES);
            assert_eq!(status["store"]["envelope"]["version"], 1);
            assert_eq!(
                status["store"]["attachments"]["max_bytes"],
                TEST_ATTACHMENT_CAP
            );
            assert_eq!(status["store"]["attachments"]["max_per_message"], 4);

            // Headerless bytes are refused before anything else is checked.
            let resp = publish(&client, &base, "", "# no envelope\n").await;
            assert_eq!(resp.status(), 400);
            let j: Value = resp.json().await.unwrap();
            assert!(j["error"].as_str().unwrap().contains("envelope"), "{j}");

            // Publish, with no proof at all (dev mode).
            let body = record("text/plain", b"# hello\n\nfirst post.\n");
            let expected = hx(&body);
            let resp = publish(&client, &base, "", body.clone()).await;
            assert_eq!(resp.status(), 200);
            let j: Value = resp.json().await.unwrap();
            assert_eq!(j["hash"], expected);
            assert_eq!(j["stored"], true);
            assert_eq!(j["held"], false);
            assert_eq!(j["bytes"], body.len());
            assert_eq!(j["kind"], "body");

            // Publishing identical bytes again is a no-op.
            let j: Value = publish(&client, &base, "chain_id=1&message_id=42", body.clone())
                .await
                .json()
                .await
                .unwrap();
            assert_eq!(j["hash"], expected);
            assert_eq!(j["stored"], false);

            // A malformed query string is refused in the same JSON shape as
            // every other bad request, not with axum's plain text.
            let resp = publish(&client, &base, "chain_id=abc", body.clone()).await;
            assert_eq!(resp.status(), 400);
            assert!(
                resp.headers()[header::CONTENT_TYPE]
                    .to_str()
                    .unwrap()
                    .starts_with("application/json")
            );
            let j: Value = resp.json().await.unwrap();
            assert!(j["error"].as_str().unwrap().contains("chain_id"), "{j}");

            // Fetch by hash, with and without the 0x prefix; bytes are exact,
            // the declared type is served, and the response is immutable and
            // sandboxed.
            for path in [
                expected.clone(),
                expected.trim_start_matches("0x").to_string(),
            ] {
                let resp = client
                    .get(format!("{base}/api/content/{path}"))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
                let h = resp.headers();
                assert_eq!(h[header::CONTENT_TYPE], "text/plain; charset=utf-8");
                assert_eq!(
                    h[header::CACHE_CONTROL],
                    "public, max-age=300, must-revalidate"
                );
                assert_eq!(h[header::ETAG], format!("\"{expected}\""));
                assert_eq!(h[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
                assert!(
                    h[header::CONTENT_SECURITY_POLICY]
                        .to_str()
                        .unwrap()
                        .contains("sandbox")
                );
                assert_eq!(resp.bytes().await.unwrap().as_ref(), body.as_slice());
            }

            // Dev mode: a record above the body cap but within the attachment
            // cap lands in the attachments store; above that is refused.
            let mid = record(
                "application/octet-stream",
                &vec![b'm'; DEFAULT_BODY_BYTES + 10],
            );
            let resp = publish(&client, &base, "", mid.clone()).await;
            assert_eq!(resp.status(), 200);
            let j: Value = resp.json().await.unwrap();
            assert_eq!(j["kind"], "attachment");
            let resp = client
                .get(format!(
                    "{base}/api/content/{}",
                    j["hash"].as_str().unwrap()
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(
                resp.headers()[header::CONTENT_TYPE],
                "application/octet-stream"
            );
            assert_eq!(resp.bytes().await.unwrap().len(), mid.len());

            let too_big = record(
                "application/octet-stream",
                &vec![b'a'; TEST_ATTACHMENT_CAP + 1],
            );
            let resp = publish(&client, &base, "", too_big).await;
            assert_eq!(resp.status(), 413);
            let j: Value = resp.json().await.unwrap();
            assert!(j["error"].as_str().unwrap().contains("exceeds"), "{j}");

            // Client-error paths.
            let unknown = hx(b"never published");
            assert_eq!(
                client
                    .get(format!("{base}/api/content/{unknown}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );
            assert_eq!(
                client
                    .get(format!("{base}/api/content/0x1234"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
            assert_eq!(publish(&client, &base, "", "").await.status(), 400);
        },
    );
}

/// Chains mode: the proof is required, routed to the named chain, and
/// each failure class maps to its own status code. Then attachments: a
/// body's `record:` references admit exactly those records, in order,
/// up to the host's cap.
#[test]
fn http_requires_anchoring_in_chains_mode() {
    // Attachments A, B, C referenced by message 1's body; D is not.
    let att = |tag: &str| record("image/png", format!("PNG-ish bytes {tag}").as_bytes());
    let (a, b, c, d) = (att("A"), att("B"), att("C"), att("D"));
    let body_text = format!(
        "# anchored on anvil\n\n![a](record:{})\n[b](record:{})\n[c](record:{})\n",
        hx(&a),
        hx(&b),
        hx(&c)
    );
    let anchored = record("text/plain", body_text.as_bytes());
    let anchored_hash = hx(&anchored);
    // Message 2 commits to a body that is never uploaded.
    let other_body = record("text/plain", b"# never uploaded\n");

    let node = Node {
        head: 100.into(),
        extra: vec![(2, other_body)],
        ..Node::new(31337, CONTRACT, anchored.clone())
    };
    with_server(
        "http-chains",
        Some(node),
        |url| Setup {
            gateway: false,
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    name: Some("anvil".into()),
                    public_rpc_url: Some("http://rpc.example:8545".into()),
                    native_currency: crate::config::NativeCurrency {
                        name: "Example coin".into(),
                        symbol: "COIN".into(),
                        decimals: 6,
                    },
                    start_block: 7,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            // Host policy for this test: two references honoured per body.
            attachments: Some(policy(2)),
            ..Setup::default()
        },
        move |Server {
                  base, client, node, ..
              }| async move {
            let node = node.unwrap();
            // Status names the chain but never leaks the RPC URL.
            let resp = client
                .get(format!("{base}/api/status"))
                .send()
                .await
                .unwrap();
            let text = resp.text().await.unwrap();
            assert!(!text.contains(&node.url), "rpc url leaked: {text}");
            let status: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(status["verifier"], "eth-rpc");
            assert_eq!(status["chains"][0]["id"], 31337);
            assert_eq!(status["chains"][0]["name"], "anvil");
            assert_eq!(
                status["chains"][0]["contract"]
                    .as_str()
                    .unwrap()
                    .to_lowercase(),
                CONTRACT.to_lowercase()
            );
            assert_eq!(
                status["chains"][0]["public_rpc_url"],
                "http://rpc.example:8545"
            );
            assert_eq!(
                status["chains"][0]["native_currency"],
                json!({"name": "Example coin", "symbol": "COIN", "decimals": 6})
            );
            assert_eq!(status["chains"][0]["start_block"], 7);
            assert_eq!(status["chains"][0]["confirmations"], 0);
            assert_eq!(status["store"]["attachments"]["max_per_message"], 2);
            assert!(status["chains"][0].get("read_rpc").is_none());
            assert!(status["chains"][0].get("aux").is_none());
            let before = node.node.counts();
            for route in ["rpc", "wallet"] {
                let resp = client.post(format!("{base}/api/chains/31337/{route}"))
                    .json(&json!({"jsonrpc":"2.0", "id":1, "method":"eth_sendRawTransaction", "params":["0x"]}))
                    .send().await.unwrap();
                assert_eq!(resp.status(), 404);
            }
            assert_eq!(
                node.node.counts(),
                before,
                "removed routes never reach the node"
            );
            let resp = client.get(format!("{base}/api/nope")).send().await.unwrap();
            assert_eq!(resp.status(), 404);
            assert_eq!(
                resp.json::<Value>().await.unwrap(),
                json!({"error": "no such API endpoint", "code": "not_found"})
            );

            let publish = |qs: &str, body: Vec<u8>| {
                let client = client.clone();
                let url = format!("{base}/api/publish{qs}");
                async move { client.post(url).body(body).send().await.unwrap() }
            };

            // No proof: 400, and nothing stored.
            assert_eq!(publish("", anchored.clone()).await.status(), 400);
            assert_eq!(
                publish("?message_id=1", anchored.clone()).await.status(),
                400
            );
            assert_eq!(
                publish("?chain_id=31337", anchored.clone()).await.status(),
                400
            );
            assert_eq!(
                client
                    .get(format!("{base}/api/content/{anchored_hash}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );

            // A chain this host does not serve: 400.
            let resp = publish("?chain_id=1&message_id=1", anchored.clone()).await;
            assert_eq!(resp.status(), 400);
            let j: Value = resp.json().await.unwrap();
            assert!(j["error"].as_str().unwrap().contains("chain 1"), "{j}");

            // An attachment before its body: 409, the body must come first.
            let resp = publish("?chain_id=31337&message_id=1", a.clone()).await;
            assert_eq!(resp.status(), 409);

            // Right chain, nonexistent message: 403.
            assert_eq!(
                publish("?chain_id=31337&message_id=0", anchored.clone())
                    .await
                    .status(),
                403
            );

            // The body: 200, stored, fetchable.
            let resp = publish("?chain_id=31337&message_id=1", anchored.clone()).await;
            assert_eq!(resp.status(), 200);
            let j: Value = resp.json().await.unwrap();
            assert_eq!(j["hash"], anchored_hash);
            assert_eq!(j["kind"], "body");
            let resp = client
                .get(format!("{base}/api/content/{anchored_hash}"))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.bytes().await.unwrap().as_ref(), anchored.as_slice());

            // Attachments A and B are referenced within the first two: accepted,
            // served with their declared type.
            for r in [&a, &b] {
                let resp = publish("?chain_id=31337&message_id=1", r.clone()).await;
                assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
                let j: Value = resp.json().await.unwrap();
                assert_eq!(j["kind"], "attachment");
                let resp = client
                    .get(format!("{base}/api/content/{}", hx(r)))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status(), 200);
                assert_eq!(resp.headers()[header::CONTENT_TYPE], "image/png");
                assert_eq!(resp.headers()[header::X_CONTENT_TYPE_OPTIONS], "nosniff");
                assert_eq!(resp.bytes().await.unwrap().as_ref(), r.as_slice());
            }
            // C is the third reference: beyond this host's cap of two.
            let resp = publish("?chain_id=31337&message_id=1", c.clone()).await;
            assert_eq!(resp.status(), 403);
            let j: Value = resp.json().await.unwrap();
            assert!(j["error"].as_str().unwrap().contains("first 2"), "{j}");
            // D is not referenced at all.
            assert_eq!(
                publish("?chain_id=31337&message_id=1", d.clone())
                    .await
                    .status(),
                403
            );
            // Nothing refused was stored.
            for r in [&c, &d] {
                assert_eq!(
                    client
                        .get(format!("{base}/api/content/{}", hx(r)))
                        .send()
                        .await
                        .unwrap()
                        .status(),
                    404
                );
            }
            // Message 2's body is not on this host: its attachments are 409.
            assert_eq!(
                publish("?chain_id=31337&message_id=2", a.clone())
                    .await
                    .status(),
                409
            );
            // An attachment over the cap: 413 even if referenced (make one).
            let huge = record(
                "application/octet-stream",
                &vec![b'h'; TEST_ATTACHMENT_CAP + 1],
            );
            assert_eq!(
                publish("?chain_id=31337&message_id=1", huge).await.status(),
                413
            );

            // Node goes away: 502, not 403 — the client should retry, not
            // conclude its post is unanchored.
            let node_url = node.url.clone();
            node.stop().await;
            let resp = publish(
                "?chain_id=31337&message_id=1",
                record("text/plain", b"# late\n"),
            )
            .await;
            assert_eq!(resp.status(), 502);
            let text = resp.text().await.unwrap();
            assert!(
                !text.contains(&node_url),
                "rpc url leaked in 502 body: {text}"
            );
        },
    );
}

/// Uploads for messages that are not accepted yet are held, not served, and
/// promoted once they reach the configured accepted depth — including uploads proved
/// only by the transaction that will post them.
#[test]
fn http_holds_until_final() {
    let att = record("image/png", b"png bytes");
    let body_a = record(
        "text/plain",
        format!("# a\n![x](record:{})\n", hx(&att)).as_bytes(),
    );
    let body_b = record("text/plain", b"# b, posted by transaction\n");

    // Message 1 (body A) is mined in block 11; the head is 12; with 5
    // confirmations only blocks <= 7 are accepted, so it is not accepted yet.
    let node = Node {
        head: 12.into(),
        messages: 1.into(),
        extra: vec![(2, body_b.clone())],
        ..Node::new(31337, CONTRACT, body_a.clone())
    };
    with_server(
        "http-final",
        Some(node),
        |url| Setup {
            gateway: false,
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            holding: HoldingConfig {
                max_bytes: 1 << 20,
                max_per_signer: 4,
                ttl: Duration::from_secs(3600),
                ..HoldingConfig::default()
            },
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            let node = node.unwrap();
            let publish = |qs: &str, body: Vec<u8>| {
                let client = client.clone();
                let url = format!("{base}/api/publish{qs}");
                async move { client.post(url).body(body).send().await.unwrap() }
            };
            let fetch = |hash: String| {
                let client = client.clone();
                let url = format!("{base}/api/content/{hash}");
                async move { client.get(url).send().await.unwrap().status() }
            };

            let status: Value = client
                .get(format!("{base}/api/status"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(status["chains"][0]["confirmations"], 5);

            // Body A, by message id: exists at latest but is not accepted → held.
            let resp = publish("?chain_id=31337&message_id=1", body_a.clone()).await;
            assert_eq!(resp.status(), 202, "{}", resp.text().await.unwrap());
            let j: Value = resp.json().await.unwrap();
            assert_eq!(j["held"], true);
            assert_eq!(j["stored"], false);
            assert_eq!(j["message_id"], 1);
            // Not served while held.
            assert_eq!(fetch(hx(&body_a)).await, 404);
            // Its attachment follows it into the holding area.
            let resp = publish("?chain_id=31337&message_id=1", att.clone()).await;
            assert_eq!(resp.status(), 202, "{}", resp.text().await.unwrap());
            // Something the body does not reference is still refused.
            assert_eq!(
                publish(
                    "?chain_id=31337&message_id=1",
                    record("text/plain", b"nope")
                )
                .await
                .status(),
                403
            );
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert_eq!(status["store"]["holding"]["entries"], 2);

            // One acceptance reader shares the confirmed head across all
            // messages in a pass, but never treats latest evidence as final.
            let heads_before = node.node.count("eth_blockNumber");
            let mut acceptance = crate::acceptance::Acceptance::new(&state);
            assert!(acceptance.accepted(31337, 1).await.unwrap().is_none());
            assert!(acceptance.accepted(31337, 1).await.unwrap().is_none());
            assert_eq!(node.node.count("eth_blockNumber"), heads_before + 1);
            assert!(acceptance.for_peer(31337, 1).await.unwrap().is_some());

            // Nothing is accepted yet: the watcher promotes nothing.
            assert_eq!(promote_once(&state).await, 0);
            assert_eq!(fetch(hx(&body_a)).await, 404);
            // The chain advances past the confirmation depth: both promoted.
            node.node.set_head(20);
            assert_eq!(promote_once(&state).await, 2);
            assert_eq!(fetch(hx(&body_a)).await, 200);
            assert_eq!(fetch(hx(&att)).await, 200);
            assert_eq!(
                get_json(&client, &format!("{base}/api/status")).await["store"]["holding"]["entries"],
                0
            );

            // Body B, proved by a transaction that is still pending.
            let tx_hash = [7u8; 32];
            let mut input = selector(crate::abi::POST_SIGNATURE).to_vec();
            input.extend_from_slice(&keccak::hash(&[body_b.as_slice()]).0);
            input.extend_from_slice(&[0u8; 32 * 4]);
            node.node.add_tx(MockTx {
                hash: tx_hash,
                from: [0xab; 20],
                to: CONTRACT.to_lowercase(),
                input: input.clone(),
                block: None,
                message_id: None,
                reverted: false,
            });
            let q = format!("?chain_id=31337&tx_hash={}", hex0x(&tx_hash));
            let resp = publish(&q, body_b.clone()).await;
            assert_eq!(resp.status(), 202, "{}", resp.text().await.unwrap());
            let j: Value = resp.json().await.unwrap();
            assert!(j["message_id"].is_null(), "id unknown while pending");
            // Bytes the transaction does not commit to, and that its held body
            // does not reference: refused.
            assert_eq!(
                publish(&q, record("text/plain", b"# other\n"))
                    .await
                    .status(),
                403
            );
            // An unknown transaction, and one to another contract.
            assert_eq!(
                publish(
                    &format!("?chain_id=31337&tx_hash={}", hex0x(&[9u8; 32])),
                    body_b.clone()
                )
                .await
                .status(),
                404
            );
            node.node.add_tx(MockTx {
                hash: [8u8; 32],
                from: [0xab; 20],
                to: "0x000000000000000000000000000000000000dead".into(),
                input,
                block: None,
                message_id: None,
                reverted: false,
            });
            assert_eq!(
                publish(
                    &format!("?chain_id=31337&tx_hash={}", hex0x(&[8u8; 32])),
                    body_b.clone()
                )
                .await
                .status(),
                403
            );
            assert_eq!(fetch(hx(&body_b)).await, 404);

            // Mined as message 2 in block 12; head 20 makes it accepted → promoted.
            node.node.mine_tx(tx_hash, 12, 2);
            node.node.set_messages(2);
            assert_eq!(promote_once(&state).await, 1);
            assert_eq!(fetch(hx(&body_b)).await, 200);
            assert_eq!(
                get_json(&client, &format!("{base}/api/status")).await["store"]["holding"]["entries"],
                0
            );
        },
    );
}

/// A sponsor list is published for clients but never restricts body storage.
/// Attachment policy and chain anchoring still apply to every author.
#[test]
fn http_sponsor_list_does_not_restrict_storage() {
    let body = record("text/plain", b"# all authors\n");
    // Message i is authored by 0x…0i; only author 1 is a member.
    let mut author1 = [0u8; 20];
    author1[19] = 1;
    let node = Node {
        head: 100.into(),
        messages: 2.into(),
        members: vec![author1].into(),
        ..Node::new(31337, CONTRACT, body.clone())
    };
    with_server(
        "http-members",
        Some(node),
        |url| Setup {
            gateway: false,
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            attachments: None,
            ..Setup::default()
        },
        move |Server { base, client, .. }| async move {
            let status = get_json(&client, &format!("{base}/api/status")).await;
            assert!(status["store"]["attachments"].is_null());

            let publish = |id: u64, b: Vec<u8>| {
                let client = client.clone();
                let url = format!("{base}/api/publish?chain_id=31337&message_id={id}");
                async move { client.post(url).body(b).send().await.unwrap() }
            };
            // Try the non-member first, so shared bytes are not already stored.
            for id in [2, 1] {
                let resp = publish(id, body.clone()).await;
                assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
            }
            let resp = publish(3, body.clone()).await;
            assert_eq!(
                resp.status(),
                403,
                "an unknown post cannot anchor an upload"
            );

            // Bytes that are not message 1's body would be an attachment, and
            // this host stores none.
            let resp = publish(1, record("image/png", b"png")).await;
            assert_eq!(resp.status(), 403);
            let j: Value = resp.json().await.unwrap();
            assert_eq!(
                j["error"],
                format!(
                    "this upload is not the message's body (the message commits to {}), and this host stores no attachments",
                    hx(&body)
                )
            );
        },
    );
}

/// A post its author deleted, or the parent's author removed, takes no more
/// content: not its body, not an attachment naming it, and not a pending post
/// created already deleted. A live post with the same bytes still stores them.
#[test]
fn deleted_and_removed_posts_take_no_content() {
    use crate::index::types::{REMOVED, TOMBSTONE};
    let image = record("image/png", b"an attachment of a deleted post");
    let image_hash = keccak::hash(&[&image]);
    let body = record(
        "text/plain",
        format!("see record:{}", hex0x(&image_hash.0)).as_bytes(),
    );
    let body_hash = keccak::hash(&[&body]);
    with_server(
        "deleted-posts",
        Some(Node {
            head: 100.into(),
            messages: 3.into(),
            txs: vec![MockTx {
                hash: [5; 32],
                from: [9; 20],
                to: CONTRACT.into(),
                input: crate::abi::post_calldata(crate::chain::Hex(body_hash.0), 0, TOMBSTONE),
                block: None,
                message_id: None,
                reverted: false,
            }]
            .into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            set_indexed_gate(&state, 31337, 1, TOMBSTONE);
            set_indexed_gate(&state, 31337, 2, REMOVED);
            let refused = |response: reqwest::Response| async move {
                assert_eq!(response.status(), 410);
                let j: Value = response.json().await.unwrap();
                assert_eq!(j["code"], "message_gone", "{j}");
            };
            for id in [1, 2] {
                let query = format!("chain_id=31337&message_id={id}");
                refused(publish(&client, &base, &query, body.clone()).await).await;
            }
            let pending = format!("chain_id=31337&tx_hash={}", hex0x(&[5; 32]));
            refused(publish(&client, &base, &pending, body.clone()).await).await;
            // The live post stores the shared body, so it is on the host, but an
            // attachment naming the deleted post is still refused.
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=3", body.clone())
                    .await
                    .status(),
                200
            );
            refused(publish(&client, &base, "chain_id=31337&message_id=1", image.clone()).await)
                .await;
            let got = client
                .get(format!("{base}/api/content/{}", hex0x(&image_hash.0)))
                .send()
                .await
                .unwrap();
            assert_eq!(got.status(), 404, "the refused attachment is not served");
        },
    );
}

/// A store-only host asks the Board for the post's gate along with the post.
#[test]
fn store_only_hosts_refuse_content_for_deleted_posts() {
    let body = record("text/plain", b"store-only and deleted");
    with_server(
        "deleted-store-only",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            gates: std::collections::HashMap::from([(1, crate::index::types::TOMBSTONE.0)]).into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            gateway: false,
            ..Setup::default()
        },
        move |Server { base, client, .. }| async move {
            let response =
                publish(&client, &base, "chain_id=31337&message_id=1", body.clone()).await;
            assert_eq!(response.status(), 410);
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=2", body.clone())
                    .await
                    .status(),
                200
            );
        },
    );
}

/// A post deleted while its upload waits for acceptance is dropped from
/// holding with its attachments, never promoted into the served store.
#[test]
fn a_held_upload_whose_post_is_deleted_is_dropped() {
    let body = record("text/plain", b"held, then deleted");
    let digest = keccak::Digest(keccak::hash(&[&body]).0);
    with_server(
        "held-deleted",
        Some(Node {
            head: 12.into(),
            messages: 1.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        |url| Setup {
            verifier: chain_verifier(
                url.unwrap(),
                ChainConfig {
                    confirmations: 5,
                    ..ChainConfig::for_test(31337, "", CONTRACT)
                },
            ),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
              }| async move {
            crate::index::follow::once(&state).await;
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=1", body.clone())
                    .await
                    .status(),
                202
            );
            node.unwrap().node.set_head(16);
            crate::index::follow::once(&state).await;
            set_indexed_gate(&state, 31337, 1, crate::index::types::TOMBSTONE);
            assert_eq!(crate::watcher::promote_once(&state).await, 0);
            let store = state.store.as_ref().unwrap();
            assert!(
                store.holding.get(31337, &digest).is_none(),
                "dropped from holding"
            );
            let got = client
                .get(format!("{base}/api/content/{}", digest.to_hex()))
                .send()
                .await
                .unwrap();
            assert_eq!(got.status(), 404);
        },
    );
}

/// An open issue, kept as its reproduction: a stranger's unmined
/// transactions carrying an author's valid signed posts are held against
/// that author's quota. Sixteen of them, each a post the author once signed
/// and anyone can copy from public calldata, fill it, and the author's own
/// next post is turned away. Whether a Base provider ever reports such a
/// transaction as pending is unverified; charging the sender instead would
/// pool every relayed post under the relayer. The test passes while the
/// issue stands and runs in CI with the other ignored tests, so a fix must
/// turn it around.
#[test]
#[ignore = "documents an unresolved issue: a stranger's pending transactions can fill an author's holding quota"]
fn a_strangers_pending_transactions_fill_an_authors_holding_quota() {
    let author = [0xaau8; 20];
    let stranger = [0x99u8; 20];
    let posts: Vec<Vec<u8>> = (0..=16)
        .map(|n| record("text/plain", format!("post number {n}").as_bytes()))
        .collect();
    let mut txs = Vec::new();
    for (n, body) in posts.iter().enumerate().take(16) {
        let words = post_words(author, keccak::hash(&[body.as_slice()]).0, n as u64 + 1);
        txs.push(MockTx {
            hash: [n as u8 + 1; 32],
            from: stranger,
            to: CONTRACT.into(),
            input: post_by_sig_input(&words, &[0x99; 65]),
            block: None,
            message_id: None,
            reverted: false,
        });
    }
    // The author's own post, sent by the author, still unmined.
    let mut own = selector(crate::abi::POST_SIGNATURE).to_vec();
    own.extend_from_slice(&keccak::hash(&[posts[16].as_slice()]).0);
    own.extend_from_slice(&[0u8; 32 * 4]);
    txs.push(MockTx {
        hash: [17; 32],
        from: author,
        to: CONTRACT.into(),
        input: own,
        block: None,
        message_id: None,
        reverted: false,
    });
    with_server(
        "review-replay-quota",
        Some(Node {
            contracts: vec![author].into(),
            txs: txs.into(),
            ..Node::new(31337, CONTRACT, posts[0].clone())
        }),
        |url| Setup {
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server { base, client, .. }| async move {
            // Each copy from another network, under the visitor limits.
            let upload = |n: u8, body: Vec<u8>| {
                client
                    .post(format!(
                        "{base}/api/publish?chain_id=31337&tx_hash=0x{}",
                        hex::encode([n; 32])
                    ))
                    .header("x-forwarded-for", format!("203.0.113.{n}"))
                    .body(body)
                    .send()
            };
            for (n, body) in posts.iter().enumerate().take(16) {
                let resp = upload(n as u8 + 1, body.clone()).await.unwrap();
                assert_eq!(
                    resp.status(),
                    202,
                    "copy {n}: {}",
                    resp.text().await.unwrap()
                );
            }
            let resp = upload(17, posts[16].clone()).await.unwrap();
            let status = resp.status().as_u16();
            let answer = resp.json::<Value>().await.unwrap();
            assert_eq!(
                (status, answer["code"].as_str()),
                (429, Some("signer_holding_limit")),
                "{answer}"
            );
        },
    );
}
