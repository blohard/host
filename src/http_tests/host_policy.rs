//! Real HTTP authorization, durable revision and blocking boundaries.
use super::*;
use k256::ecdsa::SigningKey;

#[test]
fn signed_host_lists_are_durable_scoped_and_override_curation() {
    let key = SigningKey::from_slice(&[7; 32]).unwrap();
    let point = key.verifying_key().to_encoded_point(false);
    let admin = crate::chain::Hex(
        keccak::hash(&[&point.as_bytes()[1..]]).0[12..]
            .try_into()
            .unwrap(),
    );
    let path = std::env::temp_dir().join(format!("host-policy-http-{}.json", std::process::id()));
    std::fs::write(
        &path,
        br#"{"revision":0,"curated":[],"blocked":[],"bots":[]}"#,
    )
    .unwrap();
    write_no_takedowns(&path);
    let config = crate::host_policy::Config {
        file: path.clone(),
        admin,
        admin_chain: 31337,
        origin: "https://host.example".into(),
    };
    let reopening = config.clone();
    let body = record("text/plain", b"a local policy post");
    with_server(
        "host-policy",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body.clone())
        }),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  ..
              }| async move {
            let url = format!("{base}/api/host-policy");
            let expires = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 200;
            let author = format!("0x{:040x}", 1);
            let signed = |revision: u64, list: &str, action: &str, origin: &str| {
                let message = format!(
                    "Host policy update\nHost: {origin}\nRevision: {revision}\nExpires: {expires}\nList: {list}\nAction: {action}\nAddresses:\n{author}"
                );
                let digest = alloy_primitives::eip191_hash_message(message.as_bytes());
                let (signature, recovery) =
                    key.sign_prehash_recoverable(digest.as_slice()).unwrap();
                let mut bytes = signature.to_bytes().to_vec();
                bytes.push(recovery.to_byte() + 27);
                json!({"revision": revision.to_string(), "expires":expires,"list":list,"action":action,"addresses":[author],"signature":hex0x(&bytes)})
            };
            let bad_json = client
                .post(&url)
                .header("content-type", "application/json")
                .body("{")
                .send()
                .await
                .unwrap();
            assert_eq!(bad_json.status(), 400);
            assert_eq!(
                bad_json.json::<Value>().await.unwrap()["code"],
                "invalid_policy_edit"
            );
            let mut expired = signed(0, "curated", "add", "https://host.example");
            expired["expires"] = json!(expires - 300);
            assert_eq!(
                client
                    .post(&url)
                    .json(&expired)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
            let mut tampered = signed(0, "curated", "add", "https://host.example");
            tampered["action"] = json!("remove");
            assert_eq!(
                client
                    .post(&url)
                    .json(&tampered)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                403
            );
            let wrong_host = client
                .post(&url)
                .json(&signed(0, "curated", "add", "https://another.example"))
                .send()
                .await
                .unwrap();
            assert_eq!(wrong_host.status(), 403);
            let edit = signed(0, "curated", "add", "https://host.example");
            let response = client.post(&url).json(&edit).send().await.unwrap();
            assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
            assert_eq!(
                client.post(&url).json(&edit).send().await.unwrap().status(),
                409,
                "signed edits cannot be replayed"
            );
            let mut malformed = signed(1, "blocked", "add", "https://host.example");
            malformed["addresses"] = json!(["0x0000000000000000000000000000000000000000"]);
            assert_eq!(
                client
                    .post(&url)
                    .json(&malformed)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
            let blocked = client
                .post(&url)
                .json(&signed(1, "blocked", "add", "https://host.example"))
                .send()
                .await
                .unwrap();
            assert_eq!(blocked.status(), 200);
            let policy = crate::host_policy::Policy::open(Some(reopening))
                .await
                .unwrap();
            assert_eq!(policy.snapshot().revision, 2);
            let author_bytes = crate::abi::parse_address(&author).unwrap();
            assert!(policy.blocked(&author_bytes));
            assert!(!policy.snapshot().curated(&author_bytes));
            crate::index::follow::once(&state).await;
            let feed = post_json(
                &client,
                &format!("{base}/api/chains/31337/feed"),
                json!({"filter":{"any":[{}]}}),
            )
            .await;
            assert!(
                feed["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|m| m["author"] != author)
            );
            // Hosting checks the post's author, even when identical bytes were already stored.
            assert_eq!(
                publish(&client, &base, "chain_id=31337&message_id=2", body.clone())
                    .await
                    .status(),
                200
            );
            let denied = publish(&client, &base, "chain_id=31337&message_id=1", body).await;
            assert_eq!(denied.status(), 403);
            assert_eq!(
                denied.json::<Value>().await.unwrap()["code"],
                "host_blocked"
            );
            let estimate = client.post(format!("{base}/api/chains/31337/post-estimate")).json(&json!({"author":author,"content_hash":format!("0x{}","00".repeat(32)),"parent":"0","reply_gate":format!("0x{}","00".repeat(20))})).send().await.unwrap();
            assert_eq!(estimate.status(), 403, "{}", estimate.text().await.unwrap());
            let read = client.get(&url).send().await.unwrap();
            assert_eq!(read.headers()["cache-control"], "no-store");
            assert_eq!(read.json::<Value>().await.unwrap()["revision"], "2");
            // A failed atomic replacement cannot publish an unsaved revision.
            let temp = path.with_extension(format!("tmp-{}", std::process::id()));
            std::fs::create_dir(&temp).unwrap();
            let edit = signed(2, "blocked", "remove", "https://host.example");
            let failure = client.post(&url).json(&edit).send().await.unwrap();
            assert_eq!(failure.status(), 500);
            assert_eq!(state.policy.snapshot().revision, 2);
            assert!(state.policy.blocked(&author_bytes));
            assert_eq!(blohard_policy::Lists::read(&path).unwrap().revision, 2);
            std::fs::remove_dir(temp).unwrap();
            assert_eq!(
                client.post(&url).json(&edit).send().await.unwrap().status(),
                200
            );
            assert!(!state.policy.blocked(&author_bytes));
            std::fs::remove_file(path).unwrap();
        },
    );
}

#[test]
fn contract_admin_requires_erc1271_approval() {
    let path = std::env::temp_dir().join(format!("host-policy-1271-{}.json", std::process::id()));
    std::fs::write(
        &path,
        br#"{"revision":0,"curated":[],"blocked":[],"bots":[]}"#,
    )
    .unwrap();
    write_no_takedowns(&path);
    let config = crate::host_policy::Config {
        file: path.clone(),
        admin: crate::chain::Hex([0xaa; 20]),
        admin_chain: 31337,
        origin: "https://host.example".into(),
    };
    with_server(
        "host-policy-1271",
        Some(Node::new(31337, CONTRACT, Vec::new())),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base, client, node, ..
              }| async move {
            let node = node.unwrap();
            let url = format!("{base}/api/host-policy");
            let expires = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 200;
            let mut edit = json!({"revision":"0", "expires": expires, "list":"curated", "action":"add", "addresses":[format!("0x{}", "01".repeat(20))], "signature":"0x1234"});
            assert_eq!(
                client.post(&url).json(&edit).send().await.unwrap().status(),
                403
            );
            node.node.response_overrides.lock().unwrap().insert(
                "eth_call".into(),
                json!({"result":format!("0x1626ba7e{}", "00".repeat(28))}),
            );
            assert_eq!(
                client.post(&url).json(&edit).send().await.unwrap().status(),
                200
            );
            edit["revision"] = json!("1");
            node.node.response_overrides.lock().unwrap().insert(
                "eth_call".into(),
                json!({"result":format!("0x1626ba7e{}", "01".repeat(28))}),
            );
            assert_eq!(
                client.post(&url).json(&edit).send().await.unwrap().status(),
                403,
                "dirty ABI padding is not approval"
            );
            std::fs::remove_file(path).unwrap();
        },
    );
}

#[test]
fn accounts_declare_themselves_bots_beside_the_admin_bot_list() {
    let address_of = |key: &SigningKey| -> [u8; 20] {
        let point = key.verifying_key().to_encoded_point(false);
        keccak::hash(&[&point.as_bytes()[1..]]).0[12..]
            .try_into()
            .unwrap()
    };
    let wallet_key = SigningKey::from_slice(&[9; 32]).unwrap();
    let admin_key = SigningKey::from_slice(&[7; 32]).unwrap();
    let wallet = address_of(&wallet_key);
    let path = std::env::temp_dir().join(format!("host-policy-bots-{}.json", std::process::id()));
    std::fs::write(
        &path,
        br#"{"revision":0,"curated":[],"blocked":[],"bots":[]}"#,
    )
    .unwrap();
    let declarations_file = path.with_extension("declarations.json");
    let _ = std::fs::remove_file(&declarations_file);
    write_no_takedowns(&path);
    let config = crate::host_policy::Config {
        file: path.clone(),
        admin: crate::chain::Hex(address_of(&admin_key)),
        admin_chain: 31337,
        origin: "https://host.example".into(),
    };
    let reopening = config.clone();
    let body = record("text/plain", b"a post by a bot");
    with_server(
        "host-policy-bots",
        Some(Node {
            head: 100.into(),
            messages: 2.into(),
            ..Node::new(31337, CONTRACT, body)
        }),
        move |url| Setup {
            host_policy: Some(config),
            verifier: chain_verifier(url.unwrap(), ChainConfig::for_test(31337, "", CONTRACT)),
            ..Setup::default()
        },
        move |Server {
                  base,
                  client,
                  state,
                  node,
                  ..
              }| async move {
            crate::index::follow::once(&state).await;
            let node = node.unwrap();
            let url = format!("{base}/api/bot-declaration");
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let hex = |a: &[u8; 20]| format!("0x{}", hex::encode(a));
            let sign = |key: &SigningKey, message: &str| {
                let digest = alloy_primitives::eip191_hash_message(message.as_bytes());
                let (signature, recovery) =
                    key.sign_prehash_recoverable(digest.as_slice()).unwrap();
                let mut bytes = signature.to_bytes().to_vec();
                bytes.push(recovery.to_byte() + 27);
                hex0x(&bytes)
            };
            let says = |account: &[u8; 20], bot: bool, issued: u64| {
                crate::bot_declarations::message("https://host.example", account, bot, issued)
            };
            let declaration = |account: &[u8; 20], bot: bool, issued: u64, signature: String| json!({"account": hex(account), "bot": bot, "issued": issued, "signature": signature});
            // Messages 1 and 2 in the mock chain were posted by 0x…01 and 0x…02.
            let first_author = {
                let mut a = [0u8; 20];
                a[19] = 1;
                a
            };
            let second_author = {
                let mut a = [0u8; 20];
                a[19] = 2;
                a
            };
            let send = |body: Value| client.post(&url).json(&body).send();
            let code = |response: reqwest::Response| async move {
                let status = response.status().as_u16();
                (
                    status,
                    response.json::<Value>().await.unwrap()["code"].clone(),
                )
            };
            assert_eq!(
                code(send(json!({"account": hex(&wallet)})).await.unwrap()).await,
                (400, json!("invalid_declaration"))
            );
            // A genuine signature from an account with no messages: declaring is free, so it is refused.
            assert_eq!(
                code(
                    send(declaration(
                        &wallet,
                        true,
                        now,
                        sign(&wallet_key, &says(&wallet, true, now))
                    ))
                    .await
                    .unwrap()
                )
                .await,
                (403, json!("not_an_author"))
            );
            // Refused before any signature check: a never-posted account with
            // junk for a signature costs the chain nothing.
            let chain_calls = || {
                let counts = node.node.counts();
                counts.get("eth_getCode").copied().unwrap_or(0)
                    + counts.get("eth_call").copied().unwrap_or(0)
            };
            let before = chain_calls();
            assert_eq!(
                code(
                    send(declaration(&wallet, true, now, "0x1234".into()))
                        .await
                        .unwrap()
                )
                .await,
                (403, json!("not_an_author"))
            );
            assert_eq!(
                chain_calls(),
                before,
                "no chain call for a never-posted account"
            );
            // Signing for another account's address proves nothing.
            assert_eq!(
                code(
                    send(declaration(
                        &second_author,
                        true,
                        now,
                        sign(&wallet_key, &says(&second_author, true, now))
                    ))
                    .await
                    .unwrap()
                )
                .await,
                (403, json!("wrong_signer"))
            );
            assert_eq!(
                code(
                    send(declaration(&first_author, true, now - 400, "0x1234".into()))
                        .await
                        .unwrap()
                )
                .await,
                (409, json!("stale_declaration"))
            );
            // A contract account approves through ERC-1271 on the admin chain.
            node.node.response_overrides.lock().unwrap().insert(
                "eth_call".into(),
                json!({"result":format!("0x1626ba7e{}", "00".repeat(28))}),
            );
            let declared = send(declaration(&first_author, true, now, "0x1234".into()))
                .await
                .unwrap();
            let status = declared.status();
            let policy = declared.json::<Value>().await.unwrap();
            assert_eq!(status, 200, "{policy}");
            let lower = |v: &Value| {
                v.as_array()
                    .unwrap()
                    .iter()
                    .map(|a| a.as_str().unwrap().to_lowercase())
                    .collect::<Vec<_>>()
            };
            assert_eq!(lower(&policy["declared_bots"]), vec![hex(&first_author)]);
            assert_eq!(policy["declarations"], "1");
            assert_eq!(
                policy["revision"], "0",
                "declarations never move the admin revision"
            );
            assert_eq!(
                code(
                    send(declaration(&first_author, true, now, "0x1234".into()))
                        .await
                        .unwrap()
                )
                .await,
                (409, json!("stale_declaration")),
                "a statement cannot be replayed"
            );
            let withdrawn = send(declaration(&first_author, false, now + 1, "0x1234".into()))
                .await
                .unwrap();
            assert_eq!(withdrawn.status(), 200);
            let policy = withdrawn.json::<Value>().await.unwrap();
            assert!(policy["declared_bots"].as_array().unwrap().is_empty());
            assert_eq!(policy["declarations"], "2");
            // Every policy answer carries the declarations.
            let read = client
                .get(format!("{base}/api/host-policy"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            assert_eq!(read["declarations"], "2");
            let status = client
                .get(format!("{base}/api/status"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            assert_eq!(status["host_policy"]["declarations"], "2");
            // The admin's own bots list, edited like the others.
            let expires = now + 200;
            let message = format!(
                "Host policy update\nHost: https://host.example\nRevision: 0\nExpires: {expires}\nList: bots\nAction: add\nAddresses:\n{}",
                hex(&second_author)
            );
            let edit = json!({"revision": "0", "expires": expires, "list": "bots", "action": "add",
                "addresses": [hex(&second_author)], "signature": sign(&admin_key, &message)});
            let labelled = client
                .post(format!("{base}/api/host-policy"))
                .json(&edit)
                .send()
                .await
                .unwrap();
            assert_eq!(labelled.status(), 200);
            let policy = labelled.json::<Value>().await.unwrap();
            assert_eq!(lower(&policy["bots"]), vec![hex(&second_author)]);
            assert_eq!(policy["revision"], "1");
            assert_eq!(
                policy["declarations"], "2",
                "an edit's answer carries the declarations too"
            );
            // Both survive a restart.
            let reopened = crate::host_policy::Policy::open(Some(reopening))
                .await
                .unwrap();
            let declarations = reopened.declarations();
            assert_eq!(declarations.counter, 2);
            assert_eq!(
                declarations.accounts[&alloy_primitives::Address::from(first_author)],
                crate::bot_declarations::Statement {
                    bot: false,
                    issued: now + 1
                }
            );
            assert!(
                reopened
                    .snapshot()
                    .bots
                    .contains(&alloy_primitives::Address::from(second_author))
            );
            std::fs::remove_file(path).unwrap();
            std::fs::remove_file(declarations_file).unwrap();
        },
    );
}

#[test]
fn curated_visibility_is_per_chain_and_requires_host_policy() {
    for enabled in [false, true] {
        for (id, curated) in [(8453, true), (424242, false)] {
            let path = std::env::temp_dir().join(format!(
                "host-curated-{}-{enabled}-{id}.json",
                std::process::id()
            ));
            std::fs::write(
                &path,
                br#"{"revision":0,"curated":[],"blocked":[],"bots":[]}"#,
            )
            .unwrap();
            write_no_takedowns(&path);
            let policy = enabled.then(|| crate::host_policy::Config {
                file: path.clone(),
                admin: crate::chain::Hex([1; 20]),
                admin_chain: id,
                origin: "https://host.example".into(),
            });
            with_server(
                "curated-per-chain",
                Some(Node::new(id, CONTRACT, Vec::new())),
                move |url| {
                    // Existing entries default to enabled; a chain may opt out.
                    let setting = if curated { "" } else { "curated = false" };
                    let url = url.unwrap();
                    let cfg = Config::parse(&format!(
                        r#"
                        [store]
                        [[chains]]
                        id = {id}
                        rpc_url = "{url}"
                        contract = "{CONTRACT}"
                        {setting}
                    "#
                    ))
                    .unwrap();
                    assert_eq!(cfg.chains[0].curated, curated);
                    Setup {
                        verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
                        host_policy: policy,
                        ..Setup::default()
                    }
                },
                move |Server { base, client, .. }| async move {
                    let status: Value = client
                        .get(format!("{base}/api/status"))
                        .send()
                        .await
                        .unwrap()
                        .json()
                        .await
                        .unwrap();
                    assert_eq!(status["chains"][0]["id"], id);
                    assert_eq!(status["chains"][0]["curated"], enabled && curated);
                    assert_eq!(status["host_policy"].is_object(), enabled);
                },
            );
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(crate::host_policy::takedowns::path(&path));
        }
    }
}
