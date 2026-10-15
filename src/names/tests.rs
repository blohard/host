use super::*;
use axum::{Json, Router, routing::post};
use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Default)]
struct Mock {
    answers: Mutex<VecDeque<Value>>,
    calls: Mutex<Vec<Value>>,
    blocked: std::sync::atomic::AtomicBool,
    block_at: std::sync::atomic::AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
async fn endpoint(mock: Arc<Mock>) -> (String, tokio::task::JoinHandle<()>) {
    async fn answer(State(mock): State<Arc<Mock>>, Json(req): Json<Value>) -> Json<Value> {
        mock.calls.lock().unwrap().push(req);
        if mock.blocked.load(std::sync::atomic::Ordering::Relaxed)
            || mock.block_at.load(std::sync::atomic::Ordering::Relaxed)
                == mock.calls.lock().unwrap().len()
        {
            mock.entered.notify_one();
            mock.release.notified().await;
        }
        tokio::task::yield_now().await;
        Json(
            mock.answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected upstream call"),
        )
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/", post(answer)).with_state(mock),
        )
        .await
        .unwrap();
    });
    (url, task)
}
#[tokio::test]
async fn a_disconnected_name_reader_keeps_its_visitor_slot_until_work_finishes() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().push_back(absent());
    mock.blocked
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (url, server) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let slots = Arc::new(tokio::sync::Semaphore::new(2));
    let permit = Arc::new(slots.clone().try_acquire_owned().unwrap());
    let worker = names.clone();
    let address = crate::chain::Hex([0x11; 20]);
    let reader = tokio::spawn(async move { worker.lookup_admitted(address, Some(permit)).await });
    mock.entered.notified().await;
    reader.abort();
    let _ = reader.await;
    assert_eq!(slots.available_permits(), 1);
    mock.release.notify_one();
    names.lookup(address).await.unwrap();
    assert_eq!(slots.available_permits(), 2);
    server.abort();
}

fn ok(bytes: Vec<u8>) -> Value {
    json!({"result": crate::abi::hex0x(&bytes)})
}
fn err(bytes: Vec<u8>) -> Value {
    json!({"error": {"code": 3, "data": crate::abi::hex0x(&bytes)}})
}
fn primary(value: &str) -> Value {
    ok(reverseWithGatewaysCall::abi_encode_returns(
        &reverseWithGatewaysReturn {
            primary: value.into(),
            resolver: Default::default(),
            reverseResolver: Default::default(),
        },
    ))
}
/// The reverse registrar's modern record.
fn modern_name(name: &str) -> Value {
    ok(nameForAddrCall::abi_encode_returns(&name.to_string()))
}
/// The registry pointing a name at the test resolver.
fn registry_entry() -> Value {
    ok(resolverCall::abi_encode_returns(
        &alloy_primitives::Address::from([0x33; 20]),
    ))
}
/// A resolver's forward record.
fn forward_record(address: [u8; 20]) -> Value {
    ok(addrCall::abi_encode_returns(
        &alloy_primitives::Address::from(address),
    ))
}
/// Every way a resolver broke when tried on Coinbase Developer Platform's
/// endpoint, mainnet.base.org and base-rpc.publicnode.com, word for word.
const BROKEN: [&str; 9] = [
    "EVM error: InvalidFEOpcode",
    "EVM error: OpcodeNotFound",
    "EVM error: StackUnderflow",
    "EVM error: StackOverflow",
    "EVM error: InvalidJump",
    "EVM error: OutOfOffset",
    "EVM error: CreateInitCodeSizeLimit",
    "EVM error: PrecompileError",
    "out of memory: memory limit exceeded during memory expansion",
];
fn broken(message: &str) -> Value {
    json!({"error": {"code": -32003, "message": message, "data": null}})
}
fn absent() -> Value {
    err(ResolverNotFound {
        name: Vec::<u8>::new().into(),
    }
    .abi_encode())
}
fn service(url: String, base: bool) -> Names {
    let mut names = paused_service(url, base);
    names.pause = Duration::ZERO;
    names
}

/// The production pause, for the test that exercises it.
fn paused_service(url: String, base: bool) -> Names {
    Names::new(
        NamesConfig {
            ethereum_upstream: (!base).then_some(url.clone()),
            base_upstream: base.then_some(url),
        },
        rpc::Upstream::unlimited(reqwest::Client::new()),
    )
    .unwrap()
}

#[test]
fn bounded_name_packets_and_avatar_references() {
    // Independent ENS namehash vector and DNS representation.
    assert_eq!(
        hex::encode(node("eth")),
        "93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
    );
    assert_eq!(dns("alice.eth").unwrap(), b"\x05alice\x03eth\0");
    for name in ["", ".eth", "x..eth", "x\n.eth"] {
        assert!(dns(name).is_err());
    }
    assert!(dns(&format!("{}.eth", "a".repeat(64))).is_ok());
    assert!(dns(&"a".repeat(256)).is_err());
    assert_eq!(
        avatar_url("ipfs://ipfs/cid".into()).as_deref(),
        Some("https://ipfs.io/ipfs/cid")
    );
    assert_eq!(
        avatar_url("https://example.test/a.png".into()).as_deref(),
        Some("https://example.test/a.png")
    );
    // The parser's own spelling, not the owner's.
    assert_eq!(
        avatar_url("HTTPS://Example.TEST/a b.png?x=1#f".into()).as_deref(),
        Some("https://example.test/a%20b.png?x=1#f")
    );
    assert_eq!(
        avatar_url("https://example.test".into()).as_deref(),
        Some("https://example.test/")
    );
    for value in [
        "data:image/svg+xml,test",
        "http://example.test/a",
        "javascript:alert(1)",
        "eip155:1/erc721:x/1",
    ] {
        assert!(avatar_url(value.into()).is_none());
    }
    assert!(avatar_url(format!("https://example.test/{}", "a".repeat(MAX_AVATAR))).is_none());
}

#[tokio::test]
async fn verified_ens_is_shared_and_cached_with_its_avatar() {
    let mock = Arc::new(Mock::default());
    let avatar = textCall::abi_encode_returns(&"ipfs://cid".to_string());
    mock.answers.lock().unwrap().extend([
        primary("alice.eth"),
        ok(resolveWithGatewaysCall::abi_encode_returns(
            &resolveWithGatewaysReturn {
                result: avatar.into(),
                resolver: Default::default(),
            },
        )),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let address = crate::chain::Hex([0x11; 20]);
    let answers = futures::future::join_all((0..10).map(|_| names.lookup(address))).await;
    for answer in answers {
        let entry = answer.unwrap();
        assert_eq!(entry.name.as_deref(), Some("alice.eth"));
        assert_eq!(entry.avatar.as_deref(), Some("https://ipfs.io/ipfs/cid"));
    }
    assert_eq!(names.lookup(address).await.unwrap().source, Some("ens"));
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    let data = crate::abi::decode_hex(calls[0]["params"][0]["data"].as_str().unwrap()).unwrap();
    let call = reverseWithGatewaysCall::abi_decode_validate(&data).unwrap();
    assert_eq!(call.lookupAddress.as_ref(), address.0);
    assert_eq!(call.coinType, alloy_primitives::U256::from(60));
    assert_eq!(call.gateways, [BATCH_GATEWAY]);
    assert!(calls.iter().all(|r| r["params"][0]["to"] == UNIVERSAL
        && r["params"][0]["gas"] == format!("0x{:x}", rpc::SIM_GAS_CAP)));
    task.abort();
}

/// The provider failing on the avatar read still shows the verified name,
/// but as a partial answer: kept an hour like a negative one, not the
/// found name's day, and marked so a browser keeps it no longer.
#[tokio::test]
async fn a_failed_avatar_read_shows_the_name_as_a_partial_answer_for_an_hour() {
    let outage = json!({"error":{"code": -32603, "message":"provider unavailable"}});
    let address = crate::chain::Hex([0x11; 20]);
    let mock = Arc::new(Mock::default());
    mock.answers
        .lock()
        .unwrap()
        .extend([primary("alice.eth"), outage.clone()]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let first = names.lookup(address).await.unwrap();
    assert_eq!(first.name.as_deref(), Some("alice.eth"));
    assert_eq!(first.avatar, None);
    assert_eq!(json!(first)["partial"], json!(true));
    assert_eq!(lifetime(&first), NEGATIVE_LIFETIME);
    names.lookup(address).await.unwrap();
    assert_eq!(mock.calls.lock().unwrap().len(), 2, "kept, for the hour");
    task.abort();

    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        ok(nameCall::abi_encode_returns(&"alice.base.eth".to_string())),
        registry_entry(),
        forward_record(address.0),
        outage,
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, true));
    let first = names.lookup(address).await.unwrap();
    assert_eq!(first.name.as_deref(), Some("alice.base.eth"));
    assert_eq!(first.avatar, None);
    assert_eq!(lifetime(&first), NEGATIVE_LIFETIME);
    task.abort();

    let whole = Entry {
        name: Some("alice.eth".into()),
        source: Some("ens"),
        avatar: None,
        partial: false,
    };
    assert_eq!(
        lifetime(&whole),
        SUCCESS_LIFETIME,
        "a name with no avatar record is whole"
    );
    assert!(json!(whole).get("partial").is_none());
}

/// ENS wins over basenames. With ENS failing, the basename is shown as a
/// partial answer, kept an hour rather than a day.
#[tokio::test]
async fn a_basename_behind_a_failed_ens_is_a_partial_answer_for_an_hour() {
    let address = crate::chain::Hex([0x11; 20]);
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        json!({"error":{"code": -32000, "message":"provider unavailable"}}),
        ok(nameCall::abi_encode_returns(&"alice.base.eth".to_string())),
        registry_entry(),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(&String::new())),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let mut names = Names::new(
        NamesConfig {
            ethereum_upstream: Some(url.clone()),
            base_upstream: Some(url),
        },
        rpc::Upstream::unlimited(reqwest::Client::new()),
    )
    .unwrap();
    names.pause = Duration::ZERO;
    let names = Arc::new(names);
    let first = names.lookup(address).await.unwrap();
    assert_eq!(first.name.as_deref(), Some("alice.base.eth"));
    assert_eq!(json!(first)["partial"], json!(true));
    assert_eq!(lifetime(&first), NEGATIVE_LIFETIME);
    names.lookup(address).await.unwrap();
    assert_eq!(mock.calls.lock().unwrap().len(), 5, "kept, for the hour");
    task.abort();
}

#[tokio::test]
async fn basename_must_resolve_forward_to_the_same_address() {
    let mock = Arc::new(Mock::default());
    let address = crate::chain::Hex([0x11; 20]);
    mock.answers.lock().unwrap().extend([
        ok(nameCall::abi_encode_returns(&"alice.base.eth".to_string())),
        registry_entry(),
        forward_record([0x22; 20]),
        ok(nameCall::abi_encode_returns(&"alice.base.eth".to_string())),
        registry_entry(),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(
            &"https://example.test/a.png".to_string(),
        )),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = service(url.clone(), true);
    assert!(names.basename(&url, address).await.unwrap().is_none());
    let found = names.basename(&url, address).await.unwrap().unwrap();
    assert_eq!(found.name.as_deref(), Some("alice.base.eth"));
    let calls = mock.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        7,
        "a wrong forward address stops before the avatar read"
    );
    assert_eq!(calls[0]["params"][0]["to"], BASE_REVERSE_REGISTRAR);
    let data = crate::abi::decode_hex(calls[0]["params"][0]["data"].as_str().unwrap()).unwrap();
    assert_eq!(
        nameForAddrCall::abi_decode_validate(&data)
            .unwrap()
            .addr
            .as_slice(),
        address.0
    );
    task.abort();
}

/// The registries accept any string, so a primary name shows only in ENS's
/// normalized form: a capital, an invisible character or a label mixing
/// alphabets could pass for another account's name. Such a record leaves
/// the address nameless, without an avatar or forward read.
#[tokio::test]
async fn only_normalized_primary_names_are_shown() {
    let address = crate::chain::Hex([0x11; 20]);
    for spoof in [
        "Alice.eth",
        "al\u{200d}ice.eth",
        "\u{430}lice.eth",
        "al\u{ad}ice.eth",
    ] {
        let mock = Arc::new(Mock::default());
        mock.answers.lock().unwrap().push_back(primary(spoof));
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(service(url, false));
        assert_eq!(names.lookup(address).await.unwrap().name, None, "{spoof:?}");
        assert_eq!(
            mock.calls.lock().unwrap().len(),
            1,
            "{spoof:?} read further"
        );
        task.abort();

        let based = spoof.replace(".eth", ".base.eth");
        let mock = Arc::new(Mock::default());
        mock.answers
            .lock()
            .unwrap()
            .push_back(ok(nameCall::abi_encode_returns(&based)));
        let (url, task) = endpoint(mock.clone()).await;
        let names = service(url.clone(), true);
        assert!(
            names.basename(&url, address).await.unwrap().is_none(),
            "{based:?}"
        );
        assert_eq!(
            mock.calls.lock().unwrap().len(),
            1,
            "{based:?} read forward"
        );
        task.abort();
    }
    // Normalized names beyond ASCII still show.
    for name in ["\u{1f355}.eth", "日本.eth"] {
        let mock = Arc::new(Mock::default());
        mock.answers
            .lock()
            .unwrap()
            .extend([primary(name), absent()]);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(service(url, false));
        assert_eq!(
            names.lookup(address).await.unwrap().name.as_deref(),
            Some(name)
        );
        task.abort();
    }
}

/// One failure pauses that upstream: the next lookups are unavailable at
/// once, without a call each, until the pause ends.
#[tokio::test]
async fn a_failing_upstream_is_paused_instead_of_asked_once_per_lookup() {
    let mock = Arc::new(Mock::default());
    mock.answers
        .lock()
        .unwrap()
        .extend([json!({"error":{"code": -32005, "message":"rate limited"}})]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(paused_service(url, false));
    for byte in 0x11..0x14u8 {
        let address = crate::chain::Hex([byte; 20]);
        assert_eq!(names.lookup(address).await.unwrap_err(), "name_unavailable");
    }
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        1,
        "only the first lookup reached the upstream"
    );
    task.abort();
}

/// An ABI string whose bytes need not be UTF-8, as a resolver may hold.
fn raw_string(bytes: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 64];
    out[31] = 0x20;
    out[56..64].copy_from_slice(&(bytes.len() as u64).to_be_bytes());
    let mut data = bytes.to_vec();
    data.resize(bytes.len().div_ceil(32) * 32, 0);
    out.extend(data);
    out
}

/// One address's record, however broken, is that address's answer: no
/// name, cached like any other. The next address still reaches the
/// provider, since only the provider failing pauses it.
#[tokio::test]
async fn a_broken_record_leaves_its_address_nameless_without_pausing_the_upstream() {
    let reverted = json!({"error": {"code": -32000, "message": "execution reverted"}});
    // Clients word an execution failure without revert data differently.
    let failed = [
        (
            "a revert in capitals",
            json!({"error": {"code": -32000, "message": "Execution Reverted"}}),
        ),
        (
            "an execution error by code",
            json!({"error": {"code": 3, "message": "execution error"}}),
        ),
        (
            "a call out of its gas",
            json!({"error": {"code": -32000, "message": "out of gas"}}),
        ),
        (
            "gas exhaustion named in data",
            json!({"error": {"code": -32015, "message": "VM execution error.", "data": "OutOfGas"}}),
        ),
    ];
    let ens_cases = [
        ("a primary name that is not a DNS name", primary("bad..eth")),
        ("an unexpected revert", err(vec![0xde, 0xad, 0xbe, 0xef])),
        ("a revert without data", reverted.clone()),
        ("an oversized answer", ok(vec![1; MAX_BYTES + 1])),
        (
            "an answer that does not decode",
            ok(raw_string(&[0xff, 0xfe])),
        ),
    ];
    for (case, answer) in ens_cases.into_iter().chain(failed.clone()) {
        let mock = Arc::new(Mock::default());
        mock.answers
            .lock()
            .unwrap()
            .extend([answer, primary("alice.eth"), absent()]);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(paused_service(url, false));
        let broken = crate::chain::Hex([0x11; 20]);
        let named = crate::chain::Hex([0x22; 20]);
        assert!(names.lookup(broken).await.unwrap().name.is_none(), "{case}");
        let found = names.lookup(named).await.unwrap();
        assert_eq!(found.name.as_deref(), Some("alice.eth"), "{case}");
        assert!(
            names.lookup(broken).await.unwrap().name.is_none(),
            "{case}: cached"
        );
        assert_eq!(mock.calls.lock().unwrap().len(), 3, "{case}");
        task.abort();
    }
    let basename_cases = [
        (
            "a name outside base.eth",
            ok(nameCall::abi_encode_returns(&"alice.eth".to_string())),
        ),
        (
            "a name that is not UTF-8",
            ok(raw_string(&[0xff, 0xfe, 0x2e])),
        ),
        ("a revert without data", reverted),
    ];
    for (case, answer) in basename_cases.into_iter().chain(failed) {
        let mock = Arc::new(Mock::default());
        let named = crate::chain::Hex([0x22; 20]);
        mock.answers.lock().unwrap().extend([
            answer,
            ok(nameCall::abi_encode_returns(&"bob.base.eth".to_string())),
            registry_entry(),
            forward_record(named.0),
            ok(textCall::abi_encode_returns(&String::new())),
        ]);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(paused_service(url, true));
        let broken = crate::chain::Hex([0x11; 20]);
        assert!(names.lookup(broken).await.unwrap().name.is_none(), "{case}");
        let found = names.lookup(named).await.unwrap();
        assert_eq!(found.name.as_deref(), Some("bob.base.eth"), "{case}");
        assert_eq!(mock.calls.lock().unwrap().len(), 5, "{case}");
        task.abort();
    }
}

/// The batch gateway relays what a name's own offchain resolver returned,
/// so an oversized answer leaves that address nameless, not ENS paused.
#[tokio::test]
async fn an_oversized_offchain_answer_does_not_pause_ens() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]),
        primary("alice.eth"),
        absent(),
    ]);
    let gateway = Arc::new(Mock::default());
    gateway
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"data": crate::abi::hex0x(&vec![0; MAX_BYTES])}));
    let (url, task) = endpoint(mock.clone()).await;
    let (gateway_url, gateway_task) = endpoint(gateway.clone()).await;
    let mut names = paused_service(url, false);
    names.gateway_url = gateway_url;
    let names = Arc::new(names);
    let broken = crate::chain::Hex([0x11; 20]);
    assert!(names.lookup(broken).await.unwrap().name.is_none());
    let named = names.lookup(crate::chain::Hex([0x22; 20])).await;
    assert_eq!(named.unwrap().name.as_deref(), Some("alice.eth"));
    task.abort();
    gateway_task.abort();
}

/// Fixed infrastructure returns ABI data, so an empty answer is the
/// node or chain going wrong: that pauses the provider like an outage.
#[tokio::test]
async fn an_empty_answer_is_a_provider_failure() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([ok(Vec::new())]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(paused_service(url, false));
    for byte in 0x11..0x13u8 {
        let address = crate::chain::Hex([byte; 20]);
        assert_eq!(names.lookup(address).await.unwrap_err(), "name_unavailable");
    }
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        1,
        "paused after the first"
    );
    task.abort();
}

/// This host running out of its own call budget says nothing about the
/// provider: the lookup is unavailable, and the provider is not paused.
#[tokio::test]
async fn this_hosts_own_limit_does_not_pause_an_upstream() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([absent()]);
    let (url, task) = endpoint(mock.clone()).await;
    let mut names = Names::new(
        NamesConfig {
            ethereum_upstream: Some(url),
            base_upstream: None,
        },
        // One call a minute: the first lookup spends it.
        rpc::Upstream::new(reqwest::Client::new(), rpc::Budget::new(1, 0)),
    )
    .unwrap();
    names.pause = UPSTREAM_PAUSE;
    let names = Arc::new(names);
    assert!(
        names
            .lookup(crate::chain::Hex([0x11; 20]))
            .await
            .unwrap()
            .name
            .is_none()
    );
    assert_eq!(
        names
            .lookup(crate::chain::Hex([0x22; 20]))
            .await
            .unwrap_err(),
        "name_unavailable"
    );
    assert!(names.paused.lock().unwrap().is_empty());
    task.abort();
}

#[tokio::test]
async fn an_absent_name_is_cached_but_an_outage_is_not() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        json!({"error":{"code": -32005, "message":"https://secret.example/key"}}),
        absent(),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let address = crate::chain::Hex([0x11; 20]);
    assert_eq!(names.lookup(address).await.unwrap_err(), "name_unavailable");
    assert!(names.lookup(address).await.unwrap().name.is_none());
    assert!(names.lookup(address).await.unwrap().name.is_none());
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
    task.abort();
}

fn offchain(sender: &str, urls: Vec<String>) -> Value {
    err(OffchainLookup {
        sender: sender.parse().unwrap(),
        urls,
        callData: vec![1, 2, 3].into(),
        callbackFunction: [0x12, 0x34, 0x56, 0x78].into(),
        extraData: vec![9, 8, 7].into(),
    }
    .abi_encode())
}

#[tokio::test]
async fn offchain_result_returns_to_the_fixed_resolver_for_verification() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]),
        primary("alice.eth"),
        absent(),
    ]);
    let gateway = Arc::new(Mock::default());
    gateway
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"data":"0x040506"}));
    let (url, task) = endpoint(mock.clone()).await;
    let (gateway_url, gateway_task) = endpoint(gateway.clone()).await;
    let mut names = service(url.clone(), false);
    names.gateway_url = gateway_url;
    assert_eq!(
        names
            .ens(&url, crate::chain::Hex([0x11; 20]))
            .await
            .unwrap()
            .unwrap()
            .name
            .as_deref(),
        Some("alice.eth")
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let callback = crate::abi::decode_hex(calls[1]["params"][0]["data"].as_str().unwrap()).unwrap();
    assert_eq!(callback[..4], [0x12, 0x34, 0x56, 0x78]);
    let decoded = callbackCall::abi_decode_raw_validate(&callback[4..]).unwrap();
    assert_eq!(decoded.response.as_ref(), [4, 5, 6]);
    assert_eq!(decoded.extra.as_ref(), [9, 8, 7]);
    assert_eq!(calls[1]["params"][0]["to"], UNIVERSAL);
    assert_eq!(
        gateway.calls.lock().unwrap()[0],
        json!({"sender":UNIVERSAL,"data":"0x010203"})
    );
    task.abort();
    gateway_task.abort();
}

#[tokio::test]
async fn offchain_sender_and_urls_cannot_choose_where_the_host_connects() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        offchain(LEGACY_BASE_RESOLVER, vec![BATCH_GATEWAY.into()]),
        offchain(UNIVERSAL, vec!["http://127.0.0.1/private".into()]),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let mut names = service(url.clone(), false);
    // Reaching the gateway would fail the lookup instead of answering.
    names.gateway_url = "invalid-gateway".into();
    for _ in 0..2 {
        // Refused, so the address has no name this host can verify.
        let found = names
            .ens(&url, crate::chain::Hex([0x11; 20]))
            .await
            .unwrap();
        assert!(found.is_none());
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
    task.abort();
}
#[tokio::test]
async fn offchain_data_is_not_a_name_until_the_contract_accepts_it() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]),
        err(ReverseAddressMismatch {
            primary: "spoof.eth".into(),
            primaryAddress: vec![0x22; 20].into(),
        }
        .abi_encode()),
    ]);
    let gateway = Arc::new(Mock::default());
    gateway
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"data":"0x040506"}));
    let (url, task) = endpoint(mock.clone()).await;
    let (gateway_url, gateway_task) = endpoint(gateway.clone()).await;
    let mut names = service(url.clone(), false);
    names.gateway_url = gateway_url;
    assert!(
        names
            .ens(&url, crate::chain::Hex([0x11; 20]))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
    task.abort();
    gateway_task.abort();
}

#[tokio::test]
async fn excess_distinct_names_are_refused_before_any_upstream_work() {
    let names = Arc::new(service("invalid-upstream".into(), false));
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let mut tasks = Vec::new();
    for byte in 0..MAX_PENDING as u8 {
        let (names, started, release) = (names.clone(), started.clone(), release.clone());
        tasks.push(tokio::spawn(async move {
            names
                .flights
                .run(crate::chain::Hex([byte; 20]), || None, async move {
                    started.add_permits(1);
                    release.acquire().await.unwrap().forget();
                    Ok(Entry::absent())
                })
                .await
        }));
    }
    started
        .acquire_many(MAX_PENDING as u32)
        .await
        .unwrap()
        .forget();
    assert_eq!(
        names
            .lookup(crate::chain::Hex([0xff; 20]))
            .await
            .unwrap_err(),
        "host_busy"
    );
    release.add_permits(MAX_PENDING);
    for task in tasks {
        task.await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn both_lookup_directions_share_capacity_and_release_busy_visitors() {
    let mock = Arc::new(Mock::default());
    let (url, server) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let active = names
        .active
        .clone()
        .acquire_many_owned(MAX_ACTIVE as u32)
        .await
        .unwrap();
    let visitors = Arc::new(tokio::sync::Semaphore::new(2));
    let reverse_permit = Arc::new(visitors.clone().try_acquire_owned().unwrap());
    let forward_permit = Arc::new(visitors.clone().try_acquire_owned().unwrap());
    let address = crate::chain::Hex([0x11; 20]);
    let (reverse, forward) = futures::join!(
        names.lookup_admitted(address, Some(reverse_permit)),
        names.lookup_name("alice.eth".into(), Some(forward_permit)),
    );
    assert_eq!(reverse.unwrap_err(), "host_busy");
    assert_eq!(forward.unwrap_err(), "host_busy");
    assert!(mock.calls.lock().unwrap().is_empty());
    assert_eq!(visitors.available_permits(), 2);
    drop(active);
    mock.answers.lock().unwrap().push_back(absent());
    assert!(names.lookup(address).await.unwrap().name.is_none());
    mock.answers
        .lock()
        .unwrap()
        .push_back(resolved(alloy_primitives::Address::ZERO));
    names.lookup_name("alice.eth".into(), None).await.unwrap();
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
    server.abort();
}

#[test]
fn recipient_names_are_normalized_and_bounded() {
    assert_eq!(forward::checked_name("Alice.ETH").unwrap(), "alice.eth");
    assert_eq!(
        forward::checked_name("Alice.BASE.eth").unwrap(),
        "alice.base.eth"
    );
    for name in [
        "",
        "alice",
        "alice.b",
        "alice.base.e",
        "alice.com",
        "alice..eth",
        "a/b.eth",
        "a\n.eth",
    ] {
        assert!(forward::checked_name(name).is_err(), "{name:?}");
    }
    assert!(forward::checked_name(&format!("{}.eth", "a".repeat(256))).is_err());
}

fn resolved(address: alloy_primitives::Address) -> Value {
    ok(resolveWithGatewaysCall::abi_encode_returns(
        &resolveWithGatewaysReturn {
            result: addrCall::abi_encode_returns(&address).into(),
            resolver: Default::default(),
        },
    ))
}

#[tokio::test]
async fn recipient_ens_uses_fixed_gateway_and_shares_cached_reads() {
    let mock = Arc::new(Mock::default());
    let address = alloy_primitives::Address::from([0x22; 20]);
    mock.answers.lock().unwrap().push_back(resolved(address));
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let name = forward::checked_name("Alice.ETH").unwrap();
    let answers =
        futures::future::join_all((0..10).map(|_| names.lookup_name(name.clone(), None))).await;
    for answer in answers {
        let answer = json!(answer.unwrap());
        assert_eq!(answer["name"], "alice.eth");
        assert_eq!(answer["address"], format!("{address:#x}"));
    }
    names.lookup_name(name, None).await.unwrap();
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["params"][0]["to"], UNIVERSAL);
    let data = crate::abi::decode_hex(calls[0]["params"][0]["data"].as_str().unwrap()).unwrap();
    let call = resolveWithGatewaysCall::abi_decode_validate(&data).unwrap();
    assert_eq!(call.name.as_ref(), dns("alice.eth").unwrap());
    assert_eq!(call.gateways, [BATCH_GATEWAY]);
    assert_eq!(
        addrCall::abi_decode_validate(&call.data).unwrap().node.0,
        node("alice.eth")
    );
    task.abort();
}

#[tokio::test]
async fn recipient_basename_uses_registry_resolver_and_never_ethereum() {
    let mock = Arc::new(Mock::default());
    let resolver = alloy_primitives::Address::from([0x33; 20]);
    let address = alloy_primitives::Address::from([0x22; 20]);
    mock.answers.lock().unwrap().extend([
        ok(addrCall::abi_encode_returns(&resolver)),
        ok(addrCall::abi_encode_returns(&address)),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, true));
    let answer = names
        .lookup_name("alice.base.eth".into(), None)
        .await
        .unwrap();
    assert_eq!(json!(answer)["address"], format!("{address:#x}"));
    assert!(names.lookup_name("alice.eth".into(), None).await.is_err());
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0]["params"][0]["to"],
        "0xb94704422c2a1e396835a571837aa5ae53285a95"
    );
    assert_eq!(calls[1]["params"][0]["to"], format!("{resolver:#x}"));
    task.abort();
}

#[tokio::test]
async fn recipient_missing_zero_and_malformed_answers_never_become_addresses() {
    for response in [
        absent(),
        resolved(alloy_primitives::Address::ZERO),
        ok(vec![0; 3]),
    ] {
        let mock = Arc::new(Mock::default());
        mock.answers.lock().unwrap().push_back(response);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(service(url, false));
        assert!(
            json!(names.lookup_name("alice.eth".into(), None).await.unwrap())["address"].is_null()
        );
        names.lookup_name("alice.eth".into(), None).await.unwrap();
        assert_eq!(
            mock.calls.lock().unwrap().len(),
            1,
            "absent answers share the short cache"
        );
        task.abort();
    }
}

#[tokio::test]
async fn recipient_failure_is_not_cached_as_absence() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        json!({"error": {"code": -32000, "message": "upstream unavailable"}}),
        resolved(alloy_primitives::Address::from([0x22; 20])),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    assert!(names.lookup_name("alice.eth".into(), None).await.is_err());
    assert!(
        json!(names.lookup_name("alice.eth".into(), None).await.unwrap())["address"].is_string()
    );
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
    task.abort();
}

#[tokio::test]
async fn recipient_lookup_keeps_visitor_slot_after_disconnect() {
    let mock = Arc::new(Mock::default());
    mock.answers
        .lock()
        .unwrap()
        .push_back(resolved(alloy_primitives::Address::from([0x22; 20])));
    mock.blocked
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let (url, server) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, false));
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = Arc::new(slots.clone().try_acquire_owned().unwrap());
    let worker = names.clone();
    let reader =
        tokio::spawn(async move { worker.lookup_name("alice.eth".into(), Some(permit)).await });
    mock.entered.notified().await;
    reader.abort();
    let _ = reader.await;
    assert_eq!(slots.available_permits(), 0);
    mock.release.notify_one();
    names.lookup_name("alice.eth".into(), None).await.unwrap();
    assert_eq!(slots.available_permits(), 1);
    server.abort();
}

#[tokio::test]
async fn basename_display_uses_the_names_current_resolver() {
    let mock = Arc::new(Mock::default());
    let address = crate::chain::Hex([0x11; 20]);
    let resolver = alloy_primitives::Address::from([0x33; 20]);
    mock.answers.lock().unwrap().extend([
        modern_name("alice.base.eth"),
        ok(addrCall::abi_encode_returns(&resolver)),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(
            &"https://example.test/avatar.png".to_string(),
        )),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, true));
    let answers = futures::future::join_all((0..10).map(|_| names.lookup(address))).await;
    assert!(
        answers
            .iter()
            .all(|answer| answer.as_ref().unwrap().name.as_deref() == Some("alice.base.eth"))
    );
    let found = answers.into_iter().next().unwrap().unwrap();
    assert_eq!(found.name.as_deref(), Some("alice.base.eth"));
    assert_eq!(
        found.avatar.as_deref(),
        Some("https://example.test/avatar.png")
    );
    names.lookup(address).await.unwrap();
    let calls = mock.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        4,
        "modern primary requires no legacy read; verified answer is cached"
    );
    assert_eq!(calls[0]["params"][0]["to"], BASE_REVERSE_REGISTRAR);
    let data = crate::abi::decode_hex(calls[0]["params"][0]["data"].as_str().unwrap()).unwrap();
    assert_eq!(
        nameForAddrCall::abi_decode_validate(&data)
            .unwrap()
            .addr
            .as_slice(),
        address.0
    );
    assert_eq!(
        calls[1]["params"][0]["to"],
        "0xb94704422c2a1e396835a571837aa5ae53285a95"
    );
    for call in &calls[2..] {
        assert_eq!(call["params"][0]["to"], format!("{resolver:#x}"));
    }
    task.abort();
}

#[tokio::test]
async fn basename_with_only_a_legacy_primary_still_uses_the_current_resolver() {
    let mock = Arc::new(Mock::default());
    let address = crate::chain::Hex([0x11; 20]);
    mock.answers.lock().unwrap().extend([
        ok(nameForAddrCall::abi_encode_returns(&String::new())),
        ok(nameCall::abi_encode_returns(&"alice.base.eth".to_string())),
        registry_entry(),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(&String::new())),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, true));
    assert_eq!(
        names.lookup(address).await.unwrap().name.as_deref(),
        Some("alice.base.eth")
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 5);
    assert_eq!(calls[0]["params"][0]["to"], BASE_REVERSE_REGISTRAR);
    assert_eq!(calls[1]["params"][0]["to"], LEGACY_BASE_RESOLVER);
    let data = crate::abi::decode_hex(calls[1]["params"][0]["data"].as_str().unwrap()).unwrap();
    assert_eq!(
        nameCall::abi_decode_validate(&data).unwrap().node.0,
        node(&format!("{}.80002105.reverse", "11".repeat(20)))
    );
    for call in &calls[3..] {
        assert_eq!(call["params"][0]["to"], format!("0x{}", "33".repeat(20)));
    }
    task.abort();
}

/// Registry choices and resolver answers are controlled by name owners. Bad
/// records must not pause Base for other accounts or trigger offchain requests.
#[tokio::test]
async fn custom_basename_resolver_failures_stay_local_to_the_name() {
    let resolver = alloy_primitives::Address::from([0x33; 20]);
    let registry = ok(resolverCall::abi_encode_returns(&resolver));
    let modern = modern_name("alice.base.eth");
    let address = crate::chain::Hex([0x11; 20]);
    let mut cases = vec![
        vec![ok(resolverCall::abi_encode_returns(
            &alloy_primitives::Address::ZERO,
        ))],
        vec![ok(vec![0xff; 32])],
        vec![ok(vec![0; MAX_BYTES + 1])],
        vec![registry.clone(), ok(Vec::new())],
        vec![registry.clone(), ok(vec![0xff; 32])],
        vec![registry.clone(), ok(vec![0; MAX_BYTES + 1])],
        vec![registry.clone(), err(vec![0xde, 0xad, 0xbe, 0xef])],
        vec![
            registry.clone(),
            json!({"error":{"code":3,"message":"out of gas"}}),
        ],
        vec![
            registry.clone(),
            offchain(
                &format!("{resolver:#x}"),
                vec!["http://127.0.0.1/private".into()],
            ),
        ],
    ];
    cases.extend(BROKEN.map(|message| vec![registry.clone(), broken(message)]));
    for answers in cases {
        let mock = Arc::new(Mock::default());
        let expected_calls = 1 + answers.len() + 4;
        mock.answers.lock().unwrap().push_back(modern.clone());
        mock.answers.lock().unwrap().extend(answers);
        mock.answers.lock().unwrap().extend([
            modern.clone(),
            registry.clone(),
            forward_record([0x22; 20]),
            ok(textCall::abi_encode_returns(&String::new())),
        ]);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(paused_service(url, true));
        assert!(names.lookup(address).await.unwrap().name.is_none());
        assert!(
            names.lookup(address).await.unwrap().name.is_none(),
            "negative cached"
        );
        let found = names.lookup(crate::chain::Hex([0x22; 20])).await.unwrap();
        assert_eq!(found.name.as_deref(), Some("alice.base.eth"));
        let calls = mock.calls.lock().unwrap();
        assert_eq!(calls.len(), expected_calls);
        for call in calls.iter() {
            assert_eq!(call["method"], "eth_call");
            assert_eq!(
                call["params"][0]["gas"],
                format!("0x{:x}", rpc::SIM_GAS_CAP)
            );
        }
        task.abort();
    }
}

#[tokio::test]
async fn basename_provider_failure_at_each_step_is_not_cached_as_no_name() {
    let modern = modern_name("alice.base.eth");
    let registry = registry_entry();
    let outage = json!({"error":{"code":-32005,"message":"rate limited"}});
    for prefix in [
        vec![],
        vec![modern.clone()],
        vec![modern, registry],
        vec![ok(nameForAddrCall::abi_encode_returns(&String::new()))],
    ] {
        let mock = Arc::new(Mock::default());
        let count = prefix.len() + 1;
        mock.answers.lock().unwrap().extend(prefix);
        mock.answers.lock().unwrap().push_back(outage.clone());
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(paused_service(url, true));
        let address = crate::chain::Hex([0x11; 20]);
        assert_eq!(names.lookup(address).await.unwrap_err(), "name_unavailable");
        assert!(names.cache.get(&address).is_none());
        assert_eq!(
            names
                .lookup(crate::chain::Hex([0x22; 20]))
                .await
                .unwrap_err(),
            "name_unavailable"
        );
        assert_eq!(mock.calls.lock().unwrap().len(), count, "provider paused");
        task.abort();
    }
}

#[tokio::test]
async fn empty_custom_resolver_is_also_a_missing_recipient_not_a_base_outage() {
    let mock = Arc::new(Mock::default());
    mock.answers
        .lock()
        .unwrap()
        .extend([registry_entry(), ok(Vec::new())]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(paused_service(url, true));
    let answer = names
        .lookup_name("alice.base.eth".into(), None)
        .await
        .unwrap();
    assert!(json!(answer)["address"].is_null());
    assert!(names.paused.lock().unwrap().is_empty());
    task.abort();
}

/// The extra resolver read stays inside the same detached flight and deadline.
/// Neither a disconnected reader nor a slow resolver releases permits early.
async fn slow_custom_resolver(expire: bool) {
    let mock = Arc::new(Mock::default());
    let address = crate::chain::Hex([0x11; 20]);
    mock.answers.lock().unwrap().extend([
        modern_name("alice.base.eth"),
        registry_entry(),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(&String::new())),
    ]);
    mock.block_at.store(3, std::sync::atomic::Ordering::Relaxed);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(service(url, true));
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = Arc::new(slots.clone().try_acquire_owned().unwrap());
    let worker = names.clone();
    let reader = tokio::spawn(async move { worker.lookup_admitted(address, Some(permit)).await });
    mock.entered.notified().await;
    reader.abort();
    let _ = reader.await;
    assert_eq!(slots.available_permits(), 0);
    assert_eq!(names.active.available_permits(), MAX_ACTIVE - 1);
    if expire {
        // The whole lookup must stop even while its resolver has not answered.
        let _returned = tokio::time::timeout(Duration::from_secs(32), slots.acquire())
            .await
            .expect("lookup exceeded its deadline")
            .unwrap();
        assert!(names.cache.get(&address).is_none());
        assert_eq!(
            mock.calls.lock().unwrap().len(),
            3,
            "no avatar after deadline"
        );
    } else {
        mock.release.notify_one();
        assert_eq!(
            names.lookup(address).await.unwrap().name.as_deref(),
            Some("alice.base.eth")
        );
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(mock.calls.lock().unwrap().len(), 4);
    }
    assert_eq!(names.active.available_permits(), MAX_ACTIVE);
    task.abort();
}

#[tokio::test]
async fn disconnected_custom_resolver_reader_keeps_its_slots() {
    slow_custom_resolver(false).await;
}

#[tokio::test]
#[ignore = "slow: exercises the real 30-second name lookup deadline"]
async fn custom_resolver_cannot_extend_the_lookup_deadline() {
    slow_custom_resolver(true).await;
}

/// A resolver that breaks on the avatar leaves the verified name in place, but
/// the entry is partial, kept an hour rather than a day, and nothing pauses.
#[tokio::test]
async fn a_broken_avatar_keeps_the_verified_name_as_a_partial_answer() {
    let mock = Arc::new(Mock::default());
    let address = crate::chain::Hex([0x11; 20]);
    mock.answers.lock().unwrap().extend([
        modern_name("alice.base.eth"),
        registry_entry(),
        forward_record(address.0),
        broken("EVM error: InvalidFEOpcode"),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(paused_service(url, true));
    let entry = names.lookup(address).await.unwrap();
    assert_eq!(entry.name.as_deref(), Some("alice.base.eth"));
    assert!(entry.avatar.is_none());
    assert!(entry.partial, "kept an hour, not a day");
    assert_eq!(lifetime(&entry), NEGATIVE_LIFETIME);
    assert!(names.paused.lock().unwrap().is_empty());
    task.abort();
}

/// A recipient whose resolver breaks has no address, and a different name
/// still resolves after it: Base is not paused.
#[tokio::test]
async fn a_broken_resolver_is_a_missing_recipient_and_other_names_still_resolve() {
    let mock = Arc::new(Mock::default());
    let registry = registry_entry();
    mock.answers.lock().unwrap().extend([
        registry.clone(),
        broken("EVM error: PrecompileError"),
        registry,
        forward_record([0x22; 20]),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(paused_service(url, true));
    let missing = names
        .lookup_name("mallory.base.eth".into(), None)
        .await
        .unwrap();
    assert!(json!(missing)["address"].is_null());
    let found = names
        .lookup_name("alice.base.eth".into(), None)
        .await
        .unwrap();
    assert_eq!(json!(found)["address"], format!("0x{}", "22".repeat(20)));
    assert!(names.paused.lock().unwrap().is_empty());
    task.abort();
}

/// The other side of the boundary: an error that is not a recognized way for
/// a name's own resolver to break is still the provider failing. So is a
/// recognized one from fixed infrastructure, such as the reverse registrar.
/// Each pauses Base, for names and recipients alike, and is never cached as a
/// missing name.
#[tokio::test]
async fn other_errors_still_pause_base() {
    let registry = registry_entry();
    let modern = modern_name("alice.base.eth");
    let at_resolver = |outage: Value| vec![modern.clone(), registry.clone(), outage];
    let cases = [
        at_resolver(broken("EVM error: SomethingNew")),
        at_resolver(broken("EVM error: InvalidFEOpcode at pc 0")),
        at_resolver(broken("insufficient funds for gas")),
        at_resolver(json!({"error": {"code": -32000, "message": "EVM error: InvalidFEOpcode"}})),
        at_resolver(json!({"error": {"code": -32603, "message": "internal error"}})),
        at_resolver(json!("neither a result nor an error")),
        vec![broken("EVM error: InvalidFEOpcode")],
    ];
    for answers in cases {
        let calls = answers.len();
        let outage = answers.last().unwrap().clone();
        let mock = Arc::new(Mock::default());
        mock.answers.lock().unwrap().extend(answers);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(paused_service(url, true));
        let address = crate::chain::Hex([0x11; 20]);
        assert_eq!(
            names.lookup(address).await.unwrap_err(),
            "name_unavailable",
            "{outage}"
        );
        assert!(names.cache.get(&address).is_none(), "{outage}");
        let recipient = names.lookup_name("bob.base.eth".into(), None).await;
        assert_eq!(recipient.unwrap_err(), "name_unavailable", "{outage}");
        assert_eq!(mock.calls.lock().unwrap().len(), calls, "paused: {outage}");
        task.abort();
    }
    // A provider that cannot be reached at all pauses too.
    let names = Arc::new(paused_service("http://127.0.0.1:1".into(), true));
    assert!(names.lookup(crate::chain::Hex([0x11; 20])).await.is_err());
    assert!(!names.paused.lock().unwrap().is_empty());
}

/// A service whose ENS lookups go through `gateway` and give up on it quickly,
/// as the production client gives up after ten seconds. With `basenames`, it
/// also asks Base, through the same endpoint.
fn gateway_service(url: String, gateway: String, basenames: bool) -> Names {
    let mut names = Names::new(
        NamesConfig {
            ethereum_upstream: Some(url.clone()),
            base_upstream: basenames.then_some(url),
        },
        rpc::Upstream::unlimited(reqwest::Client::new()),
    )
    .unwrap();
    names.gateway_url = gateway;
    names.offchain = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    names
}

/// A gateway that never answers, as ENS's does for 30 seconds while a name's
/// own offchain server is slow.
async fn hanging_gateway() -> (String, tokio::task::JoinHandle<()>) {
    let gateway = Arc::new(Mock::default());
    gateway
        .blocked
        .store(true, std::sync::atomic::Ordering::Relaxed);
    endpoint(gateway).await
}

/// A gateway that sends its headers, then stops partway through the body.
async fn stalling_gateway() -> (String, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut request = [0u8; 4096];
                let _ = socket.read(&mut request).await;
                let head = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{";
                let _ = socket.write_all(head.as_bytes()).await;
                std::future::pending::<()>().await;
            });
        }
    });
    (url, task)
}

/// A name's offchain server that outlasts the host's wait for the gateway is
/// that name's failure, whether the wait ends before the gateway's answer
/// starts or partway through it. The address gets no ENS name, kept an hour,
/// and ENS is not paused for anyone else.
#[tokio::test]
async fn a_slow_offchain_server_leaves_its_address_nameless_without_pausing_ens() {
    for (label, (gateway_url, gateway_task)) in [
        ("no answer", hanging_gateway().await),
        ("stalled body", stalling_gateway().await),
    ] {
        let mock = Arc::new(Mock::default());
        mock.answers.lock().unwrap().extend([
            offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]),
            primary("alice.eth"),
            absent(),
        ]);
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(gateway_service(url, gateway_url, false));
        let slow = crate::chain::Hex([0x11; 20]);
        let entry = names.lookup(slow).await.unwrap();
        assert!(entry.name.is_none(), "{label}");
        assert_eq!(lifetime(&entry), NEGATIVE_LIFETIME, "{label}");
        assert!(
            names.lookup(slow).await.unwrap().name.is_none(),
            "cached: {label}"
        );
        let named = names.lookup(crate::chain::Hex([0x22; 20])).await;
        assert_eq!(named.unwrap().name.as_deref(), Some("alice.eth"), "{label}");
        assert!(names.paused.lock().unwrap().is_empty(), "{label}");
        assert_eq!(mock.calls.lock().unwrap().len(), 3, "{label}");
        task.abort();
        gateway_task.abort();
    }
}

/// On the avatar, the same slowness leaves the verified name partial.
#[tokio::test]
async fn a_slow_offchain_avatar_keeps_the_verified_name_as_a_partial_answer() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        primary("alice.eth"),
        offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let (gateway_url, gateway_task) = hanging_gateway().await;
    let names = Arc::new(gateway_service(url, gateway_url, false));
    let entry = names.lookup(crate::chain::Hex([0x11; 20])).await.unwrap();
    assert_eq!(entry.name.as_deref(), Some("alice.eth"));
    assert!(entry.avatar.is_none());
    assert!(entry.partial, "kept an hour, not a day");
    assert!(names.paused.lock().unwrap().is_empty());
    task.abort();
    gateway_task.abort();
}

/// A recipient name whose offchain server is slow has no address, and a
/// different name still resolves after it.
#[tokio::test]
async fn a_slow_offchain_recipient_has_no_address_and_other_names_still_resolve() {
    let mock = Arc::new(Mock::default());
    let address = alloy_primitives::Address::from([0x22; 20]);
    mock.answers.lock().unwrap().extend([
        offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]),
        resolved(address),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let (gateway_url, gateway_task) = hanging_gateway().await;
    let names = Arc::new(gateway_service(url, gateway_url, false));
    let missing = names.lookup_name("mallory.eth".into(), None).await.unwrap();
    assert!(json!(missing)["address"].is_null());
    let found = names.lookup_name("alice.eth".into(), None).await.unwrap();
    assert_eq!(json!(found)["address"], format!("{address:#x}"));
    assert!(names.paused.lock().unwrap().is_empty());
    task.abort();
    gateway_task.abort();
}

/// The gateway itself failing is still an outage: a refused connection, an
/// error status or an answer without data pauses ENS, for names and
/// recipients alike, and nothing is cached.
#[tokio::test]
async fn a_failing_gateway_still_pauses_ens() {
    async fn status_gateway() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let app = Router::new().route(
                "/",
                post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        (url, task)
    }
    let no_data = Arc::new(Mock::default());
    no_data
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"error": "busy"}));
    let gateways = [
        ("refused", ("http://127.0.0.1:1".to_string(), None)),
        ("error status", {
            let (url, task) = status_gateway().await;
            (url, Some(task))
        }),
        ("no data", {
            let (url, task) = endpoint(no_data).await;
            (url, Some(task))
        }),
    ];
    for (label, (gateway_url, gateway_task)) in gateways {
        let mock = Arc::new(Mock::default());
        mock.answers
            .lock()
            .unwrap()
            .push_back(offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]));
        let (url, task) = endpoint(mock.clone()).await;
        let names = Arc::new(gateway_service(url, gateway_url, false));
        let address = crate::chain::Hex([0x11; 20]);
        assert_eq!(
            names.lookup(address).await.unwrap_err(),
            "name_unavailable",
            "{label}"
        );
        assert!(names.cache.get(&address).is_none(), "{label}");
        let recipient = names.lookup_name("bob.eth".into(), None).await;
        assert_eq!(recipient.unwrap_err(), "name_unavailable", "{label}");
        assert_eq!(mock.calls.lock().unwrap().len(), 1, "paused: {label}");
        task.abort();
        if let Some(gateway_task) = gateway_task {
            gateway_task.abort();
        }
    }
}

/// A timeout on ENS is still a failure for the answer that follows it: a
/// basename shown in its place is partial, kept an hour rather than a day, so
/// an ENS name that answers again soon is not hidden behind it. Without a
/// basename, the address has no name for the hour. ENS is not paused either way.
#[tokio::test]
async fn a_slow_offchain_server_makes_the_basename_shown_instead_partial() {
    let address = crate::chain::Hex([0x11; 20]);
    let slow = offchain(UNIVERSAL, vec![BATCH_GATEWAY.into()]);
    let basename = vec![
        modern_name("alice.base.eth"),
        registry_entry(),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(&String::new())),
    ];
    let no_basename = vec![
        ok(nameForAddrCall::abi_encode_returns(&String::new())),
        ok(nameCall::abi_encode_returns(&String::new())),
    ];
    for (expected, base_answers) in [(Some("alice.base.eth"), basename), (None, no_basename)] {
        let mock = Arc::new(Mock::default());
        let calls = 1 + base_answers.len();
        mock.answers.lock().unwrap().push_back(slow.clone());
        mock.answers.lock().unwrap().extend(base_answers);
        mock.answers
            .lock()
            .unwrap()
            .extend([primary("bob.eth"), absent()]);
        let (url, task) = endpoint(mock.clone()).await;
        let (gateway_url, gateway_task) = hanging_gateway().await;
        let names = Arc::new(gateway_service(url, gateway_url, true));
        let entry = names.lookup(address).await.unwrap();
        assert_eq!(entry.name.as_deref(), expected);
        assert_eq!(lifetime(&entry), NEGATIVE_LIFETIME, "{expected:?}");
        if expected.is_some() {
            assert!(entry.partial, "an hour, not a day");
        }
        names.lookup(address).await.unwrap();
        assert_eq!(
            mock.calls.lock().unwrap().len(),
            calls,
            "cached: {expected:?}"
        );
        let other = names.lookup(crate::chain::Hex([0x22; 20])).await.unwrap();
        assert_eq!(other.name.as_deref(), Some("bob.eth"), "ENS not paused");
        assert!(names.paused.lock().unwrap().is_empty());
        task.abort();
        gateway_task.abort();
    }
}

#[tokio::test]
async fn throttle_before_ens_does_not_cache_a_basename_or_pause_names() {
    let address = crate::chain::Hex([0x11; 20]);
    let ens = Arc::new(Mock::default());
    ens.answers
        .lock()
        .unwrap()
        .push_back(json!({"error":{"code":-32005,"message":"rate limited"}}));
    let base = Arc::new(Mock::default());
    base.answers.lock().unwrap().extend([
        modern_name("alice.base.eth"),
        registry_entry(),
        forward_record(address.0),
        ok(textCall::abi_encode_returns(&String::new())),
    ]);
    let (ens_url, ens_task) = endpoint(ens.clone()).await;
    let (base_url, base_task) = endpoint(base.clone()).await;
    let names = Arc::new(
        Names::new(
            NamesConfig {
                ethereum_upstream: Some(ens_url),
                base_upstream: Some(base_url),
            },
            rpc::Upstream::unlimited(reqwest::Client::new()),
        )
        .unwrap(),
    );
    assert!(names.lookup(address).await.is_err());
    assert!(names.cached(address).is_none());
    assert!(names.paused.lock().unwrap().is_empty());
    assert!(
        base.calls.lock().unwrap().is_empty(),
        "do not cache a fallback for congestion"
    );
    names
        .client
        .expire_cooldown(names.ethereum.as_ref().unwrap());
    ens.answers.lock().unwrap().extend([
        primary("alice.eth"),
        ok(resolveWithGatewaysCall::abi_encode_returns(
            &resolveWithGatewaysReturn {
                result: textCall::abi_encode_returns(&String::new()).into(),
                resolver: Default::default(),
            },
        )),
    ]);
    assert_eq!(
        names.lookup(address).await.unwrap().name.as_deref(),
        Some("alice.eth")
    );
    ens_task.abort();
    base_task.abort();
}

#[tokio::test]
async fn throttle_or_busy_on_an_avatar_does_not_cache_a_partial_name() {
    let address = crate::chain::Hex([0x11; 20]);
    for base in [false, true] {
        for busy in [false, true] {
            let mock = Arc::new(Mock::default());
            let records = if base {
                vec![
                    modern_name("alice.base.eth"),
                    registry_entry(),
                    forward_record(address.0),
                ]
            } else {
                vec![primary("alice.eth")]
            };
            let client = if busy {
                // Names may spend half the bucket: exactly enough to reach the avatar.
                rpc::Upstream::new(
                    reqwest::Client::new(),
                    rpc::Budget::new(records.len() as u32 * 2, 0),
                )
            } else {
                rpc::Upstream::unlimited(reqwest::Client::new())
            };
            mock.answers.lock().unwrap().extend(records.clone());
            if !busy {
                mock.answers
                    .lock()
                    .unwrap()
                    .push_back(json!({"error":{"code":-32005,"message":"rate limited"}}));
            }
            let (url, task) = endpoint(mock.clone()).await;
            let mut names = paused_service(url, base);
            names.client = client;
            let names = Arc::new(names);
            assert!(
                names.lookup(address).await.is_err(),
                "base={base}, busy={busy}"
            );
            assert!(names.cached(address).is_none());
            assert!(names.paused.lock().unwrap().is_empty());
            if !busy {
                names.client.expire_cooldown(if base {
                    names.base.as_ref().unwrap()
                } else {
                    names.ethereum.as_ref().unwrap()
                });
                let avatar =
                    textCall::abi_encode_returns(&"https://example.org/avatar.png".to_string());
                let answer = if base {
                    ok(avatar)
                } else {
                    ok(resolveWithGatewaysCall::abi_encode_returns(
                        &resolveWithGatewaysReturn {
                            result: avatar.into(),
                            resolver: Default::default(),
                        },
                    ))
                };
                mock.answers.lock().unwrap().extend(records);
                mock.answers.lock().unwrap().push_back(answer);
                let found = names.lookup(address).await.unwrap();
                assert_eq!(
                    found.avatar.as_deref(),
                    Some("https://example.org/avatar.png")
                );
                assert!(!found.partial);
            }
            task.abort();
        }
    }
}

#[tokio::test]
async fn busy_before_ens_does_not_fall_back_to_a_basename() {
    let mock = Arc::new(Mock::default());
    let (url, task) = endpoint(mock.clone()).await;
    let budget = rpc::Budget::new(1, 0);
    budget.take(rpc::Class::Client).await.unwrap();
    let names = Arc::new(
        Names::new(
            NamesConfig {
                ethereum_upstream: Some(url.clone()),
                base_upstream: Some(url),
            },
            rpc::Upstream::new(reqwest::Client::new(), budget),
        )
        .unwrap(),
    );
    let address = crate::chain::Hex([0x11; 20]);
    assert!(names.lookup(address).await.is_err());
    assert!(names.cached(address).is_none());
    assert!(names.paused.lock().unwrap().is_empty());
    assert!(mock.calls.lock().unwrap().is_empty());
    task.abort();
}

#[tokio::test]
async fn throttle_recipient_recovers_without_a_name_pause_or_negative_cache() {
    let mock = Arc::new(Mock::default());
    mock.answers.lock().unwrap().extend([
        json!({"error":{"code":-32005,"message":"project ID request rate exceeded"}}),
        resolved(alloy_primitives::Address::from([0x22; 20])),
    ]);
    let (url, task) = endpoint(mock.clone()).await;
    let names = Arc::new(paused_service(url.clone(), false));
    assert!(names.lookup_name("alice.eth".into(), None).await.is_err());
    assert!(names.paused.lock().unwrap().is_empty());
    assert!(names.forward_cache.get(&"alice.eth".to_string()).is_none());
    names.client.expire_cooldown(&url);
    assert!(
        json!(names.lookup_name("alice.eth".into(), None).await.unwrap())["address"].is_string()
    );
    task.abort();
}
