use super::{follow::*, model::*, *};
use crate::block_logs::Unreadable;
// The HTTP routes are exercised here, where the model can be built directly.
use crate::chain::{Checkpoint, Hash};
use crate::journal::CHECKPOINT_BLOCKS;
use crate::keyring_http::{HoldingPair, HoldingsQuery, TradesQuery, holdings, lookup, trades};
use crate::verifier::Verifier;
use crate::{
    abi::{address_word, hex0x, number_word},
    chain::Hex,
    config::{ChainConfig, Config as HostConfig},
    mock_node::{self, Node},
};
use crate::{
    http::{JsonPath, JsonQuery},
    server::AppState,
};
use axum::{extract::State, http::StatusCode};
use commonware_runtime::{
    Clock as _, Runner as _, Supervisor as _, deterministic, tokio as cw_tokio,
};
use commonware_storage::Context as StorageContext;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};

/// The deployed market can't change, so its ABI is kept here as test data, copied from the
/// build of https://github.com/blohard/account-keys that is verified on BaseScan.
#[test]
fn event_layouts_match_contract_abi() {
    let abi: Value =
        serde_json::from_str(include_str!("../../testdata/AccountKeys.abi.json")).unwrap();
    for (name, signature, fields) in [
        (
            "Activated",
            activation_topic(),
            vec![("creator", "address", true)],
        ),
        (
            "Trade",
            topic(),
            vec![
                ("creator", "address", true),
                ("trader", "address", true),
                ("isBuy", "bool", false),
                ("amount", "uint256", false),
                ("curveValue", "uint256", false),
                ("creatorFee", "uint256", false),
                ("devFee", "uint256", false),
                ("newSupply", "uint256", false),
            ],
        ),
        (
            "KeysTransferred",
            transfer_topic(),
            vec![
                ("creator", "address", true),
                ("from", "address", true),
                ("to", "address", true),
                ("amount", "uint256", false),
                ("referenceValue", "uint256", false),
                ("creatorFee", "uint256", false),
                ("devFee", "uint256", false),
            ],
        ),
    ] {
        let event = abi
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == "event" && e["name"] == name)
            .unwrap();
        assert_eq!(event["anonymous"], false, "{name}");
        let inputs = event["inputs"].as_array().unwrap();
        let actual: Vec<_> = inputs
            .iter()
            .map(|i| {
                (
                    i["name"].as_str().unwrap(),
                    i["type"].as_str().unwrap(),
                    i["indexed"].as_bool().unwrap(),
                )
            })
            .collect();
        assert_eq!(actual, fields, "{name} indexed fields and data word order");
        let types = actual.iter().map(|f| f.1).collect::<Vec<_>>().join(",");
        assert_eq!(
            signature,
            crate::keccak::hash(&[format!("{name}({types})").as_bytes()]).to_hex(),
            "{name} topic"
        );
    }
}

const MARKET: &str = "0x000000000000000000000000000000000000aaaa";
const CREATOR: Address = Hex([1; 20]);
fn config() -> Config {
    Config {
        chain: 84532,
        market: MARKET.parse().unwrap(),
        start_block: 10,
    }
}
fn log(block: u64, ordinal: u64, holder: Address, buy: bool, amount: u64, supply: u64) -> Value {
    let mut data = Vec::new();
    for n in [u64::from(buy), amount, 0, 0, 0, supply] {
        data.extend(number_word(n));
    }
    json!({"address":MARKET,"topics":[topic(),hex0x(&address_word(CREATOR)),hex0x(&address_word(holder))],"data":hex0x(&data),"blockNumber":format!("0x{block:x}"),"logIndex":format!("0x{ordinal:x}"),"blockHash":format!("0x{:064x}",2000+block),"transactionHash":format!("0x{:064x}",3000+block),"removed":false})
}
fn transfer_log(block: u64, ordinal: u64, from: Address, to: Address, amount: u64) -> Value {
    let mut event = log(block, ordinal, from, true, 1, 1);
    event["topics"] = json!([
        transfer_topic(),
        hex0x(&address_word(CREATOR)),
        hex0x(&address_word(from)),
        hex0x(&address_word(to))
    ]);
    let data: Vec<u8> = [amount, 0, 0, 0]
        .into_iter()
        .flat_map(number_word)
        .collect();
    event["data"] = json!(hex0x(&data));
    event
}
fn trade(holder: Address, buy: bool, amount: u64, supply: u64) -> Event {
    Event::Trade(Trade {
        creator: CREATOR,
        holder,
        buy,
        amount,
        supply,
    })
}
fn transfer(from: Address, to: Address, amount: u64) -> Event {
    Event::Transfer(Transfer {
        creator: CREATOR,
        from,
        to,
        amount,
    })
}
fn activated() -> Event {
    Event::Activated { creator: CREATOR }
}
/// A model whose creator is activated, checkpointed at the config's start.
fn blank() -> Model {
    Model {
        checkpoint: Some(Checkpoint {
            block: 10,
            hash: Hash::ZERO,
        }),
        supplies: BTreeMap::from([(CREATOR, 0)]),
        ..Model::default()
    }
}
fn at(block: u64, log: u32, event: Event) -> Logged {
    Logged {
        position: Position { block, log },
        transaction: Hex([block as u8; 32]),
        timestamp: 1_000 + block as u32,
        event,
    }
}
fn range(from: u64, through: u64, events: Vec<Logged>) -> Range {
    Range {
        from,
        through: Checkpoint {
            block: through,
            hash: Hex([through as u8; 32]),
        },
        events,
    }
}
fn ev(model: &mut Model, event: Event) -> Result<()> {
    step(model, &event)
}

#[test]
fn transfer_updates_both_feeds_and_preserves_supply() {
    let from = Hex([2; 20]);
    let to = Hex([3; 20]);
    let mut s = blank();
    ev(&mut s, trade(from, true, 2, 2)).unwrap();
    let event = transfer_log(10, 1, from, to, 2);
    let Event::Transfer(decoded) = decode_event(&event, &config()).unwrap() else {
        panic!("expected a transfer")
    };
    assert_eq!(decoded.amount, 2);
    ev(&mut s, Event::Transfer(decoded.clone())).unwrap();
    assert_eq!(s.supplies[&CREATOR], 2);
    assert_eq!(s.authors(from).1, vec![]);
    assert_eq!(s.authors(to).1, vec![CREATOR]);
    assert_eq!(s.authors(CREATOR).0, vec![to]);
    let before = serde_json::to_vec(&s).unwrap();
    let held_by = s.held_by.clone();
    assert!(
        ev(&mut s, Event::Transfer(decoded))
            .unwrap_err()
            .is::<Fatal>()
    );
    assert_eq!(serde_json::to_vec(&s).unwrap(), before);
    assert_eq!(s.held_by, held_by);
    ev(&mut s, trade(to, false, 2, 0)).unwrap();
    assert!(s.holdings.is_empty());
    assert!(s.held_by.is_empty());
}

/// Validation reads through an overlay: later events see earlier ones in
/// the same range, and a failure anywhere leaves the model untouched.
#[test]
fn a_range_is_checked_whole_without_changing_the_model() {
    let a = Hex([2; 20]);
    let b = Hex([3; 20]);
    let mut model = blank();
    let dependent = range(
        11,
        11,
        vec![
            at(11, 0, trade(a, true, 2, 2)),
            at(11, 1, transfer(a, b, 1)),
            at(11, 2, trade(b, false, 1, 1)),
        ],
    );
    let before = serde_json::to_vec(&model).unwrap();
    model.check(&config(), &dependent).unwrap();
    assert_eq!(
        serde_json::to_vec(&model).unwrap(),
        before,
        "checking changes nothing"
    );
    model.apply(dependent);
    assert_eq!(model.balance(CREATOR, a), 1);
    assert_eq!(model.balance(CREATOR, b), 0);
    assert_eq!(model.supplies[&CREATOR], 1);
    assert_eq!(model.activity[&CREATOR].items.len(), 2);
    let before = serde_json::to_vec(&model).unwrap();
    let held_by = model.held_by.clone();
    for (bad, reason) in [
        (
            range(
                12,
                12,
                vec![
                    at(12, 0, trade(a, true, 1, 2)),
                    at(12, 1, trade(b, false, 1, 1)),
                ],
            ),
            "invalid balance change",
        ),
        (
            range(
                12,
                12,
                vec![at(12, 0, trade(a, true, 1, 2)), at(12, 1, activated())],
            ),
            "duplicate activation",
        ),
        (range(13, 13, vec![]), "noncontiguous keyring range"),
        (
            range(
                12,
                12,
                vec![
                    at(12, 1, trade(a, true, 1, 2)),
                    at(12, 0, trade(a, true, 1, 3)),
                ],
            ),
            "unordered or duplicate keyring event",
        ),
        (
            range(12, 12, vec![at(13, 0, trade(a, true, 1, 2))]),
            "keyring event outside its range",
        ),
    ] {
        let error = model.check(&config(), &bad).unwrap_err();
        assert_eq!(error.to_string(), reason);
    }
    assert_eq!(serde_json::to_vec(&model).unwrap(), before);
    assert_eq!(model.held_by, held_by);
}

#[test]
fn malformed_transfers_are_rejected() {
    let from = Hex([2; 20]);
    let to = Hex([3; 20]);
    let event = transfer_log(10, 0, from, to, 1);
    for (field, value) in [
        ("removed", json!(true)),
        ("address", json!(to)),
        ("data", json!("0x")),
        ("topics", json!([transfer_topic()])),
    ] {
        let mut bad = event.clone();
        bad[field] = value;
        assert!(decode_transfer(&bad, &config()).is_err());
    }
    for bad in [
        transfer_log(10, 0, from, to, 0),
        transfer_log(10, 0, from, from, 1),
        transfer_log(10, 0, from, Address::ZERO, 1),
        transfer_log(10, 0, from, config().market, 1),
        transfer_log(10, 0, from, to, u32::MAX as u64 + 1),
    ] {
        assert!(decode_transfer(&bad, &config()).is_err());
    }
}

#[test]
fn keys_moved_to_an_account_reach_its_inbox_and_its_blocks_hide_the_sender() {
    let a = Hex([2; 20]);
    let b = Hex([3; 20]);
    let mut model = blank();
    model.apply(range(
        11,
        11,
        vec![
            at(11, 0, trade(a, true, 3, 3)),
            at(11, 1, transfer(a, b, 2)),
        ],
    ));
    let inbox = model.activity[&b].page(None, 10, None);
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    let notice = &inbox["items"][0];
    assert_eq!(notice["kind"], "received");
    assert_eq!(notice["from"], json!(a));
    assert_eq!(notice["creator"], json!(CREATOR));
    assert_eq!(notice["amount"], 2);
    assert_eq!(notice["position"]["log"], 1);
    // The creator hears about the purchase, not about keys moving between holders.
    let creator = model.activity[&CREATOR].page(None, 10, None);
    assert_eq!(creator["items"].as_array().unwrap().len(), 1);
    assert_eq!(creator["items"][0]["kind"], "trade");
    let blocked = BTreeSet::from([a]);
    assert_eq!(
        model.activity[&b].page(None, 10, Some(&blocked))["items"],
        json!([])
    );
}

#[test]
fn trade_history_is_bounded_ordered_and_filters_blocks_before_paging() {
    let mut history = TradeHistory::default();
    for log in 0..110 {
        history.push(Notice::Trade(TradeNotice {
            trader: Hex([if log % 2 == 0 { 2 } else { 3 }; 20]),
            buy: log % 2 == 0,
            amount: 1,
            position: Position { block: 10, log },
            transaction: Hex([4; 32]),
            timestamp: 100,
        }));
        if log == 99 {
            assert_eq!(history.items.len(), 100);
            assert!(!history.truncated);
        }
    }
    assert_eq!(history.items.len(), 100);
    assert_eq!(history.items.front().unwrap().position().log, 10);
    assert!(history.truncated);
    let blocked = BTreeSet::from([Hex([3; 20])]);
    let first = history.page(None, 2, Some(&blocked));
    assert_eq!(first["items"][0]["position"]["log"], 108);
    assert_eq!(first["items"][1]["position"]["log"], 106);
    let cursor: Position = serde_json::from_value(first["next"].clone()).unwrap();
    let second = history.page(Some(cursor), 2, Some(&blocked));
    assert_eq!(second["items"][0]["position"]["log"], 104);
    assert_eq!(
        history.page(None, 1, None)["items"][0]["position"]["log"],
        109
    );
}

#[tokio::test]
async fn oversized_lists_are_independent_partial_results() {
    let account = CREATOR;
    let address = |prefix: u8, n: u16| {
        let mut bytes = [0; 20];
        bytes[0] = prefix;
        bytes[18..].copy_from_slice(&n.to_be_bytes());
        Hex(bytes)
    };
    for (holder_count, holding_count) in [(257, 1), (256, 256), (1, 257), (257, 257)] {
        let mut model = blank();
        model.set_balance(account, account, 1); // Self never uses a feed slot.
        for n in 1..=holder_count {
            model.set_balance(account, address(2, n), 1);
        }
        for n in 1..=holding_count {
            model.set_balance(address(3, n), account, 1);
        }
        let cfg = HostConfig::for_test(vec![ChainConfig::for_test(
            84532,
            "http://127.0.0.1:9",
            MARKET,
        )]);
        let state = Arc::new(AppState {
            gate_checks: Default::default(),
            keyrings: [(
                84532,
                Keyring::detached_with(config(), model, KeyState::Ready),
            )]
            .into(),
            verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
            notice: None,
            policy: Default::default(),
            store: None,
            gateway: None,
        });
        let response = lookup(State(state), JsonPath((84532, account))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        for (field, count, prefix) in [("holders", holder_count, 2), ("holding", holding_count, 3)]
        {
            let expected: Vec<_> = (1..=count.min(256)).map(|n| address(prefix, n)).collect();
            assert_eq!(value[field], json!(expected));
            assert_eq!(value["truncated"][field], count > 256);
        }
    }
}

#[tokio::test]
async fn keyring_routes_select_the_chain_even_when_markets_share_an_address() {
    let holder = Hex([2; 20]);
    let mut indexes = BTreeMap::new();
    for (chain, amount) in [(8453, 2), (84532, 7)] {
        let mut model = blank();
        model.set_balance(CREATOR, holder, amount);
        indexes.insert(
            chain,
            Keyring::detached_with(Config { chain, ..config() }, model, KeyState::Ready),
        );
    }
    let cfg = HostConfig::for_test(vec![
        ChainConfig::for_test(8453, "http://127.0.0.1:9", MARKET),
        ChainConfig::for_test(84532, "http://127.0.0.1:9", MARKET),
    ]);
    let state = Arc::new(AppState {
        gate_checks: Default::default(),
        keyrings: indexes,
        verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
        notice: None,
        policy: Default::default(),
        store: None,
        gateway: None,
    });
    for (chain, expected) in [(8453, 2), (84532, 7)] {
        let response = crate::keyring_http::balances(
            State(state.clone()),
            JsonPath((chain, CREATOR)),
            JsonQuery(crate::keyring_http::BalanceQuery {
                view: BalanceView::Holders,
                after: None,
                limit: 20,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap();
        let page: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(page["entries"][0]["balance"], expected);
    }
    state.keyrings[&8453].set_ready(false);
    assert_eq!(
        lookup(State(state.clone()), JsonPath((8453, holder)))
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        lookup(State(state), JsonPath((84532, holder)))
            .await
            .status(),
        StatusCode::OK
    );
}

#[test]
fn management_pages_reach_every_balance_and_keep_self_separate() {
    let account = Hex([128; 20]);
    let market = config().market;
    let mut model = blank();
    model.set_balance(account, account, 9);
    for n in 1..=600u16 {
        let mut bytes = [0; 20];
        bytes[18..].copy_from_slice(&n.to_be_bytes());
        let address = Hex(bytes);
        model.set_balance(account, address, n as u64);
        model.set_balance(address, account, (601 - n) as u64);
    }
    for view in [BalanceView::Holding, BalanceView::Holders] {
        let mut after = None;
        let mut seen = Vec::new();
        loop {
            let page = model.balance_page(market, account, view, after, 64);
            assert_eq!(page["own_balance"], 9);
            assert_eq!(page["total"], 600);
            assert_eq!(page["checkpoint"]["block"], "10");
            let entries = page["entries"].as_array().unwrap();
            assert!(entries.len() <= 64);
            for entry in entries {
                let address: Address = entry["address"].as_str().unwrap().parse().unwrap();
                assert_ne!(address, account);
                let n = u16::from_be_bytes(address.0[18..].try_into().unwrap()) as u64;
                assert_eq!(
                    entry["balance"],
                    match view {
                        BalanceView::Holders => n,
                        BalanceView::Holding => 601 - n,
                    }
                );
                seen.push(address);
            }
            after = page["next"].as_str().map(|s| s.parse().unwrap());
            if after.is_none() {
                break;
            }
        }
        assert_eq!(seen.len(), 600);
        assert!(seen.windows(2).all(|w| w[0] < w[1]));
        // A cursor remains usable after its balance disappears.
        let cursor = seen[255];
        model.set_balance(account, cursor, 0);
        model.set_balance(cursor, account, 0);
        let page = model.balance_page(market, account, view, Some(cursor), 256);
        assert_eq!(page["entries"][0]["address"], json!(seen[256]));
        model.set_balance(account, cursor, 256);
        model.set_balance(cursor, account, 345);
    }
    let self_only = model.balance_page(
        market,
        account,
        BalanceView::Holding,
        Some(Hex([255; 20])),
        64,
    );
    assert_eq!(self_only["entries"], json!([]));
    assert_eq!(self_only["next"], Value::Null);
    assert_eq!(self_only["own_balance"], 9);
}

#[test]
fn trades_track_each_holder_and_last_sale_removes_membership() {
    let mut s = blank();
    let a = Hex([2; 20]);
    let b = Hex([3; 20]);
    for event in [
        log(10, 0, a, true, 3, 3),
        log(10, 1, b, true, 2, 5),
        log(11, 0, a, false, 2, 3),
    ] {
        ev(&mut s, Event::Trade(decode(&event, &config()).unwrap())).unwrap();
    }
    assert_eq!(s.holdings[&CREATOR][&a], 1);
    assert_eq!(s.holdings[&CREATOR][&b], 2);
    ev(&mut s, trade(a, false, 1, 2)).unwrap();
    assert!(!s.holdings[&CREATOR].contains_key(&a));
    let before = serde_json::to_vec(&s).unwrap();
    assert!(ev(&mut s, trade(a, false, 1, 1)).is_err());
    assert_eq!(serde_json::to_vec(&s).unwrap(), before);
}

#[test]
fn both_views_follow_ownership_direction_and_exclude_self() {
    let mut s = blank();
    let holder = Hex([2; 20]);
    ev(&mut s, trade(CREATOR, true, 1, 1)).unwrap();
    ev(&mut s, trade(holder, true, 1, 2)).unwrap();
    assert_eq!(s.authors(CREATOR), (vec![holder], vec![]));
    assert_eq!(s.authors(holder), (vec![], vec![CREATOR]));
    ev(&mut s, trade(holder, false, 1, 1)).unwrap();
    assert_eq!(s.authors(CREATOR), (vec![], vec![]));
    assert_eq!(s.authors(holder), (vec![], vec![]));
    assert!(s.supplies.contains_key(&CREATOR));
}

#[test]
fn activation_with_no_purchase_is_retained() {
    let log = json!({"address":MARKET,"topics":[activation_topic(),hex0x(&address_word(Hex([4;20])))],"data":"0x","removed":false});
    let Event::Activated { creator } = decode_event(&log, &config()).unwrap() else {
        panic!("expected activation")
    };
    let mut s = Model::default();
    ev(&mut s, Event::Activated { creator }).unwrap();
    assert_eq!(s.supplies.get(&creator), Some(&0));
    assert!(!s.holdings.contains_key(&creator));
}

#[test]
fn malformed_and_foreign_events_are_rejected() {
    let base = log(10, 0, Hex([2; 20]), true, 1, 1);
    for (field, value) in [
        ("address", json!(Address::ZERO)),
        ("removed", json!(true)),
        ("data", json!("0x00")),
        ("topics", json!([topic()])),
    ] {
        let mut changed = base.clone();
        changed[field] = value;
        assert!(decode(&changed, &config()).is_err());
    }
    // Trades have no batch limit; only a zero amount is malformed.
    assert!(decode(&log(10, 0, Hex([2; 20]), true, 0, 0), &config()).is_err());
    assert!(decode(&log(10, 0, Hex([2; 20]), true, 1000, 1000), &config()).is_ok());
    let mut s = blank();
    assert!(ev(&mut s, trade(Hex([2; 20]), true, 1, 2)).is_err());
}

// Storage: the writer, driven directly, on the deterministic runtime.

// Writer-only tests bypass the follower's chain-ID check.
fn verified_chain(keyring: &Keyring) {
    keyring.view.write().unwrap().state = KeyState::Syncing;
}
async fn submit(keyring: &Keyring, range: Range) -> Result<()> {
    keyring
        .command(|answer| Command::Range { range, answer })
        .await
}
fn model(keyring: &Keyring) -> Model {
    keyring.view.read().unwrap().model.clone()
}
async fn reopen<E: StorageContext + commonware_runtime::Spawner + commonware_runtime::Clock>(
    ctx: &E,
    keyring: Arc<Keyring>,
    label: &'static str,
) -> Result<Arc<Keyring>> {
    drop(keyring);
    // Let the writer drain and close before another handle opens the partition.
    ctx.sleep(Duration::from_millis(1)).await;
    Keyring::open(ctx.child(label), config()).await
}
fn seeded() -> Vec<Range> {
    let a = Hex([2; 20]);
    let b = Hex([3; 20]);
    vec![
        range(
            10,
            10,
            vec![at(10, 0, activated()), at(10, 1, trade(a, true, 3, 3))],
        ),
        range(
            11,
            11,
            vec![
                at(11, 0, transfer(a, b, 2)),
                at(11, 1, trade(b, false, 1, 2)),
            ],
        ),
    ]
}

impl Range {
    /// The same events, moved into the single block `block`.
    fn relabel(mut self, block: u64) -> Self {
        self.from = block;
        self.through = Checkpoint {
            block,
            hash: Hex([block as u8; 32]),
        };
        for logged in &mut self.events {
            logged.position.block = block;
        }
        self
    }
}

#[test]
fn chain_specific_journals_keep_balances_separate_after_reopening() {
    deterministic::Runner::default().start(|ctx| async move {
        let base = Config {
            chain: 8453,
            ..config()
        };
        let mainnet = Keyring::open(ctx.child("base"), base.clone())
            .await
            .unwrap();
        let sepolia = Keyring::open(ctx.child("sepolia"), config()).await.unwrap();
        verified_chain(&mainnet);
        verified_chain(&sepolia);
        submit(&mainnet, seeded().remove(0)).await.unwrap();
        for range in seeded() {
            submit(&sepolia, range).await.unwrap();
        }
        drop(mainnet);
        drop(sepolia);
        ctx.sleep(Duration::from_millis(1)).await;
        let mainnet = Keyring::open(ctx.child("base_reopened"), base)
            .await
            .unwrap();
        let sepolia = Keyring::open(ctx.child("sepolia_reopened"), config())
            .await
            .unwrap();
        assert_eq!(model(&mainnet).supplies[&CREATOR], 3);
        assert_eq!(model(&sepolia).supplies[&CREATOR], 2);
    });
}

#[test]
fn replay_restores_balances_reverse_lookups_and_trade_history() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        for r in seeded() {
            submit(&keyring, r).await.unwrap();
        }
        let served = model(&keyring);
        let restored = reopen(&ctx, keyring, "restart").await.unwrap();
        let replayed = model(&restored);
        assert_eq!(
            serde_json::to_vec(&replayed).unwrap(),
            serde_json::to_vec(&served).unwrap()
        );
        assert_eq!(replayed.held_by, served.held_by);
        assert_eq!(replayed.activity[&CREATOR].items.len(), 2);
        assert_eq!(replayed.authors(Hex([3; 20])).1, vec![CREATOR]);
        assert_eq!(
            restored.view.read().unwrap().state,
            KeyState::Starting,
            "a restart verifies the chain before serving"
        );
    });
}

#[test]
fn empty_ranges_stay_in_ram_and_the_next_journaled_range_covers_them() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        for block in 11..=20 {
            submit(&keyring, range(block, block, vec![])).await.unwrap();
        }
        assert_eq!(model(&keyring).checkpoint.unwrap().block, 20);
        let keyring = reopen(&ctx, keyring, "short").await.unwrap();
        assert_eq!(
            model(&keyring).checkpoint.unwrap().block,
            10,
            "a crash loses only unjournaled empty progress"
        );
        verified_chain(&keyring);
        submit(&keyring, range(11, 20, vec![])).await.unwrap();
        let (a, b) = (Hex([2; 20]), Hex([3; 20]));
        submit(
            &keyring,
            range(
                21,
                21,
                vec![
                    at(21, 0, transfer(a, b, 2)),
                    at(21, 1, trade(b, false, 1, 2)),
                ],
            ),
        )
        .await
        .unwrap();
        let last_skipped = 21 + CHECKPOINT_BLOCKS - 1;
        submit(&keyring, range(22, last_skipped, vec![]))
            .await
            .unwrap();
        submit(&keyring, range(last_skipped + 1, last_skipped + 1, vec![]))
            .await
            .unwrap();
        let served = model(&keyring);
        let keyring = reopen(&ctx, keyring, "full").await.unwrap();
        let replayed = model(&keyring);
        assert_eq!(
            replayed.checkpoint.unwrap().block,
            last_skipped + 1,
            "a full interval of empty blocks is journaled as one record"
        );
        assert_eq!(
            serde_json::to_vec(&replayed).unwrap(),
            serde_json::to_vec(&served).unwrap()
        );
    });
}

#[test]
fn a_journal_for_another_configuration_disables_only_the_keyring() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        // Opening binds even an empty journal before the first chain poll.
        drop(keyring);
        ctx.sleep(Duration::from_millis(1)).await;
        let moved = Config {
            start_block: 11,
            ..config()
        };
        let error = Keyring::open(ctx.child("moved"), moved)
            .await
            .err()
            .unwrap();
        assert!(format!("{error:#}").contains("another market or format"));
        let keyring = Keyring::open(ctx.child("same"), config()).await.unwrap();
        assert!(model(&keyring).checkpoint.is_none());
        assert!(keyring.read_ready(|_| ()).is_none());
    });
}

#[test]
fn a_contradiction_is_refused_without_writing_or_serving_it() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        let before = model(&keyring);
        let bad = range(
            11,
            11,
            vec![
                at(11, 0, trade(Hex([2; 20]), true, 1, 4)),
                at(11, 1, activated()),
            ],
        );
        let error = submit(&keyring, bad).await.unwrap_err();
        assert!(error.is::<Fatal>());
        assert_eq!(
            serde_json::to_vec(&model(&keyring)).unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(
            serde_json::to_vec(&model(&keyring)).unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
    });
}

#[test]
fn a_failed_identity_sync_prevents_opening_and_can_be_retried() {
    deterministic::Runner::default().start(|ctx| async move {
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        assert!(Keyring::open(ctx.child("failed"), config()).await.is_err());
        ctx.storage_fault_config().write().sync_rate = None;
        let keyring = Keyring::open(ctx.child("retry"), config()).await.unwrap();
        assert!(model(&keyring).checkpoint.is_none());
        assert!(keyring.read_ready(|_| ()).is_none());
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(model(&keyring).checkpoint.unwrap().block, 10);
    });
}

#[test]
fn a_failed_checkpoint_sync_stops_the_writer_and_replays_only_durable_progress() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        submit(&keyring, range(11, 50, vec![])).await.unwrap();
        let before = model(&keyring);
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        assert!(keyring.flush().await.unwrap_err().is::<Fatal>());
        assert!(matches!(
            keyring.view.read().unwrap().state,
            KeyState::Stopped(_)
        ));
        assert!(keyring.read_ready(|_| ()).is_none());
        assert_eq!(
            serde_json::to_vec(&model(&keyring)).unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
        ctx.storage_fault_config().write().sync_rate = None;
        assert!(keyring.flush().await.unwrap_err().is::<Fatal>());
        assert!(
            submit(&keyring, range(51, 60, vec![]))
                .await
                .unwrap_err()
                .is::<Fatal>()
        );
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(model(&keyring).checkpoint.unwrap().block, 10);
    });
}

#[test]
fn a_failed_sync_stops_the_keyring_without_serving_the_range() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        let before = model(&keyring);
        ctx.storage_fault_config().write().sync_rate = Some(commonware_utils::probability!(1.0));
        let error = submit(&keyring, seeded().remove(1)).await.unwrap_err();
        assert!(error.is::<Fatal>());
        {
            let view = keyring.view.read().unwrap();
            assert!(matches!(view.state, KeyState::Stopped(_)));
            assert_eq!(
                serde_json::to_vec(&view.model).unwrap(),
                serde_json::to_vec(&before).unwrap()
            );
        }
        assert!(keyring.read_ready(|_| ()).is_none());
        assert!(submit(&keyring, seeded().remove(1)).await.is_err());
        ctx.storage_fault_config().write().sync_rate = None;
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(model(&keyring).checkpoint.unwrap().block, 10);
    });
}

/// An unfinished write at the tail is dropped; a cut into records that
/// were synced disables the keyring rather than serving a shorter history.
#[test]
fn a_torn_tail_is_dropped_and_a_cut_into_synced_records_is_refused() {
    use commonware_runtime::{Blob as _, Storage as _, WriteOptions};
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        let holder = Hex([2; 20]);
        submit(&keyring, range(10, 10, vec![at(10, 0, activated())]))
            .await
            .unwrap();
        // Enough records to span more than one 4 KiB page of the data blob.
        for block in 11..=40u64 {
            let supply = block - 10;
            submit(
                &keyring,
                range(
                    block,
                    block,
                    vec![at(block, 0, trade(holder, true, 1, supply))],
                ),
            )
            .await
            .unwrap();
        }
        let served = model(&keyring);
        drop(keyring);
        ctx.sleep(Duration::from_millis(1)).await;
        let partition = format!("keyring-{}-{}_data", config().chain, config().market);
        let names = ctx.scan(&partition).await.unwrap();
        assert_eq!(names.len(), 1, "one data blob in {partition}");
        let (blob, size) = ctx.open(&partition, &names[0]).await.unwrap();
        assert!(
            size > 4096,
            "the records span more than one page: {size} bytes"
        );
        blob.write_at(size, vec![0xff; 7], WriteOptions::default())
            .await
            .unwrap();
        blob.sync().await.unwrap();
        drop(blob);
        let reopened = Keyring::open(ctx.child("junk"), config()).await.unwrap();
        assert_eq!(
            serde_json::to_vec(&model(&reopened)).unwrap(),
            serde_json::to_vec(&served).unwrap()
        );
        drop(reopened);
        ctx.sleep(Duration::from_millis(1)).await;
        let (blob, size) = ctx.open(&partition, &names[0]).await.unwrap();
        blob.resize(size - 3).await.unwrap();
        blob.sync().await.unwrap();
        drop(blob);
        let error = Keyring::open(ctx.child("cut"), config())
            .await
            .err()
            .expect("a cut into synced records must not serve a shorter history");
        assert!(
            format!("{error:#}").contains("corruption"),
            "the refusal names the corruption: {error:#}"
        );
    });
}

#[test]
fn flush_journals_ram_progress_and_is_a_no_op_when_nothing_is_pending() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        keyring.flush().await.unwrap();
        submit(&keyring, seeded().remove(0)).await.unwrap();
        submit(&keyring, range(11, 50, vec![])).await.unwrap();
        keyring.flush().await.unwrap();
        keyring.flush().await.unwrap();
        let served = model(&keyring);
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(model(&keyring).checkpoint.unwrap().block, 50);
        assert_eq!(
            serde_json::to_vec(&model(&keyring)).unwrap(),
            serde_json::to_vec(&served).unwrap()
        );
    });
}

/// The writer takes commands in order, so a flush sent after a range can
/// never be written before it.
#[test]
fn a_flush_never_overtakes_a_range_queued_before_it() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        submit(&keyring, range(11, 20, vec![])).await.unwrap();
        let (sent, range_done) = oneshot::channel();
        keyring
            .commands
            .send(Command::Range {
                range: seeded().remove(1).relabel(21),
                answer: sent,
            })
            .await
            .unwrap();
        let (sent, flush_done) = oneshot::channel();
        keyring
            .commands
            .send(Command::Flush { answer: sent })
            .await
            .unwrap();
        flush_done.await.unwrap().unwrap();
        range_done.await.unwrap().unwrap();
        let served = model(&keyring);
        assert_eq!(served.checkpoint.unwrap().block, 21);
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(
            serde_json::to_vec(&model(&keyring)).unwrap(),
            serde_json::to_vec(&served).unwrap()
        );
    });
}

#[test]
fn shutdown_stops_the_follower_and_leaves_nothing_unjournaled() {
    deterministic::Runner::default().start(|ctx| async move {
        let keyring = Keyring::open(ctx.child("first"), config()).await.unwrap();
        verified_chain(&keyring);
        submit(&keyring, seeded().remove(0)).await.unwrap();
        submit(&keyring, range(11, 30, vec![])).await.unwrap();
        keyring.shutdown().await.unwrap();
        assert!(keyring.stopping.load(std::sync::atomic::Ordering::Relaxed));
        let keyring = reopen(&ctx, keyring, "restart").await.unwrap();
        assert_eq!(model(&keyring).checkpoint.unwrap().block, 30);
    });
}

// Ingestion against a mock node, on the tokio runtime.

static LABELS: AtomicUsize = AtomicUsize::new(0);
fn label() -> &'static str {
    Box::leak(format!("keyring{}", LABELS.fetch_add(1, Ordering::Relaxed)).into_boxed_str())
}
fn with_node<F, Fut>(name: &str, run: F)
where
    F: FnOnce(cw_tokio::Context) -> Fut,
    Fut: Future<Output = ()>,
{
    let dir = std::env::temp_dir().join(format!("blohard-keyring-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    cw_tokio::Runner::new(cw_tokio::Config::default().with_storage_directory(&dir)).start(run);
    let _ = std::fs::remove_dir_all(&dir);
}
async fn open(ctx: &cw_tokio::Context) -> Arc<Keyring> {
    Keyring::open(ctx.child(label()), config()).await.unwrap()
}
async fn reopened(ctx: &cw_tokio::Context, keyring: Arc<Keyring>) -> Arc<Keyring> {
    drop(keyring);
    tokio::time::sleep(Duration::from_millis(10)).await;
    open(ctx).await
}

async fn seeded_index(ctx: &cw_tokio::Context) -> (mock_node::Running, AppState, Arc<Keyring>) {
    let running = mock_node::spawn(Node::new(84532, MARKET, vec![])).await;
    running.node.set_head(24);
    let holder = Hex([2; 20]);
    running
        .node
        .extra_logs
        .lock()
        .unwrap()
        .extend([json!({"address":MARKET,"topics":[activation_topic(),hex0x(&address_word(CREATOR))],"data":"0x","blockNumber":"0xa","logIndex":"0x0","blockHash":format!("0x{:064x}",2010),"transactionHash":format!("0x{:064x}",3010),"removed":false}), log(10, 1, holder, true, 2, 2)]);
    let cfg = HostConfig::for_test(vec![ChainConfig::for_test(84532, &running.url, MARKET)]);
    let state = AppState {
        gate_checks: Default::default(),
        keyrings: Default::default(),
        verifier: Verifier::from_config(&cfg, reqwest::Client::new()).unwrap(),
        notice: None,
        policy: Default::default(),
        store: None,
        gateway: None,
    };
    let index = open(ctx).await;
    running.node.set_head(12);
    assert!(!index.poll(&state.verifier, 2000, &mut None).await.unwrap());
    assert!(
        model(&index).holdings.is_empty(),
        "unconfirmed trades stay unindexed"
    );
    running.node.set_head(12 + MIN_CONFIRMATIONS);
    assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
    assert_eq!(index.status()["checkpoint"]["block"], "12");
    assert_eq!(index.status()["minimum_confirmations"], MIN_CONFIRMATIONS);
    assert_eq!(model(&index).holdings[&CREATOR][&holder], 2);
    (running, state, index)
}

#[test]
fn verified_holdings_survive_a_transient_error_but_expire_without_progress() {
    with_node("grace", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        index.set_ready(true);
        // Every log read fails, however narrow the range.
        running.node.log_outage.store(100, Ordering::Relaxed);
        running.node.set_head(25);
        let error = index
            .poll(&state.verifier, 2000, &mut None)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("non-json"),
            "a transport failure, retryable: {error:#}"
        );
        assert!(
            error.downcast_ref::<Fatal>().is_none() && error.downcast_ref::<Unreadable>().is_none()
        );
        assert_eq!(
            running.node.count("eth_getBlockReceipts"),
            0,
            "no receipts for a transport failure"
        );
        assert!(index.read_ready(|_| ()).is_some());
        assert_eq!(index.status()["state"], "ready");
        index.view.write().unwrap().ready_at -= Duration::from_secs(61);
        assert!(index.read_ready(|_| ()).is_none());
        assert_eq!(index.status()["state"], "syncing");
        running.node.log_outage.store(0, Ordering::Relaxed);
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        index.set_ready(true);
        assert!(index.read_ready(|_| ()).is_some());
        stop(&index.view, "contradictory history");
        index.set_ready(true);
        assert!(index.read_ready(|_| ()).is_none());
        running.stop().await;
    });
}

#[test]
fn journal_open_failure_preserves_market_identity_and_reports_stopped() {
    let index = Keyring::unavailable(config());
    assert_eq!(index.status()["market"], json!(config().market));
    assert_eq!(index.status()["state"], "stopped");
    assert!(
        index.status()["reason"]
            .as_str()
            .unwrap()
            .contains("journal could not open")
    );
    assert!(index.read_ready(|_| ()).is_none());
}

#[test]
fn transfers_replay_durably_with_buys_and_redemptions() {
    with_node("transfers", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let from = Hex([2; 20]);
        let to = Hex([3; 20]);
        running.node.set_head(25);
        // Supply ends at one. Same-block ordering must process the gift before its redemption.
        running.node.extra_logs.lock().unwrap().extend([
            log(13, 1, to, false, 1, 1),
            transfer_log(13, 0, from, to, 2),
        ]);
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        let restored = reopened(&ctx, index).await;
        assert!(
            restored
                .poll(&state.verifier, 2000, &mut None)
                .await
                .unwrap()
        );
        let saved = model(&restored);
        assert_eq!(saved.supplies[&CREATOR], 1);
        assert_eq!(saved.authors(from).1, vec![]);
        assert_eq!(saved.authors(to).1, vec![CREATOR]);
        assert_eq!(saved.holdings[&CREATOR][&to], 1);
        running.stop().await;
    });
}

#[test]
fn trade_notices_replay_with_timestamps_and_exclude_own_trades() {
    with_node("notices", |ctx| async move {
        let (running, mut state, index) = seeded_index(&ctx).await;
        running.node.set_head(25);
        running.node.extra_logs.lock().unwrap().extend([
            log(13, 0, CREATOR, true, 1, 3),
            log(13, 1, Hex([2; 20]), false, 1, 2),
        ]);
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        let restored = reopened(&ctx, index).await;
        assert!(
            restored
                .poll(&state.verifier, 2000, &mut None)
                .await
                .unwrap()
        );
        let snapshot = model(&restored);
        let history = &snapshot.activity[&CREATOR];
        assert_eq!(history.items.len(), 2);
        let sold: Vec<&TradeNotice> = history
            .items
            .iter()
            .map(|n| match n {
                Notice::Trade(t) => t,
                Notice::Received(_) => panic!("only trades reach the creator here"),
            })
            .collect();
        assert!(sold[0].buy);
        assert!(!sold[1].buy);
        assert_eq!(sold[1].timestamp, 1_700_000_026);
        assert_eq!(sold[1].position, Position { block: 13, log: 1 });
        assert_ne!(sold[1].transaction, Hash::ZERO);
        restored.set_ready(true);
        state.keyrings.insert(84532, restored.clone());
        let before = running.node.counts();
        let response = trades(
            State(Arc::new(state)),
            JsonPath((84532, CREATOR)),
            JsonQuery(TradesQuery {
                before_block: None,
                before_log: None,
                limit: 20,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            running.node.counts(),
            before,
            "notification reads must not call RPC"
        );
        running.stop().await;
    });
}

#[test]
fn gate_holdings_are_bounded_index_reads_even_past_feed_limit() {
    with_node("holdings", |ctx| async move {
        let (running, mut state, index) = seeded_index(&ctx).await;
        state.keyrings.insert(84532, index.clone());
        index.set_ready(true);
        let gate = Hex([7; 20]);
        let holder = Hex([255; 20]);
        {
            let mut view = index.view.write().unwrap();
            for n in 0..300u16 {
                let mut a = [0; 20];
                a[18..].copy_from_slice(&n.to_be_bytes());
                view.model.set_balance(CREATOR, Hex(a), 1);
            }
            view.model.set_balance(CREATOR, holder, 1);
        }
        let ask = |count| HoldingsQuery {
            gate,
            pairs: (0..count)
                .map(|_| HoldingPair {
                    creator: CREATOR,
                    holder,
                })
                .collect(),
        };
        let state = Arc::new(state);
        let before = running.node.counts();
        assert_eq!(
            holdings(State(state.clone()), JsonPath(84532), axum::Json(ask(1)))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        state
            .gate_checks
            .insert(84532, gate, crate::gate_validation::Kind::Keys);
        for count in [0, 51] {
            assert_eq!(
                holdings(
                    State(state.clone()),
                    JsonPath(84532),
                    axum::Json(ask(count))
                )
                .await
                .status(),
                StatusCode::BAD_REQUEST
            );
        }
        let query = HoldingsQuery {
            gate,
            pairs: vec![
                HoldingPair {
                    creator: CREATOR,
                    holder,
                },
                HoldingPair {
                    creator: CREATOR,
                    holder: Hex([254; 20]),
                },
                HoldingPair {
                    creator: holder,
                    holder: CREATOR,
                },
            ],
        };
        let response = holdings(State(state.clone()), JsonPath(84532), axum::Json(query)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["held"], json!([true, false, false]));
        index.set_ready(false);
        assert_eq!(
            holdings(State(state.clone()), JsonPath(84532), axum::Json(ask(1)))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            running.node.counts(),
            before,
            "display reads never call RPC"
        );
        running.stop().await;
    });
}

#[test]
fn management_endpoint_is_bounded_and_never_reads_rpc() {
    with_node("management", |ctx| async move {
        let (running, mut state, index) = seeded_index(&ctx).await;
        state.keyrings.insert(84532, index.clone());
        index.set_ready(true);
        let before = running.node.counts();
        let router = crate::routes::router(Arc::new(state), None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::new();
        for (suffix, status) in [
            ("", StatusCode::OK),
            ("?view=holders&limit=256", StatusCode::OK),
            (
                "?view=holding&after=0x0000000000000000000000000000000000000000",
                StatusCode::OK,
            ),
            ("?view=other", StatusCode::BAD_REQUEST),
            ("?after=bad", StatusCode::BAD_REQUEST),
            ("?limit=0", StatusCode::BAD_REQUEST),
            ("?limit=257", StatusCode::BAD_REQUEST),
            ("?limit=-1", StatusCode::BAD_REQUEST),
            ("?limit=1&limit=2", StatusCode::BAD_REQUEST),
        ] {
            let response = client
                .get(format!(
                    "{url}/api/chains/84532/keyring/{CREATOR}/balances{suffix}"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{suffix}");
        }
        for (suffix, status) in [
            ("", StatusCode::OK),
            ("?limit=1&before_block=10&before_log=2", StatusCode::OK),
            ("?limit=0", StatusCode::BAD_REQUEST),
            ("?limit=101", StatusCode::BAD_REQUEST),
            ("?before_block=10", StatusCode::BAD_REQUEST),
            ("?before_log=0", StatusCode::BAD_REQUEST),
            ("?before_block=bad&before_log=0", StatusCode::BAD_REQUEST),
        ] {
            let response = client
                .get(format!(
                    "{url}/api/chains/84532/keyring/{CREATOR}/trades{suffix}"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{suffix}");
        }
        index.set_ready(false);
        for (chain, status) in [
            (84532, StatusCode::SERVICE_UNAVAILABLE),
            (8453, StatusCode::NOT_FOUND),
        ] {
            let response = client
                .get(format!(
                    "{url}/api/chains/{chain}/keyring/{CREATOR}/balances"
                ))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), status);
        }
        assert_eq!(running.node.counts(), before);
        server.abort();
        running.stop().await;
    });
}

#[test]
#[ignore = "slow: waits through two real 10-second RPC retry delays"]
fn backfill_regains_throughput_after_repeated_rpc_errors() {
    with_node("backfill", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let before = running.node.count("eth_getLogs");
        running.node.set_head(6024);
        running
            .node
            .extra_logs
            .lock()
            .unwrap()
            .push(log(5513, 0, Hex([2; 20]), true, 1, 3));
        running
            .node
            .log_refusals
            .store(2, std::sync::atomic::Ordering::Relaxed);
        let runner = index.clone();
        tokio::spawn(async move { runner.run(&state.verifier).await });
        tokio::time::timeout(Duration::from_secs(35), async {
            while index.read_ready(|_| ()).is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("backfill should recover and become ready");
        let requests = running.node.count("eth_getLogs") - before;
        let saved = model(&index);
        running.stop().await;
        assert_eq!(saved.checkpoint.unwrap().block, 6012);
        assert_eq!(saved.holdings[&CREATOR][&Hex([2; 20])], 3);
        assert!(
            requests <= 7,
            "recovery must grow the range rather than repeatedly fetching 500 blocks: {requests} requests"
        );
    });
}

#[test]
fn transition_failures_are_fatal_without_publishing_partial_batches() {
    with_node("transitions", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let holder = Hex([2; 20]);
        let before = serde_json::to_vec(&model(&index)).unwrap();
        let reverse = model(&index).held_by;
        let seed = running.node.extra_logs.lock().unwrap().clone();
        running.node.set_head(25);
        let valid = log(13, 0, holder, true, 1, 3);
        let mut unactivated = log(13, 1, holder, true, 1, 1);
        unactivated["topics"][1] = json!(hex0x(&address_word(Hex([9; 20]))));
        let mut duplicate_activation = seed[0].clone();
        duplicate_activation["blockNumber"] = json!("0xd");
        duplicate_activation["blockHash"] = json!(format!("0x{:064x}", 2013));
        duplicate_activation["logIndex"] = json!("0x1");
        for (bad, reason) in [
            (
                log(13, 1, holder, true, 1, 5),
                "trade supply disagrees with history",
            ),
            (
                log(13, 1, Hex([3; 20]), false, 1, 2),
                "invalid balance change",
            ),
            (log(13, 1, holder, false, 4, 0), "invalid supply change"),
            (unactivated, "trade before activation"),
            (duplicate_activation, "duplicate activation"),
        ] {
            *running.node.extra_logs.lock().unwrap() =
                [seed.clone(), vec![valid.clone(), bad]].concat();
            let error = index
                .poll(&state.verifier, 2000, &mut None)
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), reason);
            let saved = model(&index);
            assert_eq!(serde_json::to_vec(&saved).unwrap(), before);
            assert_eq!(saved.held_by, reverse);
            assert!(
                error.downcast_ref::<Fatal>().is_some(),
                "{reason} must stop the worker"
            );
        }
        // Nothing from the refused ranges reached the journal.
        let restored = reopened(&ctx, index).await;
        assert_eq!(serde_json::to_vec(&model(&restored)).unwrap(), before);
        running.stop().await;
    });
}

#[test]
fn provider_failures_remain_retryable_and_recover() {
    with_node("providers", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let holder = Hex([2; 20]);
        let seed = running.node.extra_logs.lock().unwrap().clone();
        running.node.set_head(25);
        let valid = log(13, 0, holder, true, 1, 3);
        running.node.block_receipts.store(false, Ordering::Relaxed);
        running.node.response_overrides.lock().unwrap().insert(
            "eth_getLogs".into(),
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query limit exceeded"}}),
        );
        let error = index
            .poll(&state.verifier, 2000, &mut None)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<Fatal>().is_none());
        assert!(error.downcast_ref::<Unreadable>().is_none());
        assert!(format!("{error:#}").contains("poll 1 of 16"), "{error:#}");
        running.node.response_overrides.lock().unwrap().clear();
        running.node.block_receipts.store(true, Ordering::Relaxed);
        let mut replaced = valid.clone();
        replaced["blockHash"] = json!(format!("0x{:064x}", 9999));
        let mut malformed = valid.clone();
        malformed["data"] = json!("0x00");
        for bad in [replaced, malformed] {
            *running.node.extra_logs.lock().unwrap() = [seed.clone(), vec![bad]].concat();
            let error = index
                .poll(&state.verifier, 2000, &mut None)
                .await
                .unwrap_err();
            assert!(error.downcast_ref::<Fatal>().is_none());
            assert_eq!(model(&index).checkpoint.unwrap().block, 12);
        }
        *running.node.extra_logs.lock().unwrap() = [seed, vec![valid]].concat();
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        assert_eq!(model(&index).holdings[&CREATOR][&holder], 3);
        running.stop().await;
    });
}

#[test]
fn polling_checks_the_saved_checkpoint_once_when_advancing_or_idle() {
    with_node("checkpoint-reads", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let before = running.node.count("eth_getBlockByNumber");
        running.node.set_head(25);
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        assert_eq!(
            running.node.count("eth_getBlockByNumber") - before,
            3,
            "an empty new range needs two boundary reads and one saved-checkpoint read"
        );
        assert_eq!(
            running.node.last("eth_getBlockByNumber")[0],
            "0xc",
            "the saved checkpoint is checked after the fetched range"
        );
        let before = running.node.count("eth_getBlockByNumber");
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        assert_eq!(
            running.node.count("eth_getBlockByNumber") - before,
            1,
            "an idle poll still verifies its checkpoint"
        );
        assert_eq!(running.node.last("eth_getBlockByNumber")[0], "0xd");
        running.stop().await;
    });
}

#[test]
fn a_replaced_checkpoint_is_fatal_on_advancing_idle_and_lagging_polls() {
    with_node("checkpoint-replaced", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let before = serde_json::to_vec(&model(&index)).unwrap();
        running
            .node
            .block_hashes
            .lock()
            .unwrap()
            .insert(12, [9; 32]);
        for head in [25, 24, 23] {
            running.node.set_head(head);
            let error = index
                .poll(&state.verifier, 2000, &mut None)
                .await
                .unwrap_err();
            assert!(
                error.is::<Fatal>(),
                "replaced history at head {head}: {error}"
            );
            assert_eq!(serde_json::to_vec(&model(&index)).unwrap(), before);
        }
        running.stop().await;
    });
}

/// An endpoint that reports another chain stops the keyring as it starts,
/// rather than indexing that chain's trades as this one's.
#[test]
fn a_keyring_whose_endpoint_reports_another_chain_stops() {
    with_node("wrong-chain", |ctx| async move {
        // The node reports chain 1; the keyring and its configuration say 84532.
        let running = mock_node::spawn(Node::new(1, MARKET, vec![])).await;
        running.node.set_head(24);
        let cfg = HostConfig::for_test(vec![ChainConfig::for_test(84532, &running.url, MARKET)]);
        let verifier = Verifier::from_config(&cfg, reqwest::Client::new()).unwrap();
        let index = open(&ctx).await;
        let error = index.poll(&verifier, 2000, &mut None).await.unwrap_err();
        assert!(error.is::<Fatal>(), "{error:#}");
        assert!(model(&index).checkpoint.is_none());
        running.stop().await;
    });
}

#[test]
fn backfill_restart_catchup_and_reorg_detection() {
    with_node("restart", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let holder = Hex([2; 20]);
        let identity_reads = running.node.count("eth_chainId");
        assert!(identity_reads > 0, "seeding checked chain identity");
        running.node.set_head(25);
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        assert_eq!(
            running.node.count("eth_chainId"),
            identity_reads,
            "chain identity is established once per startup"
        );
        let index = reopened(&ctx, index).await;
        assert!(index.read_ready(|_| ()).is_none());
        assert_eq!(model(&index).authors(holder).1, vec![CREATOR]);
        // The idle block 13 was not journaled, so the restart reads it again.
        assert_eq!(model(&index).checkpoint.unwrap().block, 12);
        running
            .node
            .extra_logs
            .lock()
            .unwrap()
            .push(log(13, 0, holder, false, 2, 0));
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        assert_eq!(running.node.last("eth_getLogs")[0]["fromBlock"], "0xd");
        assert!(model(&index).holdings.is_empty());
        assert!(model(&index).authors(holder).1.is_empty());
        running
            .node
            .block_hashes
            .lock()
            .unwrap()
            .insert(13, [9; 32]);
        let error = index
            .poll(&state.verifier, 2000, &mut None)
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<Fatal>().is_some());
        running.stop().await;
    });
}

/// Block 13 holds more market logs than the provider answers as logs, so
/// its receipts are read instead: an activation, a trade and a transfer,
/// beside a log of another contract, a market log of another kind, and a
/// trade in a transaction that failed, which count for nothing.
#[test]
fn a_dense_block_is_read_from_its_receipts() {
    with_node("dense-receipts", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let mut refused = None;
        let (holder, other, newcomer) = (Hex([2; 20]), Hex([3; 20]), Hex([5; 20]));
        let failed = format!("0x{:064x}", 7777);
        let mut lost = log(13, 3, holder, true, 5, 8);
        lost["transactionHash"] = json!(failed);
        let mut foreign = log(13, 4, holder, true, 9, 9);
        foreign["address"] = json!("0x000000000000000000000000000000000000bbbb");
        let mut unrelated = log(13, 5, holder, true, 9, 9);
        unrelated["topics"][0] = json!(format!("0x{:064x}", 4242));
        running.node.failed_txs.lock().unwrap().insert(failed);
        running.node.extra_logs.lock().unwrap().extend([
            json!({"address":MARKET,"topics":[activation_topic(),hex0x(&address_word(newcomer))],"data":"0x","blockNumber":"0xd","logIndex":"0x0","blockHash":format!("0x{:064x}",2013),"transactionHash":format!("0x{:064x}",3013),"removed":false}),
            log(13, 1, holder, true, 1, 3),
            transfer_log(13, 2, holder, other, 1),
            lost,
            foreign,
            unrelated,
        ]);
        running.node.max_logs.store(2, Ordering::Relaxed);
        running.node.set_head(25);
        assert!(
            index
                .poll(&state.verifier, 2000, &mut refused)
                .await
                .unwrap()
        );
        assert_eq!(running.node.count("eth_getBlockReceipts"), 1);
        let model = model(&index);
        assert_eq!(model.checkpoint.unwrap().block, 13);
        assert_eq!(model.supplies[&CREATOR], 3, "the buy, not the failed one");
        assert_eq!(model.holdings[&CREATOR][&holder], 2);
        assert_eq!(model.holdings[&CREATOR][&other], 1);
        assert!(model.supplies.contains_key(&newcomer), "activated");
        assert!(refused.is_none());
        running.stop().await;
    });
}

/// Receipts that name another hash for the block, as after a replacement
/// or from a confused provider, are not taken in and are retried.
#[test]
fn receipts_for_another_hash_are_retried_not_taken() {
    with_node("receipts-hash", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let mut refused = None;
        running.node.extra_logs.lock().unwrap().extend([
            log(13, 1, Hex([2; 20]), true, 1, 3),
            log(13, 2, Hex([2; 20]), true, 1, 4),
            log(13, 3, Hex([2; 20]), true, 1, 5),
        ]);
        running.node.max_logs.store(2, Ordering::Relaxed);
        running.node.set_head(25);
        running.node.response_overrides.lock().unwrap().insert(
            "eth_getBlockReceipts".into(),
            json!({"jsonrpc":"2.0","id":1,"result":[{"status":"0x1","transactionHash":format!("0x{:064x}",3013),"blockNumber":"0xd","blockHash":format!("0x{:064x}",9999),"logs":[]}]}),
        );
        let error = index
            .poll(&state.verifier, 2000, &mut refused)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("receipt disagrees with header"),
            "{error:#}"
        );
        assert!(error.downcast_ref::<Unreadable>().is_none());
        assert_eq!(model(&index).checkpoint.unwrap().block, 12);
        assert!(refused.is_none(), "a failed read is not a refusal");
        running.node.response_overrides.lock().unwrap().clear();
        assert!(
            index
                .poll(&state.verifier, 2000, &mut refused)
                .await
                .unwrap()
        );
        assert_eq!(model(&index).supplies[&CREATOR], 5);
        running.stop().await;
    });
}

/// A block replaced while its receipts were read, holding no market log
/// before or after, is caught by the end header read again: nothing is
/// committed on the old hash.
#[test]
fn a_block_replaced_during_an_empty_receipts_read_is_not_committed() {
    with_node("receipts-replaced", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        running.node.set_head(25);
        running.node.response_overrides.lock().unwrap().insert(
            "eth_getLogs".into(),
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query limit exceeded"}}),
        );
        *running.node.replace_after_read.lock().unwrap() = Some(mock_node::Replacement {
            block: 13,
            hash: [9; 32],
            logs: Vec::new(),
        });
        let error = index
            .poll(&state.verifier, 2000, &mut None)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("range changed during fetch"),
            "{error:#}"
        );
        assert_eq!(model(&index).checkpoint.unwrap().block, 12);
        running.node.response_overrides.lock().unwrap().clear();
        assert!(index.poll(&state.verifier, 2000, &mut None).await.unwrap());
        assert_eq!(model(&index).checkpoint.unwrap().hash, Hex([9; 32]));
        running.stop().await;
    });
}

/// Without receipts, a block refused as logs is counted once per poll in
/// which both were tried. Transport failures neither count nor clear it, a
/// range taken in clears it, and the sixteenth refusal stops the keyring
/// naming the block.
#[test]
fn persistent_refusals_without_receipts_stop_the_keyring() {
    with_node("refusals", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        let mut refused = None;
        let refuse = || {
            running.node.response_overrides.lock().unwrap().insert(
                "eth_getLogs".into(),
                json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query limit exceeded"}}),
            );
        };
        let allow = || running.node.response_overrides.lock().unwrap().clear();
        running.node.block_receipts.store(false, Ordering::Relaxed);
        running.node.set_head(25);
        refuse();
        for n in 1..=3 {
            let error = index
                .poll(&state.verifier, 2000, &mut refused)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains(&format!("poll {n} of 16")),
                "{error:#}"
            );
        }
        allow();
        running.node.log_outage.store(100, Ordering::Relaxed);
        index
            .poll(&state.verifier, 2000, &mut refused)
            .await
            .unwrap_err();
        running.node.log_outage.store(0, Ordering::Relaxed);
        assert_eq!(
            refused,
            Some((13, 3)),
            "a transport failure neither counts nor clears"
        );
        // Logs that come back but cannot be taken in clear nothing either.
        let seed = running.node.extra_logs.lock().unwrap().clone();
        let mut malformed = log(13, 1, Hex([2; 20]), true, 1, 3);
        malformed["data"] = json!("0x00");
        running.node.extra_logs.lock().unwrap().push(malformed);
        index
            .poll(&state.verifier, 2000, &mut refused)
            .await
            .unwrap_err();
        assert_eq!(model(&index).checkpoint.unwrap().block, 12);
        assert_eq!(
            refused,
            Some((13, 3)),
            "a range not taken in neither counts nor clears"
        );
        *running.node.extra_logs.lock().unwrap() = seed;
        assert!(
            index
                .poll(&state.verifier, 2000, &mut refused)
                .await
                .unwrap()
        );
        assert!(refused.is_none(), "a range taken in clears it");
        // Head 25 accepted block 13 alone; the next refused block is 14.
        running.node.set_head(40);
        refuse();
        for n in 1..crate::block_logs::PERSISTENT_REFUSALS {
            let error = index
                .poll(&state.verifier, 2000, &mut refused)
                .await
                .unwrap_err();
            assert!(
                error.downcast_ref::<Unreadable>().is_none(),
                "poll {n}: {error:#}"
            );
        }
        let error = index
            .poll(&state.verifier, 2000, &mut refused)
            .await
            .unwrap_err();
        let stopped = error
            .downcast_ref::<Unreadable>()
            .expect("stopped naming the block");
        assert_eq!(stopped.block, 14);
        assert!(error.downcast_ref::<Fatal>().is_none(), "not a rebuild");
        running.stop().await;
    });
}

/// Receipts over the bound stop the keyring at once, naming the block,
/// with its journal intact: the operator changes endpoint or limit.
#[test]
fn receipts_over_the_bound_stop_the_keyring_with_its_journal_intact() {
    with_node("receipts-bound", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        running.node.extra_logs.lock().unwrap().extend([
            log(13, 1, Hex([2; 20]), true, 1, 3),
            log(13, 2, Hex([2; 20]), true, 1, 4),
            log(13, 3, Hex([2; 20]), true, 1, 5),
        ]);
        running.node.max_logs.store(2, Ordering::Relaxed);
        running.node.set_head(25);
        index.receipt_response.store(16, Ordering::Relaxed);
        index.run(&state.verifier).await;
        assert_eq!(index.status()["state"], "stopped");
        let reason = index.status()["reason"].as_str().unwrap().to_string();
        assert!(
            reason.contains("block 13 cannot be read") && reason.contains("response bound"),
            "{reason}"
        );
        assert_eq!(
            model(&index).checkpoint.unwrap().block,
            12,
            "history before it is kept"
        );
        running.stop().await;
    });
}

#[test]
fn keyring_rate_throttle_does_not_try_smaller_ranges_or_receipts() {
    with_node("keyring-rate-throttle", |ctx| async move {
        let (running, state, index) = seeded_index(&ctx).await;
        running.node.set_head(30);
        running.node.response_overrides.lock().unwrap().insert(
            "eth_getLogs".into(),
            json!({"error":{"code":-32005,"message":"rate limited"}}),
        );
        let before = running.node.count("eth_getLogs");
        let mut refused = None;
        assert!(
            index
                .poll(&state.verifier, 2000, &mut refused)
                .await
                .is_err()
        );
        assert_eq!(running.node.count("eth_getLogs"), before + 1);
        assert_eq!(running.node.count("eth_getBlockReceipts"), 0);
        assert_eq!(
            refused, None,
            "throttling cannot count toward stopping the keyring"
        );
        assert_eq!(model(&index).checkpoint.unwrap().block, 12);
        running.stop().await;
    });
}
