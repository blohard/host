use super::http::unavailable_for;
use super::*;
use axum::{http::StatusCode, response::IntoResponse};

fn exhaust_draft_budget(service: &Previews) {
    let mut budget = service.draft_budget.lock().unwrap();
    let now = budget.last_refill();
    for _ in 0..20 {
        assert!(budget.take(now));
    }
    assert!(!budget.take(now));
}

#[test]
fn post_allowance_is_rolling_chain_scoped_and_never_evicted_early() {
    let mut budget = PostBudget::default();
    let now = Instant::now();
    assert_eq!(budget.take((1, 1), now), Ok(()));
    assert_eq!(budget.take((1, 1), now + Duration::from_secs(10)), Ok(()));
    assert_eq!(budget.take((1, 1), now + Duration::from_secs(299)), Err(1));
    assert_eq!(budget.take((1, 1), now + POST_WINDOW), Ok(()));
    assert_eq!(budget.take((1, 1), now + POST_WINDOW), Err(10));
    assert_eq!(budget.take((2, 1), now + POST_WINDOW), Ok(()));
    for id in 2..POST_BUDGET_CAP as u64 {
        assert_eq!(budget.take((1, id), now + POST_WINDOW), Ok(()));
    }
    assert_eq!(budget.0.len(), POST_BUDGET_CAP);
    assert_eq!(budget.take((3, 1), now + POST_WINDOW), Err(300));
    assert_eq!(budget.take((1, 1), now + POST_WINDOW), Err(10));
    assert_eq!(budget.take((3, 1), now + POST_WINDOW * 2), Ok(()));
    assert_eq!(budget.0.len(), 1);
}

/// Blocking work keeps the fetch's slots until it ends, even after its
/// waiter gives up at the deadline, and then frees them.
#[tokio::test]
async fn blocking_work_keeps_its_slots_after_the_waiter_gives_up() {
    let slots = Arc::new(Semaphore::new(1));
    let permits = FetchPermits {
        _shared: slots.clone().try_acquire_owned().unwrap(),
        _draft: None,
        _visitor: None,
    };
    let (finish, finished) = std::sync::mpsc::channel::<()>();
    let work = blocking_with(permits, move || finished.recv().unwrap());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), work)
            .await
            .is_err(),
        "the waiter gives up while the work runs"
    );
    assert_eq!(slots.available_permits(), 0, "the work still owns its slot");
    finish.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while slots.available_permits() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the slot is free once the work ends");
}

#[tokio::test]
async fn joined_readers_spend_one_post_attempt_and_refusals_do_not_poison_urls() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let shared = service.slots.acquire_many(8).await.unwrap();
    let url = link_url("example.org/").unwrap();
    let a = service.read_admitted(url.clone(), PreviewSource::Post { chain: 1, id: 1 }, None);
    let b = service.read_admitted(url.clone(), PreviewSource::Post { chain: 1, id: 1 }, None);
    tokio::pin!(a, b);
    assert!(futures::poll!(&mut a).is_pending());
    assert!(futures::poll!(&mut b).is_pending());
    drop(shared);
    assert!(matches!(a.await, Err(15)));
    assert!(matches!(b.await, Err(15)));
    assert_eq!(service.post_budget.lock().unwrap().0[&(1, 1)][0], None);
    service
        .post_budget
        .lock()
        .unwrap()
        .take((1, 1), Instant::now())
        .unwrap();
    assert!(matches!(
        service
            .read_admitted(url.clone(), PreviewSource::Post { chain: 1, id: 1 }, None)
            .await,
        Err(1..=300)
    ));
    assert!(service.cache.get(&url.to_string()).is_none());
    // Another post can authorize the same URL after a rate refusal.
    assert!(matches!(
        service
            .read_admitted(url.clone(), PreviewSource::Post { chain: 1, id: 2 }, None)
            .await,
        Err(15)
    ));
    let until = Instant::now() + Duration::from_secs(60);
    service.cache.put(
        url.to_string(),
        Entry {
            value: Ok(None),
            until,
        },
        Duration::from_secs(60),
    );
    assert!(
        service
            .read_admitted(url, PreviewSource::Post { chain: 1, id: 1 }, None)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn abandoned_published_readers_keep_their_visitor_permits() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let _shared = service.slots.acquire_many(8).await.unwrap();
    let visitor = Arc::new(Semaphore::new(2));
    for name in ["one", "two"] {
        let permit = Arc::new(visitor.clone().try_acquire_owned().unwrap());
        let mut read = Box::pin(service.read_admitted(
            link_url(&format!("{name}.example")).unwrap(),
            PreviewSource::Post { chain: 1, id: 1 },
            Some(permit),
        ));
        assert!(futures::poll!(&mut read).is_pending());
        drop(read);
    }
    assert_eq!(
        visitor.available_permits(),
        0,
        "abandoned requests must not free a fetch's visitor slot"
    );
    tokio::time::timeout(Duration::from_secs(4), async {
        while visitor.available_permits() != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn draft_refusals_never_reach_published_readers() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let _shared = service.slots.acquire_many(8).await.unwrap();
    exhaust_draft_budget(&service);
    let url = link_url("example.org/").unwrap();
    let mut draft = Box::pin(service.read(url.clone(), true));
    let mut published = Box::pin(service.read(url, false));
    assert!(futures::poll!(&mut draft).is_pending());
    assert!(futures::poll!(&mut published).is_pending());
    assert!(matches!(draft.await, Err(5)));
    assert!(
        matches!(published.await, Err(60)),
        "published readers must wait for shared capacity, not inherit the draft budget refusal"
    );
}

#[test]
fn draft_results_are_not_discoverable_in_the_public_cache() {
    let service = Previews::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    service.remember(
        PreviewSource::Draft,
        "https://secret.example/".into(),
        &Entry {
            value: Ok(None),
            until: Instant::now() + Duration::from_secs(60),
        },
    );
    for path in [
        "/api/link-preview?chain=1&id=2&link=https://secret.example/",
        "/api/link-preview/draft?link=https://secret.example/",
    ] {
        assert!(service.cached(&path.parse().unwrap()).is_none());
    }
}

#[tokio::test]
async fn abandoned_draft_readers_cannot_free_detached_fetch_slots() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    // Keep work waiting inside the fetch task without making network calls.
    let _shared = service.slots.acquire_many(8).await.unwrap();
    let mut readers = Vec::new();
    for name in ["one", "two"] {
        let service = service.clone();
        readers.push(tokio::spawn(async move {
            service
                .read(link_url(&format!("{name}.example")).unwrap(), true)
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.drafts.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for reader in readers {
        reader.abort();
        let _ = reader.await;
    }
    assert_eq!(
        service.drafts.available_permits(),
        0,
        "fetches still own the two draft slots after readers leave"
    );
    assert!(matches!(
        service.read(link_url("three.example").unwrap(), true).await,
        Err(2)
    ));
    assert!(
        service
            .cache
            .get(&"https://three.example/".into())
            .is_none(),
        "local busy is never cached"
    );
    tokio::time::timeout(Duration::from_secs(4), async {
        while service.drafts.available_permits() != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn draft_budget_allows_twenty_then_one_every_five_seconds() {
    let service = Previews::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    let mut budget = service.draft_budget.lock().unwrap();
    let now = budget.last_refill();
    for _ in 0..20 {
        assert!(budget.take(now));
    }
    assert!(!budget.take(now));
    assert!(!budget.take(now + Duration::from_secs(4)));
    assert!(budget.take(now + Duration::from_secs(5)));
    assert!(!budget.take(now + Duration::from_secs(5)));
    for _ in 0..20 {
        assert!(budget.take(now + Duration::from_secs(1000)));
    }
    assert!(
        !budget.take(now + Duration::from_secs(1000)),
        "idle time cannot enlarge the burst"
    );
}

#[tokio::test]
async fn draft_rate_refusals_are_uncached_and_hits_cost_no_admission() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    exhaust_draft_budget(&service);
    let url = link_url("example.org").unwrap();
    let Err(seconds) = service.read(url.clone(), true).await else {
        panic!("budget must refuse");
    };
    assert_eq!(seconds, 5);
    let response = unavailable_for(seconds);
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "5");
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(service.cache.get(&url.to_string()).is_none());
    assert_eq!(service.drafts.available_permits(), 2);
    let entry = Entry {
        value: Ok(None),
        until: Instant::now() + Duration::from_secs(60),
    };
    service.remember(
        PreviewSource::Post { chain: 1, id: 1 },
        url.to_string(),
        &entry,
    );
    let _slots = service.drafts.acquire_many(2).await.unwrap();
    for draft in [false, true] {
        assert_eq!(
            service.read(url.clone(), draft).await.unwrap().until,
            entry.until,
            "either endpoint reuses a published result without renewing it"
        );
    }
    assert_eq!(service.slots.available_permits(), 8);
}

#[tokio::test]
async fn identical_draft_readers_spend_one_slot_and_one_token() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let _shared = service.slots.acquire_many(8).await.unwrap();
    let url = link_url("example.org").unwrap();
    let a = service.read(url.clone(), true);
    let b = service.read(url, true);
    tokio::pin!(a, b);
    assert!(futures::poll!(&mut a).is_pending());
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.drafts.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(futures::poll!(&mut b).is_pending());
    assert_eq!(service.drafts.available_permits(), 1);
    let mut budget = service.draft_budget.lock().unwrap();
    let now = budget.last_refill();
    for _ in 0..19 {
        assert!(budget.take(now));
    }
    assert!(!budget.take(now));
}

#[test]
fn published_cache_is_reusable_by_drafts_and_slots_are_separate() {
    let service = Previews::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    let entry = Entry {
        value: Ok(Some(Metadata {
            title: "draft".into(),
            description: "description".into(),
            image: None,
        })),
        until: Instant::now() + Duration::from_secs(120),
    };
    service.cache.put(
        "https://example.org/".into(),
        entry,
        Duration::from_secs(120),
    );
    for path in [
        "/api/link-preview/draft?link=example.org",
        "/api/link-preview?chain=1&id=2&link=example.org",
    ] {
        assert_eq!(
            service.cached(&path.parse().unwrap()).unwrap().status(),
            200
        );
    }
    assert!(
        service
            .cached(
                &"/api/link-preview/draft?link=example.org&extra=1"
                    .parse()
                    .unwrap()
            )
            .is_none()
    );
    let a = service.drafts.clone().try_acquire_owned().unwrap();
    let b = service.drafts.clone().try_acquire_owned().unwrap();
    assert!(service.drafts.clone().try_acquire_owned().is_err());
    assert_eq!(
        service.slots.available_permits(),
        8,
        "draft admission does not itself consume shared slots"
    );
    drop((a, b));
    assert_eq!(service.drafts.available_permits(), 2);
}

#[tokio::test]
async fn cached_answers_need_no_discovery_and_keep_their_original_expiry() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let url = outbound::url("https://article.example/path").unwrap();
    let until = Instant::now() + Duration::from_secs(123);
    for value in [Ok(None), Err(())] {
        service.cache.put(
            url.to_string(),
            Entry {
                value: value.clone(),
                until,
            },
            Duration::from_secs(123),
        );
        let entry = service.read(url.clone(), false).await.unwrap();
        assert_eq!(entry.until, until);
        assert_eq!(entry.value, value);
        let response = service
            .cached(
                &"/api/link-preview?chain=1&id=2&link=https%3A%2F%2Farticle.example%2Fpath"
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        if value.is_ok() {
            assert_eq!(response.status(), StatusCode::OK);
            let remaining: u64 = response.headers()["cache-control"]
                .to_str()
                .unwrap()
                .strip_prefix("public, max-age=")
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                remaining < 123,
                "a cache hit must not renew the full lifetime"
            );
        } else {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()["cache-control"], "no-store");
        }
    }
}

#[tokio::test]
async fn local_slot_exhaustion_does_not_cache_a_url_failure() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let slots = service.slots.acquire_many(8).await.unwrap();
    let url = outbound::url("https://example.org/").unwrap();
    assert!(service.read(url.clone(), false).await.is_err());
    assert!(service.cache.get(&url.to_string()).is_none());
    drop(slots);
}

#[test]
fn permanent_refusals_are_cached_as_absent_and_temporary_cooldowns_keep_retry_after() {
    assert!(matches!(
        Entry::from_fetch(Err(outbound::Failure::Busy)),
        Err(5)
    ));
    // An outcome's entry, and the lifetime it was given in seconds.
    let made = |fetched| {
        let before = Instant::now();
        let entry = Entry::from_fetch(fetched).unwrap();
        let lifetime = entry.until.duration_since(before).as_secs();
        (entry, lifetime)
    };
    let metadata = Metadata {
        title: "t".into(),
        description: String::new(),
        image: None,
    };
    assert_eq!(made(Ok(Some((metadata.clone(), false)))).1, 86400);
    assert_eq!(
        made(Ok(Some((metadata, true)))).1,
        3600,
        "an image check that may recover is retried within the hour"
    );
    assert_eq!(made(Err(outbound::Failure::timeout())).1, 300);
    for outcome in [Ok(None), Err(outbound::Failure::NoPreview)] {
        let (entry, lifetime) = made(outcome);
        assert_eq!(lifetime, 3600);
        assert_eq!(entry.value, Ok(None));
        assert_eq!(entry.into_response().status(), StatusCode::OK);
    }
    let (entry, lifetime) = made(Err(outbound::Failure::Temporary(Duration::from_secs(600))));
    assert_eq!(lifetime, 600);
    let until = entry.until;
    let response = entry.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let seconds: u64 = response.headers()["retry-after"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(seconds <= 600);
    assert!(Duration::from_secs(seconds) >= until.saturating_duration_since(Instant::now()));
}

#[test]
fn preview_link_text_cannot_span_words_or_markup() {
    for link in [
        "example.org/a b",
        "example.org/a\nb",
        "example.org/a\tb",
        "example.org/a\0b",
        "example.org/a<b",
        "example.org/a>b",
        "example.org/a\"b",
        "example.org/a`b",
        "example.org/a“b",
    ] {
        assert!(link_url(link).is_err(), "{link:?}");
    }
    for link in ["example.org/a%20b", "example.org/café", "example.org/a(b)"] {
        assert!(link_url(link).is_ok(), "{link:?}");
    }
}

#[test]
fn preview_text_removes_invisible_formatting_and_trailing_spaces() {
    assert_eq!(
        text("a\u{200b}\u{200e}\u{061c}\u{2060}\u{feff}b", 256),
        "ab"
    );
    assert_eq!(text("a 𐀀", 3), "a");
    assert_eq!(text("a \u{202e} \u{2066}", 256), "a");
    // Joining characters matter to scripts and emoji; retain them.
    assert_eq!(text("👩\u{200d}💻", 256), "👩\u{200d}💻");
}

#[tokio::test]
async fn local_discovery_failure_does_not_poison_a_urls_cache() {
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let url = outbound::url("https://example.org/").unwrap();
    let _ = service.read(url.clone(), false).await;
    assert!(service.cache.get(&url.to_string()).is_none());
}
