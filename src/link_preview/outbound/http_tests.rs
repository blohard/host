use super::*;
use axum::{Router, response::IntoResponse, routing::get};
use std::sync::atomic::{AtomicUsize, Ordering};

async fn fetch(client: &Client, guard: &Guard, target: Url) -> Result<(Url, Vec<u8>), Failure> {
    let destinations = Destinations::default();
    let destination = destinations.take(&target)?;
    let mut page = super::fetch(client, guard, target, &destinations, destination).await?;
    let mut bytes = Vec::new();
    page.read_until(&mut bytes, HTML_BYTES).await?;
    Ok((page.url, bytes))
}

#[tokio::test]
async fn local_admission_refusals_preserve_the_post_allowance() {
    use super::super::{PreviewSource, Previews};
    let service = Arc::new(
        Previews::new(Config {
            public_names: vec!["operator.invalid".into()],
        })
        .unwrap(),
    );
    let target = url("https://busy.example/one").unwrap();
    let first = service.fetcher.destinations.take(&target).unwrap();
    let second = service.fetcher.destinations.take(&target).unwrap();
    for path in ["one", "two"] {
        assert!(matches!(
            service
                .read_admitted(
                    url(&format!("https://busy.example/{path}")).unwrap(),
                    PreviewSource::Post { chain: 1, id: 1 },
                    None
                )
                .await,
            Err(5)
        ));
    }
    assert!(
        service.post_budget.lock().unwrap().0.is_empty(),
        "a busy destination spends no post allowance"
    );
    drop((first, second));

    let shared = service.slots.acquire_many(8).await.unwrap();
    let (a, b) = futures::join!(
        service.read_admitted(
            url("https://one.example/").unwrap(),
            PreviewSource::Post { chain: 1, id: 1 },
            None
        ),
        service.read_admitted(
            url("https://two.example/").unwrap(),
            PreviewSource::Post { chain: 1, id: 1 },
            None
        )
    );
    assert!(matches!((a, b), (Err(60), Err(60))));
    assert!(
        service.post_budget.lock().unwrap().0.is_empty(),
        "a full shared pool spends no post allowance"
    );
    drop(shared);

    // Both attempts remain available. Discovery is deliberately unready,
    // so admitted work stops without contacting an external server.
    for _ in 0..2 {
        assert!(matches!(
            service
                .read_admitted(
                    target.clone(),
                    PreviewSource::Post { chain: 1, id: 1 },
                    None
                )
                .await,
            Err(15)
        ));
    }
    assert!(matches!(
        service
            .read_admitted(target, PreviewSource::Post { chain: 1, id: 1 }, None)
            .await,
        Err(300)
    ));
    assert_eq!(
        service.slots.available_permits(),
        8,
        "post refusal releases local capacity"
    );
}

/// A page that never finishes ends at the fetch's deadline with the
/// five-minute cooldown, and the fetch's slots are free again.
#[tokio::test]
async fn a_page_that_never_finishes_ends_at_the_deadline() {
    use super::super::{PreviewSource, Previews};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/stall",
        get(|| async {
            use futures::StreamExt;
            let chunks = futures::stream::once(async {
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"<head>"))
            })
            .chain(futures::stream::pending());
            (
                [("content-type", "text/html")],
                axum::body::Body::from_stream(chunks),
            )
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut service = Previews::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve("stall.example", address);
    service.fetcher.client = Some(client.build().unwrap());
    service
        .fetcher
        .discovery
        .lock()
        .await
        .update(Instant::now(), HashSet::new())
        .unwrap();
    let link = url("http://stall.example/stall").unwrap();
    let (destination, permits) = service
        .admit(&link, PreviewSource::Post { chain: 1, id: 1 }, None)
        .await
        .unwrap();
    assert_eq!(service.slots.available_permits(), 7);
    let start = Instant::now();
    let deadline = start + Duration::from_millis(300);
    let entry = service
        .fetch_card(link.clone(), destination, permits, deadline)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(2),
        "{elapsed:?}"
    );
    assert_eq!(entry.value, Err(()));
    let cooldown = entry.until.saturating_duration_since(Instant::now());
    assert!(
        cooldown > Duration::from_secs(290) && cooldown <= Duration::from_secs(300),
        "{cooldown:?}"
    );
    assert_eq!(
        service.slots.available_permits(),
        8,
        "the shared slot is free"
    );
    let free = [(); 2].map(|_| service.fetcher.destinations.take(&link));
    assert!(free.iter().all(Result::is_ok), "both host slots are free");
    server.abort();
}

#[tokio::test]
async fn one_post_with_many_subdomains_cannot_starve_a_healthy_preview() {
    use super::super::{PreviewSource, Previews};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (started, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/slow",
            get(move || {
                let started = started.clone();
                async move {
                    use futures::StreamExt;
                    let chunks = futures::stream::once(async move {
                        started.send(()).unwrap();
                        Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"<head>"))
                    })
                    .chain(futures::stream::pending());
                    (
                        [("content-type", "text/html")],
                        axum::body::Body::from_stream(chunks),
                    )
                }
            }),
        )
        .route(
            "/page",
            get(|| async { ([("content-type", "text/html")], "<title>healthy</title>") }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve("healthy.example", address);
    let links: Vec<_> = (0..32)
        .map(|n| format!("http://a{n}.attacker.example/slow"))
        .collect();
    for link in &links {
        client = client.resolve(url(link).unwrap().host_str().unwrap(), address);
    }
    let mut service = Previews::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    service.fetcher.client = Some(client.build().unwrap());
    service
        .fetcher
        .discovery
        .lock()
        .await
        .update(Instant::now(), HashSet::new())
        .unwrap();
    let service = Arc::new(service);
    // All links fit one real body. Only its first link is used by the UI,
    // but the public endpoint accepts any link authorized by this post.
    let body = format!("version/1 text/plain\n{}", links.join(" "));
    assert!(body.len() <= 4000);
    for link in &links {
        assert!(super::super::http::contains_link(body.as_bytes(), link));
    }
    let mut readers = Vec::new();
    for link in links {
        let service = service.clone();
        readers.push(tokio::spawn(async move {
            let visitor = Arc::new(Semaphore::new(2));
            let permit = Arc::new(visitor.try_acquire_owned().unwrap());
            service
                .read_admitted(
                    url(&link).unwrap(),
                    PreviewSource::Post {
                        chain: 84532,
                        id: 1,
                    },
                    Some(permit),
                )
                .await
        }));
    }
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), arrivals.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let healthy = tokio::time::timeout(
        Duration::from_millis(500),
        service.read_admitted(
            url("http://healthy.example/page").unwrap(),
            PreviewSource::Post {
                chain: 84532,
                id: 2,
            },
            None,
        ),
    )
    .await;
    // Abandoning every reader must not reset the post allowance or free its
    // two running fetches. New subdomains still cannot start more work.
    for reader in readers {
        reader.abort();
        let _ = reader.await;
    }
    let late = tokio::time::timeout(
        Duration::from_millis(100),
        service.read_admitted(
            url("http://new.attacker.example/slow").unwrap(),
            PreviewSource::Post {
                chain: 84532,
                id: 1,
            },
            None,
        ),
    )
    .await;
    server.abort();
    assert_eq!(
        healthy.unwrap().unwrap().value.unwrap().unwrap().title,
        "healthy"
    );
    assert!(matches!(late, Ok(Err(1..=300))));
    assert_eq!(service.slots.available_permits(), 6);
    assert!(
        arrivals.try_recv().is_err(),
        "only two attack fetches reached the server"
    );
}

#[tokio::test]
async fn slow_destination_cannot_fill_shared_slots_even_through_redirects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (started, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/slow",
            get(move || {
                let started = started.clone();
                async move {
                    started.send(()).unwrap();
                    std::future::pending::<&'static str>().await
                }
            }),
        )
        .route(
            "/redirect",
            get(|| async {
                (
                    axum::http::StatusCode::FOUND,
                    [("location", "http://slow.example/slow")],
                )
            }),
        )
        .route(
            "/page",
            get(|| async { ([("content-type", "text/html")], "<title>healthy</title>") }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .resolve("slow.example", address)
        .resolve("healthy.example", address)
        .build()
        .unwrap();
    let guard = Guard {
        names: Arc::new(HashSet::new()),
        ips: HashSet::new(),
    };
    let destinations = Arc::new(Destinations::default());
    let shared = Arc::new(Semaphore::new(8));
    let mut requests = Vec::new();
    // Four visitors, each allowed two distinct URLs, target one slow site.
    for n in 0..8 {
        let (client, guard, destinations, shared) = (
            client.clone(),
            guard.clone(),
            destinations.clone(),
            shared.clone(),
        );
        requests.push(tokio::spawn(async move {
            let target = url(&format!("http://slow.example/slow?link={n}")).unwrap();
            let Ok((destination, _slot)) = destinations.admit(&target, &shared).await else {
                return;
            };
            let _ = super::fetch(&client, &guard, target, &destinations, destination).await;
        }));
    }
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), arrivals.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let healthy = url("http://healthy.example/page").unwrap();
    let result = tokio::time::timeout(Duration::from_millis(200), async {
        let (destination, _slot) = destinations.admit(&healthy, &shared).await.unwrap();
        super::fetch(&client, &guard, healthy, &destinations, destination).await
    })
    .await;
    let redirect = url("http://healthy.example/redirect").unwrap();
    let redirected = tokio::time::timeout(Duration::from_millis(200), async {
        let (destination, _slot) = destinations.admit(&redirect, &shared).await.unwrap();
        super::fetch(&client, &guard, redirect, &destinations, destination).await
    })
    .await;
    for request in requests {
        request.abort();
        let _ = request.await;
    }
    server.abort();
    assert!(
        result.unwrap().is_ok(),
        "an unrelated preview still completes"
    );
    assert_eq!(redirected.unwrap().unwrap_err(), Failure::Busy);
    assert_eq!(shared.available_permits(), 8);
    let target = url("http://slow.example/new").unwrap();
    assert!(
        destinations.take(&target).is_ok(),
        "cancellation releases the destination"
    );
}

#[test]
fn destination_table_is_bounded_and_does_not_evict_live_permits() {
    let destinations = Destinations::default();
    let mut live = Vec::new();
    for n in 0..128 {
        live.push(
            destinations
                .take(&url(&format!("https://host{n}.example/")).unwrap())
                .unwrap(),
        );
    }
    assert!(matches!(
        destinations.take(&url("https://extra.example/").unwrap()),
        Err(Failure::Busy)
    ));
    let same = url("https://host0.example/").unwrap();
    let second = destinations.take(&same).unwrap();
    assert!(matches!(destinations.take(&same), Err(Failure::Busy)));
    drop(second);
    drop(live);
    assert!(
        destinations
            .take(&url("https://extra.example/").unwrap())
            .is_ok()
    );
    assert_eq!(destinations.0.lock().unwrap().len(), 1);
    assert_eq!(
        Failure::timeout(),
        Failure::Temporary(Duration::from_secs(300))
    );
}

/// Complete heads stop early; incomplete ones keep reading within the
/// byte and time limits, without stopping at the first title alone.
#[tokio::test]
async fn preview_stops_at_a_finished_head_or_its_byte_and_time_limits() {
    use super::super::{PreviewSource, Previews};
    use axum::extract::Path;
    use futures::StreamExt as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/{case}",
        get(|Path(case): Path<String>| async move {
            let html = match case.as_str() {
                "late" => format!(
                    "<head><script>{}</script><title>late video title</title></head>",
                    " ".repeat(700 << 10)
                ),
                "early" => format!(
                    "<head><title>early title</title></head><body>{}",
                    " ".repeat(FIRST_HTML_BYTES)
                ),
                "later-description" => format!(
                    "<head><title>early title</title><script>{}</script><meta name=description content='later description'></head>",
                    " ".repeat(100 << 10)
                ),
                "head-ended" => format!("<head></head>{}", " ".repeat(FIRST_HTML_BYTES)),
                "body-started" => format!("<head><body>{}", " ".repeat(FIRST_HTML_BYTES)),
                "stalled" => " ".repeat(FIRST_HTML_BYTES),
                _ => " ".repeat(HTML_BYTES),
            };
            let chunks = futures::stream::once(async move {
                Ok::<_, std::io::Error>(bytes::Bytes::from(html))
            });
            // The long heads end normally. The other pages never finish:
            // a successful answer proves we stopped reading them ourselves.
            let body = if matches!(case.as_str(), "late" | "later-description") {
                axum::body::Body::from_stream(chunks)
            } else {
                axum::body::Body::from_stream(chunks.chain(futures::stream::pending()))
            };
            ([("content-type", "text/html")], body)
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut service = Previews::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    service.fetcher.client = Some(
        Client::builder()
            .no_proxy()
            .resolve("public.example", address)
            .build()
            .unwrap(),
    );
    service
        .fetcher
        .discovery
        .lock()
        .await
        .update(Instant::now(), HashSet::new())
        .unwrap();
    for (id, (case, expected)) in [
        ("late", Ok(Some("late video title"))),
        ("early", Ok(Some("early title"))),
        ("later-description", Ok(Some("early title"))),
        ("empty", Ok(None)),
        ("head-ended", Ok(None)),
        ("body-started", Ok(None)),
        ("stalled", Err(())),
    ]
    .into_iter()
    .enumerate()
    {
        let link = url(&format!("http://public.example/{case}")).unwrap();
        let (destination, permits) = service
            .admit(
                &link,
                PreviewSource::Post {
                    chain: 1,
                    id: id as u64,
                },
                None,
            )
            .await
            .unwrap();
        let entry = service
            .fetch_card(
                link.clone(),
                destination,
                permits,
                Instant::now()
                    + if case == "stalled" {
                        Duration::from_millis(100)
                    } else {
                        Duration::from_secs(2)
                    },
            )
            .await
            .unwrap();
        if case == "later-description" {
            assert_eq!(
                entry.value.as_ref().unwrap().as_ref().unwrap().description,
                "later description"
            );
        }
        assert_eq!(
            entry.value.map(|preview| preview.map(|m| m.title)),
            expected.map(|title| title.map(str::to_owned)),
            "{case}"
        );
        if case == "stalled" {
            assert!(entry.until > Instant::now() + Duration::from_secs(290));
        }
        assert_eq!(
            service.slots.available_permits(),
            8,
            "{case}: shared slots released"
        );
        let slots = [(); 2].map(|_| service.fetcher.destinations.take(&link));
        assert!(
            slots.iter().all(Result::is_ok),
            "{case}: destination slots released"
        );
    }
    server.abort();
}

#[tokio::test]
async fn large_html_keeps_head_metadata_and_interrupted_compression_is_temporary() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/large", get(|| async {
            ([("content-type", "text/html")], format!("<head><title>large page</title></head><body>{}", "x".repeat(HTML_BYTES + 64 * 1024)))
        }))
        .route("/broken", get(|| async {
            ([("content-type", "text/html"), ("content-encoding", "gzip")], hex::decode("1f8b08000000000002ffb3c928c9cdb1b3c9484d4cb1b329c92cc949b52bcf2fca2eb6d187706cf4215249f9299576997925a94545a50525a92936fa6011a034483f00651aaf").unwrap())
        }));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = Client::builder()
        .no_proxy()
        .gzip(true)
        .resolve("public.example", address)
        .build()
        .unwrap();
    let guard = Guard {
        names: Arc::new(HashSet::new()),
        ips: HashSet::new(),
    };
    let large = fetch(&client, &guard, url("http://public.example/large").unwrap()).await;
    let broken = fetch(
        &client,
        &guard,
        url("http://public.example/broken").unwrap(),
    )
    .await;
    task.abort();
    assert_eq!(
        broken.unwrap_err(),
        Failure::temporary(),
        "an interrupted download is retryable"
    );
    let (url, bytes) = large.unwrap();
    assert_eq!(bytes.len(), HTML_BYTES);
    let parsed =
        super::super::metadata::parse(&bytes, &url, Instant::now() + Duration::from_secs(1))
            .unwrap()
            .metadata
            .unwrap();
    assert_eq!(parsed.title, "large page");
}

#[tokio::test]
async fn a_connector_dns_refusal_remains_permanent_through_reqwest() {
    struct Reject;
    impl Resolve for Reject {
        fn resolve(&self, _: Name) -> Resolving {
            Box::pin(async { Err(Box::new(Refused) as Box<dyn std::error::Error + Send + Sync>) })
        }
    }
    let client = Client::builder()
        .no_proxy()
        .dns_resolver(Arc::new(Reject))
        .build()
        .unwrap();
    let guard = Guard {
        names: Arc::new(HashSet::new()),
        ips: HashSet::new(),
    };
    assert_eq!(
        fetch(&client, &guard, url("http://public.example/").unwrap())
            .await
            .unwrap_err(),
        Failure::NoPreview
    );
}

#[tokio::test]
async fn redirects_are_rechecked_and_fetching_is_html_only_and_bounded() {
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = requests.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/{path}",
        get(
            move |axum::extract::Path(path): axum::extract::Path<String>| {
                counted.fetch_add(1, Ordering::SeqCst);
                async move {
                    match path.as_str() {
                        "busy" => (axum::http::StatusCode::TOO_MANY_REQUESTS, [("retry-after", "600")]).into_response(),
                        "unavailable" => axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(),
                        "missing" => axum::http::StatusCode::NOT_FOUND.into_response(),
                        "denied" => axum::http::StatusCode::FORBIDDEN.into_response(),
                        "cross" => (
                            axum::http::StatusCode::FOUND,
                            [("location", "http://other.example/page")],
                        )
                            .into_response(),
                        "private" => (
                            axum::http::StatusCode::FOUND,
                            [("location", "http://127.1/secret")],
                        )
                            .into_response(),
                        "operator" => (
                            axum::http::StatusCode::FOUND,
                            [("location", "http://blohard.social/secret")],
                        )
                            .into_response(),
                        "loop" => (axum::http::StatusCode::FOUND, [("location", "/loop")])
                            .into_response(),
                        "compressed" => ([("content-type", "text/html"), ("content-encoding", "gzip")], hex::decode("1f8b08000000000002ffedc13101000000c2a0da8b6f085fa000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004e039f7c1e4401001000").unwrap()).into_response(),
                "picture" => {
                            ([("content-type", "image/png")], "not html").into_response()
                        }
                        "large" => {
                            ([("content-type", "text/html")], "x".repeat(HTML_BYTES + 1))
                                .into_response()
                        }
                        _ => ([("content-type", "text/html")], "<title>works</title>")
                            .into_response(),
                    }
                }
            },
        ),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    // Only this test's client pins synthetic public names to its fixture.
    // Production always installs Guard as the connector's DNS resolver.
    let client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .gzip(true)
        .resolve("public.example", address)
        .resolve("other.example", address)
        .build()
        .unwrap();
    let guard = Guard {
        names: Arc::new(HashSet::from(["blohard.social".into()])),
        ips: HashSet::new(),
    };
    let initial = |path| url(&format!("http://public.example/{path}")).unwrap();
    let (final_url, bytes) = fetch(&client, &guard, initial("cross")).await.unwrap();
    assert_eq!(final_url.host_str(), Some("other.example"));
    assert_eq!(bytes, b"<title>works</title>");
    for path in ["private", "operator", "picture", "missing", "denied"] {
        let before = requests.load(Ordering::SeqCst);
        assert!(
            matches!(
                fetch(&client, &guard, initial(path)).await,
                Err(Failure::NoPreview)
            ),
            "{path}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            before + 1,
            "a forbidden redirect is never contacted"
        );
    }
    for path in ["large", "compressed"] {
        let (_, bytes) = fetch(&client, &guard, initial(path)).await.unwrap();
        assert_eq!(bytes.len(), HTML_BYTES);
    }
    let before = requests.load(Ordering::SeqCst);
    assert!(fetch(&client, &guard, initial("loop")).await.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), before + 4);
    assert_eq!(
        fetch(&client, &guard, initial("busy")).await.unwrap_err(),
        Failure::Temporary(Duration::from_secs(600))
    );
    assert_eq!(
        fetch(&client, &guard, initial("unavailable"))
            .await
            .unwrap_err(),
        Failure::temporary()
    );
    task.abort();
}
