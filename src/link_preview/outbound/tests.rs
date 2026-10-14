use super::*;
#[tokio::test]
async fn discovery_snapshots_fail_closed_until_ready_and_after_expiry() {
    let fetcher = Fetcher::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    assert!(matches!(
        fetcher.guard().await,
        Err(Failure::DiscoveryUnavailable)
    ));
    assert!(matches!(
        fetcher.image("https://example.org/pic.png").await,
        Err(Failure::DiscoveryUnavailable)
    ));
    assert!(!fetcher.ready().await);
    let old: IpAddr = "8.8.8.8".parse().unwrap();
    fetcher
        .discovery
        .lock()
        .await
        .update(Instant::now(), HashSet::from([old]))
        .unwrap();
    assert!(fetcher.ready().await);
    let guard = fetcher.guard().await.unwrap();
    assert!(guard.check_url(&url("https://8.8.8.8/").unwrap()).is_err());
    assert!(
        guard
            .check_url(&url("https://operator.invalid/").unwrap())
            .is_err()
    );
    // A failed refresh retains the last good snapshot and its original age.
    assert!(fetcher.refresh().await.is_err());
    assert!(fetcher.guard().await.unwrap().ips.contains(&old));
    fetcher.discovery.lock().await.checked = Some(Instant::now() - REFRESH);
    assert!(!fetcher.ready().await);
    assert!(matches!(
        fetcher.guard().await,
        Err(Failure::DiscoveryUnavailable)
    ));
    let new = "1.1.1.1".parse().unwrap();
    fetcher
        .discovery
        .lock()
        .await
        .update(Instant::now(), HashSet::from([new]))
        .unwrap();
    let guard = fetcher.guard().await.unwrap();
    assert!(guard.ips.contains(&old) && guard.ips.contains(&new));
}

#[tokio::test]
async fn image_checks_refuse_policy_violations_and_pass_public_literals() {
    let fetcher = Fetcher::new(Config {
        public_names: vec!["operator.invalid".into()],
    })
    .unwrap();
    fetcher
        .discovery
        .lock()
        .await
        .update(Instant::now(), HashSet::from(["1.1.1.1".parse().unwrap()]))
        .unwrap();
    for refused in [
        "http://8.8.8.8/i.png",
        "https://operator.invalid/i.png",
        "https://1.1.1.1/i.png",
        "https://127.0.0.1/i.png",
        "not a url",
    ] {
        assert_eq!(
            fetcher.image(refused).await,
            Err(Failure::NoPreview),
            "{refused}"
        );
    }
    assert!(fetcher.image("https://8.8.8.8/i.png").await.is_ok());
}

#[test]
fn retry_after_honors_seconds_and_http_dates() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000000);
    let date = httpdate::fmt_http_date(now + Duration::from_secs(600));
    for value in ["600", &date] {
        assert_eq!(
            retry_after(Some(&value.parse().unwrap()), now),
            Duration::from_secs(600)
        );
    }
    assert_eq!(
        retry_after(Some(&u64::MAX.to_string().parse().unwrap()), now),
        Duration::from_secs(3600),
        "a failure is kept an hour at most"
    );
    let long_date = httpdate::fmt_http_date(now + Duration::from_secs(3 * 86400));
    assert_eq!(
        retry_after(Some(&long_date.parse().unwrap()), now),
        Duration::from_secs(3600)
    );
    for value in ["invalid", "-1", "0"] {
        assert_eq!(
            retry_after(Some(&value.parse().unwrap()), now),
            Duration::from_secs(60)
        );
    }
}
#[test]
fn rejects_local_literal_encodings_ports_and_special_networks() {
    for input in [
        "https://[2606:4700:4700::1111]/",
        "http://127.1/",
        "http://2130706433/",
        "http://0x7f000001/",
        "http://[::ffff:127.0.0.1]/",
        "http://[64:ff9b::7f00:1]/",
        "http://[2002:7f00:1::]/",
        "http://[2001:db8::1]/",
        "http://100.64.1.2/",
        "http://192.168.1.1/",
        "http://169.254.169.254/",
        "http://224.0.0.1/",
        "file:///etc/passwd",
        "http://a:b@example.org/",
        "https://example.org:8080/",
        "https://localhost/",
        "https://name.local/",
    ] {
        assert!(url(input).is_err(), "{input}");
    }
    for input in ["https://example.org:443/a#x", "http://8.8.8.8/"] {
        assert!(url(input).is_ok(), "{input}");
    }
}
#[test]
fn blocks_operator_names_aliases_and_mixed_dns_answers() {
    let guard = Guard {
        names: Arc::new(HashSet::from(["blohard.social".into()])),
        ips: HashSet::from(["8.8.8.8".parse().unwrap()]),
    };
    for name in [
        "https://blohard.social/",
        "https://BLOHARD.social./",
        "http://8.8.8.8/",
    ] {
        assert!(guard.check_url(&url(name).unwrap()).is_err());
    }
    for ips in [vec!["1.1.1.1:0", "127.0.0.1:0"], vec!["8.8.8.8:0"], vec![]] {
        assert!(
            guard
                .addresses(ips.into_iter().map(|s| s.parse().unwrap()).collect())
                .is_err()
        );
    }
    assert!(guard.addresses(vec!["1.1.1.1:0".parse().unwrap()]).is_ok());
    assert!(
        guard
            .addresses(vec!["[2606:4700:4700::1111]:0".parse().unwrap()])
            .is_err()
    );
    let v4: Vec<_> = guard
        .addresses(vec![
            "1.1.1.1:0".parse().unwrap(),
            "[2606:4700:4700::1111]:0".parse().unwrap(),
        ])
        .unwrap()
        .collect();
    assert_eq!(v4, vec!["1.1.1.1:0".parse::<SocketAddr>().unwrap()]);
}
#[test]
fn discovery_retains_old_addresses_until_a_successful_refresh_after_a_week() {
    let mut d = Discovery::default();
    let now = Instant::now();
    let a = "1.1.1.1".parse().unwrap();
    let b = "8.8.8.8".parse().unwrap();
    d.update(now, HashSet::from([a])).unwrap();
    d.update(now + REFRESH, HashSet::from([b])).unwrap();
    assert!(d.seen.contains_key(&a));
    // No update (a failed lookup) changes nothing, even after expiry.
    assert_eq!(d.seen.len(), 2);
    d.update(now + RETAIN, HashSet::from([b])).unwrap();
    assert_eq!(d.seen.keys().copied().collect::<Vec<_>>(), vec![b]);
}

#[test]
fn the_first_public_name_identifies_the_fetcher() {
    let fetcher = Fetcher::new(Config {
        public_names: vec!["Board.Example.".into(), "other.example".into()],
    })
    .unwrap();
    assert_eq!(
        fetcher.user_agent,
        "blohardbot/1.0 (+https://board.example/about)"
    );
}
