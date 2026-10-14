//! The blohard host: content storage and an accepted-chain index behind one
//! HTTP API. Either role can run alone; the reference host uses both.
//!
//! - **Store**: immutable bodies and attachments in commonware Freezers.
//!   Accepted index records authenticate uploads and peer copies; a store-only
//!   host verifies them through its private node. Provisional uploads wait in
//!   RAM and are not served until accepted. Attachments must be named by a
//!   stored or held body; provisional bytes remain unserved.
//! - **Gateway**: journaled chain facts and body references, with ordered RAM
//!   indexes serving records, ancestry, feeds, replies, membership and changes.
//!   Bounded admission, estimation, receipt and verified-name services handle
//!   the remaining reads. There is no public Ethereum RPC proxy.
//! - `/api/status` publishes configuration and progress; `/api/content/{hash}`
//!   serves stored bytes. `--web-dir` serves the built app.
//!
//! `server` wires the roles; `routes` mounts HTTP; `index` owns ingestion and
//! queries; `publish`, `peers` and `watcher` admit, fetch and promote content;
//! `store` and `holding` own bytes. `services` and `names` implement live reads.
//! `chain` shares identifiers and header reads; `verifier` and `rpc` provide
//! private chain access. `acceptance` selects the accepted facts for content
//! admission, and `reads` serves status and content.
//! Pure encodings live in `abi`; `http`, `flights` and `expiring` share HTTP
//! refusals, bounded concurrent work and temporary answers. `journal` shares
//! configuration and durable-write helpers between the two indexes.

mod abi;
mod acceptance;
mod block_logs;
mod bot_declarations;
mod chain;
mod config;
mod envelope;
mod expiring;
mod flights;
mod gate_validation;
mod holding;
mod host_policy;
mod http;
#[cfg(test)]
mod http_tests;
mod index;
mod indexed;
mod journal;
mod keccak;
mod keyring;
mod keyring_http;
mod link_preview;
#[cfg(test)]
pub(crate) mod mock_node;
mod names;
mod peers;
mod publish;
mod reads;
mod routes;
mod rpc;
mod server;
mod services;
mod share;
mod share_cache;
mod store;
mod token_bucket;
mod verifier;
mod visitor;
mod watcher;

use clap::Parser;
use commonware_runtime::{Runner as _, tokio as cw_tokio};
use config::{Config, GatewayConfig, StoreConfig};
use holding::HoldingConfig;
use routes::router;
use server::{Options, dev_attachment_policy, open_state};
use std::{path::PathBuf, sync::Arc, time::Duration};
use verifier::Verifier;

#[derive(Parser)]
#[command(
    name = "blohard-host",
    about = "blohard host: a content store for message bodies and attachments, a chain gateway for the web app, or both"
)]
struct Args {
    /// Address to serve HTTP on.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: String,

    /// Directory for the storage partitions (content stores, index journals).
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// TOML config: the chains this host serves and the roles it plays
    /// (see config.example.toml). Required unless --allow-all.
    #[arg(
        long,
        conflicts_with = "allow_all",
        required_unless_present = "allow_all"
    )]
    config: Option<PathBuf>,

    /// Accept publishes without on-chain proof (development only). Attachments
    /// are accepted with default caps and no reference check.
    #[arg(long)]
    allow_all: bool,

    /// Directory of the built web app's files to serve at `/`. Paths that
    /// match no file fall back to its index.html so client-side routes work.
    /// Without it, a minimal built-in page is served.
    #[arg(long)]
    web_dir: Option<PathBuf>,

    /// Threads serving requests and background work. Defaults to one per
    /// core, so a bigger machine serves more without a config change.
    #[arg(long)]
    worker_threads: Option<std::num::NonZeroUsize>,
}

/// Commit the stores and log failures. A failed store needs restart.
async fn commit(state: &server::AppState) {
    if let Some(store) = &state.store
        && let Err(e) = store.commit().await
    {
        tracing::error!("store commit failed: {e:#}");
    }
}

/// The work a host runs beside its HTTP server: started once the state is
/// open, and stopped in order once the server has stopped.
struct Background {
    preview_task: Option<tokio::task::JoinHandle<()>>,
    gate_validation_task: tokio::task::JoinHandle<()>,
    committer_task: tokio::task::JoinHandle<()>,
    promoter_task: tokio::task::JoinHandle<()>,
    sweeps: Vec<tokio::task::JoinHandle<()>>,
}

impl Background {
    fn start(state: &Arc<server::AppState>) -> Self {
        for keyring in state.keyrings.values() {
            let keyring = Arc::clone(keyring);
            let state = state.clone();
            tokio::spawn(async move { keyring.run(&state.verifier).await });
        }
        let preview_task = state
            .gateway
            .as_ref()
            .and_then(|g| g.link_previews.clone())
            .map(|previews| tokio::spawn(async move { previews.run().await }));
        let gate_validation_task = gate_validation::start(state.clone());
        index::follow::start(state.clone());
        let sweeps = index::bodies::start(state.clone());
        // Every few seconds: commit what the stores took since the last tick,
        // and, on its own clock, promote held uploads whose messages became
        // accepted. Separate loops keep slow promotion reads from delaying
        // content commits. Crash loss depends on the last completed commit,
        // not a promised wall-clock interval. Both need the store role.
        let committer_task = every(Duration::from_secs(3), state, |state| async move {
            commit(&state).await
        });
        let promoter_task = every(Duration::from_secs(3), state, |state| async move {
            watcher::promote_once(&state).await;
        });
        Self {
            preview_task,
            gate_validation_task,
            committer_task,
            promoter_task,
            sweeps,
        }
    }

    /// Stop in order, so the final commit covers every write.
    async fn stop(self, state: &server::AppState) {
        // 1. The link-preview refresh, which keeps nothing on disk.
        if let Some(task) = self.preview_task {
            task.abort();
            let _ = task.await;
        }
        // 2. Journal the empty progress each index served from RAM, so a
        //    clean restart resumes from the checkpoint it last served.
        for (chain, keyring) in &state.keyrings {
            flush_or_warn(
                &format!("chain {chain} keyring checkpoint"),
                keyring.shutdown(),
            )
            .await;
        }
        if let Some(gateway) = &state.gateway {
            for (chain, index) in &gateway.indexes {
                flush_or_warn(&format!("chain {chain} index checkpoint"), index.flush()).await;
            }
        }
        // 3. The other loops: gate checks, promotion, commits and body
        //    sweeps. abort() only asks; awaiting each task is what
        //    guarantees it has stopped, so none writes after the commit.
        let mut tasks = vec![
            self.gate_validation_task,
            self.promoter_task,
            self.committer_task,
        ];
        tasks.extend(self.sweeps);
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        // 4. Uploads already admitted: they keep going after their client
        //    disconnects, and the commit should cover what they write. No
        //    new upload can start, since the server has stopped. After ten
        //    seconds the commit goes ahead anyway: best effort, logged. A
        //    write under way still finishes first, as it holds the store's
        //    lock.
        if let Some(store) = &state.store
            && tokio::time::timeout(Duration::from_secs(10), store.uploads.drain())
                .await
                .is_err()
        {
            tracing::warn!("uploads still running after ten seconds; committing anyway");
        }
        // 5. The final commit.
        commit(state).await;
    }
}

/// Run `work` at once, then every `period` until aborted. A run that takes
/// longer than `period` is followed straight away by one more, and the
/// schedule restarts from there: missed runs are not made up in a burst.
fn every<Fut>(
    period: Duration,
    state: &Arc<server::AppState>,
    work: fn(Arc<server::AppState>) -> Fut,
) -> tokio::task::JoinHandle<()>
where
    Fut: Future<Output = ()> + Send + 'static,
{
    let state = state.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            work(state.clone()).await;
        }
    })
}

/// Wait up to five seconds for a checkpoint flush, and log one that fails or
/// takes longer.
async fn flush_or_warn(what: &str, flush: impl Future<Output = anyhow::Result<()>>) {
    match tokio::time::timeout(Duration::from_secs(5), flush).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(?error, "{what} not flushed"),
        Err(_) => tracing::warn!("{what} flush timed out"),
    }
}

/// Resolves on SIGTERM or ctrl-c.
async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutting down");
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // One HTTP client for every private chain and name upstream. A stalled upstream must fail a request, not hold it open.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client");
    // Both roles in dev mode; whatever the config enables otherwise.
    let (verifier, store, gateway, notice, keyrings, policy) = if args.allow_all {
        tracing::warn!("running in --allow-all mode: publishes are NOT verified on-chain");
        let store = StoreConfig {
            max_body_bytes: 0,
            page_cache_mib: 0,
            attachments: Some(dev_attachment_policy()),
            peers: Vec::new(),
        };
        (
            Verifier::AllowAll,
            Some(store),
            Some(GatewayConfig::default()),
            None,
            Vec::new(),
            None,
        )
    } else {
        let path = args.config.as_deref().expect("clap enforces --config");
        let cfg = Config::load(path).unwrap_or_else(|e| {
            eprintln!("error: {e:#}");
            std::process::exit(2);
        });
        let verifier = Verifier::from_config(&cfg, client.clone()).unwrap_or_else(|e| {
            eprintln!("error: {e:#}");
            std::process::exit(2);
        });
        (
            verifier,
            cfg.store,
            cfg.gateway,
            cfg.notice,
            cfg.keyrings,
            cfg.host_policy,
        )
    };
    if args.web_dir.is_some() && gateway.is_none() {
        tracing::warn!("--web-dir given but there is no [gateway]: the web app is not served");
    }

    let workers = args
        .worker_threads
        .or_else(|| std::thread::available_parallelism().ok())
        .map_or(2, std::num::NonZeroUsize::get);
    let cfg = cw_tokio::Config::default()
        .with_storage_directory(&args.data_dir)
        .with_worker_threads(workers);
    let runner = cw_tokio::Runner::new(cfg);
    runner.start(|context| async move {
        let state = open_state(
            context,
            verifier,
            Options {
                store,
                gateway,
                holding: HoldingConfig::default(),
                notice,
                keyrings,
                policy,
            },
            client,
        )
        .await
        .expect("failed to open storage");
        let state = Arc::new(state);
        let background = Background::start(&state);

        let listener = tokio::net::TcpListener::bind(&args.listen)
            .await
            .expect("failed to bind listen address");
        tracing::info!(workers, "serving on http://{}", args.listen);
        // Finish in-flight requests and commit the stores on a clean stop.
        // Held uploads remain memory-only; a failed commit is logged.
        axum::serve(
            listener,
            router(state.clone(), args.web_dir)
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
        background.stop(&state).await;
        tracing::info!("stopped");
    });
}
