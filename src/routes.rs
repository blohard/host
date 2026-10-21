//! The HTTP surface: route registration, admission policies and middleware.
//!
//! Every API route declares its policy through `ApiRoutes`; `PageRoutes`
//! keeps app and share pages outside that API. Handlers live in `publish`
//! (uploads), `reads` (status and content), `indexed` (indexed queries),
//! `keyring_http` (account keys), `services` and `names` (bounded live reads),
//! `link_preview` (external previews) and `share` (public page metadata).

use crate::http::json_said;
use crate::publish::http::publish;
use crate::reads::{content, status};
use crate::server::AppState;
use axum::{
    Router,
    body::Body,
    extract::DefaultBodyLimit,
    http::{
        HeaderValue, Method, Request, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER},
    },
    middleware::{self, Next},
    response::{Html, Response},
    routing::{MethodRouter, any, get, post},
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, TraceLayer},
};

/// The most any one request may take, from first byte to last.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Caching for the web app's files. Vite names everything under `/assets/`
/// by content hash, so those may be kept for a year; everything else
/// (`index.html`, `embed.html`, `embed.js`, icons) is an entry point a browser
/// must check with the server before reusing, or a page it kept would ask for
/// asset files a later build has removed. The check is a conditional request
/// the file service answers with 304, which carries the header too so a
/// changed policy reaches copies already kept. Retaining assets for already
/// open tabs is the deployer's responsibility. A miss is never stored.
async fn web_cache_headers(req: Request<Body>, next: Next) -> Response {
    let hashed = req.uri().path().starts_with("/assets/");
    let mut res = next.run(req).await;
    let status = res.status();
    let value = if !(status.is_success() || status == StatusCode::NOT_MODIFIED) {
        "no-store"
    } else if hashed {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    res.headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(value));
    res
}

/// API answers are not to be stored unless a handler says so: a content 404
/// means "not here yet" and the status changes as the host runs. A successful
/// content download sets its own five-minute policy (reads.rs), which stands.
async fn api_cache_headers(req: Request<Body>, next: Next) -> Response {
    let mut res = next.run(req).await;
    if !res.headers().contains_key(CACHE_CONTROL) {
        res.headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    res
}

/// Every API route declares its admission policy at registration. The raw
/// router stays private until all API routes in a group have been registered.
#[derive(Clone, Copy)]
enum Admission {
    Free,
    /// A record this host has, or a request that can lead to no peer work,
    /// is free; one naming a message for a record this host lacks spends
    /// the reader's live-read budget, since checking the message asks the
    /// chain before any peer is tried.
    Content,
    Read,
    Name,
    ReverseName,
    Publish,
    Preview,
    DraftPreview,
}
struct ApiRoutes {
    routes: Router<Arc<AppState>>,
    state: Arc<AppState>,
    limits: Arc<crate::visitor::Limits>,
}
impl ApiRoutes {
    fn new(state: Arc<AppState>, limits: Arc<crate::visitor::Limits>) -> Self {
        Self {
            routes: Router::new(),
            state,
            limits,
        }
    }
    fn route(
        mut self,
        path: &str,
        method: MethodRouter<Arc<AppState>>,
        admission: Admission,
    ) -> Self {
        let state = self.state.clone();
        let limits = self.limits.clone();
        self.routes = self.routes.route(
            path,
            method.layer(middleware::from_fn(
                move |mut request: axum::extract::Request, next: Next| {
                    let state = state.clone();
                    let limits = limits.clone();
                    async move {
                        use crate::visitor::Kind;
                        let kind = match admission {
                            Admission::Free => return next.run(request).await,
                            Admission::Read => Kind::Read,
                            Admission::Content => {
                                if !crate::reads::asks_the_chain(&mut request, &state).await {
                                    return next.run(request).await;
                                }
                                Kind::Read
                            }
                            Admission::Name => Kind::Name,
                            Admission::ReverseName => {
                                if let Some(response) =
                                    crate::names::cached_response(&mut request, &state).await
                                {
                                    return response;
                                }
                                Kind::Name
                            }
                            Admission::Publish => {
                                if crate::publish::http::declared_size(request.headers(), &state).1
                                {
                                    Kind::SmallPublish
                                } else {
                                    Kind::Publish
                                }
                            }
                            Admission::Preview | Admission::DraftPreview => {
                                if let Some(response) =
                                    crate::link_preview::authorize_or_cached_response(
                                        &request, &state,
                                    )
                                {
                                    return response;
                                }
                                if matches!(admission, Admission::DraftPreview) {
                                    Kind::DraftPreview
                                } else {
                                    Kind::Preview
                                }
                            }
                        };
                        crate::visitor::guard(request, next, limits, kind).await
                    }
                },
            )),
        );
        self
    }
    fn finish(self) -> Router<Arc<AppState>> {
        self.routes
    }
}

/// Routes outside the API: share pages and the app shell. They take no
/// admission policy, so an `/api` path registered here would skip the visitor
/// budget; building the router refuses one.
struct PageRoutes(Router<Arc<AppState>>);
impl PageRoutes {
    fn new() -> Self {
        Self(Router::new())
    }
    fn route(self, path: &str, method: MethodRouter<Arc<AppState>>) -> Self {
        assert!(
            path != "/api" && !path.starts_with("/api/"),
            "{path} is an API route and must declare its admission policy"
        );
        Self(self.0.route(path, method))
    }
    fn finish(self) -> Router<Arc<AppState>> {
        self.0
    }
}

/// The HTTP surface. Separated from `main` so tests can drive it directly.
pub fn router(state: Arc<AppState>, web_dir: Option<PathBuf>) -> Router {
    // Let a handler see a slightly-too-large body so it can answer 413 with
    // our own message instead of axum's generic one. The publish route takes
    // content up to the store's cap; domain requests buffer far less.
    let body_limit = state.max_upload() + 1024;
    // Serving the built app requires the gateway role.
    let web_dir = web_dir.filter(|_| state.gateway.is_some());
    let visitor_limits = Arc::new(crate::visitor::Limits::default());
    let domain = ApiRoutes::new(state.clone(), visitor_limits.clone())
        .route(
            "/api/chains/{chain_id}/keyring/activity",
            get(crate::keyring_http::activity),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/keyring/{holder}",
            get(crate::keyring_http::lookup),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/keyring/{holder}/balances",
            get(crate::keyring_http::balances),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/keyring/{holder}/trades",
            get(crate::keyring_http::trades),
            Admission::Free,
        )
        .route(
            "/api/names/{address}",
            get(crate::names::lookup),
            Admission::ReverseName,
        )
        .route(
            "/api/names/resolve/{name}",
            get(crate::names::resolve),
            Admission::Name,
        )
        .route(
            "/api/chains/{chain_id}/key-holdings",
            post(crate::keyring_http::holdings),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/admission",
            post(crate::services::admission),
            Admission::Read,
        )
        .route(
            "/api/chains/{chain_id}/post-estimate",
            post(crate::services::post_estimate),
            Admission::Read,
        )
        .route(
            "/api/chains/{chain_id}/transaction/{hash}",
            get(crate::services::transaction),
            Admission::Read,
        )
        .route(
            "/api/chains/{chain_id}/state",
            post(crate::indexed::snapshots),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/list",
            post(crate::indexed::list),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/lookup",
            post(crate::indexed::lookup),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/feed",
            post(crate::indexed::feed),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/message/{id}",
            get(crate::indexed::message),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/replies",
            post(crate::indexed::replies),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/changes",
            post(crate::indexed::changes),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/references",
            post(crate::indexed::references),
            Admission::Free,
        )
        .route(
            "/api/chains/{chain_id}/notifications",
            post(crate::indexed::notifications),
            Admission::Free,
        )
        .finish()
        .layer(DefaultBodyLimit::max(64 << 10))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers([CONTENT_TYPE])
                .expose_headers([RETRY_AFTER]),
        );
    // Preview fetches are app-origin only; the public API's permissive CORS
    // policy must not authorize browser calls to these resource-consuming routes.
    let previews = ApiRoutes::new(state.clone(), visitor_limits.clone())
        .route(
            "/api/host-policy",
            get(crate::host_policy::http::read).post(crate::host_policy::http::write),
            Admission::Read,
        )
        .route(
            "/api/bot-declaration",
            post(crate::host_policy::http::declare),
            Admission::Read,
        )
        .route(
            "/api/host-policy/takedowns",
            post(crate::host_policy::http::write_takedowns),
            Admission::Read,
        )
        .route(
            "/api/host-policy/takedowns/read",
            post(crate::host_policy::http::takedowns),
            Admission::Read,
        )
        .route(
            "/api/link-preview",
            get(crate::link_preview::lookup),
            Admission::Preview,
        )
        .route(
            "/api/link-preview/draft",
            get(crate::link_preview::draft),
            Admission::DraftPreview,
        )
        .finish();
    let api = ApiRoutes::new(state.clone(), visitor_limits)
        .route("/api/status", get(status), Admission::Free)
        .route("/api/publish", post(publish), Admission::Publish)
        .route("/api/content/{hash}", get(content), Admission::Content)
        // Unknown API paths must not fall through to the web app's index.html.
        .route("/api/{*rest}", any(api_not_found), Admission::Free)
        .finish()
        .merge(previews.layer(DefaultBodyLimit::max(16 << 10)))
        .merge(domain)
        .layer(middleware::from_fn(api_cache_headers));
    let app = match web_dir {
        Some(dir) => {
            // Hashed assets answer 404 when missing: the page fallback must not
            // hand out index.html under a name a browser would keep for a year.
            let share = Arc::new(crate::share::Pages::new(dir.clone()));
            // One handler serves every profile tab; each route names its tab.
            use crate::share::ProfileTab;
            let profile = |tab: ProfileTab| get(crate::share::profile).layer(axum::Extension(tab));
            let assets = ServeDir::new(dir.join("assets"));
            let pages = ServeDir::new(&dir).fallback(ServeFile::new(dir.join("index.html")));
            let web = Router::new()
                .nest_service("/assets", assets)
                .fallback_service(pages)
                .layer(middleware::from_fn(web_cache_headers));
            let pages = PageRoutes::new()
                .route("/p/{chain_id}/{post_id}", get(crate::share::page))
                .route("/u/{chain_id}/{address}", profile(ProfileTab::Posts))
                .route(
                    "/u/{chain_id}/{address}/replies",
                    profile(ProfileTab::Replies),
                )
                .route("/u/{chain_id}/{address}/keys", profile(ProfileTab::Keys))
                .route(
                    "/u/{chain_id}/{address}/keyring",
                    profile(ProfileTab::Keyring),
                )
                .route(
                    "/u/{chain_id}/{address}/keyring/holders",
                    profile(ProfileTab::Holders),
                )
                .route("/about", get(crate::share::about))
                .route("/keys/{chain_id}/about", get(crate::share::keys_about))
                .finish()
                .layer(axum::Extension(share));
            api.merge(pages).fallback_service(web)
        }
        None => api.merge(PageRoutes::new().route("/", get(index)).finish()),
    };
    // Each request's span carries method, path and client (as Caddy forwards
    // it) into what is logged while it runs. Its closing line is debug: Caddy's
    // access log already records every request, and one info line each is
    // what a busy host would spend its log on.
    // A 5xx is already logged where it is produced, so the classifier's own
    // line is a warning, not a second error.
    let trace = TraceLayer::new_for_http()
        .on_failure(DefaultOnFailure::new().level(tracing::Level::WARN))
        .make_span_with(|req: &Request<Body>| {
            // The address the visitor is charged as, not the header's whole value.
            let client = crate::http::client(
                req.extensions()
                    .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                    .map(|c| c.0),
                req.headers(),
            );
            tracing::info_span!("http", method = %req.method(), path = %req.uri().path(), %client)
        })
        .on_response(
            |res: &axum::http::Response<Body>, latency: Duration, _: &tracing::Span| {
                // A timeout is worth a warning: a slow client, or a stuck upstream.
                let status = res.status().as_u16();
                if res.status() == StatusCode::REQUEST_TIMEOUT {
                    tracing::warn!(status, ?latency, "request timed out");
                } else {
                    tracing::debug!(status, ?latency, "finished");
                }
            },
        );
    // A request that has not finished in a minute, body included, is cut off
    // with a 408, so a slow sender cannot hold a handler open.
    app.layer(DefaultBodyLimit::max(body_limit))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REQUEST_TIMEOUT,
        ))
        .layer(trace)
        .with_state(state)
}

async fn api_not_found() -> Response {
    json_said(StatusCode::NOT_FOUND, "not_found", "no such API endpoint")
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

#[cfg(test)]
mod page_route_tests {
    use super::*;

    #[test]
    fn page_routes_refuse_api_paths_which_would_skip_admission() {
        for path in [
            "/api",
            "/api/",
            "/api/new-endpoint",
            "/api/chains/{chain_id}/feed",
        ] {
            let refused = std::panic::catch_unwind(|| {
                PageRoutes::new().route(path, get(|| async { "" }));
            });
            assert!(refused.is_err(), "{path}");
        }
        for path in ["/", "/about", "/apiary", "/p/{chain_id}/{post_id}"] {
            PageRoutes::new().route(path, get(|| async { "" }));
        }
    }
}
