//! Outbound HTTP for untrusted links. DNS answers used by the connector are
//! checked, redirects are explicit, and the operator's public addresses are
//! excluded as well as non-public networks.
use anyhow::{Result, anyhow, ensure};
use reqwest::{
    Client, Url,
    dns::{Addrs, Name, Resolve, Resolving},
};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

pub const URL_BYTES: usize = 2048;
pub const FIRST_HTML_BYTES: usize = 32 << 10;
pub const PREVIEW_HTML_BYTES: usize = 512 << 10;
pub const HTML_BYTES: usize = 1 << 20;
const REFRESH: Duration = Duration::from_secs(300);
const RETAIN: Duration = Duration::from_secs(7 * 86400);

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Every public DNS name through which this machine/network is reachable. The first one
    /// also identifies this host to the sites it fetches.
    pub public_names: Vec<String>,
}

pub fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192
                    && (b == 168 || (b == 0 && (c == 0 || c == 2)) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        // Globally routed IPv6 can still reach another device on the home LAN.
        // This fetcher uses IPv4 only, including when a name has both families.
        IpAddr::V6(_) => false,
    }
}

fn hostname(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}
fn literal(host: &str) -> Option<IpAddr> {
    host.trim_matches(['[', ']']).parse().ok()
}

pub fn url(input: &str) -> Result<Url> {
    ensure!(input.len() <= URL_BYTES, "url too long");
    let mut url = Url::parse(input)?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "unsupported scheme"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "credentials forbidden"
    );
    ensure!(url.port().is_none(), "nonstandard port");
    let host = url.host_str().ok_or_else(|| anyhow!("missing host"))?;
    ensure!(literal(host).is_none_or(public), "non-public address");
    // Single-label/local names must not reach the machine's DNS search path.
    ensure!(
        literal(host).is_some() || host.trim_end_matches('.').contains('.'),
        "local name"
    );
    ensure!(
        !hostname(host).ends_with(".local") && !hostname(host).ends_with(".localhost"),
        "local name"
    );
    url.set_fragment(None);
    ensure!(url.as_str().len() <= URL_BYTES, "url too long");
    Ok(url)
}

#[derive(Clone)]
struct Guard {
    names: Arc<HashSet<String>>,
    ips: HashSet<IpAddr>,
}
impl Guard {
    fn check_url(&self, url: &Url) -> Result<()> {
        let host = url.host_str().ok_or_else(|| anyhow!("missing host"))?;
        ensure!(!self.names.contains(&hostname(host)), "operator hostname");
        ensure!(
            literal(host).is_none_or(|ip| public(ip) && !self.ips.contains(&ip)),
            "operator address"
        );
        Ok(())
    }
    fn addresses(&self, addresses: Vec<SocketAddr>) -> Result<Addrs> {
        let addresses: Vec<_> = addresses.into_iter().filter(SocketAddr::is_ipv4).collect();
        ensure!(
            !addresses.is_empty() && addresses.len() <= 32,
            "invalid dns answer"
        );
        ensure!(
            addresses
                .iter()
                .all(|a| public(a.ip()) && !self.ips.contains(&a.ip())),
            "forbidden dns answer"
        );
        Ok(Box::new(addresses.into_iter()))
    }
}
impl Resolve for Guard {
    fn resolve(&self, name: Name) -> Resolving {
        let guard = self.clone();
        Box::pin(async move {
            if guard.names.contains(&hostname(name.as_str())) {
                return Err(Box::new(Refused) as Box<dyn std::error::Error + Send + Sync>);
            }
            // This is the connector's own resolution, not a preflight followed
            // by a second, unchecked DNS lookup. Absolute names avoid search suffixes.
            let name = format!("{}.", hostname(name.as_str()));
            let addresses = tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            guard
                .addresses(addresses)
                .map_err(|_| Box::new(Refused) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

#[derive(Default)]
struct Discovery {
    checked: Option<Instant>,
    seen: HashMap<IpAddr, Instant>,
}
impl Discovery {
    fn update(&mut self, now: Instant, addresses: HashSet<IpAddr>) -> Result<()> {
        // Called only when every configured name resolved successfully. Failed
        // refreshes neither forget old addresses nor advance the expiry clock.
        let mut seen = self.seen.clone();
        for address in addresses {
            seen.insert(address, now);
        }
        seen.retain(|_, last| now.duration_since(*last) < RETAIN);
        ensure!(seen.len() <= 256, "too many operator addresses");
        self.seen = seen;
        self.checked = Some(now);
        Ok(())
    }
}

// Only active destinations are retained. Admission precedes the global pool so
// requests for one slow host cannot queue while holding all shared capacity.
#[derive(Default)]
struct Destinations(StdMutex<HashMap<String, Weak<Semaphore>>>);
#[derive(Debug)]
pub(super) struct Destination {
    host: String,
    _permit: OwnedSemaphorePermit,
}
impl Destinations {
    fn take(&self, url: &Url) -> Result<Destination, Failure> {
        let host = hostname(url.host_str().ok_or(Failure::NoPreview)?);
        let mut hosts = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slots = match hosts.get(&host).and_then(Weak::upgrade) {
            Some(slots) => slots,
            None => {
                hosts.retain(|_, slots| slots.strong_count() != 0);
                if hosts.len() >= 128 {
                    return Err(Failure::Busy);
                }
                let slots = Arc::new(Semaphore::new(2));
                hosts.insert(host.clone(), Arc::downgrade(&slots));
                slots
            }
        };
        let permit = slots.try_acquire_owned().map_err(|_| Failure::Busy)?;
        Ok(Destination {
            host,
            _permit: permit,
        })
    }
    async fn admit(
        &self,
        url: &Url,
        shared: &Arc<Semaphore>,
    ) -> Result<(Destination, OwnedSemaphorePermit), u64> {
        let destination = self.take(url).map_err(|_| super::RETRY_BUSY)?;
        let slot = tokio::time::timeout(Duration::from_secs(3), shared.clone().acquire_owned())
            .await
            .map_err(|_| super::RETRY_NO_SLOT)?
            .map_err(|_| super::RETRY_NO_SLOT)?;
        Ok((destination, slot))
    }
}

pub struct Fetcher {
    names: Arc<HashSet<String>>,
    user_agent: String,
    discovery: Mutex<Discovery>,
    destinations: Destinations,
    #[cfg(test)]
    client: Option<Client>,
}
impl Fetcher {
    pub fn new(config: Config) -> Result<Self> {
        ensure!(
            !config.public_names.is_empty() && config.public_names.len() <= 16,
            "link previews need 1 to 16 public names"
        );
        let mut names = HashSet::new();
        let mut user_agent = None;
        for name in config.public_names {
            let parsed = url(&format!("https://{name}/"))?;
            let host = parsed.host_str().unwrap();
            ensure!(
                literal(host).is_none() && hostname(host) == hostname(&name),
                "expected a DNS name"
            );
            // Identify as a bot so sites can serve metadata without running JavaScript,
            // pointing them at this host's about page.
            user_agent.get_or_insert_with(|| {
                format!("blohardbot/1.0 (+https://{}/about)", hostname(host))
            });
            names.insert(hostname(host));
        }
        Ok(Self {
            names: Arc::new(names),
            user_agent: user_agent.unwrap(),
            discovery: Mutex::new(Discovery::default()),
            destinations: Destinations::default(),
            #[cfg(test)]
            client: None,
        })
    }
    pub fn allows_origin(&self, origin: &str) -> bool {
        Url::parse(origin).is_ok_and(|url| {
            url.scheme() == "https"
                && url.origin().ascii_serialization() == origin
                && url.port().is_none()
                && url.host_str().is_some_and(|name| self.names.contains(name))
        })
    }

    // Discovery runs independently of requests. Refresh before the five-minute
    // snapshot expires, but fail closed if DNS stays unavailable beyond it.
    pub async fn run(&self) {
        let mut warned = false;
        loop {
            let delay = match self.refresh().await {
                Ok(()) => {
                    if warned {
                        tracing::info!("preview address discovery recovered");
                        warned = false;
                    }
                    Duration::from_secs(240)
                }
                Err(error) => {
                    if !self.ready().await && !warned {
                        tracing::warn!(
                            ?error,
                            "link previews unavailable: operator address discovery is not current"
                        );
                        warned = true;
                    } else {
                        tracing::debug!(?error, "preview address discovery unavailable");
                    }
                    Duration::from_secs(15)
                }
            };
            tokio::time::sleep(delay).await;
        }
    }

    async fn refresh(&self) -> Result<()> {
        let addresses = tokio::time::timeout(Duration::from_secs(5), async {
            let mut addresses = HashSet::new();
            for name in self.names.iter() {
                let absolute = format!("{name}.");
                let ips: Vec<_> = tokio::net::lookup_host((absolute.as_str(), 0))
                    .await?
                    .filter(SocketAddr::is_ipv4)
                    .map(|a| a.ip())
                    .collect();
                ensure!(
                    !ips.is_empty() && ips.len() <= 32,
                    "operator DNS unavailable"
                );
                ensure!(
                    ips.iter().copied().all(public),
                    "operator DNS must return public addresses"
                );
                addresses.extend(ips);
            }
            Ok::<_, anyhow::Error>(addresses)
        })
        .await??;
        self.discovery
            .lock()
            .await
            .update(Instant::now(), addresses)
    }

    pub async fn ready(&self) -> bool {
        self.discovery
            .lock()
            .await
            .checked
            .is_some_and(|at| at.elapsed() < REFRESH)
    }

    async fn guard(&self) -> Result<Guard, Failure> {
        let discovery = self.discovery.lock().await;
        if discovery.checked.is_none_or(|at| at.elapsed() >= REFRESH) {
            return Err(Failure::DiscoveryUnavailable);
        }
        Ok(Guard {
            names: self.names.clone(),
            ips: discovery.seen.keys().copied().collect(),
        })
    }
    pub async fn admit(
        &self,
        url: &Url,
        shared: &Arc<Semaphore>,
    ) -> Result<(Destination, OwnedSemaphorePermit), u64> {
        self.destinations.admit(url, shared).await
    }
    /// Open a page within the caller's deadline, which also covers its body
    /// reads and parsing. The page keeps its destination slot until dropped.
    pub async fn fetch(&self, initial: Url, destination: Destination) -> Result<Page, Failure> {
        let guard = self.guard().await?;
        // A fresh client prevents an idle connection outliving its address
        // policy. Never inherit proxy configuration, cookies or credentials.
        let client = Client::builder()
            .no_proxy()
            .referer(false)
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(Arc::new(guard.clone()))
            .http1_only()
            .user_agent(self.user_agent.as_str())
            .gzip(true)
            .brotli(true)
            .deflate(true)
            .connect_timeout(Duration::from_secs(3))
            .build()
            .map_err(Failure::request)?;
        #[cfg(test)]
        let client = self.client.as_ref().unwrap_or(&client).clone();
        fetch(&client, &guard, initial, &self.destinations, destination).await
    }
    /// Validate an image address without requesting its bytes. This cannot
    /// police a browser's later DNS lookup or an image server's redirects.
    /// A refused address is a stable lack of an image; a lookup that timed out
    /// or failed may recover, and is reported as temporary.
    pub async fn image(&self, input: &str) -> Result<Url, Failure> {
        let guard = self.guard().await?;
        let url = url(input).map_err(|_| Failure::NoPreview)?;
        if url.scheme() != "https" || guard.check_url(&url).is_err() {
            return Err(Failure::NoPreview);
        }
        let host = url.host_str().unwrap();
        if literal(host).is_none() {
            let name = host.parse().map_err(|_| Failure::NoPreview)?;
            match tokio::time::timeout(Duration::from_secs(3), guard.resolve(name)).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) if error.downcast_ref::<Refused>().is_some() => {
                    return Err(Failure::NoPreview);
                }
                Ok(Err(_)) | Err(_) => return Err(Failure::temporary()),
            }
        }
        Ok(url)
    }
}

/// A policy refusal is a stable lack of a preview. Network failures can recover.
#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    NoPreview,
    Temporary(Duration),
    DiscoveryUnavailable,
    Busy,
}
impl Failure {
    pub fn temporary() -> Self {
        Self::Temporary(Duration::from_secs(60))
    }
    pub fn timeout() -> Self {
        Self::Temporary(Duration::from_secs(300))
    }
    fn request(error: reqwest::Error) -> Self {
        // Reqwest wraps connector DNS errors. Preserve a rejected address's
        // policy classification through those wrappers, without matching text.
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        while let Some(e) = source {
            if e.is::<Refused>() {
                return Self::NoPreview;
            }
            source = e.source();
        }
        if error.is_timeout() {
            Self::timeout()
        } else {
            Self::temporary()
        }
    }
}
#[derive(Debug)]
struct Refused;
impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("forbidden preview address")
    }
}
impl std::error::Error for Refused {}

fn retry_after(value: Option<&reqwest::header::HeaderValue>, now: SystemTime) -> Duration {
    let wait = value
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let v = v.trim();
            v.parse::<u64>().ok().map(Duration::from_secs).or_else(|| {
                httpdate::parse_http_date(v)
                    .ok()
                    .map(|date| date.duration_since(now).unwrap_or_default())
            })
        })
        .unwrap_or_default()
        .max(Duration::from_secs(60));
    // A failure is a negative answer, kept an hour at most.
    wait.min(crate::expiring::NEGATIVE_LIFETIME)
}

/// A checked HTML response, read in bounded prefixes without another request.
/// Its destination stays occupied until the body is finished or abandoned.
#[derive(Debug)]
pub struct Page {
    pub url: Url,
    response: reqwest::Response,
    _destination: Destination,
}
impl Page {
    /// Read through this checkpoint, or EOF. One response chunk may cross
    /// the checkpoint, but never the hard cap. True means EOF was reached.
    pub async fn read_until(&mut self, bytes: &mut Vec<u8>, limit: usize) -> Result<bool, Failure> {
        while bytes.len() < limit.min(HTML_BYTES) {
            let Some(chunk) = self.response.chunk().await.map_err(Failure::request)? else {
                return Ok(true);
            };
            // Reqwest decodes compression before yielding chunks.
            let take = chunk.len().min(HTML_BYTES - bytes.len());
            bytes.extend_from_slice(&chunk[..take]);
        }
        Ok(false)
    }
}

async fn fetch(
    client: &Client,
    guard: &Guard,
    mut target: Url,
    destinations: &Destinations,
    mut destination: Destination,
) -> Result<Page, Failure> {
    for hop in 0..=3 {
        guard.check_url(&target).map_err(|_| Failure::NoPreview)?;
        if hostname(target.host_str().unwrap()) != destination.host {
            destination = destinations.take(&target)?;
        }
        let response = client
            .get(target.clone())
            .header("accept", "text/html")
            .send()
            .await
            .map_err(Failure::request)?;
        let status = response.status();
        if status.as_u16() == 429 || status.is_server_error() {
            return Err(Failure::Temporary(retry_after(
                response.headers().get("retry-after"),
                SystemTime::now(),
            )));
        }
        if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            if hop == 3 {
                return Err(Failure::NoPreview);
            }
            let location = response
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .ok_or(Failure::NoPreview)?;
            target = target
                .join(location)
                .ok()
                .and_then(|u| url(u.as_str()).ok())
                .ok_or(Failure::NoPreview)?;
            continue;
        }
        if !status.is_success() {
            return Err(Failure::NoPreview);
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if !content_type
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .eq_ignore_ascii_case("text/html")
        {
            return Err(Failure::NoPreview);
        }
        return Ok(Page {
            url: target,
            response,
            _destination: destination,
        });
    }
    Err(Failure::NoPreview)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod http_tests;
