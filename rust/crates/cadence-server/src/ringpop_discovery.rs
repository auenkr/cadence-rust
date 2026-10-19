//! Go-compatible Ringpop seed discovery for static, file, DNS, and DNS-SRV modes.

use crate::config::{BootstrapMode, RingpopConfig};
use hickory_resolver::{error::ResolveErrorKind, Resolver};
use rand::Rng;
use serde::de::{SeqAccess, Visitor};
use std::{
    collections::{HashMap, HashSet},
    fmt, fs,
    net::IpAddr,
    path::PathBuf,
    sync::Arc,
    thread,
};

pub trait DiscoverProvider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("JSON syntax error: {0}")]
    JsonSyntax(serde_json::Error),
    #[error("JSON value has the wrong type: {0}")]
    JsonType(serde_json::Error),
    #[error("unknown bootstrap mode {0:?}")]
    UnsupportedMode(String),
    #[error("{error}")]
    DnsLookup { host: String, error: String },
    #[error("could not resolve host: {0}")]
    DnsSrvLookup(String),
    #[error("could not seperate host from domain {0:?}")]
    InvalidSrvSeed(String),
    #[error("invalid host:port {0:?}")]
    InvalidHostPort(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticHosts {
    hosts: Vec<String>,
}

impl StaticHosts {
    pub fn new(hosts: impl IntoIterator<Item = String>) -> Self {
        Self {
            hosts: hosts.into_iter().collect(),
        }
    }
}

impl DiscoverProvider for StaticHosts {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        Ok(self.hosts.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonFile {
    path: PathBuf,
}

impl JsonFile {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl DiscoverProvider for JsonFile {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        let data = fs::read(&self.path)?;
        // encoding/json unmarshals JSON null into a nil []string, and null
        // array elements into empty strings. serde's Vec<String> rejects both.
        // Go's encoding/json replaces malformed UTF-8 inside JSON strings
        // with U+FFFD. Keep malformed bytes outside strings intact so they
        // remain syntax errors instead of being reinterpreted as string data.
        let normalized = replace_invalid_utf8_in_strings(&data);
        // encoding/json scans the whole document before decoding its target,
        // so a syntax error later in the document takes precedence over an
        // earlier target-type mismatch. Validate the raw JSON first; RawValue
        // does not parse numeric tokens into bounded machine numbers.
        serde_json::from_slice::<Box<serde_json::value::RawValue>>(&normalized)
            .map_err(DiscoveryError::JsonSyntax)?;
        if normalized
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| byte == b'-' || byte.is_ascii_digit())
        {
            // Avoid parsing a root number into serde_json's bounded f64/i64
            // representation. Go sees a valid number and reports that the
            // target []string has the wrong type, even for huge exponents.
            return Err(DiscoveryError::JsonType(
                <serde_json::Error as serde::de::Error>::custom(
                    "expected array or null, got number",
                ),
            ));
        }
        serde_json::from_slice::<GoStringList>(&normalized)
            .map(|hosts| hosts.0)
            .map_err(|error| {
                if error.is_data() {
                    DiscoveryError::JsonType(error)
                } else {
                    DiscoveryError::JsonSyntax(error)
                }
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsLookupError {
    NotFound,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SrvRecord {
    pub target: String,
    pub port: u16,
    pub priority: u16,
    pub weight: u16,
}

/// Injectable DNS boundary used by both provider modes and their Go oracle tests.
pub trait DnsResolver: Send + Sync {
    fn lookup_host(&self, host: &str) -> Result<Vec<String>, DnsLookupError>;
    fn lookup_srv(
        &self,
        service: &str,
        proto: &str,
        name: &str,
    ) -> Result<Vec<SrvRecord>, DnsLookupError>;
}

/// System resolver backed by Linux getaddrinfo for LookupHost parity and Hickory
/// for SRV queries. Queries run on short-lived blocking threads so the
/// synchronous provider also works from inside a Tokio runtime.
pub struct SystemDnsResolver {
    resolver: std::sync::OnceLock<Result<Arc<Resolver>, String>>,
}

impl SystemDnsResolver {
    pub fn from_system_conf() -> Self {
        Self {
            resolver: std::sync::OnceLock::new(),
        }
    }

    fn resolver(&self) -> Result<Arc<Resolver>, DnsLookupError> {
        self.resolver
            .get_or_init(|| {
                let (config, mut options) = hickory_resolver::system_conf::read_system_conf()
                    .map_err(|error| error.to_string())?;
                // Go's net.DefaultResolver does not keep an in-process answer cache.
                options.cache_size = 0;
                Resolver::new(config, options)
                    .map(Arc::new)
                    .map_err(|error| error.to_string())
            })
            .clone()
            .map_err(DnsLookupError::Other)
    }

    fn on_resolver_thread<T: Send + 'static>(
        &self,
        query: impl FnOnce(Arc<Resolver>) -> Result<T, hickory_resolver::error::ResolveError> + Send,
    ) -> Result<T, DnsLookupError> {
        thread::scope(|scope| {
            scope
                .spawn(move || {
                    // Construct Hickory's owned Tokio runtime on this blocking
                    // thread too; callers may themselves be inside Tokio.
                    let resolver = self.resolver()?;
                    query(resolver).map_err(|error| match error.kind() {
                        // Go LookupHost treats both NXDOMAIN and successful
                        // responses without A/AAAA records as not-found.
                        ResolveErrorKind::NoRecordsFound { .. } => DnsLookupError::NotFound,
                        _ => DnsLookupError::Other(error.to_string()),
                    })
                })
                .join()
        })
        .map_err(|_| DnsLookupError::Other("resolver thread panicked".into()))?
    }
}

impl Drop for SystemDnsResolver {
    fn drop(&mut self) {
        let resolver = match self.resolver.take() {
            Some(Ok(resolver)) => resolver,
            Some(Err(_)) | None => return,
        };
        // Hickory's tokio-runtime feature owns a Tokio Runtime inside Resolver.
        // Tokio panics if that runtime is dropped from within another runtime,
        // so move the final resolver reference to a plain thread in that case.
        if tokio::runtime::Handle::try_current().is_ok() {
            let _ = thread::spawn(move || drop(resolver)).join();
        } else {
            drop(resolver);
        }
    }
}

impl DnsResolver for SystemDnsResolver {
    fn lookup_host(&self, host: &str) -> Result<Vec<String>, DnsLookupError> {
        // net.Resolver.LookupHost rejects the empty name before consulting
        // either libc or the DNS resolver.
        if host.is_empty() {
            return Err(DnsLookupError::NotFound);
        }
        // net.Resolver.LookupHost returns IP literals directly (including
        // scoped IPv6 literals) without consulting DNS or normalizing their
        // textual spelling. Hickory's IP parser does not preserve that
        // behavior, so handle literals at this boundary.
        if let Some(literal) = host_literal(host) {
            return Ok(vec![literal]);
        }
        #[cfg(target_os = "linux")]
        {
            let host = host.to_owned();
            thread::spawn(move || lookup_host_getaddrinfo(&host))
                .join()
                .map_err(|_| DnsLookupError::Other("resolver thread panicked".into()))?
        }
        #[cfg(not(target_os = "linux"))]
        {
            let host = host.to_owned();
            self.on_resolver_thread(move |resolver| {
                resolver.lookup_ip(host).map(|addresses| {
                    addresses
                        .into_iter()
                        .map(|address| address.to_string())
                        .collect()
                })
            })
        }
    }

    fn lookup_srv(
        &self,
        service: &str,
        proto: &str,
        name: &str,
    ) -> Result<Vec<SrvRecord>, DnsLookupError> {
        let query = format!("_{service}._{proto}.{name}");
        let mut records = self.on_resolver_thread(move |resolver| {
            resolver.srv_lookup(query).map(|lookup| {
                lookup
                    .iter()
                    .map(|record| SrvRecord {
                        target: record.target().to_utf8(),
                        port: record.port(),
                        priority: record.priority(),
                        weight: record.weight(),
                    })
                    .collect::<Vec<SrvRecord>>()
            })
        })?;
        // Go net.LookupSRV sorts by ascending priority, then RFC 2782 weighted
        // selection within each priority group. Its random order is inherently
        // nondeterministic, but this implements the same selection rule.
        sort_srv_records(&mut records);
        Ok(records)
    }
}

/// Match Go's Linux cgo LookupHost call: AF_UNSPEC, SOCK_STREAM and
/// AI_CANONNAME|AI_V4MAPPED|AI_ALL, with a null service (and no AI_ADDRCONFIG).
/// libc applies the host's NSS ordering and returns addresses in the order
/// consumed by Go's cgo resolver. This intentionally matches the cgo path; Go
/// may choose its pure-Go resolver under netgo/GODEBUG or compatible NSS
/// configurations, while Rust has no corresponding Go resolver-selection mode.
#[cfg(target_os = "linux")]
fn lookup_host_getaddrinfo(host: &str) -> Result<Vec<String>, DnsLookupError> {
    use std::{
        ffi::{CStr, CString},
        mem, ptr,
    };

    if host.is_empty() {
        return Err(DnsLookupError::NotFound);
    }
    let host = CString::new(host).map_err(|error| DnsLookupError::Other(error.to_string()))?;
    let mut hints: libc::addrinfo = unsafe { mem::zeroed() };
    hints.ai_flags = libc::AI_CANONNAME | libc::AI_V4MAPPED | libc::AI_ALL;
    hints.ai_socktype = libc::SOCK_STREAM;
    hints.ai_family = libc::AF_UNSPEC;

    let mut result = ptr::null_mut();
    let status = unsafe { libc::getaddrinfo(host.as_ptr(), ptr::null(), &hints, &mut result) };
    if status != 0 {
        if status == libc::EAI_NONAME || status == libc::EAI_NODATA {
            return Err(DnsLookupError::NotFound);
        }
        let message = if status == libc::EAI_SYSTEM {
            std::io::Error::last_os_error().to_string()
        } else {
            unsafe { CStr::from_ptr(libc::gai_strerror(status)) }
                .to_string_lossy()
                .into_owned()
        };
        return Err(DnsLookupError::Other(message));
    }

    struct AddrInfoGuard(*mut libc::addrinfo);
    impl Drop for AddrInfoGuard {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { libc::freeaddrinfo(self.0) };
            }
        }
    }
    let _guard = AddrInfoGuard(result);
    let mut addresses = Vec::new();
    let mut current = result;
    while !current.is_null() {
        let entry = unsafe { &*current };
        if entry.ai_socktype == libc::SOCK_STREAM && !entry.ai_addr.is_null() {
            match entry.ai_family {
                libc::AF_INET => {
                    let address = unsafe { &*(entry.ai_addr.cast::<libc::sockaddr_in>()) };
                    addresses.push(
                        std::net::Ipv4Addr::from(address.sin_addr.s_addr.to_ne_bytes()).to_string(),
                    );
                }
                libc::AF_INET6 => {
                    let address = unsafe { &*(entry.ai_addr.cast::<libc::sockaddr_in6>()) };
                    let ip = std::net::Ipv6Addr::from(address.sin6_addr.s6_addr);
                    if let Some(mapped) = ip.to_ipv4_mapped() {
                        addresses
                            .push(format!("{mapped}{}", interface_zone(address.sin6_scope_id)));
                    } else {
                        addresses.push(format!("{ip}{}", interface_zone(address.sin6_scope_id)));
                    }
                }
                _ => {}
            }
        }
        current = entry.ai_next;
    }
    Ok(addresses)
}

#[cfg(target_os = "linux")]
fn interface_zone(index: u32) -> String {
    if index == 0 {
        return String::new();
    }
    let mut name = [0 as libc::c_char; libc::IF_NAMESIZE];
    let zone = unsafe { libc::if_indextoname(index, name.as_mut_ptr()) };
    if zone.is_null() {
        return format!("%{index}");
    }
    let name = unsafe { std::ffi::CStr::from_ptr(zone) }.to_string_lossy();
    format!("%{name}")
}

fn host_literal(host: &str) -> Option<String> {
    if host.parse::<IpAddr>().is_ok() {
        return Some(host.to_owned());
    }
    let (address, zone) = host.split_once('%')?;
    if !zone.is_empty() && address.parse::<std::net::Ipv6Addr>().is_ok() {
        Some(host.to_owned())
    } else {
        None
    }
}

fn sort_srv_records(records: &mut [SrvRecord]) {
    records.sort_by_key(|record| (record.priority, record.weight));
    let mut start = 0;
    while start < records.len() {
        let priority = records[start].priority;
        let end = records[start..]
            .iter()
            .position(|record| record.priority != priority)
            .map_or(records.len(), |offset| start + offset);
        let group = &mut records[start..end];
        let mut sum: u32 = group.iter().map(|record| record.weight as u32).sum();
        for position in 0..group.len() {
            if sum == 0 || position + 1 == group.len() {
                break;
            }
            let selected = rand::thread_rng().gen_range(0..sum);
            let mut cumulative = 0u32;
            for index in position..group.len() {
                cumulative += group[index].weight as u32;
                if cumulative > selected {
                    group.swap(position, index);
                    break;
                }
            }
            sum -= group[position].weight as u32;
        }
        start = end;
    }
}

pub struct DnsProvider {
    unresolved_hosts: HashSet<String>,
    resolver: Arc<dyn DnsResolver>,
}

impl DnsProvider {
    pub fn new(hosts: impl IntoIterator<Item = String>, resolver: Arc<dyn DnsResolver>) -> Self {
        Self {
            unresolved_hosts: hosts.into_iter().collect(),
            resolver,
        }
    }
}

impl DiscoverProvider for DnsProvider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        let mut results = Vec::new();
        let mut resolved_hosts = HashMap::<String, Vec<String>>::new();
        for host_port in &self.unresolved_hosts {
            let (host, port) = split_host_port(host_port)
                .ok_or_else(|| DiscoveryError::InvalidHostPort(host_port.clone()))?;
            let addresses = match resolved_hosts.get(host) {
                Some(addresses) => addresses.clone(),
                None => match self.resolver.lookup_host(host) {
                    Ok(addresses) => {
                        resolved_hosts.insert(host.to_owned(), addresses.clone());
                        addresses
                    }
                    // Go caches successful lookups only, so separate seed ports
                    // repeat a missing-host lookup in the same Hosts call.
                    Err(DnsLookupError::NotFound) => continue,
                    Err(DnsLookupError::Other(error)) => {
                        return Err(DiscoveryError::DnsLookup {
                            host: host.to_owned(),
                            error,
                        });
                    }
                },
            };
            results.extend(
                addresses
                    .iter()
                    .map(|address| join_host_port(address, port)),
            );
        }
        Ok(results)
    }
}

pub struct DnsSrvProvider {
    unresolved_hosts: HashSet<String>,
    resolver: Arc<dyn DnsResolver>,
}

impl DnsSrvProvider {
    pub fn new(hosts: impl IntoIterator<Item = String>, resolver: Arc<dyn DnsResolver>) -> Self {
        Self {
            unresolved_hosts: hosts.into_iter().collect(),
            resolver,
        }
    }
}

impl DiscoverProvider for DnsSrvProvider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        let mut results = Vec::new();
        // This intentionally mirrors Go's cache key quirk: only the service
        // label participates, even when two configured hosts use other domains.
        let mut resolved_hosts = HashMap::<String, Vec<String>>::new();
        for seed in &self.unresolved_hosts {
            let parts: Vec<_> = seed.split('.').collect();
            if parts.len() <= 2 {
                return Err(DiscoveryError::InvalidSrvSeed(seed.clone()));
            }
            let service = parts[0];
            let domain = parts[1..].join(".");
            if let Some(resolved) = resolved_hosts.get(service) {
                results.extend(resolved.iter().cloned());
                continue;
            }
            let records = self
                .resolver
                .lookup_srv(service, "tcp", &domain)
                .map_err(|_| DiscoveryError::DnsSrvLookup(format!("{service}.{domain}")))?;
            let mut service_results = Vec::new();
            for record in records {
                let addresses = match self.resolver.lookup_host(&record.target) {
                    Ok(addresses) => addresses,
                    Err(DnsLookupError::NotFound) => continue,
                    Err(DnsLookupError::Other(error)) => {
                        return Err(DiscoveryError::DnsLookup {
                            host: record.target,
                            error,
                        });
                    }
                };
                service_results.extend(
                    addresses
                        .iter()
                        .map(|address| join_host_port(address, &record.port.to_string())),
                );
            }
            resolved_hosts.insert(service.to_owned(), service_results.clone());
            results.extend(service_results);
        }
        Ok(results)
    }
}

fn split_host_port(address: &str) -> Option<(&str, &str)> {
    let colon = address.rfind(':')?;
    if let Some(bracketed) = address.strip_prefix('[') {
        let close = bracketed.find(']')?;
        if close + 2 != colon
            || bracketed[..close].chars().any(|c| c == '[' || c == ']')
            || address[colon + 1..].chars().any(|c| c == '[' || c == ']')
        {
            return None;
        }
        return Some((&bracketed[..close], &address[colon + 1..]));
    }
    let host = &address[..colon];
    if host.contains(':') || address.chars().any(|c| c == '[' || c == ']') {
        None
    } else {
        Some((host, &address[colon + 1..]))
    }
}

fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn replace_invalid_utf8_in_strings(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;

    while index < input.len() {
        let byte = input[index];
        if in_string && !escaped && byte == b'\\' {
            if let Some(first) = json_unicode_escape(input, index) {
                if (0xD800..=0xDBFF).contains(&first) {
                    if let Some(second) = json_unicode_escape(input, index + 6) {
                        if (0xDC00..=0xDFFF).contains(&second) {
                            output.extend_from_slice(&input[index..index + 12]);
                            index += 12;
                            continue;
                        }
                    }
                    output.extend_from_slice(br"\uFFFD");
                    index += 6;
                    continue;
                }
                if (0xDC00..=0xDFFF).contains(&first) {
                    output.extend_from_slice(br"\uFFFD");
                    index += 6;
                    continue;
                }
            }
        }
        if byte < 0x80 {
            output.push(byte);
            index += 1;
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
            } else if byte == b'"' {
                in_string = true;
            }
            continue;
        }

        let width = match byte {
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => 1,
        };
        let candidate_end = index.saturating_add(width).min(input.len());
        let sequence = &input[index..candidate_end];
        if width > 1 && sequence.len() == width && std::str::from_utf8(sequence).is_ok() {
            output.extend_from_slice(sequence);
            index = candidate_end;
        } else if in_string && !escaped {
            output.extend_from_slice("�".as_bytes());
            // encoding/json consumes one invalid leading byte at a time, so
            // malformed continuation and truncated sequences can produce
            // more than one replacement character.
            index += std::str::from_utf8(sequence)
                .err()
                .and_then(|error| error.error_len())
                .unwrap_or(1);
        } else {
            output.extend_from_slice(sequence);
            index = candidate_end;
        }
    }
    output
}

fn json_unicode_escape(input: &[u8], start: usize) -> Option<u16> {
    let escape = input.get(start..start + 6)?;
    if escape[0] != b'\\' || escape[1] != b'u' {
        return None;
    }
    let mut value = 0u16;
    for byte in &escape[2..] {
        value = (value << 4)
            | match byte {
                b'0'..=b'9' => (byte - b'0') as u16,
                b'a'..=b'f' => (byte - b'a' + 10) as u16,
                b'A'..=b'F' => (byte - b'A' + 10) as u16,
                _ => return None,
            };
    }
    Some(value)
}

struct GoStringList(Vec<String>);

impl<'de> serde::Deserialize<'de> for GoStringList {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ListVisitor;
        impl<'de> Visitor<'de> for ListVisitor {
            type Value = GoStringList;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON array of strings or null")
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(GoStringList(Vec::new()))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut hosts = Vec::new();
                let mut first_type_error = None;
                while let Some(raw) = seq.next_element::<Box<serde_json::value::RawValue>>()? {
                    match raw.get() {
                        "null" => hosts.push(String::new()),
                        raw if raw.starts_with('"') => {
                            hosts
                                .push(serde_json::from_str(raw).map_err(serde::de::Error::custom)?);
                        }
                        raw => {
                            first_type_error.get_or_insert_with(|| {
                                format!("expected string or null, got {}", json_token_type(raw))
                            });
                        }
                    }
                }
                if let Some(error) = first_type_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(GoStringList(hosts))
            }

            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got boolean"))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got number"))
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got number"))
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got number"))
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got string"))
            }
            fn visit_string<E: serde::de::Error>(self, _: String) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got string"))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                _: A,
            ) -> Result<Self::Value, A::Error> {
                Err(serde::de::Error::custom(
                    "expected array or null, got object",
                ))
            }
        }

        deserializer.deserialize_any(ListVisitor)
    }
}

fn json_token_type(raw: &str) -> &'static str {
    match raw.as_bytes().first() {
        Some(b'"') => "string",
        Some(b'[') => "array",
        Some(b'{') => "object",
        Some(b't' | b'f') => "boolean",
        Some(b'n') => "null",
        _ => "number",
    }
}

pub enum Provider {
    Static(StaticHosts),
    File(JsonFile),
    Dns(DnsProvider),
    DnsSrv(DnsSrvProvider),
}

impl Provider {
    pub fn from_config(config: &RingpopConfig) -> Result<Self, DiscoveryError> {
        match config.bootstrap_mode {
            BootstrapMode::Hosts => Ok(Self::Static(StaticHosts::new(
                config.bootstrap_hosts.clone(),
            ))),
            BootstrapMode::File => Ok(Self::File(JsonFile::new(config.bootstrap_file.clone()))),
            BootstrapMode::Dns => {
                let resolver: Arc<dyn DnsResolver> =
                    Arc::new(SystemDnsResolver::from_system_conf());
                Ok(Self::Dns(DnsProvider::new(
                    config.bootstrap_hosts.clone(),
                    resolver,
                )))
            }
            BootstrapMode::DnsSrv => {
                let resolver: Arc<dyn DnsResolver> =
                    Arc::new(SystemDnsResolver::from_system_conf());
                Ok(Self::DnsSrv(DnsSrvProvider::new(
                    config.bootstrap_hosts.clone(),
                    resolver,
                )))
            }
            BootstrapMode::Custom => Err(DiscoveryError::UnsupportedMode("custom".into())),
            BootstrapMode::None => Err(DiscoveryError::UnsupportedMode("0".into())),
        }
    }
}

impl DiscoverProvider for Provider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        match self {
            Self::Static(provider) => provider.hosts(),
            Self::File(provider) => provider.hosts(),
            Self::Dns(provider) => provider.hosts(),
            Self::DnsSrv(provider) => provider.hosts(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::Write;
    use std::sync::Mutex;

    type HostResponseMap =
        HashMap<String, VecDeque<Result<Vec<String>, DnsLookupError>>>;
    type SrvResponseMap = HashMap<
        (String, String, String),
        VecDeque<Result<Vec<SrvRecord>, DnsLookupError>>,
    >;

    #[derive(Default)]
    struct FakeDnsResolver {
        host_responses: Mutex<HostResponseMap>,
        srv_responses: Mutex<SrvResponseMap>,
        host_calls: Mutex<Vec<String>>,
        srv_calls: Mutex<Vec<(String, String, String)>>,
    }

    impl FakeDnsResolver {
        fn host(&self, name: &str, response: Result<Vec<&str>, DnsLookupError>) {
            self.host_responses
                .lock()
                .unwrap()
                .entry(name.into())
                .or_default()
                .push_back(
                    response.map(|addresses| addresses.into_iter().map(str::to_owned).collect()),
                );
        }

        fn srv(&self, service: &str, proto: &str, name: &str, records: Vec<SrvRecord>) {
            self.srv_responses
                .lock()
                .unwrap()
                .entry((service.into(), proto.into(), name.into()))
                .or_default()
                .push_back(Ok(records));
        }

        fn host_call_count(&self, name: &str) -> usize {
            self.host_calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.as_str() == name)
                .count()
        }
    }

    impl DnsResolver for FakeDnsResolver {
        fn lookup_host(&self, host: &str) -> Result<Vec<String>, DnsLookupError> {
            self.host_calls.lock().unwrap().push(host.to_owned());
            self.host_responses
                .lock()
                .unwrap()
                .get_mut(host)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| {
                    Err(DnsLookupError::Other(format!(
                        "no fake response for {host}"
                    )))
                })
        }

        fn lookup_srv(
            &self,
            service: &str,
            proto: &str,
            name: &str,
        ) -> Result<Vec<SrvRecord>, DnsLookupError> {
            let key = (service.to_owned(), proto.to_owned(), name.to_owned());
            self.srv_calls.lock().unwrap().push(key.clone());
            self.srv_responses
                .lock()
                .unwrap()
                .get_mut(&key)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| {
                    Err(DnsLookupError::Other(format!(
                        "no fake SRV response for {key:?}"
                    )))
                })
        }
    }

    fn dns_resolver(resolver: FakeDnsResolver) -> Arc<dyn DnsResolver> {
        Arc::new(resolver)
    }

    #[test]
    fn static_hosts_preserve_order_duplicates_and_strings() {
        let p = StaticHosts::new([" host:1 ".to_string(), "host:1".into(), "raw".into()]);
        assert_eq!(p.hosts().unwrap(), [" host:1 ", "host:1", "raw"]);
        assert_eq!(p.hosts().unwrap(), [" host:1 ", "host:1", "raw"]);
    }

    #[test]
    fn file_reads_each_time_and_preserves_array_contents() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(br#"["a:1","a:1"," raw "]"#).unwrap();
        let p = JsonFile::new(f.path());
        assert_eq!(p.hosts().unwrap(), ["a:1", "a:1", " raw "]);
        fs::write(f.path(), br#"["updated:2"]"#).unwrap();
        assert_eq!(p.hosts().unwrap(), ["updated:2"]);
        fs::write(f.path(), br#"["a","#).unwrap();
        assert!(matches!(p.hosts(), Err(DiscoveryError::JsonSyntax(_))));
        fs::write(f.path(), b"{}").unwrap();
        assert!(matches!(p.hosts(), Err(DiscoveryError::JsonType(_))));
    }

    #[test]
    fn file_matches_go_string_replacement_and_number_type_errors() {
        let f = tempfile::NamedTempFile::new().unwrap();
        // Go encoding/json replaces malformed UTF-8 bytes and escaped lone
        // UTF-16 surrogates in JSON strings with U+FFFD.
        fs::write(f.path(), b"[\"\xff\",\"\\ud800\"]").unwrap();
        assert_eq!(JsonFile::new(f.path()).hosts().unwrap(), ["�", "�"]);

        // Even an exponent outside f64's range is valid JSON syntax. Go
        // reports a target-type error when decoding it into []string.
        fs::write(f.path(), b"[1e100000]").unwrap();
        assert!(matches!(
            JsonFile::new(f.path()).hosts(),
            Err(DiscoveryError::JsonType(_))
        ));
    }

    #[test]
    fn malformed_utf8_outside_strings_remains_json_syntax_error() {
        let f = tempfile::NamedTempFile::new().unwrap();
        fs::write(f.path(), b"[\xff]").unwrap();
        assert!(matches!(
            JsonFile::new(f.path()).hosts(),
            Err(DiscoveryError::JsonSyntax(_))
        ));
    }

    #[test]
    fn dns_provider_matches_go_cache_errors_and_host_port_formatting() {
        let resolver = FakeDnsResolver::default();
        resolver.host(
            "seed.example",
            Ok(vec!["10.0.0.1", "2001:db8::1", "10.0.0.1"]),
        );
        let provider = DnsProvider::new(
            [
                "seed.example:1111".into(),
                "seed.example:2222".into(),
                "seed.example:1111".into(),
            ],
            dns_resolver(resolver),
        );
        let mut actual = provider.hosts().unwrap();
        actual.sort();
        let mut expected = vec![
            "10.0.0.1:1111",
            "[2001:db8::1]:1111",
            "10.0.0.1:1111",
            "10.0.0.1:2222",
            "[2001:db8::1]:2222",
            "10.0.0.1:2222",
        ];
        expected.sort();
        assert_eq!(actual, expected);

        let resolver = Arc::new(FakeDnsResolver::default());
        resolver.host("missing.example", Err(DnsLookupError::NotFound));
        resolver.host("missing.example", Err(DnsLookupError::NotFound));
        let provider = DnsProvider::new(
            ["missing.example:1".into(), "missing.example:2".into()],
            resolver.clone(),
        );
        assert!(provider.hosts().unwrap().is_empty());
        // Go caches successful empty answers but not DNS not-found errors.
        // Two seed ports for the same host therefore issue two not-found calls.
        assert_eq!(resolver.host_call_count("missing.example"), 2);
    }

    #[test]
    fn dns_provider_relooks_up_between_hosts_calls_and_propagates_bad_inputs() {
        let resolver = FakeDnsResolver::default();
        resolver.host("seed.example", Ok(vec!["10.0.0.1"]));
        resolver.host("seed.example", Ok(vec!["10.0.0.2"]));
        let provider = DnsProvider::new(["seed.example:4".into()], dns_resolver(resolver));
        assert_eq!(provider.hosts().unwrap(), ["10.0.0.1:4"]);
        assert_eq!(provider.hosts().unwrap(), ["10.0.0.2:4"]);

        let resolver = FakeDnsResolver::default();
        let provider = DnsProvider::new(["broken".into()], dns_resolver(resolver));
        assert!(matches!(
            provider.hosts(),
            Err(DiscoveryError::InvalidHostPort(_))
        ));

        let resolver = FakeDnsResolver::default();
        resolver.host(
            "failed.example",
            Err(DnsLookupError::Other("timeout".into())),
        );
        let provider = DnsProvider::new(["failed.example:4".into()], dns_resolver(resolver));
        assert!(matches!(
            provider.hosts(),
            Err(DiscoveryError::DnsLookup { .. })
        ));
    }

    #[test]
    fn split_and_join_host_port_match_go_ipv6_and_string_port_rules() {
        assert_eq!(
            split_host_port("seed.example:abc"),
            Some(("seed.example", "abc"))
        );
        assert_eq!(
            split_host_port("[2001:db8::1]:0007"),
            Some(("2001:db8::1", "0007"))
        );
        assert_eq!(split_host_port("2001:db8::1:7"), None);
        assert_eq!(split_host_port("[2001:db8::1]x:7"), None);
        assert_eq!(join_host_port("2001:db8::1", "0007"), "[2001:db8::1]:0007");
        assert_eq!(join_host_port("10.0.0.1", "abc"), "10.0.0.1:abc");
    }

    #[test]
    fn system_dns_literal_fast_path_matches_go_without_normalizing() {
        for literal in [
            "192.0.2.1",
            "2001:0db8::1",
            "fe80::1%eth0",
            "fe80::1%zone%extra",
        ] {
            assert_eq!(host_literal(literal).as_deref(), Some(literal));
        }
        for not_literal in ["", "example.test", "192.0.2.1%eth0", "fe80::1%"] {
            assert_eq!(host_literal(not_literal), None);
        }
        #[cfg(target_os = "linux")]
        assert_eq!(
            lookup_host_getaddrinfo("").unwrap_err(),
            DnsLookupError::NotFound
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn system_lookup_matches_go_system_oracle() {
        let Ok(oracle) = std::env::var("CADENCE_GO_SYSTEM_LOOKUP_HOST_ORACLE") else {
            // The cross-language test runner supplies this dynamic result.
            return;
        };
        let oracle: HashMap<String, Vec<String>> = serde_json::from_str(&oracle).unwrap();
        assert_eq!(
            lookup_host_getaddrinfo("localhost").unwrap(),
            oracle["localhost"]
        );
        assert_eq!(host_literal("2001:0db8::1").unwrap(), oracle["literal"][0]);
    }

    #[test]
    fn dns_srv_provider_preserves_targets_ipv6_and_trailing_dot_domain() {
        let resolver = FakeDnsResolver::default();
        resolver.srv(
            "api",
            "tcp",
            "example.net.",
            vec![
                SrvRecord {
                    target: "v4.example.net.".into(),
                    port: 7755,
                    priority: 0,
                    weight: 1,
                },
                SrvRecord {
                    target: "v6.example.net.".into(),
                    port: 7566,
                    priority: 0,
                    weight: 1,
                },
                SrvRecord {
                    target: "missing.example.net.".into(),
                    port: 7896,
                    priority: 0,
                    weight: 1,
                },
            ],
        );
        resolver.host("v4.example.net.", Ok(vec!["10.0.0.1", "10.0.0.2"]));
        resolver.host("v6.example.net.", Ok(vec!["2001:db8::2"]));
        resolver.host("missing.example.net.", Err(DnsLookupError::NotFound));
        let provider = DnsSrvProvider::new(["api.example.net.".into()], dns_resolver(resolver));
        assert_eq!(
            provider.hosts().unwrap(),
            ["10.0.0.1:7755", "10.0.0.2:7755", "[2001:db8::2]:7566",]
        );

        let resolver = FakeDnsResolver::default();
        let provider = DnsSrvProvider::new(["api.example".into()], dns_resolver(resolver));
        assert!(matches!(
            provider.hosts(),
            Err(DiscoveryError::InvalidSrvSeed(_))
        ));
    }

    #[test]
    fn dns_srv_provider_caches_by_service_only_and_requeries_each_call() {
        let resolver = FakeDnsResolver::default();
        resolver.srv(
            "api",
            "tcp",
            "one.example",
            vec![SrvRecord {
                target: "one-target.".into(),
                port: 80,
                priority: 0,
                weight: 1,
            }],
        );
        resolver.srv(
            "api",
            "tcp",
            "two.example",
            vec![SrvRecord {
                target: "two-target.".into(),
                port: 80,
                priority: 0,
                weight: 1,
            }],
        );
        resolver.srv(
            "api",
            "tcp",
            "one.example",
            vec![SrvRecord {
                target: "next-target.".into(),
                port: 81,
                priority: 0,
                weight: 1,
            }],
        );
        resolver.srv(
            "api",
            "tcp",
            "two.example",
            vec![SrvRecord {
                target: "next-target.".into(),
                port: 81,
                priority: 0,
                weight: 1,
            }],
        );
        for (target, ip) in [
            ("one-target.", "10.1.0.1"),
            ("two-target.", "10.2.0.1"),
            ("next-target.", "10.3.0.1"),
        ] {
            resolver.host(target, Ok(vec![ip]));
            resolver.host(target, Ok(vec![ip]));
        }
        let provider = DnsSrvProvider::new(
            ["api.one.example".into(), "api.two.example".into()],
            dns_resolver(resolver),
        );
        let first = provider.hosts().unwrap();
        assert_eq!(first.len(), 2);
        assert!(first == ["10.1.0.1:80", "10.1.0.1:80"] || first == ["10.2.0.1:80", "10.2.0.1:80"]);
        let second = provider.hosts().unwrap();
        assert_eq!(second, ["10.3.0.1:81", "10.3.0.1:81"]);
    }

    #[test]
    fn system_srv_ordering_keeps_priority_and_rfc2782_weight_groups() {
        let mut records = vec![
            SrvRecord {
                target: "low-weight".into(),
                port: 1,
                priority: 2,
                weight: 1,
            },
            SrvRecord {
                target: "high-priority".into(),
                port: 2,
                priority: 0,
                weight: 10,
            },
            SrvRecord {
                target: "high-weight".into(),
                port: 3,
                priority: 2,
                weight: 9,
            },
            SrvRecord {
                target: "middle-priority".into(),
                port: 4,
                priority: 1,
                weight: 0,
            },
        ];
        sort_srv_records(&mut records);
        assert_eq!(
            records
                .iter()
                .map(|record| record.priority)
                .collect::<Vec<_>>(),
            [0, 1, 2, 2]
        );
        assert_eq!(records[0].target, "high-priority");
        assert_eq!(records[1].target, "middle-priority");
        assert_eq!(
            records[2..].iter().map(|record| record.weight).sum::<u16>(),
            10
        );
    }

    #[test]
    fn missing_file_is_io_error_and_config_selects_supported_provider() {
        assert!(matches!(
            JsonFile::new("/does/not/exist").hosts(),
            Err(DiscoveryError::Io(_))
        ));
        let cfg = crate::config::Config::from_yaml(
            "ringpop: {name: matching, bootstrapMode: hosts, bootstrapHosts: [' x ', 'x']}",
        )
        .unwrap();
        let provider = Provider::from_config(&cfg.ringpop).unwrap();
        assert_eq!(provider.hosts().unwrap(), [" x ", "x"]);

        let dns = RingpopConfig {
            name: "matching".into(),
            bootstrap_mode: BootstrapMode::Dns,
            bootstrap_hosts: vec!["localhost:7934".into()],
            ..Default::default()
        };
        assert!(matches!(
            Provider::from_config(&dns).unwrap(),
            Provider::Dns(_)
        ));
    }

    #[tokio::test]
    async fn system_resolver_can_be_initialized_and_dropped_inside_tokio() {
        let resolver = SystemDnsResolver::from_system_conf();
        resolver
            .on_resolver_thread(|_| Ok::<_, hickory_resolver::error::ResolveError>(()))
            .unwrap();
        drop(resolver);
    }

    #[test]
    fn matches_actual_go_provider_oracle_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_bootstrap_provider.json"
        ))
        .unwrap();
        let inputs: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_bootstrap_provider_inputs.json"
        ))
        .unwrap();
        assert_eq!(inputs["fileInputs"], fixture["fileInputs"]);
        let mixed_mode =
            crate::config::Config::from_yaml("ringpop: {name: x, bootstrapMode: HoStS}").unwrap();
        assert_eq!(
            mixed_mode.ringpop.bootstrap_mode as i32,
            fixture["bootstrapModes"]["mixed"]["mode"].as_i64().unwrap() as i32
        );
        let upper_mode =
            crate::config::Config::from_yaml("ringpop: {name: x, bootstrapMode: DNS-SRV}").unwrap();
        assert_eq!(
            upper_mode.ringpop.bootstrap_mode as i32,
            fixture["bootstrapModes"]["upper"]["mode"].as_i64().unwrap() as i32
        );
        let unknown_mode =
            crate::config::Config::from_yaml("ringpop: {name: x, bootstrapMode: WeIrD}")
                .unwrap_err()
                .to_string();
        assert!(unknown_mode.contains(
            fixture["bootstrapModes"]["unknown"]["error"]
                .as_str()
                .unwrap()
        ));
        for (raw, expected) in inputs["fileInputs"]
            .as_array()
            .unwrap()
            .iter()
            .zip(fixture["fileObservations"].as_array().unwrap())
        {
            let f = tempfile::NamedTempFile::new().unwrap();
            fs::write(f.path(), raw.as_str().unwrap()).unwrap();
            let actual = JsonFile::new(f.path()).hosts();
            match expected["errorType"].as_str().unwrap() {
                "" => assert_eq!(
                    actual.unwrap(),
                    expected["hosts"]
                        .as_array()
                        .map_or_else(Vec::new, |values| values
                            .iter()
                            .map(|value| value.as_str().unwrap().to_owned())
                            .collect()),
                    "raw input must match Go provider result"
                ),
                "*json.SyntaxError" => assert!(
                    matches!(actual, Err(DiscoveryError::JsonSyntax(_))),
                    "raw input {raw:?}: {actual:?}"
                ),
                "*json.UnmarshalTypeError" => assert!(
                    matches!(actual, Err(DiscoveryError::JsonType(_))),
                    "raw input {raw:?}: {actual:?}"
                ),
                other => panic!("unhandled Go error category {other}"),
            }
        }
        for (name, raw) in [
            ("invalidUtf8InString", b"[\"\xff\"]".as_slice()),
            (
                "invalidUtf8ContinuationInString",
                b"[\"\xe2(\xa1\"]".as_slice(),
            ),
            ("escapedLoneSurrogate", br#"["\ud800"]"#.as_slice()),
            ("invalidUtf8OutsideString", b"[\xff]".as_slice()),
            ("largeExponent", b"[1e100000]".as_slice()),
            (
                "largeInteger",
                b"[999999999999999999999999999999999999999999999999999999]".as_slice(),
            ),
            ("topLevelLargeExponent", b"1e100000".as_slice()),
            (
                "topLevelLargeInteger",
                b"999999999999999999999999999999999999999999999999999999".as_slice(),
            ),
        ] {
            let f = tempfile::NamedTempFile::new().unwrap();
            fs::write(f.path(), raw).unwrap();
            let actual = JsonFile::new(f.path()).hosts();
            let expected = &fixture["byteObservations"][name];
            match expected["errorType"].as_str().unwrap() {
                "" => assert_eq!(
                    actual.unwrap(),
                    expected["hosts"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                ),
                "*json.SyntaxError" => assert!(
                    matches!(actual, Err(DiscoveryError::JsonSyntax(_))),
                    "{name}: {actual:?}"
                ),
                "*json.UnmarshalTypeError" => assert!(
                    matches!(actual, Err(DiscoveryError::JsonType(_))),
                    "{name}: {actual:?}"
                ),
                other => panic!("unhandled Go byte-input error category {other}"),
            }
        }
        let empty = tempfile::NamedTempFile::new().unwrap();
        fs::write(empty.path(), b"[]").unwrap();
        assert!(JsonFile::new(empty.path()).hosts().unwrap().is_empty());
        let static_hosts: Vec<String> = serde_json::from_value(fixture["static"].clone()).unwrap();
        assert_eq!(
            StaticHosts::new(static_hosts).hosts().unwrap(),
            serde_json::from_value::<Vec<String>>(fixture["static"].clone()).unwrap()
        );
        let file_hosts: Vec<String> = serde_json::from_value(fixture["file"].clone()).unwrap();
        let f = tempfile::NamedTempFile::new().unwrap();
        fs::write(f.path(), serde_json::to_vec(&file_hosts).unwrap()).unwrap();
        let file = JsonFile::new(f.path());
        assert_eq!(
            file.hosts().unwrap(),
            serde_json::from_value::<Vec<String>>(fixture["file"].clone()).unwrap()
        );
        let updated: Vec<String> =
            serde_json::from_value(fixture["fileAfterUpdate"].clone()).unwrap();
        fs::write(f.path(), serde_json::to_vec(&updated).unwrap()).unwrap();
        assert_eq!(file.hosts().unwrap(), updated);
        fs::write(f.path(), b"{}").unwrap();
        assert!(matches!(file.hosts(), Err(DiscoveryError::JsonType(_))));
        let malformed_error = fixture["malformedErrorType"].as_str().unwrap();
        assert_eq!(malformed_error, "*json.SyntaxError");
        fs::write(f.path(), br#"["a","#).unwrap();
        assert!(matches!(file.hosts(), Err(DiscoveryError::JsonSyntax(_))));
        let file_config = RingpopConfig {
            bootstrap_mode: BootstrapMode::File,
            bootstrap_file: f.path().to_string_lossy().into_owned(),
            ..Default::default()
        };
        assert!(matches!(
            Provider::from_config(&file_config).unwrap(),
            Provider::File(_)
        ));
        let cfg = crate::config::Config::from_yaml(
            "ringpop: {name: matching, bootstrapMode: hosts, bootstrapHosts: [' x ', x, x]}",
        )
        .unwrap();
        assert_eq!(
            cfg.ringpop.bootstrap_hosts,
            serde_json::from_value::<Vec<String>>(fixture["configHosts"].clone()).unwrap()
        );
        let mut cfg = cfg;
        cfg.validate_ringpop().unwrap();
        assert_eq!(
            cfg.ringpop.max_join_duration_nanos,
            fixture["configMaxJoinDurationNanos"].as_i64().unwrap()
        );
        let config_errors = &fixture["configErrors"];
        let mut missing_name = crate::config::RingpopConfig::default();
        assert_eq!(
            missing_name.validate().unwrap_err().to_string(),
            config_errors["missingName"]
        );
        let mut unknown = crate::config::RingpopConfig {
            name: "x".into(),
            ..Default::default()
        };
        assert_eq!(
            unknown.validate().unwrap_err().to_string(),
            config_errors["unknownMode"]
        );
        let mut missing_file = crate::config::RingpopConfig {
            name: "x".into(),
            bootstrap_mode: BootstrapMode::File,
            ..Default::default()
        };
        assert_eq!(
            missing_file.validate().unwrap_err().to_string(),
            config_errors["missingFile"]
        );
        let mut missing_hosts = crate::config::RingpopConfig {
            name: "x".into(),
            bootstrap_mode: BootstrapMode::Hosts,
            ..Default::default()
        };
        assert_eq!(
            missing_hosts.validate().unwrap_err().to_string(),
            config_errors["missingHosts"]
        );
        let mut custom = crate::config::RingpopConfig {
            name: "x".into(),
            bootstrap_mode: BootstrapMode::Custom,
            ..Default::default()
        };
        assert_eq!(
            custom.validate().unwrap_err().to_string(),
            config_errors["custom"]
        );
        for (key, source) in [
            ("max", "9223372036854775807ns"),
            ("min", "-9223372036854775808ns"),
            ("fraction", "1.000000001s"),
            ("preciseThirdHour", "0.333333333333333333h"),
            ("preciseAlmostHour", "0.999999999999999999h"),
        ] {
            let config: crate::config::Config = crate::config::Config::from_yaml(&format!("ringpop: {{name: x, bootstrapMode: hosts, bootstrapHosts: [x], maxJoinDuration: {source}}}")).unwrap();
            assert_eq!(
                config.ringpop.max_join_duration_nanos,
                fixture["durations"][key].as_i64().unwrap()
            );
        }
    }
}
