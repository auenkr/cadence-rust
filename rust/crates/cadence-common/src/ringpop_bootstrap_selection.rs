//! Ringpop v0.10 bootstrap target selection (selection only; no network/retry).
use rand::Rng;
use std::collections::{BTreeMap, HashSet};

pub const DEFAULT_TIMEOUT_NANOS: i64 = 1_000_000_000;
pub const DEFAULT_MAX_JOIN_DURATION_NANOS: i64 = 120_000_000_000;
pub const DEFAULT_SIZE: i64 = 3;
pub const DEFAULT_PARALLELISM: i64 = 2;

/// `net.SplitHostPort`-compatible host extraction for ordinary host:port and
/// bracketed IPv6 addresses. Invalid input captures as Go does: the empty host.
pub fn capture_host(hostport: &str) -> &str {
    if let Some(rest) = hostport.strip_prefix('[') {
        if let Some((host, suffix)) = rest.split_once(']') {
            if !host.contains(['[', ']'])
                && suffix.starts_with(':')
                && !suffix[1..].contains([':', '[', ']'])
            {
                return host;
            }
        }
        return "";
    }
    let Some((host, port)) = hostport.rsplit_once(':') else {
        return "";
    };
    if host.contains([':', '[', ']']) || port.contains([':', '[', ']']) {
        ""
    } else {
        host
    }
}

/// Selects a position from a nonempty pool. A seam keeps selection testable;
/// production uses a thread-local random index as Go's `rand.Intn` does.
pub trait IndexChooser {
    fn choose(&mut self, upper_exclusive: usize) -> usize;
}

#[derive(Default)]
pub struct RandomIndex;
impl IndexChooser for RandomIndex {
    fn choose(&mut self, upper_exclusive: usize) -> usize {
        rand::thread_rng().gen_range(0..upper_exclusive)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub timeout_nanos: i64,
    pub max_join_duration_nanos: i64,
    pub size: i64,
    pub parallelism: i64,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            timeout_nanos: DEFAULT_TIMEOUT_NANOS,
            max_join_duration_nanos: DEFAULT_MAX_JOIN_DURATION_NANOS,
            size: DEFAULT_SIZE,
            parallelism: DEFAULT_PARALLELISM,
        }
    }
}
impl Options {
    /// Go's SelectDuration/SelectInt use the default only for zero; signed
    /// nonzero values are retained unchanged.
    pub fn from_signed(timeout: i64, max_duration: i64, size: i64, parallelism: i64) -> Self {
        let d = Self::default();
        Self {
            timeout_nanos: if timeout == 0 {
                d.timeout_nanos
            } else {
                timeout
            },
            max_join_duration_nanos: if max_duration == 0 {
                d.max_join_duration_nanos
            } else {
                max_duration
            },
            size: if size == 0 { d.size } else { size },
            parallelism: if parallelism == 0 {
                d.parallelism
            } else {
                parallelism
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapSelection {
    local: String,
    pub bootstrap_hosts: Vec<String>,
    pub potential: Vec<String>,
    pub preferred: Vec<String>,
    pub non_preferred: Vec<String>,
    pub options: Options,
    pub effective_size: i64,
    preferred_round: Vec<String>,
    non_preferred_round: Vec<String>,
}

impl BootstrapSelection {
    pub fn new(local: &str, seeds: &[String], requested: Options) -> Self {
        let mut bootstrap_hosts = seeds.to_vec();
        if !bootstrap_hosts.iter().any(|seed| seed == local) {
            bootstrap_hosts.push(local.to_owned());
        }
        // Go's HostPortsByHost ignores strings net.SplitHostPort cannot parse,
        // groups by captured host and preserves every original duplicate.
        let mut hosts: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for address in &bootstrap_hosts {
            let host = capture_host(address);
            if !host.is_empty() {
                hosts
                    .entry(host.to_owned())
                    .or_default()
                    .push(address.clone());
            }
        }
        let local_host = capture_host(local);
        let potential = hosts
            .values()
            .flatten()
            .filter(|address| address.as_str() != local)
            .cloned()
            .collect::<Vec<_>>();
        let preferred = hosts
            .iter()
            .filter(|(host, _)| host.as_str() != local_host)
            .flat_map(|(_, addresses)| addresses.iter().cloned())
            .collect::<Vec<_>>();
        let non_preferred = if preferred.is_empty() {
            potential.clone()
        } else {
            hosts
                .get(local_host)
                .into_iter()
                .flatten()
                .filter(|address| address.as_str() != local)
                .cloned()
                .collect()
        };
        let mut options = requested;
        options.timeout_nanos = if options.timeout_nanos == 0 {
            DEFAULT_TIMEOUT_NANOS
        } else {
            options.timeout_nanos
        };
        options.max_join_duration_nanos = if options.max_join_duration_nanos == 0 {
            DEFAULT_MAX_JOIN_DURATION_NANOS
        } else {
            options.max_join_duration_nanos
        };
        options.size = if options.size == 0 {
            DEFAULT_SIZE
        } else {
            options.size
        };
        options.parallelism = if options.parallelism == 0 {
            DEFAULT_PARALLELISM
        } else {
            options.parallelism
        };
        let effective_size = options
            .size
            .min(i64::try_from(potential.len()).unwrap_or(i64::MAX));
        Self {
            local: local.to_owned(),
            bootstrap_hosts,
            potential,
            preferred,
            non_preferred,
            options,
            effective_size,
            preferred_round: Vec::new(),
            non_preferred_round: Vec::new(),
        }
    }

    /// Equivalent to Go Init. The preferred lists intentionally include joined
    /// nodes; Go filters the joined addresses only from potentialNodes.
    pub fn init(&mut self, joined: &[String]) {
        let joined: HashSet<&str> = joined.iter().map(String::as_str).collect();
        self.potential = self
            .bootstrap_hosts
            .iter()
            .filter(|a| {
                a.as_str() != self.local
                    && !joined.contains(a.as_str())
                    && !capture_host(a).is_empty()
            })
            .cloned()
            .collect();
        if self.preferred.is_empty() {
            self.non_preferred.clone_from(&self.potential);
        } else {
            self.non_preferred = self
                .bootstrap_hosts
                .iter()
                .filter(|a| {
                    capture_host(a) == capture_host(&self.local) && a.as_str() != self.local
                })
                .cloned()
                .collect();
        }
        self.preferred_round.clone_from(&self.preferred);
        self.non_preferred_round.clone_from(&self.non_preferred);
    }

    pub fn select_group_with(
        &mut self,
        joined: &[String],
        chooser: &mut impl IndexChooser,
    ) -> Vec<String> {
        if self.preferred_round.is_empty() && self.non_preferred_round.is_empty() {
            self.init(joined);
        }
        let remaining = self
            .effective_size
            .wrapping_sub(i64::try_from(joined.len()).unwrap_or(i64::MAX));
        let cap = remaining.wrapping_mul(self.options.parallelism);
        let mut group = Vec::new();
        // Go stops only when len(group) equals the signed cap. A negative cap
        // can never be reached, so the loop drains the available round pool.
        while cap < 0 || i64::try_from(group.len()).unwrap_or(i64::MAX) < cap {
            let pool = if !self.preferred_round.is_empty() {
                &mut self.preferred_round
            } else {
                &mut self.non_preferred_round
            };
            if pool.is_empty() {
                break;
            }
            let index = chooser.choose(pool.len()).min(pool.len() - 1);
            group.push(pool.remove(index));
        }
        group
    }

    pub fn select_group(&mut self, joined: &[String]) -> Vec<String> {
        self.select_group_with(joined, &mut RandomIndex)
    }
}
