//! Go `common/quotas/global` limiting-host side: per-key usage tracking,
//! fallback limiters, key modes, the aggregator update mapping and the
//! periodic update loop. Aggregation itself runs in History.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cadence_thrift::value::{Struct, Value};

pub const USAGE_ANY_TYPE: &str = "cadence:loadbalanced:update_request";
pub const USAGE_QUOTAS_ANY_TYPE: &str = "cadence:loadbalanced:update_response_used";
const MAX_FAILED_UPDATES: i64 = 9;
const INITIAL_FAILED_UPDATES: i64 = i64::MIN;
const GC_AFTER_IDLE: i64 = 5;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Calls {
    pub allowed: i64,
    pub rejected: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UpdateEntry {
    pub weight: f64,
    pub used_rps: f64,
}

#[derive(Debug, Default)]
pub struct UpdateResult {
    pub weights: HashMap<String, UpdateEntry>,
    pub err: Option<String>,
}

fn saturating_i32(v: i64) -> i32 {
    v.clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

fn bytes(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

/// Go `rpc.updateToAny`: returns the thrift-binary `WeightedRatelimitUsage`.
pub fn update_to_bytes(host: &str, elapsed: Duration, load: &HashMap<String, Calls>) -> Vec<u8> {
    let mut keys: Vec<&String> = load.keys().collect();
    keys.sort();
    let calls = keys
        .into_iter()
        .map(|k| {
            let c = load[k];
            (
                bytes(k),
                Value::Struct(Struct {
                    fields: vec![
                        (10, Value::I32(saturating_i32(c.allowed))),
                        (20, Value::I32(saturating_i32(c.rejected))),
                    ],
                }),
            )
        })
        .collect();
    let s = Struct {
        fields: vec![
            (10, bytes(host)),
            (
                20,
                Value::I32(saturating_i32(elapsed.as_millis().min(i64::MAX as u128) as i64)),
            ),
            (30, Value::Map(calls)),
        ],
    };
    let mut w = cadence_thrift::protocol::Writer::new();
    cadence_thrift::value::write_struct(&mut w, &s);
    w.into_bytes()
}

/// Go `shared.SanityCheckFloat` (NaN/Inf and range).
fn sane(min: f64, v: f64, max: f64, what: &str) -> Result<(), String> {
    if v.is_nan() {
        return Err(format!("{what} is NaN"));
    }
    if v.is_infinite() && max.is_finite() {
        return Err(format!("{what} is infinite"));
    }
    if v < min || v > max {
        return Err(format!("{what} out of range: {v}"));
    }
    Ok(())
}

/// Go `rpc.anyToWeights` plus the client's response validation.
pub fn weights_from_bytes(data: &[u8]) -> Result<HashMap<String, UpdateEntry>, String> {
    let mut r = cadence_thrift::protocol::Reader::new(data);
    let s = cadence_thrift::value::read_struct(&mut r).map_err(|e| e.to_string())?;
    let mut out = HashMap::new();
    if let Some(Value::Map(items)) = s.get(10) {
        for (k, v) in items {
            let (Value::Bytes(k), Value::Struct(v)) = (k, v) else {
                return Err("malformed quota entry".into());
            };
            let key = String::from_utf8(k.clone()).map_err(|e| e.to_string())?;
            let weight = v.opt_f64(10).map_err(|e| e.to_string())?.unwrap_or(0.0);
            let used = v.opt_f64(20).map_err(|e| e.to_string())?.unwrap_or(0.0);
            sane(0.0, weight, 1.0, "weight").map_err(|m| format!("bad value for key {key:?}: {m}"))?;
            sane(0.0, used, f64::INFINITY, "used rps")
                .map_err(|m| format!("bad value for key {key:?}: {m}"))?;
            out.insert(
                key,
                UpdateEntry {
                    weight,
                    used_rps: used,
                },
            );
        }
    }
    Ok(out)
}

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Go `rpc.Client`.
pub trait AggregatorClient: Send + Sync {
    fn update(&self, period: Duration, load: HashMap<String, Calls>) -> BoxFuture<UpdateResult>;
}

// ---- limiters -------------------------------------------------------------

struct Bucket {
    rps: f64,
    tokens: f64,
    last: Instant,
}

impl Bucket {
    fn new(rps: f64) -> Self {
        Self {
            rps,
            tokens: rps.max(1.0),
            last: Instant::now(),
        }
    }
    fn set_rps(&mut self, rps: f64) {
        self.rps = rps;
    }
    fn take(&mut self) -> bool {
        let burst = self.rps.max(1.0);
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rps).min(burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
    fn wait_time(&self) -> Duration {
        let need = ((1.0 - self.tokens) / self.rps.max(1e-9)).max(0.001);
        Duration::from_secs_f64(need.min(1.0))
    }
}

#[derive(Default)]
pub struct AtomicUsage {
    allowed: AtomicI64,
    rejected: AtomicI64,
    idle: AtomicI64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UsageMetrics {
    pub allowed: i64,
    pub rejected: i64,
    pub idle: i64,
}

impl AtomicUsage {
    pub fn count(&self, allowed: bool) {
        if allowed {
            self.allowed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.rejected.fetch_add(1, Ordering::Relaxed);
        }
        self.idle.store(0, Ordering::Relaxed);
    }
    pub fn collect(&self) -> UsageMetrics {
        let mut m = UsageMetrics {
            allowed: self.allowed.swap(0, Ordering::Relaxed),
            rejected: self.rejected.swap(0, Ordering::Relaxed),
            idle: 0,
        };
        if m.allowed + m.rejected == 0 {
            m.idle = self.idle.fetch_add(1, Ordering::Relaxed) + 1;
        }
        m
    }
}

/// Go `internal.FallbackLimiter`: a primary bucket fed by aggregator weights
/// plus a fallback (per-member) bucket used at startup and after failures.
pub struct FallbackLimiter {
    failed_updates: AtomicI64,
    usage: AtomicUsage,
    primary: Mutex<Bucket>,
    fallback: Mutex<Bucket>,
    fallback_rps: Arc<dyn Fn() -> f64 + Send + Sync>,
}

impl FallbackLimiter {
    pub fn new(fallback_rps: Arc<dyn Fn() -> f64 + Send + Sync>) -> Self {
        let initial = fallback_rps();
        Self {
            failed_updates: AtomicI64::new(INITIAL_FAILED_UPDATES),
            usage: AtomicUsage::default(),
            primary: Mutex::new(Bucket::new(0.0)),
            fallback: Mutex::new(Bucket::new(initial)),
            fallback_rps,
        }
    }

    fn mode(&self) -> (bool, bool) {
        let failed = self.failed_updates.load(Ordering::Relaxed);
        (failed < 0, failed > MAX_FAILED_UPDATES)
    }

    pub fn collect(&self) -> (UsageMetrics, bool, bool) {
        let usage = self.usage.collect();
        let (starting, failing) = self.mode();
        (usage, starting, failing)
    }

    pub fn fallback_limit(&self) -> f64 {
        (self.fallback_rps)()
    }

    pub fn update(&self, rps: f64) {
        let mut p = self.primary.lock().unwrap_or_else(|e| e.into_inner());
        if p.rps != rps {
            p.set_rps(rps);
        }
        self.failed_updates.store(0, Ordering::Relaxed);
    }

    pub fn failed_update(&self) -> i64 {
        self.failed_updates.fetch_add(1, Ordering::Relaxed).saturating_add(1)
    }

    /// Go `both()`: shadowed(primary, shadow); the shadow always consumes.
    fn try_take(&self) -> bool {
        let fallback_rps = (self.fallback_rps)();
        let mut fb = self.fallback.lock().unwrap_or_else(|e| e.into_inner());
        fb.set_rps(fallback_rps);
        let (starting, failing) = self.mode();
        if starting {
            return fb.take();
        }
        let mut p = self.primary.lock().unwrap_or_else(|e| e.into_inner());
        let (a, b) = (fb.take(), p.take());
        if failing {
            a
        } else {
            b
        }
    }

    pub fn allow(&self) -> bool {
        let ok = self.try_take();
        self.usage.count(ok);
        ok
    }

    pub fn retry_after(&self) -> Duration {
        let (starting, failing) = self.mode();
        let fb = self.fallback.lock().unwrap_or_else(|e| e.into_inner());
        if starting || failing {
            return fb.wait_time();
        }
        self.primary.lock().unwrap_or_else(|e| e.into_inner()).wait_time()
    }
}

/// Go `internal.CountedLimiter` over the local per-member limiter.
pub struct CountedLimiter {
    bucket: Mutex<Bucket>,
    rps: Arc<dyn Fn() -> f64 + Send + Sync>,
    usage: AtomicUsage,
}

impl CountedLimiter {
    pub fn new(rps: Arc<dyn Fn() -> f64 + Send + Sync>) -> Self {
        let initial = rps();
        Self {
            bucket: Mutex::new(Bucket::new(initial)),
            rps,
            usage: AtomicUsage::default(),
        }
    }
    pub fn allow(&self) -> bool {
        let mut b = self.bucket.lock().unwrap_or_else(|e| e.into_inner());
        b.set_rps((self.rps)());
        let ok = b.take();
        self.usage.count(ok);
        ok
    }
    pub fn retry_after(&self) -> Duration {
        self.bucket.lock().unwrap_or_else(|e| e.into_inner()).wait_time()
    }
    pub fn collect(&self) -> UsageMetrics {
        self.usage.collect()
    }
}

// ---- collection -----------------------------------------------------------

/// Go `collection.keyMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMode {
    Disabled,
    Local,
    Global,
    LocalShadowGlobal,
    GlobalShadowLocal,
}

impl KeyMode {
    /// Unknown modes are forcibly disabled (Go logs an error).
    pub fn parse(s: &str) -> Self {
        match s {
            "local" => Self::Local,
            "global" => Self::Global,
            "local-shadow-global" => Self::LocalShadowGlobal,
            "global-shadow-local" => Self::GlobalShadowLocal,
            _ => Self::Disabled,
        }
    }
    fn uses_local(self) -> bool {
        matches!(
            self,
            Self::Local | Self::LocalShadowGlobal | Self::GlobalShadowLocal
        )
    }
    fn uses_global(self) -> bool {
        matches!(
            self,
            Self::Global | Self::GlobalShadowLocal | Self::LocalShadowGlobal
        )
    }
}

/// Result of consulting the collection for one key.
pub enum Decision {
    /// Collection does not handle this key; caller uses its own local limiter.
    Disabled,
    /// Collection decided: `Ok` admitted, `Err(retry_after)` rejected.
    Decided(Result<(), Duration>),
}

pub type RpsFn = Arc<dyn Fn(&str) -> f64 + Send + Sync>;

pub struct Collection {
    prefix: String,
    key_mode: Arc<dyn Fn(&str) -> String + Send + Sync>,
    update_interval: Arc<dyn Fn() -> Duration + Send + Sync>,
    target_rps: RpsFn,
    /// per-member (local/fallback) rps for a key.
    local_rps: RpsFn,
    aggs: Arc<dyn AggregatorClient>,
    global: Mutex<HashMap<String, Arc<FallbackLimiter>>>,
    local: Mutex<HashMap<String, Arc<CountedLimiter>>>,
}

impl Collection {
    pub fn new(
        name: &str,
        update_interval: Arc<dyn Fn() -> Duration + Send + Sync>,
        target_rps: RpsFn,
        local_rps: RpsFn,
        key_mode: Arc<dyn Fn(&str) -> String + Send + Sync>,
        aggs: Arc<dyn AggregatorClient>,
    ) -> Arc<Self> {
        Arc::new(Self {
            prefix: format!("{name}:"),
            key_mode,
            update_interval,
            target_rps,
            local_rps,
            aggs,
            global: Mutex::new(HashMap::new()),
            local: Mutex::new(HashMap::new()),
        })
    }

    fn mode(&self, key: &str) -> KeyMode {
        KeyMode::parse(&(self.key_mode)(&format!("{}{key}", self.prefix)))
    }

    fn global_limiter(&self, key: &str) -> Arc<FallbackLimiter> {
        let mut map = self.global.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(key.to_owned())
            .or_insert_with(|| {
                let (f, k) = (self.local_rps.clone(), key.to_owned());
                Arc::new(FallbackLimiter::new(Arc::new(move || f(&k))))
            })
            .clone()
    }

    fn local_limiter(&self, key: &str) -> Arc<CountedLimiter> {
        let mut map = self.local.lock().unwrap_or_else(|e| e.into_inner());
        map.entry(key.to_owned())
            .or_insert_with(|| {
                let (f, k) = (self.local_rps.clone(), key.to_owned());
                Arc::new(CountedLimiter::new(Arc::new(move || f(&k))))
            })
            .clone()
    }

    /// Go `Collection.For(key)` + one `Allow`.
    pub fn acquire(&self, key: &str) -> Decision {
        let run = |primary: Result<(), Duration>| Decision::Decided(primary);
        match self.mode(key) {
            KeyMode::Disabled => Decision::Disabled,
            KeyMode::Local => {
                let l = self.local_limiter(key);
                run(if l.allow() { Ok(()) } else { Err(l.retry_after()) })
            }
            KeyMode::Global => {
                let g = self.global_limiter(key);
                run(if g.allow() { Ok(()) } else { Err(g.retry_after()) })
            }
            KeyMode::LocalShadowGlobal => {
                let (l, g) = (self.local_limiter(key), self.global_limiter(key));
                let _ = g.allow();
                run(if l.allow() { Ok(()) } else { Err(l.retry_after()) })
            }
            KeyMode::GlobalShadowLocal => {
                let (l, g) = (self.local_limiter(key), self.global_limiter(key));
                let _ = l.allow();
                run(if g.allow() { Ok(()) } else { Err(g.retry_after()) })
            }
        }
    }

    /// One tick of Go `backgroundUpdateLoop`; returns after the update call.
    pub async fn tick(&self, since: Duration) {
        let mut stale_local = Vec::new();
        for (k, v) in self.local.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            let counts = v.collect();
            if counts.idle > GC_AFTER_IDLE || !self.mode(k).uses_local() {
                stale_local.push(k.clone());
            }
        }
        {
            let mut local = self.local.lock().unwrap_or_else(|e| e.into_inner());
            for k in stale_local {
                local.remove(&k);
            }
        }
        let mut usage: HashMap<String, Calls> = HashMap::new();
        let mut stale = Vec::new();
        for (k, v) in self.global.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            let (counts, _, _) = v.collect();
            if counts.idle > GC_AFTER_IDLE || !self.mode(k).uses_global() {
                stale.push(k.clone());
                continue;
            }
            usage.insert(
                format!("{}{k}", self.prefix),
                Calls {
                    allowed: counts.allowed,
                    rejected: counts.rejected,
                },
            );
        }
        {
            let mut global = self.global.lock().unwrap_or_else(|e| e.into_inner());
            for k in stale {
                global.remove(&k);
            }
        }
        if usage.is_empty() {
            return;
        }
        let res = match tokio::time::timeout(
            Duration::from_secs(10),
            self.aggs.update(since, usage.clone()),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => UpdateResult {
                weights: HashMap::new(),
                err: Some("aggregator update timed out".into()),
            },
        };
        if let Some(e) = &res.err {
            eprintln!("global ratelimiter aggregator update error: {e}");
        }
        for (gkey, info) in &res.weights {
            usage.remove(gkey);
            let Some(lkey) = gkey.strip_prefix(&self.prefix) else {
                eprintln!("global ratelimiter: bad global key structure returned: {gkey}");
                continue;
            };
            let limiter = self.global_limiter(lkey);
            let target = (self.target_rps)(lkey);
            limiter.update(boost_rps(target, limiter.fallback_limit(), info.weight));
        }
        for gkey in usage.keys() {
            if let Some(lkey) = gkey.strip_prefix(&self.prefix) {
                self.global_limiter(lkey).failed_update();
            }
        }
    }

    /// Spawn the periodic update loop; returns a handle to abort on stop.
    pub fn start(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let this = self.clone();
        Some(handle.spawn(async move {
            let mut interval = (this.update_interval)();
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            let mut last = Instant::now();
            loop {
                ticker.tick().await;
                let now = Instant::now();
                let next = (this.update_interval)();
                if next != interval && !next.is_zero() {
                    interval = next;
                    ticker = tokio::time::interval(next);
                    ticker.tick().await;
                }
                this.tick(now.duration_since(last)).await;
                last = now;
            }
        }))
    }
}

/// Go `boostRPS`.
pub fn boost_rps(target: f64, fallback: f64, weight: f64) -> f64 {
    let baseline = target * weight;
    if baseline < fallback {
        fallback
    } else {
        baseline
    }
}

/// Go `rpc.client`: groups keys by the History ring peer owning each key and
/// sends one `RatelimitUpdate` per peer, merging the weights.
pub struct HistoryAggregators {
    backend: crate::workflow_backend::ProductionBackend,
    membership: cadence_service_core::admin::MembershipSlot,
    host: String,
}

impl HistoryAggregators {
    pub fn new(
        backend: crate::workflow_backend::ProductionBackend,
        membership: cadence_service_core::admin::MembershipSlot,
        host: String,
    ) -> Self {
        Self {
            backend,
            membership,
            host,
        }
    }

    fn peers(&self, keys: &[String]) -> Result<HashMap<String, Vec<String>>, String> {
        use cadence_common::membership::{Host, Ring};
        let m = self.membership.get().ok_or("membership unavailable")?;
        let members = m
            .members("cadence-history")
            .map_err(|e| format!("unable to get history peers: {e}"))?;
        if members.is_empty() {
            return Err("unable to get history peers: no peers available".into());
        }
        let ring = Ring::new(members.into_iter().map(|identity| Host {
            address: identity.clone(),
            identity,
            grpc_address: String::new(),
        }));
        let mut out: HashMap<String, Vec<String>> = HashMap::new();
        for k in keys {
            let h = ring
                .lookup(k)
                .ok_or_else(|| format!("unable to find history peer for key {k:?}"))?;
            out.entry(h.address.clone()).or_default().push(k.clone());
        }
        Ok(out)
    }
}

impl AggregatorClient for HistoryAggregators {
    fn update(&self, period: Duration, load: HashMap<String, Calls>) -> BoxFuture<UpdateResult> {
        let backend = self.backend.clone();
        let host = self
            .membership
            .get()
            .and_then(|m| m.who_am_i().ok())
            .unwrap_or_else(|| self.host.clone());
        let keys: Vec<String> = load.keys().cloned().collect();
        let peers = self.peers(&keys);
        Box::pin(async move {
            let peers = match peers {
                Ok(p) => p,
                Err(e) => {
                    return UpdateResult {
                        weights: HashMap::new(),
                        err: Some(e),
                    }
                }
            };
            let calls = peers.into_iter().map(|(peer, pkeys)| {
                let backend = backend.clone();
                let sub: HashMap<String, Calls> = pkeys
                    .iter()
                    .filter_map(|k| load.get(k).map(|c| (k.clone(), *c)))
                    .collect();
                let data = update_to_bytes(&host, period, &sub);
                async move {
                    let any = Struct {
                        fields: vec![
                            (10, bytes(USAGE_ANY_TYPE)),
                            (20, Value::Bytes(data)),
                        ],
                    };
                    let envelope = Struct {
                        fields: vec![(10, Value::Struct(any))],
                    };
                    let resp = backend
                        .call_with_timeout(
                            &peer,
                            "cadence-history",
                            "HistoryService",
                            "RatelimitUpdate",
                            envelope,
                            Duration::from_secs(10),
                        )
                        .await
                        .map_err(|e| format!("{e:?}"))?;
                    let Some(Value::Struct(any)) = resp.get(10) else {
                        return Err("missing response data".to_owned());
                    };
                    if any.opt_string(10).map_err(|e| e.to_string())?.as_deref()
                        != Some(USAGE_QUOTAS_ANY_TYPE)
                    {
                        return Err("unrecognized Any type".to_owned());
                    }
                    let value = any
                        .opt_bytes(20)
                        .map_err(|e| e.to_string())?
                        .unwrap_or_default();
                    weights_from_bytes(&value)
                }
            });
            let mut result = UpdateResult::default();
            let mut errs = Vec::new();
            for r in futures_join_all(calls).await {
                match r {
                    Ok(w) => result.weights.extend(w),
                    Err(e) => errs.push(e),
                }
            }
            if !errs.is_empty() {
                result.err = Some(errs.join("; "));
            }
            result
        })
    }
}

async fn futures_join_all<F, T>(futs: impl IntoIterator<Item = F>) -> Vec<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futs.into_iter().map(tokio::spawn).collect();
    let mut out = Vec::new();
    for h in handles {
        if let Ok(v) = h.await {
            out.push(v);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_round_trips_and_sanity_checks() {
        let mut load = HashMap::new();
        load.insert(
            "user:d".to_owned(),
            Calls {
                allowed: i64::MAX,
                rejected: 3,
            },
        );
        let data = update_to_bytes("host", Duration::from_secs(3), &load);
        let mut r = cadence_thrift::protocol::Reader::new(&data);
        let s = cadence_thrift::value::read_struct(&mut r).unwrap();
        assert_eq!(s.opt_string(10).unwrap().as_deref(), Some("host"));
        assert_eq!(s.opt_i32(20).unwrap(), Some(3000));

        let entry = |weight: f64, used: f64| {
            Value::Struct(Struct {
                fields: vec![(10, Value::F64(weight)), (20, Value::F64(used))],
            })
        };
        let resp = |weight: f64, used: f64| {
            let s = Struct {
                fields: vec![(10, Value::Map(vec![(bytes("user:d"), entry(weight, used))]))],
            };
            let mut w = cadence_thrift::protocol::Writer::new();
            cadence_thrift::value::write_struct(&mut w, &s);
            w.into_bytes()
        };
        let ok = weights_from_bytes(&resp(0.5, 7.0)).unwrap();
        assert_eq!(ok["user:d"].weight, 0.5);
        assert!(weights_from_bytes(&resp(1.5, 1.0)).is_err());
        assert!(weights_from_bytes(&resp(0.5, -1.0)).is_err());
        assert!(weights_from_bytes(&resp(f64::NAN, 1.0)).is_err());
        assert!(weights_from_bytes(&resp(0.5, f64::INFINITY)).is_ok());
    }

    #[test]
    fn boost_matches_go() {
        assert_eq!(boost_rps(100.0, 10.0, 0.5), 50.0);
        assert_eq!(boost_rps(100.0, 60.0, 0.5), 60.0);
    }

    #[test]
    fn fallback_limiter_startup_and_failure_modes() {
        let l = FallbackLimiter::new(Arc::new(|| 2.0));
        assert!(l.collect().1, "starts up in fallback mode");
        assert!(l.allow());
        assert!(l.allow());
        assert!(!l.allow());
        l.update(100.0);
        assert!(!l.collect().1);
        for _ in 0..10 {
            l.failed_update();
        }
        assert!(l.collect().2, "too many failures");
    }

    #[test]
    fn key_modes_parse_and_unknown_is_disabled() {
        assert_eq!(KeyMode::parse("global"), KeyMode::Global);
        assert_eq!(KeyMode::parse("bogus"), KeyMode::Disabled);
        assert_eq!(KeyMode::parse(""), KeyMode::Disabled);
        assert!(KeyMode::GlobalShadowLocal.uses_local());
        assert!(!KeyMode::Local.uses_global());
    }

    struct NoAggs;
    impl AggregatorClient for NoAggs {
        fn update(&self, _: Duration, load: HashMap<String, Calls>) -> BoxFuture<UpdateResult> {
            Box::pin(async move {
                UpdateResult {
                    weights: load
                        .keys()
                        .map(|k| {
                            (
                                k.clone(),
                                UpdateEntry {
                                    weight: 0.5,
                                    used_rps: 1.0,
                                },
                            )
                        })
                        .collect(),
                    err: None,
                }
            })
        }
    }

    #[tokio::test]
    async fn collection_applies_weights_and_disables_unknown_modes() {
        let c = Collection::new(
            "user",
            Arc::new(|| Duration::from_secs(1)),
            Arc::new(|_| 100.0),
            Arc::new(|_| 4.0),
            Arc::new(|k| {
                if k == "user:off" {
                    "disabled".into()
                } else {
                    "global".into()
                }
            }),
            Arc::new(NoAggs),
        );
        assert!(matches!(c.acquire("off"), Decision::Disabled));
        assert!(matches!(c.acquire("d"), Decision::Decided(Ok(()))));
        c.tick(Duration::from_secs(1)).await;
        let g = c.global_limiter("d");
        assert!(!g.collect().1, "weights applied, no longer starting up");
    }
}
