//! Port of `go/common/quotas/global/algorithm/requestweighted.go` and the
//! aggregator half of `go/common/quotas/global/rpc/mapping.go`.
//!
//! Running-weighted-average calculator for global ratelimits. Unaware of RPC or
//! desired RPS: tracks observed rates per (limit, host) and returns proportions.
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use cadence_thrift::value::{Struct, Value};

use crate::config_collection::Prop;

pub const USAGE_ANY_TYPE: &str = "cadence:loadbalanced:update_request";
pub const USAGE_QUOTAS_ANY_TYPE: &str = "cadence:loadbalanced:update_response_used";

/// Go `guessImpossibleRps`.
const GUESS_IMPOSSIBLE_RPS: f64 = 10_000_000.0;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Requests {
    pub accepted: i64,
    pub rejected: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct HostUsage {
    pub weight: f64,
    pub used: f64,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateParams {
    pub id: String,
    pub load: HashMap<String, Requests>,
    pub elapsed: Duration,
}

impl UpdateParams {
    /// Go `UpdateParams.Validate`.
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("empty caller ID".into());
        }
        if self.elapsed.is_zero() {
            return Err(format!("elapsed time must be positive, got {:?}", self.elapsed));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Metrics {
    pub host_limits: i64,
    pub limits: i64,
    pub removed_host_limits: i64,
    pub removed_limits: i64,
}

pub struct Config {
    pub new_data_weight: Prop<f64>,
    pub update_interval: Prop<Duration>,
    pub decay_after: Prop<Duration>,
    pub gc_after: Prop<Duration>,
}

#[derive(Clone, Copy)]
struct Snapshot {
    now: Instant,
    weight: f64,
    rate: Duration,
    decay_after: Duration,
    gc_after: Duration,
}

impl Snapshot {
    fn validate(&self) -> Result<(), String> {
        let mut errs = Vec::new();
        if self.weight < 0.0 {
            errs.push(format!("new data weight cannot be negative: {}", self.weight));
        }
        if self.rate.is_zero() {
            errs.push(format!("update rate must be positive: {:?}", self.rate));
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(format!("bad ratelimiter config; {}", errs.join("; ")))
        }
    }

    fn should_gc(&self, age: Duration) -> bool {
        age >= self.gc_after
    }

    /// Go `missedUpdateScalar`.
    fn missed_update_scalar(&self, age: Duration) -> f64 {
        if age < self.decay_after {
            return 1.0;
        }
        if age >= self.gc_after {
            return 0.0;
        }
        let missed = age.as_nanos() / self.rate.as_nanos().max(1);
        if missed < 1 {
            return 1.0;
        }
        (1.0 - self.weight).powf(missed as f64)
    }
}

#[derive(Clone, Copy)]
struct Entry {
    last_update: Instant,
    accepted: f64,
    rejected: f64,
}

fn weighted(newer: f64, older: f64, weight: f64) -> f64 {
    newer * weight + older * (1.0 - weight)
}

/// Go `shared.SanityLogFloat`: logs violations, returns the value unchanged.
fn sanity(lower: f64, actual: f64, upper: f64, what: &str) -> f64 {
    let msg = if actual.is_nan() {
        "is NaN, should be impossible"
    } else if actual.is_infinite() {
        "is Inf, should be impossible"
    } else if actual < lower {
        "is below lower bound, should be impossible"
    } else if upper < actual {
        "is above upper bound, should be impossible"
    } else {
        return actual;
    };
    tracing::error!(lower, actual, upper, "{what} {msg}");
    actual
}

/// Go `algorithm.impl` (`RequestWeighted`).
pub struct RequestWeighted {
    cfg: Config,
    usage: Mutex<HashMap<String, HashMap<String, Entry>>>,
}

impl RequestWeighted {
    /// Go `algorithm.New`: validates the config by taking a snapshot.
    pub fn new(cfg: Config) -> Result<Self, String> {
        let a = Self { cfg, usage: Mutex::new(HashMap::new()) };
        a.snapshot(Instant::now()).map_err(|e| format!("invalid config: {e}"))?;
        Ok(a)
    }

    fn snapshot(&self, now: Instant) -> Result<Snapshot, String> {
        let s = Snapshot {
            now,
            weight: (self.cfg.new_data_weight)(),
            rate: (self.cfg.update_interval)(),
            decay_after: (self.cfg.decay_after)(),
            gc_after: (self.cfg.gc_after)(),
        };
        s.validate()?;
        Ok(s)
    }

    pub fn update(&self, p: &UpdateParams) -> Result<(), String> {
        self.update_at(p, Instant::now())
    }

    pub fn update_at(&self, p: &UpdateParams, now: Instant) -> Result<(), String> {
        p.validate().map_err(|e| format!("bad args to update: {e}"))?;
        let mut usage = self.usage.lock().unwrap();
        let snap = self.snapshot(now)?;
        for (key, req) in &p.load {
            let prev = usage.get(key).and_then(|ih| ih.get(&p.id)).copied();
            // elapsed capped to at least 1s
            let elapsed = (p.elapsed.as_nanos() as f64).max(1e9);
            let secs = elapsed / 1e9;
            let aps = sanity(0.0, req.accepted as f64 / secs, GUESS_IMPOSSIBLE_RPS, "accepted rps");
            let rps = sanity(0.0, req.rejected as f64 / secs, GUESS_IMPOSSIBLE_RPS, "rejected rps");
            if rps + aps == 0.0 {
                continue;
            }
            let fresh = Entry { last_update: snap.now, accepted: aps, rejected: rps };
            let next = match prev {
                None => fresh,
                Some(prev) => {
                    let age = snap.now.saturating_duration_since(prev.last_update);
                    if snap.should_gc(age) {
                        fresh
                    } else {
                        let reduce = snap.missed_update_scalar(age);
                        Entry {
                            last_update: snap.now,
                            accepted: sanity(0.0, weighted(aps, prev.accepted * reduce, snap.weight), GUESS_IMPOSSIBLE_RPS, "weighted accepted rps"),
                            rejected: sanity(0.0, weighted(rps, prev.rejected * reduce, snap.weight), GUESS_IMPOSSIBLE_RPS, "weighted rejected rps"),
                        }
                    }
                }
            };
            usage.entry(key.clone()).or_default().insert(p.id.clone(), next);
        }
        Ok(())
    }

    /// Go `getWeightsLocked`.
    fn weights_locked(
        usage: &mut HashMap<String, HashMap<String, Entry>>,
        key: &str,
        snap: &Snapshot,
    ) -> (HashMap<String, f64>, f64, Metrics) {
        let mut met = Metrics::default();
        let Some(ir) = usage.get_mut(key) else { return (HashMap::new(), 0.0, met) };
        if ir.is_empty() {
            return (HashMap::new(), 0.0, met);
        }
        let mut weights = HashMap::with_capacity(ir.len());
        let mut total = 0.0;
        let mut used = 0.0;
        let mut expired = Vec::new();
        for (id, reqs) in ir.iter() {
            let age = snap.now.saturating_duration_since(reqs.last_update);
            if snap.should_gc(age) {
                expired.push(id.clone());
                met.removed_host_limits += 1;
                continue;
            }
            let reduce = sanity(0.0, snap.missed_update_scalar(age), 1.0, "missed update");
            let actual = (reqs.accepted + reqs.rejected) * reduce;
            weights.insert(id.clone(), actual);
            total += actual;
            used += reqs.accepted * reduce;
            met.host_limits += 1;
        }
        for id in expired {
            ir.remove(&id);
        }
        if ir.is_empty() {
            usage.remove(key);
            met.removed_limits += 1;
            return (HashMap::new(), 0.0, met);
        }
        for w in weights.values_mut() {
            *w = sanity(0.0, *w / total, 1.0, "normalized weight");
        }
        met.limits = 1;
        (weights, used, met)
    }

    pub fn host_usage(&self, host: &str, limits: &[String]) -> Result<HashMap<String, HostUsage>, String> {
        self.host_usage_at(host, limits, Instant::now())
    }

    pub fn host_usage_at(&self, host: &str, limits: &[String], now: Instant) -> Result<HashMap<String, HostUsage>, String> {
        let mut usage = self.usage.lock().unwrap();
        let snap = self.snapshot(now)?;
        let mut out = HashMap::with_capacity(limits.len());
        for lim in limits {
            let (hosts, used, _met) = Self::weights_locked(&mut usage, lim, &snap);
            if !hosts.is_empty() {
                out.insert(
                    lim.clone(),
                    HostUsage {
                        used: sanity(0.0, used, GUESS_IMPOSSIBLE_RPS, "used rps"),
                        weight: sanity(0.0, hosts.get(host).copied().unwrap_or(0.0), 1.0, "computed weight"),
                    },
                );
            }
        }
        Ok(out)
    }

    pub fn gc(&self) -> Result<Metrics, String> {
        self.gc_at(Instant::now())
    }

    pub fn gc_at(&self, now: Instant) -> Result<Metrics, String> {
        let mut usage = self.usage.lock().unwrap();
        let snap = self.snapshot(now)?;
        let mut m = Metrics::default();
        usage.retain(|_, dat| {
            dat.retain(|_, reqs| {
                if snap.should_gc(snap.now.saturating_duration_since(reqs.last_update)) {
                    m.removed_host_limits += 1;
                    false
                } else {
                    m.host_limits += 1;
                    true
                }
            });
            if dat.is_empty() {
                m.removed_limits += 1;
                false
            } else {
                m.limits += 1;
                true
            }
        });
        Ok(m)
    }
}

fn bytes(s: &str) -> Value {
    Value::Bytes(s.as_bytes().to_vec())
}

/// Go `rpc.AnyToAggregatorUpdate` (thrift `WeightedRatelimitUsage`).
pub fn any_to_aggregator_update(value_type: &str, value: &[u8]) -> Result<UpdateParams, String> {
    if value_type != USAGE_ANY_TYPE {
        return Err(format!("unrecognized Any type: {value_type:?}"));
    }
    let mut r = cadence_thrift::protocol::Reader::new(value);
    let s = cadence_thrift::value::read_struct(&mut r).map_err(|e| format!("serialization error: {e}"))?;
    let ser = |e: cadence_thrift::Error| format!("serialization error: {e}");
    let mut load = HashMap::new();
    if let Some(Value::Map(items)) = s.get(30) {
        for (k, v) in items {
            let (Value::Bytes(k), Value::Struct(v)) = (k, v) else {
                return Err("serialization error: malformed calls entry".into());
            };
            let key = String::from_utf8(k.clone()).map_err(|e| format!("serialization error: {e}"))?;
            load.insert(
                key,
                Requests {
                    accepted: i64::from(v.opt_i32(10).map_err(ser)?.unwrap_or(0)),
                    rejected: i64::from(v.opt_i32(20).map_err(ser)?.unwrap_or(0)),
                },
            );
        }
    }
    let par = UpdateParams {
        id: s.opt_string(10).map_err(ser)?.unwrap_or_default(),
        elapsed: Duration::from_millis(s.opt_i32(20).map_err(ser)?.unwrap_or(0).max(0) as u64),
        load,
    };
    par.validate()?;
    Ok(par)
}

/// Go `rpc.AggregatorWeightsToAny` (thrift `WeightedRatelimitUsageQuotas`).
pub fn aggregator_weights_to_bytes(weights: &HashMap<String, HostUsage>) -> Vec<u8> {
    let mut keys: Vec<&String> = weights.keys().collect();
    keys.sort();
    let quotas = keys
        .into_iter()
        .map(|k| {
            let v = weights[k];
            (bytes(k), Value::Struct(Struct { fields: vec![(10, Value::F64(v.weight)), (20, Value::F64(v.used))] }))
        })
        .collect();
    let s = Struct { fields: vec![(10, Value::Map(quotas))] };
    let mut w = cadence_thrift::protocol::Writer::new();
    cadence_thrift::value::write_struct(&mut w, &s);
    w.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn agg() -> RequestWeighted {
        RequestWeighted::new(Config {
            new_data_weight: Arc::new(|| 0.5),
            update_interval: Arc::new(|| Duration::from_secs(3)),
            decay_after: Arc::new(|| Duration::from_secs(6)),
            gc_after: Arc::new(|| Duration::from_secs(30)),
        })
        .unwrap()
    }

    fn params(id: &str, key: &str, acc: i64, rej: i64) -> UpdateParams {
        UpdateParams {
            id: id.into(),
            load: HashMap::from([(key.to_string(), Requests { accepted: acc, rejected: rej })]),
            elapsed: Duration::from_secs(1),
        }
    }

    #[test]
    fn validate_rejects_empty_id_and_zero_elapsed() {
        assert!(UpdateParams::default().validate().is_err());
        let mut p = params("a", "k", 1, 0);
        p.elapsed = Duration::ZERO;
        assert!(p.validate().is_err());
    }

    #[test]
    fn bad_config_is_rejected() {
        let r = RequestWeighted::new(Config {
            new_data_weight: Arc::new(|| 0.5),
            update_interval: Arc::new(|| Duration::ZERO),
            decay_after: Arc::new(|| Duration::from_secs(6)),
            gc_after: Arc::new(|| Duration::from_secs(30)),
        });
        assert!(r.is_err());
    }

    #[test]
    fn weights_are_proportional_to_requests() {
        let a = agg();
        let t = Instant::now();
        a.update_at(&params("h1", "k", 30, 0), t).unwrap();
        a.update_at(&params("h2", "k", 10, 0), t).unwrap();
        let u = a.host_usage_at("h1", &["k".to_string()], t).unwrap();
        assert!((u["k"].weight - 0.75).abs() < 1e-9);
        assert!((u["k"].used - 40.0).abs() < 1e-9);
        let u2 = a.host_usage_at("h2", &["k".to_string()], t).unwrap();
        assert!((u2["k"].weight - 0.25).abs() < 1e-9);
        // unknown host: weight zero, limit still reported
        assert_eq!(a.host_usage_at("h3", &["k".to_string()], t).unwrap()["k"].weight, 0.0);
    }

    #[test]
    fn zero_load_is_not_recorded() {
        let a = agg();
        a.update_at(&params("h1", "k", 0, 0), Instant::now()).unwrap();
        assert!(a.host_usage("h1", &["k".to_string()]).unwrap().is_empty());
    }

    #[test]
    fn second_update_is_rolling_average() {
        let a = agg();
        let t = Instant::now();
        a.update_at(&params("h1", "k", 10, 0), t).unwrap();
        a.update_at(&params("h1", "k", 30, 0), t + Duration::from_secs(1)).unwrap();
        let u = a.host_usage_at("h1", &["k".to_string()], t + Duration::from_secs(1)).unwrap();
        assert!((u["k"].used - 20.0).abs() < 1e-9);
    }

    #[test]
    fn old_data_decays_then_is_collected() {
        let a = agg();
        let t = Instant::now();
        a.update_at(&params("h1", "k", 10, 0), t).unwrap();
        // 9s old: missed = 3 -> 0.5^3 = 0.125
        let u = a.host_usage_at("h1", &["k".to_string()], t + Duration::from_secs(9)).unwrap();
        assert!((u["k"].used - 1.25).abs() < 1e-9);
        // 30s old: gc'd
        assert!(a.host_usage_at("h1", &["k".to_string()], t + Duration::from_secs(30)).unwrap().is_empty());
        assert_eq!(a.gc_at(t + Duration::from_secs(30)).unwrap(), Metrics::default());
    }

    #[test]
    fn gc_reports_removed_and_remaining() {
        let a = agg();
        let t = Instant::now();
        a.update_at(&params("h1", "old", 5, 0), t).unwrap();
        a.update_at(&params("h1", "new", 5, 0), t + Duration::from_secs(25)).unwrap();
        let m = a.gc_at(t + Duration::from_secs(31)).unwrap();
        assert_eq!(m, Metrics { host_limits: 1, limits: 1, removed_host_limits: 1, removed_limits: 1 });
    }

    #[test]
    fn mapping_round_trips_through_thrift() {
        let weights = HashMap::from([("k".to_string(), HostUsage { weight: 0.25, used: 7.5 })]);
        let b = aggregator_weights_to_bytes(&weights);
        let mut r = cadence_thrift::protocol::Reader::new(&b);
        let s = cadence_thrift::value::read_struct(&mut r).unwrap();
        let Some(Value::Map(m)) = s.get(10) else { panic!("no quotas") };
        assert_eq!(m.len(), 1);

        // update request: build the thrift bytes and decode
        let st = Struct {
            fields: vec![
                (10, bytes("host")),
                (20, Value::I32(3000)),
                (30, Value::Map(vec![(bytes("k"), Value::Struct(Struct { fields: vec![(10, Value::I32(4)), (20, Value::I32(2))] }))])),
            ],
        };
        let mut w = cadence_thrift::protocol::Writer::new();
        cadence_thrift::value::write_struct(&mut w, &st);
        let p = any_to_aggregator_update(USAGE_ANY_TYPE, &w.into_bytes()).unwrap();
        assert_eq!(p.id, "host");
        assert_eq!(p.elapsed, Duration::from_secs(3));
        assert_eq!(p.load["k"], Requests { accepted: 4, rejected: 2 });
        assert!(any_to_aggregator_update("bogus", &[]).is_err());
    }
}
