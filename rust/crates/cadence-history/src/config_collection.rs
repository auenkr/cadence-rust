//! Minimal port of Go `common/dynamicconfig.Collection` used by the history
//! `config.New`. Values come from a `DynamicClient`; any lookup or type error
//! falls back to the key's default, exactly like Go's typed getters.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;

pub type Map = BTreeMap<String, Value>;

/// Filter values for a single dynamic config lookup (Go `FilterOption`s).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Filters {
    pub domain_name: Option<String>,
    pub domain_id: Option<String>,
    pub shard_id: Option<i64>,
    pub task_list: Option<String>,
    pub workflow_id: Option<String>,
}

/// Go `dynamicconfig.Client`: raw JSON-typed value lookup by key + filters.
pub trait DynamicClient: Send + Sync {
    fn get_value(&self, key: &str, filters: &Filters) -> Option<Value>;
}

/// Go `dynamicconfig.NewInMemoryClient`.
#[derive(Default)]
pub struct InMemoryClient(Mutex<BTreeMap<String, Value>>);

impl InMemoryClient {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn update_value(&self, key: &str, value: Value) {
        if let Ok(mut m) = self.0.lock() {
            m.insert(key.to_string(), value);
        }
    }
}

impl DynamicClient for InMemoryClient {
    fn get_value(&self, key: &str, _filters: &Filters) -> Option<Value> {
        self.0.lock().ok().and_then(|m| m.get(key).cloned())
    }
}

/// Conversion of a raw JSON value into a typed property value.
pub trait FromDynamic: Sized + Clone + Send + Sync + 'static {
    fn from_dynamic(v: &Value) -> Option<Self>;
}

impl FromDynamic for i64 {
    fn from_dynamic(v: &Value) -> Option<Self> {
        v.as_i64().or_else(|| v.as_f64().map(|f| f as i64))
    }
}
impl FromDynamic for bool {
    fn from_dynamic(v: &Value) -> Option<Self> {
        v.as_bool()
    }
}
impl FromDynamic for f64 {
    fn from_dynamic(v: &Value) -> Option<Self> {
        v.as_f64()
    }
}
impl FromDynamic for String {
    fn from_dynamic(v: &Value) -> Option<Self> {
        v.as_str().map(str::to_owned)
    }
}
impl FromDynamic for Map {
    fn from_dynamic(v: &Value) -> Option<Self> {
        v.as_object()
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}
impl FromDynamic for Duration {
    /// Go `GetDurationValue`: strings use `time.ParseDuration`, numbers are
    /// interpreted as seconds.
    fn from_dynamic(v: &Value) -> Option<Self> {
        match v {
            Value::String(s) => parse_go_duration(s),
            Value::Number(n) => n.as_f64().filter(|f| *f >= 0.0).map(Duration::from_secs_f64),
            _ => None,
        }
    }
}

/// Subset of Go `time.ParseDuration` ("1h30m", "250ms", "1.5s", "0").
pub fn parse_go_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s == "0" {
        return Some(Duration::ZERO);
    }
    let mut rest = s;
    let mut total = 0f64;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let num: f64 = rest[..end].parse().ok()?;
        rest = &rest[end..];
        let unit_end = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let mult = match &rest[..unit_end] {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 6e10,
            "h" => 3.6e12,
            _ => return None,
        };
        total += num * mult;
        rest = &rest[unit_end..];
    }
    Some(Duration::from_nanos(total as u64))
}

pub type Prop<T> = Arc<dyn Fn() -> T + Send + Sync>;
pub type PropDomain<T> = Arc<dyn Fn(&str) -> T + Send + Sync>;
pub type PropShard<T> = Arc<dyn Fn(i32) -> T + Send + Sync>;
pub type PropDomainTaskList<T> = Arc<dyn Fn(&str, &str) -> T + Send + Sync>;
pub type PropDomainIDWorkflowID<T> = Arc<dyn Fn(&str, &str) -> T + Send + Sync>;

/// Go `dynamicconfig.Collection`.
#[derive(Clone)]
pub struct Collection {
    client: Arc<dyn DynamicClient>,
}

impl Collection {
    pub fn new(client: Arc<dyn DynamicClient>) -> Self {
        Self { client }
    }

    fn lookup<T: FromDynamic>(
        client: &Arc<dyn DynamicClient>,
        key: &str,
        filters: &Filters,
        default: &T,
    ) -> T {
        client
            .get_value(key, filters)
            .and_then(|v| T::from_dynamic(&v))
            .unwrap_or_else(|| default.clone())
    }

    pub fn prop<T: FromDynamic>(&self, key: &str, default: impl Into<T>) -> Prop<T> {
        let (c, k) = (self.client.clone(), key.to_string());
        let default: T = default.into();
        Arc::new(move || Self::lookup(&c, &k, &Filters::default(), &default))
    }

    pub fn prop_domain<T: FromDynamic>(&self, key: &str, default: impl Into<T>) -> PropDomain<T> {
        let (c, k) = (self.client.clone(), key.to_string());
        let default: T = default.into();
        Arc::new(move |domain: &str| {
            let f = Filters {
                domain_name: Some(domain.to_string()),
                ..Default::default()
            };
            Self::lookup(&c, &k, &f, &default)
        })
    }

    pub fn prop_domain_id<T: FromDynamic>(&self, key: &str, default: impl Into<T>) -> PropDomain<T> {
        let (c, k) = (self.client.clone(), key.to_string());
        let default: T = default.into();
        Arc::new(move |domain_id: &str| {
            let f = Filters {
                domain_id: Some(domain_id.to_string()),
                ..Default::default()
            };
            Self::lookup(&c, &k, &f, &default)
        })
    }

    pub fn prop_shard<T: FromDynamic>(&self, key: &str, default: impl Into<T>) -> PropShard<T> {
        let (c, k) = (self.client.clone(), key.to_string());
        let default: T = default.into();
        Arc::new(move |shard_id: i32| {
            let f = Filters {
                shard_id: Some(shard_id as i64),
                ..Default::default()
            };
            Self::lookup(&c, &k, &f, &default)
        })
    }

    pub fn prop_domain_task_list<T: FromDynamic>(
        &self,
        key: &str,
        default: impl Into<T>,
    ) -> PropDomainTaskList<T> {
        let (c, k) = (self.client.clone(), key.to_string());
        let default: T = default.into();
        Arc::new(move |domain: &str, task_list: &str| {
            let f = Filters {
                domain_name: Some(domain.to_string()),
                task_list: Some(task_list.to_string()),
                ..Default::default()
            };
            Self::lookup(&c, &k, &f, &default)
        })
    }

    pub fn prop_domain_id_workflow_id<T: FromDynamic>(
        &self,
        key: &str,
        default: impl Into<T>,
    ) -> PropDomainIDWorkflowID<T> {
        let (c, k) = (self.client.clone(), key.to_string());
        let default: T = default.into();
        Arc::new(move |domain_id: &str, workflow_id: &str| {
            let f = Filters {
                domain_id: Some(domain_id.to_string()),
                workflow_id: Some(workflow_id.to_string()),
                ..Default::default()
            };
            Self::lookup(&c, &k, &f, &default)
        })
    }
}

/// Go `ConvertIntMapToDynamicConfigMapProperty`.
pub fn int_map(entries: &[(i64, i64)]) -> Map {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), Value::from(*v)))
        .collect()
}

/// Go `definition.GetDefaultIndexedKeys()` as a dynamic-config map value.
pub fn default_indexed_keys_map() -> Map {
    cadence_api::search_attributes::default_indexed_keys()
        .into_iter()
        .map(|(k, v)| (k, Value::from(v as i32)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_go_durations() {
        assert_eq!(parse_go_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_go_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_go_duration("bogus"), None);
    }

    #[test]
    fn falls_back_to_default_on_missing_or_wrong_type() {
        let client = Arc::new(InMemoryClient::new());
        client.update_value("k.int", Value::from("not-an-int"));
        let dc = Collection::new(client.clone());
        assert_eq!(dc.prop::<i64>("k.int", 7)(), 7);
        client.update_value("k.int", Value::from(9));
        assert_eq!(dc.prop::<i64>("k.int", 7)(), 9);
        assert_eq!(dc.prop_domain::<bool>("k.none", true)("d"), true);
    }
}
