//! Local evaluation semantics used by Unleash client-go v4.5.0.
//!
//! This module is deliberately separate from repository polling: callers
//! provide the cached `/api/client/features` response and an evaluation
//! context. Network refresh and metrics lifecycle are owned by the repository
//! adapter.

use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::collections::HashMap;

fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub remote_address: String,
    #[serde(default)]
    pub app_name: String,
    #[serde(default)]
    pub environment: String,
    #[serde(default)]
    pub current_time: String,
    #[serde(default, deserialize_with = "null_default")]
    pub properties: HashMap<String, String>,
}

impl Context {
    pub fn field(&self, name: &str) -> &str {
        match name {
            "userId" => &self.user_id,
            "sessionId" => &self.session_id,
            "remoteAddress" => &self.remote_address,
            "appName" => &self.app_name,
            "environment" => &self.environment,
            "currentTime" => &self.current_time,
            _ => self.properties.get(name).map(String::as_str).unwrap_or(""),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FeatureResponse {
    #[serde(default, deserialize_with = "null_default")]
    pub features: Vec<Feature>,
    #[serde(default, deserialize_with = "null_default")]
    pub segments: Vec<Segment>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Feature {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, deserialize_with = "null_default")]
    pub strategies: Vec<Strategy>,
    #[serde(default, deserialize_with = "null_default")]
    pub variants: Vec<VariantInternal>,
    #[serde(default, deserialize_with = "null_default")]
    pub dependencies: Vec<Dependency>,
    #[serde(default)]
    pub impression_data: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Strategy {
    #[serde(default)]
    pub name: String,
    #[serde(default, deserialize_with = "null_default")]
    pub parameters: Map<String, Value>,
    #[serde(default, deserialize_with = "null_default")]
    pub constraints: Vec<Constraint>,
    #[serde(default, deserialize_with = "null_default")]
    pub variants: Vec<VariantInternal>,
    #[serde(default, deserialize_with = "null_default")]
    pub segments: Vec<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Segment {
    #[serde(default)]
    pub id: i64,
    #[serde(default, deserialize_with = "null_default")]
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Constraint {
    #[serde(default)]
    pub context_name: String,
    #[serde(default)]
    pub operator: String,
    #[serde(default, deserialize_with = "null_default")]
    pub values: Vec<String>,
    #[serde(default)]
    pub value: String,
    #[serde(default)]
    pub case_insensitive: bool,
    #[serde(default)]
    pub inverted: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dependency {
    #[serde(default)]
    pub feature: String,
    pub enabled: Option<bool>,
    pub variants: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariantInternal {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub payload: Payload,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub feature_enabled: bool,
    #[serde(default)]
    pub weight: i64,
    #[serde(default)]
    pub stickiness: String,
    #[serde(default, deserialize_with = "null_default")]
    pub overrides: Vec<Override>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct Payload {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Override {
    #[serde(default)]
    pub context_name: String,
    #[serde(default, deserialize_with = "null_default")]
    pub values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Variant {
    pub name: String,
    pub enabled: bool,
    pub feature_enabled: bool,
    pub payload: Payload,
}

impl From<&VariantInternal> for Variant {
    fn from(v: &VariantInternal) -> Self {
        Self {
            name: v.name.clone(),
            enabled: v.enabled,
            feature_enabled: v.feature_enabled,
            payload: v.payload.clone(),
        }
    }
}

/// Evaluate a feature using the SDK's registered built-in strategies. Unknown
/// strategy names are skipped, as in unleash-client-go v4.5.0.
pub fn is_enabled(snapshot: &FeatureResponse, name: &str, context: &Context) -> bool {
    enabled(snapshot, name, context, 0)
}

fn enabled(snapshot: &FeatureResponse, name: &str, ctx: &Context, depth: usize) -> bool {
    if depth > 16 {
        return false;
    }
    let Some(feature) = snapshot.features.iter().find(|f| f.name == name) else {
        return false;
    };
    if !feature.enabled {
        return false;
    }
    for dep in &feature.dependencies {
        let Some(parent) = snapshot.features.iter().find(|f| f.name == dep.feature) else {
            return false;
        };
        if !parent.dependencies.is_empty() {
            return false;
        }
        let parent_on = enabled(snapshot, &dep.feature, ctx, depth + 1);
        let parent_variant = get_variant(snapshot, &dep.feature, ctx, depth + 1);
        let expected = dep.enabled.unwrap_or(true);
        if parent_on != expected {
            return false;
        }
        if expected
            && dep.variants.as_ref().is_some_and(|vs| !vs.is_empty())
            && !dep
                .variants
                .as_ref()
                .unwrap()
                .contains(&parent_variant.name)
        {
            return false;
        }
    }
    if feature.strategies.is_empty() {
        return true;
    }
    feature
        .strategies
        .iter()
        .any(|s| strategy_matches(snapshot, s, ctx))
}

fn strategy_matches(snapshot: &FeatureResponse, s: &Strategy, ctx: &Context) -> bool {
    let segments: HashMap<i64, &Segment> =
        snapshot.segments.iter().map(|seg| (seg.id, seg)).collect();
    let seg_ok = s.segments.iter().all(|id| {
        segments
            .get(id)
            .is_some_and(|seg| constraints(ctx, &seg.constraints))
    });
    seg_ok && constraints(ctx, &s.constraints) && strategy_enabled(&s.name, &s.parameters, ctx)
}

fn strategy_enabled(name: &str, p: &Map<String, Value>, c: &Context) -> bool {
    let string = |key: &str| p.get(key).and_then(Value::as_str).unwrap_or("");
    let pct = |key: &str| {
        p.get(key)
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
    };
    match name {
        "default" => true,
        "userWithId" => {
            !string("userIds").is_empty()
                && string("userIds").split(',').any(|x| x.trim() == c.user_id)
        }
        "applicationHostname" => {
            let hostname = std::env::var("HOSTNAME")
                .ok()
                .filter(|h| !h.is_empty())
                .or_else(|| {
                    std::fs::read_to_string("/etc/hostname")
                        .ok()
                        .map(|h| h.trim().to_owned())
                })
                .unwrap_or_else(|| "undefined".into())
                .to_lowercase();
            string("hostNames")
                .split(',')
                .any(|x| x.to_lowercase() == hostname)
        }
        "remoteAddress" => ip_match(&c.remote_address, string("IPs")),
        "gradualRolloutUserId" => p
            .get("groupId")
            .and_then(Value::as_str)
            .is_some_and(|group| rollout(&c.user_id, group, pct("percentage"))),
        "gradualRolloutSessionId" => p
            .get("groupId")
            .and_then(Value::as_str)
            .is_some_and(|group| rollout(&c.session_id, group, pct("percentage"))),
        "flexibleRollout" => {
            let st = string("stickiness");
            let id = match st {
                "" | "default" if !c.user_id.is_empty() => c.user_id.clone(),
                "" | "default" if !c.session_id.is_empty() => c.session_id.clone(),
                "" | "default" | "random" => (rand::random::<u32>() % 10_000 + 1).to_string(),
                other => c.field(other).to_owned(),
            };
            rollout(&id, string("groupId"), pct("rollout"))
        }
        // Random rollout intentionally has no deterministic context key.
        "gradualRolloutRandom" => {
            pct("percentage").is_some_and(|p| p >= (rand::random::<u32>() % 100 + 1) as f64)
        }
        _ => false,
    }
}

fn rollout(id: &str, group: &str, percentage: Option<f64>) -> bool {
    if id.is_empty() {
        return false;
    }
    let bucket = normalized(&format!("{group}:{id}"), 0) % 100 + 1;
    percentage.is_some_and(|p| p > 0.0 && f64::from(bucket) <= p)
}

/// MurmurHash3 x86 32-bit, matching github.com/twmb/murmur3 SeedNew32.
fn normalized(data: &str, seed: u32) -> u32 {
    let b = data.as_bytes();
    let mut h = seed;
    let mut i = 0;
    while i + 4 <= b.len() {
        let mut k = u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        k = k.wrapping_mul(0xcc9e2d51);
        k = k.rotate_left(15);
        k = k.wrapping_mul(0x1b873593);
        h ^= k;
        h = h.rotate_left(13);
        h = h.wrapping_mul(5).wrapping_add(0xe6546b64);
        i += 4;
    }
    let rem = &b[i..];
    let mut k = 0u32;
    if rem.len() == 3 {
        k ^= (rem[2] as u32) << 16;
    }
    if rem.len() >= 2 {
        k ^= (rem[1] as u32) << 8;
    }
    if !rem.is_empty() {
        k ^= rem[0] as u32;
        k = k.wrapping_mul(0xcc9e2d51);
        k = k.rotate_left(15);
        k = k.wrapping_mul(0x1b873593);
        h ^= k;
    }
    h ^= b.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^ (h >> 16)
}

fn ip_match(ip: &str, list: &str) -> bool {
    use std::net::IpAddr;
    let Ok(target) = ip.trim().parse::<IpAddr>() else {
        return false;
    };
    list.split(',').any(|entry| {
        let entry = entry.trim();
        if let Some((network, bits)) = entry.split_once('/') {
            let (Ok(net), Ok(bits)) = (network.parse::<IpAddr>(), bits.parse::<u8>()) else {
                return false;
            };
            match (target, net) {
                (IpAddr::V4(a), IpAddr::V4(n)) if bits <= 32 => {
                    let m = if bits == 0 {
                        0
                    } else {
                        u32::MAX << (32 - bits)
                    };
                    u32::from(a) & m == u32::from(n) & m
                }
                (IpAddr::V6(a), IpAddr::V6(n)) if bits <= 128 => {
                    let m = if bits == 0 {
                        0
                    } else {
                        u128::MAX << (128 - bits)
                    };
                    u128::from(a) & m == u128::from(n) & m
                }
                _ => false,
            }
        } else {
            entry.parse::<IpAddr>().is_ok_and(|n| n == target)
        }
    })
}

fn constraints(ctx: &Context, cs: &[Constraint]) -> bool {
    cs.iter().all(|c| {
        let actual = ctx.field(&c.context_name);
        let a = if c.case_insensitive {
            actual.to_lowercase()
        } else {
            actual.to_owned()
        };
        let found = match c.operator.as_str() {
            "IN" => Some(c.values.iter().any(|value| value == actual)),
            "NOT_IN" => Some(!c.values.iter().any(|value| value == actual)),
            "STR_CONTAINS" | "STR_STARTS_WITH" | "STR_ENDS_WITH" => c
                .values
                .iter()
                .any(|value| {
                    let value = if c.case_insensitive {
                        value.to_lowercase()
                    } else {
                        value.clone()
                    };
                    match c.operator.as_str() {
                        "STR_CONTAINS" => a.contains(&value),
                        "STR_STARTS_WITH" => a.starts_with(&value),
                        _ => a.ends_with(&value),
                    }
                })
                .into(),
            "NUM_EQ" | "NUM_LT" | "NUM_LTE" | "NUM_GT" | "NUM_GTE" => actual
                .parse::<f64>()
                .ok()
                .zip(c.value.parse::<f64>().ok())
                .map(|(x, y)| match c.operator.as_str() {
                    "NUM_EQ" => x == y,
                    "NUM_LT" => x < y,
                    "NUM_LTE" => x <= y,
                    "NUM_GT" => x > y,
                    _ => x >= y,
                }),
            "DATE_BEFORE" | "DATE_AFTER" => {
                let context_time = if actual.is_empty() {
                    Some(Utc::now())
                } else {
                    DateTime::parse_from_rfc3339(actual)
                        .ok()
                        .map(|t| t.with_timezone(&Utc))
                };
                let constraint_time = DateTime::parse_from_rfc3339(&c.value)
                    .ok()
                    .map(|t| t.with_timezone(&Utc));
                context_time.zip(constraint_time).map(|(x, y)| {
                    if c.operator == "DATE_BEFORE" {
                        x < y
                    } else {
                        x > y
                    }
                })
            }
            "SEMVER_EQ" | "SEMVER_LT" | "SEMVER_GT" => Version::parse(actual)
                .ok()
                .zip(Version::parse(&c.value).ok())
                .map(|(x, y)| match c.operator.as_str() {
                    "SEMVER_EQ" => x == y,
                    "SEMVER_LT" => x < y,
                    _ => x > y,
                }),
            _ => None,
        };
        found
            .map(|matched| if c.inverted { !matched } else { matched })
            .unwrap_or(false)
    })
}

pub fn get_variant(snapshot: &FeatureResponse, name: &str, ctx: &Context, depth: usize) -> Variant {
    let disabled = || Variant {
        name: "disabled".into(),
        enabled: false,
        feature_enabled: false,
        payload: Payload::default(),
    };
    if depth > 16 {
        return disabled();
    }
    let Some(f) = snapshot.features.iter().find(|f| f.name == name) else {
        return disabled();
    };
    if !enabled(snapshot, name, ctx, depth) {
        return disabled();
    }
    for s in &f.strategies {
        if strategy_matches(snapshot, s, ctx) && !s.variants.is_empty() {
            if let Some(v) = choose_variant(
                &s.variants,
                s.parameters
                    .get("groupId")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                ctx,
            ) {
                return v;
            }
        }
    }
    choose_variant(&f.variants, &f.name, ctx).unwrap_or_else(|| Variant {
        name: "disabled".into(),
        enabled: true,
        feature_enabled: true,
        payload: Payload::default(),
    })
}

fn choose_variant(vs: &[VariantInternal], group: &str, c: &Context) -> Option<Variant> {
    for v in vs {
        for o in &v.overrides {
            if o.values.iter().any(|x| x == c.field(&o.context_name)) {
                let mut out = Variant::from(v);
                out.enabled = true;
                out.feature_enabled = true;
                return Some(out);
            }
        }
    }
    let total: i64 = vs.iter().map(|v| v.weight).sum();
    if total == 0 {
        return None;
    }
    let stick = vs.first().map(|v| v.stickiness.as_str()).unwrap_or("");
    let mut id = if !stick.is_empty() && stick != "default" {
        c.field(stick).to_owned()
    } else if !c.user_id.is_empty() {
        c.user_id.clone()
    } else if !c.session_id.is_empty() {
        c.session_id.clone()
    } else if !c.remote_address.is_empty() {
        c.remote_address.clone()
    } else {
        String::new()
    };
    if id.is_empty() {
        id = (rand::random::<u32>() % 10_000 + 1).to_string();
    }
    let target = normalized(&format!("{group}:{id}"), 86_028_157) % total as u32 + 1;
    let mut sum = 0u32;
    for v in vs {
        sum = sum.saturating_add(v.weight.max(0) as u32);
        if sum >= target {
            let mut out = Variant::from(v);
            out.enabled = true;
            out.feature_enabled = true;
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn go_hash_rollout_fixture() {
        assert_eq!(normalized("gr1:123", 86_028_157) % 100 + 1, 96);
        assert_eq!(normalized("gr1:123", 0) % 100 + 1, 73);
        assert_eq!(normalized("groupX:999", 0) % 100 + 1, 25);
    }

    #[test]
    fn rollout_buckets_match_go_across_percentage_boundaries_and_ids() {
        let fixtures = [("123", 73_u32), ("999", 25), ("alice", 13), ("bob", 37)];
        for (id, bucket) in fixtures {
            for (percentage, expected) in [
                (0.0, false),
                (1.0, bucket <= 1),
                (50.0, bucket <= 50),
                (100.0, true),
            ] {
                assert_eq!(
                    rollout(
                        id,
                        if id == "999" { "groupX" } else { "gr1" },
                        Some(percentage)
                    ),
                    expected,
                    "id={id}, bucket={bucket}, percentage={percentage}"
                );
            }
        }
    }
    #[test]
    fn user_rollout_and_disabled_default() {
        let snap: FeatureResponse = serde_json::from_str(r#"{"features":[{"name":"on","enabled":true,"strategies":[{"name":"userWithId","parameters":{"userIds":"alice,bob"}}]},{"name":"off","enabled":false}]}"#).unwrap();
        assert!(is_enabled(
            &snap,
            "on",
            &Context {
                user_id: "alice".into(),
                ..Default::default()
            }
        ));
        assert!(!is_enabled(
            &snap,
            "on",
            &Context {
                user_id: "eve".into(),
                ..Default::default()
            }
        ));
        assert_eq!(
            get_variant(&snap, "off", &Context::default(), 0).name,
            "disabled"
        );
    }

    #[test]
    fn go_feature_and_variant_json_may_omit_optional_fields_or_use_null() {
        let snapshot: FeatureResponse = serde_json::from_str(
            r#"{"features":[
              {"name":"minimal","enabled":true,"strategies":[{"name":"default"}],"dependencies":null},
              {"name":"variant","enabled":true,"strategies":[{"name":"default"}],
               "variants":[{"name":"v1","weight":100,"payload":{"type":"string","value":"payload"}}]}
            ],"segments":null}"#,
        )
        .unwrap();
        assert!(is_enabled(&snapshot, "minimal", &Context::default()));
        let selected = get_variant(
            &snapshot,
            "variant",
            &Context {
                user_id: "alice".into(),
                ..Default::default()
            },
            0,
        );
        assert_eq!(selected.name, "v1");
        assert_eq!(selected.payload.value, "payload");
    }
}
