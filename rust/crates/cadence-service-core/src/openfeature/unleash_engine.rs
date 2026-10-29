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
use std::sync::OnceLock;

static APPLICATION_HOSTNAME: OnceLock<String> = OnceLock::new();

fn application_hostname() -> &'static str {
    APPLICATION_HOSTNAME
        .get_or_init(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|hostname| hostname.trim().to_lowercase())
                .filter(|hostname| !hostname.is_empty())
                .unwrap_or_else(|| "undefined".into())
        })
        .as_str()
}

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
    /// Raw domainName from Go string filters, when supplied through the byte API.
    #[serde(skip)]
    pub domain_bytes: Option<Vec<u8>>,
}

impl Context {
    pub fn field_bytes(&self, name: &str) -> &[u8] {
        if name == "domainName" {
            if let Some(bytes) = &self.domain_bytes { return bytes; }
        }
        self.field(name).as_bytes()
    }

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
    evaluate(snapshot, name, context, 0).enabled
}

struct Evaluation {
    enabled: bool,
    strategy_variant: Option<Variant>,
}

impl Evaluation {
    fn disabled() -> Self {
        Self {
            enabled: false,
            strategy_variant: None,
        }
    }
}

fn evaluate(snapshot: &FeatureResponse, name: &str, ctx: &Context, depth: usize) -> Evaluation {
    if depth > 16 {
        return Evaluation::disabled();
    }
    let Some(feature) = snapshot.features.iter().find(|f| f.name == name) else {
        return Evaluation::disabled();
    };
    if !feature.enabled {
        return Evaluation::disabled();
    }
    for dep in &feature.dependencies {
        let Some(parent) = snapshot.features.iter().find(|f| f.name == dep.feature) else {
            return Evaluation::disabled();
        };
        if !parent.dependencies.is_empty() {
            return Evaluation::disabled();
        }
        let parent_result = evaluate(snapshot, &dep.feature, ctx, depth + 1);
        let expected = dep.enabled.unwrap_or(true);
        if parent_result.enabled != expected {
            return Evaluation::disabled();
        }
        if expected
            && dep.variants.as_ref().is_some_and(|vs| !vs.is_empty())
            && parent_result
                .strategy_variant
                .as_ref()
                .is_some_and(|variant| !dep.variants.as_ref().unwrap().contains(&variant.name))
        {
            return Evaluation::disabled();
        }
    }
    if feature.strategies.is_empty() {
        return Evaluation {
            enabled: true,
            strategy_variant: None,
        };
    }
    let segments: HashMap<i64, &Segment> =
        snapshot.segments.iter().map(|seg| (seg.id, seg)).collect();
    for strategy in &feature.strategies {
        if !matches!(
            strategy.name.as_str(),
            "default"
                | "userWithId"
                | "applicationHostname"
                | "remoteAddress"
                | "gradualRolloutUserId"
                | "gradualRolloutSessionId"
                | "gradualRolloutRandom"
                | "flexibleRollout"
        ) {
            continue;
        }
        let mut segment_constraints = Vec::new();
        for id in &strategy.segments {
            let Some(segment) = segments.get(id) else {
                // Go's repository returns an error for an absent segment and
                // Client.isEnabled immediately returns a disabled result.
                return Evaluation::disabled();
            };
            segment_constraints.extend(segment.constraints.iter());
        }
        if !constraints(
            ctx,
            &segment_constraints.into_iter().cloned().collect::<Vec<_>>(),
        ) || !constraints(ctx, &strategy.constraints)
            || !strategy_enabled(&strategy.name, &strategy.parameters, ctx)
        {
            continue;
        }
        let variant = if strategy.variants.is_empty() {
            None
        } else {
            let Some(group) = strategy.parameters.get("groupId").and_then(Value::as_str) else {
                return Evaluation::disabled();
            };
            Some(choose_variant(&strategy.variants, group, ctx).unwrap_or_else(disabled_variant))
        };
        // Client.isEnabled returns from the first matching strategy.
        return Evaluation {
            enabled: true,
            strategy_variant: variant,
        };
    }
    Evaluation::disabled()
}

fn disabled_variant() -> Variant {
    Variant {
        name: "disabled".into(),
        enabled: false,
        feature_enabled: false,
        payload: Payload::default(),
    }
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
        "applicationHostname" => string("hostNames")
            .split(',')
            .any(|x| x.to_lowercase() == application_hostname()),
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
            if !matches!(st, "" | "default" | "random") {
                return rollout_bytes(c.field_bytes(st), string("groupId"), pct("rollout"));
            }
            let id = match st {
                "" | "default" if !c.user_id.is_empty() => c.user_id.clone(),
                "" | "default" if !c.session_id.is_empty() => c.session_id.clone(),
                "" | "default" | "random" => (rand::random::<u32>() % 10_000 + 1).to_string(),
                other => String::from_utf8_lossy(c.field_bytes(other)).into_owned(),
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
    rollout_bytes(id.as_bytes(), group, percentage)
}

fn rollout_bytes(id: &[u8], group: &str, percentage: Option<f64>) -> bool {
    if id.is_empty() { return false; }
    let mut key = group.as_bytes().to_vec();
    key.push(b':');
    key.extend_from_slice(id);
    let bucket = normalized_bytes(&key, 0) % 100 + 1;
    percentage.is_some_and(|p| p > 0.0 && f64::from(bucket) <= p)
}

/// MurmurHash3 x86 32-bit, matching github.com/twmb/murmur3 SeedNew32.
#[cfg(test)]
fn normalized(data: &str, seed: u32) -> u32 {
    normalized_bytes(data.as_bytes(), seed)
}

fn normalized_bytes(b: &[u8], seed: u32) -> u32 {
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
                (address, IpAddr::V4(network)) if bits <= 32 => {
                    as_ipv4(address).is_some_and(|address| ipv4_in_subnet(address, network, bits))
                }
                (address, IpAddr::V6(network)) if bits <= 128 => {
                    if let Some(network4) = network.to_ipv4_mapped() {
                        if bits >= 96 {
                            as_ipv4(address)
                                .is_some_and(|address| ipv4_in_subnet(address, network4, bits - 96))
                        } else {
                            ipv6_in_subnet(as_ipv6(address), network, bits)
                        }
                    } else {
                        ipv6_in_subnet(as_ipv6(address), network, bits)
                    }
                }
                _ => false,
            }
        } else {
            entry
                .parse::<IpAddr>()
                .is_ok_and(|network| match (target, network) {
                    (IpAddr::V4(a), IpAddr::V4(b)) => a == b,
                    (IpAddr::V6(a), IpAddr::V6(b)) => a == b,
                    (IpAddr::V4(a), IpAddr::V6(b)) | (IpAddr::V6(b), IpAddr::V4(a)) => {
                        b.to_ipv4_mapped() == Some(a)
                    }
                })
        }
    })
}

fn ipv4_in_subnet(address: std::net::Ipv4Addr, network: std::net::Ipv4Addr, bits: u8) -> bool {
    let mask = if bits == 0 {
        0
    } else {
        u32::MAX << (32 - bits)
    };
    u32::from(address) & mask == u32::from(network) & mask
}

fn as_ipv4(address: std::net::IpAddr) -> Option<std::net::Ipv4Addr> {
    match address {
        std::net::IpAddr::V4(address) => Some(address),
        std::net::IpAddr::V6(address) => address.to_ipv4_mapped(),
    }
}

fn as_ipv6(address: std::net::IpAddr) -> std::net::Ipv6Addr {
    match address {
        std::net::IpAddr::V4(address) => address.to_ipv6_mapped(),
        std::net::IpAddr::V6(address) => address,
    }
}

fn ipv6_in_subnet(address: std::net::Ipv6Addr, network: std::net::Ipv6Addr, bits: u8) -> bool {
    let mask = if bits == 0 {
        0
    } else {
        u128::MAX << (128 - bits)
    };
    u128::from(address) & mask == u128::from(network) & mask
}

// Go range decodes each invalid byte as RuneError; unicode.ToLower maps one
// rune to one rune (unlike Rust's full, potentially expanding lowercase).
fn go_lower(mut bytes: &[u8]) -> Vec<u8> {
    let mut output = String::new();
    while !bytes.is_empty() {
        match std::str::from_utf8(bytes) {
            Ok(valid) => {
                output.extend(valid.chars().map(|c| c.to_lowercase().next().unwrap_or(c)));
                break;
            }
            Err(error) => {
                let valid = std::str::from_utf8(&bytes[..error.valid_up_to()]).unwrap();
                output.extend(valid.chars().map(|c| c.to_lowercase().next().unwrap_or(c)));
                output.push('\u{fffd}');
                bytes = &bytes[error.valid_up_to() + 1..];
            }
        }
    }
    output.into_bytes()
}

fn constraints(ctx: &Context, cs: &[Constraint]) -> bool {
    cs.iter().all(|c| {
        let bytes = ctx.field_bytes(&c.context_name);
        let actual = std::str::from_utf8(bytes).unwrap_or("\0");
        let a = if c.case_insensitive {
            go_lower(bytes)
        } else {
            bytes.to_vec()
        };
        let found = match c.operator.as_str() {
            "IN" => Some(c.values.iter().any(|value| value.as_bytes() == bytes)),
            "NOT_IN" => Some(!c.values.iter().any(|value| value.as_bytes() == bytes)),
            "STR_CONTAINS" | "STR_STARTS_WITH" | "STR_ENDS_WITH" => c
                .values
                .iter()
                .any(|value| {
                    let value = if c.case_insensitive {
                        go_lower(value.as_bytes())
                    } else {
                        value.as_bytes().to_vec()
                    };
                    let value = value.as_slice();
                    match c.operator.as_str() {
                        "STR_CONTAINS" => value.is_empty() || a.windows(value.len()).any(|window| window == value),
                        "STR_STARTS_WITH" => a.starts_with(value),
                        _ => a.ends_with(value),
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
    if depth > 16 {
        return disabled_variant();
    }
    let Some(f) = snapshot.features.iter().find(|f| f.name == name) else {
        return disabled_variant();
    };
    let result = evaluate(snapshot, name, ctx, depth);
    if !result.enabled {
        return disabled_variant();
    }
    if let Some(variant) = result.strategy_variant {
        return variant;
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
            if o.values.iter().any(|x| x.as_bytes() == c.field_bytes(&o.context_name)) {
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
        c.field_bytes(stick).to_vec()
    } else if !c.user_id.is_empty() {
        c.user_id.as_bytes().to_vec()
    } else if !c.session_id.is_empty() {
        c.session_id.as_bytes().to_vec()
    } else if !c.remote_address.is_empty() {
        c.remote_address.as_bytes().to_vec()
    } else {
        Vec::new()
    };
    if id.is_empty() {
        id = (rand::random::<u32>() % 10_000 + 1).to_string().into_bytes();
    }
    let mut key = group.as_bytes().to_vec();
    key.push(b':');
    key.extend_from_slice(&id);
    let target = normalized_bytes(&key, 86_028_157) % total as u32 + 1;
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
    fn raw_stickiness_hashes_original_bytes() {
        let mut context = Context::default();
        context.domain_bytes = Some(vec![0xff, b'a']);
        let group = (0..100).map(|n| format!("group{n}"))
            .find(|group| {
                let mut raw = group.as_bytes().to_vec();
                raw.extend_from_slice(b":\xffa");
                normalized_bytes(&raw, 86_028_157) % 100 != normalized(&format!("{group}:�a"), 86_028_157) % 100
            }).unwrap();
        let mut raw = group.as_bytes().to_vec();
        raw.extend_from_slice(b":\xffa");
        let bucket = normalized_bytes(&raw, 86_028_157) % 100 + 1;
        let variants = vec![
            VariantInternal { name: "first".into(), weight: i64::from(bucket), stickiness: "domainName".into(), ..Default::default() },
            VariantInternal { name: "second".into(), weight: i64::from(100 - bucket), ..Default::default() },
        ];
        assert_eq!(choose_variant(&variants, &group, &context).unwrap().name, "first");
    }

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
    fn application_hostname_strategy_uses_cached_os_hostname() {
        let hostname = application_hostname();
        let parameters = serde_json::json!({"hostNames": hostname});
        assert!(strategy_enabled(
            "applicationHostname",
            parameters.as_object().unwrap(),
            &Context::default()
        ));
    }

    #[test]
    fn first_matching_strategy_controls_variant_result() {
        let snapshot: FeatureResponse = serde_json::from_str(
            r#"{"features":[{"name":"flag","enabled":true,
              "strategies":[
                {"name":"default"},
                {"name":"default","parameters":{"groupId":"later"},
                 "variants":[{"name":"later-strategy","weight":100,"payload":{"value":"wrong"}}]}
              ],
              "variants":[{"name":"feature-level","weight":100,"payload":{"value":"right"}}]}]}"#,
        )
        .unwrap();
        let variant = get_variant(
            &snapshot,
            "flag",
            &Context {
                user_id: "alice".into(),
                ..Default::default()
            },
            0,
        );
        assert_eq!(variant.name, "feature-level");
        assert_eq!(variant.payload.value, "right");
    }

    #[test]
    fn matching_strategy_variants_require_string_group_id() {
        let snapshot: FeatureResponse = serde_json::from_str(
            r#"{"features":[{"name":"flag","enabled":true,"strategies":[
              {"name":"default","variants":[{"name":"v1","weight":100}]}
            ]}]}"#,
        )
        .unwrap();
        assert!(!is_enabled(&snapshot, "flag", &Context::default()));
        assert_eq!(
            get_variant(&snapshot, "flag", &Context::default(), 0).name,
            "disabled"
        );
    }

    #[test]
    fn missing_segment_stops_strategy_evaluation() {
        let snapshot: FeatureResponse = serde_json::from_str(
            r#"{"features":[{"name":"flag","enabled":true,"strategies":[
              {"name":"default","segments":[404]}, {"name":"default"}
            ]}]}"#,
        )
        .unwrap();
        assert!(!is_enabled(&snapshot, "flag", &Context::default()));
    }

    #[test]
    fn remote_address_matches_go_ipv4_mapped_ip_and_network_semantics() {
        assert!(ip_match("::ffff:192.0.2.10", "192.0.2.10"));
        assert!(ip_match("::ffff:192.0.2.10", "192.0.2.0/24"));
        assert!(ip_match("192.0.2.10", "::ffff:192.0.2.0/120"));
        assert!(!ip_match("192.0.2.10", "::ffff:192.0.2.128/121"));
    }

    #[test]
    fn gradual_flexible_and_random_strategies_match_go_rollout_fixtures() {
        let context = Context {
            user_id: "123".into(),
            session_id: "123".into(),
            ..Default::default()
        };
        let gradual = serde_json::json!({"groupId":"gr1","percentage":73});
        assert!(strategy_enabled(
            "gradualRolloutUserId",
            gradual.as_object().unwrap(),
            &context
        ));
        assert!(strategy_enabled(
            "gradualRolloutSessionId",
            gradual.as_object().unwrap(),
            &context
        ));
        let just_below = serde_json::json!({"groupId":"gr1","percentage":72});
        assert!(!strategy_enabled(
            "gradualRolloutUserId",
            just_below.as_object().unwrap(),
            &context
        ));
        let flexible = serde_json::json!({"groupId":"gr1","rollout":73,"stickiness":"default"});
        assert!(strategy_enabled(
            "flexibleRollout",
            flexible.as_object().unwrap(),
            &context
        ));
        let random = serde_json::json!({"percentage":100});
        assert!(strategy_enabled(
            "gradualRolloutRandom",
            random.as_object().unwrap(),
            &context
        ));
    }

    #[test]
    fn string_numeric_date_and_semver_constraints_match_go_contract() {
        let context = Context {
            current_time: "2025-01-01T00:00:00Z".into(),
            properties: HashMap::from([
                ("plan".into(), "PROD".into()),
                ("score".into(), "12.5".into()),
                ("version".into(), "1.4.0".into()),
            ]),
            ..Default::default()
        };
        let cs: Vec<Constraint> = serde_json::from_value(serde_json::json!([
            {"contextName":"plan","operator":"STR_CONTAINS","values":["prod"],"caseInsensitive":true},
            {"contextName":"score","operator":"NUM_GTE","value":"12"},
            {"contextName":"version","operator":"SEMVER_GT","value":"1.3.9"},
            {"contextName":"currentTime","operator":"DATE_AFTER","value":"2024-12-31T23:59:59Z"}
        ])).unwrap();
        assert!(constraints(&context, &cs));
        let bad: Constraint = serde_json::from_value(serde_json::json!({
            "contextName":"version","operator":"SEMVER_EQ","value":"v1.4.0","inverted":true
        }))
        .unwrap();
        assert!(!constraints(&context, &[bad]));
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
