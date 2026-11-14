//! Go `common/rpc.HeaderForwardingMiddleware` and the `headerForwardingRules`
//! static config: inbound headers matching an ordered add/remove rule list
//! are copied onto outbound calls unless the outbound call already has them.

use std::collections::{BTreeMap, HashMap};

use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Go `config.HeaderRule`; YAML keys are `add` and `match` (no struct tags).
#[derive(Debug, Clone)]
pub struct HeaderRule {
    pub add: bool,
    pub matcher: Regex,
}

impl PartialEq for HeaderRule {
    fn eq(&self, other: &Self) -> bool {
        self.add == other.add && self.matcher.as_str() == other.matcher.as_str()
    }
}

#[derive(Serialize, Deserialize)]
struct RawRule {
    #[serde(default, alias = "Add")]
    add: bool,
    #[serde(default, rename = "match", alias = "Match")]
    matcher: String,
}

impl<'de> Deserialize<'de> for HeaderRule {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw = RawRule::deserialize(d)?;
        let matcher = Regex::new(&raw.matcher).map_err(|error| {
            D::Error::custom(format!(
                "invalid match regex in header forwarding rule: {:?}, err: {error}",
                raw.matcher
            ))
        })?;
        Ok(Self {
            add: raw.add,
            matcher,
        })
    }
}

impl Serialize for HeaderRule {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        RawRule {
            add: self.add,
            matcher: self.matcher.as_str().to_owned(),
        }
        .serialize(s)
    }
}

/// Headers to add to an outbound request (Go `Call`): rules run in order over
/// the inbound header names; a later `add: false` rule removes a pending
/// header; names already on the outbound request are never overwritten.
pub fn forwarded_headers(
    rules: &[HeaderRule],
    inbound: &HashMap<String, String>,
    outbound: &HashMap<String, String>,
) -> BTreeMap<String, String> {
    let mut names: Vec<&String> = inbound.keys().collect();
    names.sort();
    let mut pending = BTreeMap::new();
    for rule in rules {
        for name in &names {
            if !rule.matcher.is_match(name) || outbound.contains_key(*name) {
                continue;
            }
            if rule.add {
                pending.insert((*name).clone(), inbound[*name].clone());
            } else {
                pending.remove(*name);
            }
        }
    }
    pending
}

tokio::task_local! {
    /// Headers of the inbound RPC currently being handled (Go
    /// `yarpc.CallFromContext(ctx).HeaderNames()`).
    pub static INBOUND_HEADERS: HashMap<String, String>;
}

tokio::task_local! {
    /// Deadline of the inbound RPC currently being handled (Go context deadline).
    pub static INBOUND_DEADLINE: Option<std::time::Instant>;
}

/// Go context propagation: outbound wire TTL is `min(configured, caller remaining)`.
pub fn clamp_to_inbound_deadline(configured: std::time::Duration) -> std::time::Duration {
    let remaining = INBOUND_DEADLINE
        .try_with(|d| *d)
        .ok()
        .flatten()
        .map(|d| d.saturating_duration_since(std::time::Instant::now()));
    remaining.map_or(configured, |r| configured.min(r))
}

/// Go `getForwardingRules` element parsing: `{"Add": bool, "Match": regex}`.
pub fn rules_from_dynamic(items: &[serde_json::Value]) -> Result<Vec<HeaderRule>, String> {
    items
        .iter()
        .map(|item| {
            let add = item.get("Add").and_then(serde_json::Value::as_bool);
            let matcher = item.get("Match").and_then(serde_json::Value::as_str);
            let (Some(add), Some(matcher)) = (add, matcher) else {
                return Err(format!(
                    "invalid generic types for header forwarding rule: {item}"
                ));
            };
            let matcher = Regex::new(matcher).map_err(|e| {
                format!("invalid match regex in header forwarding rule: {matcher:?}, err: {e}")
            })?;
            Ok(HeaderRule { add, matcher })
        })
        .collect()
}

/// Go default of `admin.HeaderForwardingRules`: forward everything except `grpc-*`.
pub fn default_dynamic_rules() -> Vec<HeaderRule> {
    vec![
        HeaderRule {
            add: true,
            matcher: Regex::new("").expect("empty regex"),
        },
        HeaderRule {
            add: false,
            matcher: Regex::new("^grpc-.*").expect("grpc regex"),
        },
    ]
}

/// Headers to attach to an outbound call made while an inbound call is in scope.
pub fn forward_for_current_call(
    rules: &[HeaderRule],
    existing: &[(String, String)],
) -> Vec<(String, String)> {
    if rules.is_empty() {
        return Vec::new();
    }
    INBOUND_HEADERS
        .try_with(|inbound| {
            let outbound: HashMap<String, String> = existing.iter().cloned().collect();
            forwarded_headers(rules, inbound, &outbound).into_iter().collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn clamp_uses_min_of_budget_and_caller_remaining_solfix10() {
        use std::time::{Duration, Instant};
        assert_eq!(clamp_to_inbound_deadline(Duration::from_secs(30)), Duration::from_secs(30));
        let d = Instant::now() + Duration::from_secs(5);
        let got = INBOUND_DEADLINE
            .scope(Some(d), async { clamp_to_inbound_deadline(Duration::from_secs(30)) })
            .await;
        assert!(got <= Duration::from_secs(5));
        let got = INBOUND_DEADLINE
            .scope(Some(d), async { clamp_to_inbound_deadline(Duration::from_secs(1)) })
            .await;
        assert_eq!(got, Duration::from_secs(1));
    }

    fn rules(yaml: &str) -> Vec<HeaderRule> {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn map(items: &[(&str, &str)]) -> HashMap<String, String> {
        items
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn parses_go_yaml_and_applies_add_then_remove() {
        let rules = rules("- add: true\n  match: '^x-'\n- add: false\n  match: '^x-secret'\n");
        let inbound = map(&[("x-a", "1"), ("x-secret", "2"), ("other", "3")]);
        let out = forwarded_headers(&rules, &inbound, &HashMap::new());
        assert_eq!(out, BTreeMap::from([("x-a".to_owned(), "1".to_owned())]));
    }

    #[test]
    fn does_not_overwrite_existing_outbound_header() {
        let rules = rules("- add: true\n  match: '.*'\n");
        let out = forwarded_headers(
            &rules,
            &map(&[("a", "in"), ("b", "in")]),
            &map(&[("a", "out")]),
        );
        assert_eq!(out, BTreeMap::from([("b".to_owned(), "in".to_owned())]));
    }

    #[test]
    fn invalid_regex_is_a_config_error() {
        let error = serde_yaml::from_str::<Vec<HeaderRule>>("- add: true\n  match: '('\n");
        assert!(error.is_err());
    }

    #[test]
    fn dynamic_rules_parse_and_default_drop_grpc() {
        let items = vec![serde_json::json!({"Add": true, "Match": "^x-"})];
        assert_eq!(rules_from_dynamic(&items).unwrap().len(), 1);
        assert!(rules_from_dynamic(&[serde_json::json!({"Add": 1})]).is_err());
        assert!(rules_from_dynamic(&[serde_json::json!({"Add": true, "Match": "("})]).is_err());
        let inbound = HashMap::from([
            ("x-a".to_owned(), "1".to_owned()),
            ("grpc-timeout".to_owned(), "2".to_owned()),
        ]);
        let out = forwarded_headers(&default_dynamic_rules(), &inbound, &HashMap::new());
        assert_eq!(out.len(), 1);
        assert!(out.contains_key("x-a"));
    }

    #[tokio::test]
    async fn forwards_only_inside_inbound_scope() {
        let rules = default_dynamic_rules();
        assert!(forward_for_current_call(&rules, &[]).is_empty());
        let got = INBOUND_HEADERS
            .scope(HashMap::from([("x-a".to_owned(), "1".to_owned())]), async {
                forward_for_current_call(&rules, &[])
            })
            .await;
        assert_eq!(got, vec![("x-a".to_owned(), "1".to_owned())]);
    }
}
