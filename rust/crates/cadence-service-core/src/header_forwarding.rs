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

#[cfg(test)]
mod tests {
    use super::*;

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
}
