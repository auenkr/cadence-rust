//! Pure helpers from Go `archiver/util.go`.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde::Serialize;

use crate::archiver::WORKFLOW_START_TO_CLOSE_TIMEOUT;

/// Maximum time one archival iteration may spend pumping requests.
pub fn max_archival_iteration_timeout() -> Duration {
    WORKFLOW_START_TO_CLOSE_TIMEOUT / 2
}

/// Fingerprints a request for the pumped-versus-handled check.
///
/// Go uses `farm.Fingerprint64` over deterministic JSON. The fingerprint only
/// compares requests within a single workflow run and is never persisted, so
/// this port uses FNV-1a 64 over the same deterministic JSON bytes. `BTreeMap`
/// keeps key order stable, which `HashMap` would not.
pub fn hash<T: Serialize>(value: &T) -> u64 {
    let bytes = serde_json::to_vec(value).expect("archival request serializes to JSON");
    fnv1a_64(&bytes)
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes
        .iter()
        .fold(OFFSET_BASIS, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(PRIME))
}

/// Reports whether two hash lists hold the same values with the same counts,
/// ignoring order (Go `hashesEqual`).
pub fn hashes_equal(a: &[u64], b: &[u64]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for elem in a {
        *counts.entry(*elem).or_insert(0) += 1;
    }
    for elem in b {
        let count = counts.entry(*elem).or_insert(0);
        if *count == 0 {
            return false;
        }
        *count -= 1;
    }
    true
}

/// Converts raw search-attribute bytes to strings (Go `convertSearchAttributesToString`).
/// Go's `string(bytes)` keeps invalid UTF-8 as-is; Rust `String` cannot, so
/// invalid sequences become U+FFFD. Search attributes are UTF-8 JSON in practice.
pub fn convert_search_attributes_to_string(
    search_attributes: &BTreeMap<String, Vec<u8>>,
) -> BTreeMap<String, String> {
    search_attributes
        .iter()
        .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).into_owned()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archiver::request::{ArchivalTarget, ArchiveRequest, CloseStatus};

    #[test]
    fn hash_is_deterministic_for_go_test_instances() {
        let instances: Vec<serde_json::Value> = vec![
            serde_json::json!("some random string"),
            serde_json::json!({"key1": "value1", "key2": "value2", "key3": "value3"}),
            serde_json::json!(["value1", "value2", "value3"]),
        ];
        for instance in &instances {
            let expected = hash(instance);
            for _ in 0..100 {
                assert_eq!(expected, hash(instance));
            }
        }

        let request = ArchiveRequest {
            domain_id: "some random domainID".into(),
            shard_id: 0,
            branch_token: Some(vec![1, 2, 3]),
            next_event_id: 123,
            close_status: CloseStatus::ContinuedAsNew,
            memo: Some(crate::archiver::request::Memo {
                fields: Some(BTreeMap::from([
                    ("memoKey1".into(), vec![1, 2, 3]),
                    ("memoKey2".into(), vec![4, 5, 6]),
                ])),
            }),
            search_attributes: Some(BTreeMap::from([
                ("customKey1".into(), vec![1, 2, 3]),
                ("customKey2".into(), vec![4, 5, 6]),
            ])),
            targets: Some(vec![ArchivalTarget::History, ArchivalTarget::Visibility]),
            ..Default::default()
        };
        let expected = hash(&request);
        for _ in 0..100 {
            assert_eq!(expected, hash(&request));
        }
    }

    #[test]
    fn hash_distinguishes_different_requests() {
        let first = ArchiveRequest {
            run_id: "a".into(),
            ..Default::default()
        };
        let second = ArchiveRequest {
            run_id: "b".into(),
            ..Default::default()
        };
        assert_ne!(hash(&first), hash(&second));
    }

    #[test]
    fn hashes_equal_matches_go_multiset_cases() {
        let cases: &[(&[u64], &[u64], bool)] = &[
            (&[], &[], true),
            (&[1, 2, 3], &[1, 2, 3], true),
            (&[1, 2], &[1, 2, 3], false),
            (&[1, 2, 3], &[1, 2], false),
            (&[1, 2, 5, 5, 5], &[1, 2, 5, 5, 5], true),
            (&[1, 2, 5, 5], &[1, 2, 5, 5, 5], false),
            (&[1, 2, 5, 5, 5, 5], &[1, 2, 5, 5, 5], false),
            (&[3, 1, 2], &[1, 2, 3], true),
        ];
        for (a, b, equal) in cases {
            assert_eq!(hashes_equal(a, b), *equal, "a={a:?} b={b:?}");
        }
    }

    #[test]
    fn iteration_timeout_is_half_the_workflow_timeout() {
        assert_eq!(
            max_archival_iteration_timeout(),
            Duration::from_secs(15 * 24 * 60 * 60)
        );
    }

    #[test]
    fn search_attributes_convert_to_strings() {
        let attributes = BTreeMap::from([("CustomKey".to_owned(), b"value".to_vec())]);
        let converted = convert_search_attributes_to_string(&attributes);
        assert_eq!(converted.get("CustomKey").map(String::as_str), Some("value"));
    }
}
