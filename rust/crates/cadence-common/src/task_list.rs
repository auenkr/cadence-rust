//! Matching task-list names, ported from Go service/matching/tasklist/identifier.go.
use crate::go_string::GoString;

const PREFIX: &str = "/__cadence_sys/";

/// Native queue names preserve Go string bytes during partition routing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskListNameBytes {
    name: GoString,
    root: GoString,
    partition: i64,
}

impl TaskListNameBytes {
    pub fn parse(name: &[u8]) -> Result<Self, GoString> {
        let mut result = Self {
            name: name.into(),
            root: name.into(),
            partition: 0,
        };
        if name.starts_with(PREFIX.as_bytes()) {
            let invalid = || {
                let mut error = b"invalid partitioned task list name ".to_vec();
                error.extend_from_slice(name);
                GoString::from(error)
            };
            let offset = name.iter().rposition(|b| *b == b'/').ok_or_else(invalid)?;
            if offset <= PREFIX.len() {
                return Err(invalid());
            }
            let suffix = &name[offset + 1..];
            let digits = suffix.strip_prefix(b"+").unwrap_or(suffix);
            if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
                return Err(invalid());
            }
            // Only the ASCII numeric suffix needs UTF-8 conversion. The root
            // remains the original bytes, as with Go's strings.LastIndex.
            let suffix = std::str::from_utf8(suffix).map_err(|_| invalid())?;
            result.partition = suffix.parse::<i64>().map_err(|_| invalid())?;
            if result.partition <= 0 {
                return Err(invalid());
            }
            result.root = name[PREFIX.len()..offset].into();
        }
        Ok(result)
    }

    pub fn name(&self) -> &GoString {
        &self.name
    }
    pub fn root(&self) -> &GoString {
        &self.root
    }
    pub fn partition(&self) -> i64 {
        self.partition
    }
    pub fn is_root(&self) -> bool {
        self.partition == 0
    }

    pub fn partition_name(&self, partition: i64) -> GoString {
        if partition == 0 {
            return self.root.clone();
        }
        let mut name = PREFIX.as_bytes().to_vec();
        name.extend_from_slice(self.root.as_bytes());
        name.push(b'/');
        name.extend_from_slice(partition.to_string().as_bytes());
        name.into()
    }

    pub fn parent(&self, degree: i64) -> GoString {
        if self.is_root() || degree == 0 {
            return GoString::default();
        }
        let numerator = self.partition.wrapping_add(degree).wrapping_sub(1);
        self.partition_name(numerator.wrapping_div(degree).wrapping_sub(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskListName {
    name: String,
    root: String,
    partition: i64,
}

impl TaskListName {
    pub fn parse(name: &str) -> Result<Self, String> {
        let parsed = TaskListNameBytes::parse(name.as_bytes()).map_err(|error| {
            error
                .into_utf8()
                .expect("UTF-8 input with ASCII error prefix")
        })?;
        Ok(Self {
            name: name.to_owned(),
            root: parsed
                .root
                .into_utf8()
                .expect("UTF-8 input split at ASCII slash"),
            partition: parsed.partition,
        })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn root(&self) -> &str {
        &self.root
    }
    pub fn partition(&self) -> i64 {
        self.partition
    }
    pub fn is_root(&self) -> bool {
        self.partition == 0
    }
    pub fn partition_name(&self, partition: i64) -> String {
        if partition == 0 {
            self.root.clone()
        } else {
            format!("{PREFIX}{}/{partition}", self.root)
        }
    }
    pub fn parent(&self, degree: i64) -> String {
        if self.is_root() || degree == 0 {
            return String::new();
        }
        // Go int arithmetic wraps before division, including overflow.
        let numerator = self.partition.wrapping_add(degree).wrapping_sub(1);
        let parent = numerator.wrapping_div(degree).wrapping_sub(1);
        self.partition_name(parent)
    }
}

/// Go's API mapper treats the deprecated count fields as authoritative when
/// they disagree with the metadata map size, synthesizing empty partitions.
pub fn partition_map<T: Default>(
    count: i32,
    partitions: std::collections::HashMap<i32, T>,
) -> std::collections::HashMap<i32, T> {
    if count == partitions.len() as i32 {
        partitions
    } else {
        (0..count).map(|id| (id, T::default())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_task_list_routing_matches_the_original_go_identifier() {
        let Ok(path) = std::env::var("MATCHING_GO_RAW_STRING_ORACLE") else {
            return;
        };
        let rows: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        fn unhex(value: &serde_json::Value) -> Vec<u8> {
            let hex = value.as_str().unwrap();
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect()
        }
        for row in rows.as_array().unwrap() {
            for key in ["taskList", "taskListPartition"] {
                let case = &row[key];
                let input = unhex(&case["inputHex"]);
                match TaskListNameBytes::parse(&input) {
                    Err(error) => assert_eq!(error.as_bytes(), unhex(&case["errorHex"])),
                    Ok(parsed) => {
                        assert_eq!(case["errorHex"], "");
                        assert_eq!(parsed.root().as_bytes(), unhex(&case["rootHex"]));
                        assert_eq!(parsed.partition(), case["partition"].as_i64().unwrap());
                        for degree in [0, 1, 2, 8, i64::MAX] {
                            assert_eq!(
                                parsed.parent(degree).as_bytes(),
                                unhex(&case["parentsHex"][degree.to_string()])
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn raw_task_list_partition_routing_preserves_name_identity() {
        for root in [
            vec![0xff],
            vec![0xfe],
            vec![0xef, 0xbf, 0xbd],
            vec![b'a', 0, b'b'],
        ] {
            let parsed = TaskListNameBytes::parse(&root).unwrap();
            assert_eq!(parsed.root().as_bytes(), root);
            assert!(parsed.parent(2).is_empty());
            let partition = parsed.partition_name(7);
            let routed = TaskListNameBytes::parse(partition.as_bytes()).unwrap();
            assert_eq!(routed.root(), parsed.root());
            assert_eq!(routed.name(), &partition);
            assert_eq!(routed.partition(), 7);
            assert_eq!(routed.parent(2), parsed.partition_name(3));
            assert_eq!(routed.parent(8).as_bytes(), root);
        }
    }

    #[test]
    fn raw_task_list_partition_errors_keep_original_bytes() {
        for suffix in [
            b"0".as_slice(),
            b"-1",
            b"9223372036854775808",
            &[0xff],
            b"+",
        ] {
            let mut name = PREFIX.as_bytes().to_vec();
            name.extend_from_slice(&[0xfe, b'/', 0xff, b'/']);
            name.extend_from_slice(suffix);
            let mut expected = b"invalid partitioned task list name ".to_vec();
            expected.extend_from_slice(&name);
            assert_eq!(
                TaskListNameBytes::parse(&name).unwrap_err().as_bytes(),
                expected
            );
        }
        let mut name = PREFIX.as_bytes().to_vec();
        name.extend_from_slice(&[0xff, b'/', b'+', b'0', b'1']);
        assert_eq!(TaskListNameBytes::parse(&name).unwrap().partition(), 1);
    }
    #[test]
    fn matching_partition_mapper_go_fixtures() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-partition-wire.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let map: std::collections::HashMap<i32, Vec<String>> =
                serde_json::from_value(case["partitions"].clone()).unwrap();
            assert_eq!(
                serde_json::to_value(partition_map(case["count"].as_i64().unwrap() as i32, map))
                    .unwrap(),
                case["expected"]
            );
        }
    }

    #[test]
    fn matching_identifier_go_fixtures() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-identifiers.json"
        ))
        .unwrap();
        for case in fixtures.as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            match TaskListName::parse(name) {
                Err(error) => assert_eq!(error, case["error"].as_str().unwrap(), "{name}"),
                Ok(parsed) => {
                    assert_eq!(case["error"], "", "{name}");
                    assert_eq!(parsed.root(), case["root"].as_str().unwrap(), "{name}");
                    assert_eq!(
                        parsed.partition(),
                        case["partition"].as_i64().unwrap(),
                        "{name}"
                    );
                    for degree in [0, 1, 2, 3, 20, i64::MAX] {
                        assert_eq!(
                            parsed.parent(degree),
                            case["parents"][degree.to_string()].as_str().unwrap(),
                            "{name}, degree {degree}"
                        );
                    }
                }
            }
        }
    }
}
