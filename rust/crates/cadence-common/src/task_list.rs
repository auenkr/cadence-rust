//! Matching task-list names, ported from Go service/matching/tasklist/identifier.go.
const PREFIX: &str = "/__cadence_sys/";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskListName {
    name: String,
    root: String,
    partition: i64,
}

impl TaskListName {
    pub fn parse(name: &str) -> Result<Self, String> {
        let mut result = Self {
            name: name.into(),
            root: name.into(),
            partition: 0,
        };
        if name.starts_with(PREFIX) {
            let invalid = || format!("invalid partitioned task list name {name}");
            let offset = name.rfind('/').ok_or_else(invalid)?;
            if offset <= PREFIX.len() {
                return Err(invalid());
            }
            let suffix = &name[offset + 1..];
            // strconv.Atoi accepts an optional sign and ASCII decimal digits.
            let digits = suffix.strip_prefix('+').unwrap_or(suffix);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid());
            }
            result.partition = suffix.parse::<i64>().map_err(|_| invalid())?;
            if result.partition <= 0 {
                return Err(invalid());
            }
            result.root = name[PREFIX.len()..offset].into();
        }
        Ok(result)
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
