//! Archive request payloads from Go `archiver/client.go`.
//!
//! The archival signal and continue-as-new payloads are exchanged with Go
//! workers unchanged, so every field keeps Go's exported name (Go declares no
//! JSON tags here) and nil slices/maps/pointers stay `null`. `[]byte` values
//! are base64 strings, as `encoding/json` writes them.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Go `types.WorkflowExecutionCloseStatus`, encoded as its `MarshalText` name.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CloseStatus {
    #[default]
    Completed,
    Failed,
    Canceled,
    Terminated,
    ContinuedAsNew,
    TimedOut,
}

impl CloseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Canceled => "CANCELED",
            Self::Terminated => "TERMINATED",
            Self::ContinuedAsNew => "CONTINUED_AS_NEW",
            Self::TimedOut => "TIMED_OUT",
        }
    }
}

impl Serialize for CloseStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CloseStatus {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Go's UnmarshalText upper-cases its input before matching.
        match String::deserialize(deserializer)?.to_uppercase().as_str() {
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELED" => Ok(Self::Canceled),
            "TERMINATED" => Ok(Self::Terminated),
            "CONTINUED_AS_NEW" => Ok(Self::ContinuedAsNew),
            "TIMED_OUT" => Ok(Self::TimedOut),
            other => Err(serde::de::Error::custom(format!(
                "unknown workflow execution close status {other}"
            ))),
        }
    }
}

/// Go `archiver.ArchivalTarget`, an `int` that encodes as a JSON number.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchivalTarget {
    History,
    Visibility,
}

impl Serialize for ArchivalTarget {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(match self {
            Self::History => 0,
            Self::Visibility => 1,
        })
    }
}

impl<'de> Deserialize<'de> for ArchivalTarget {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match i64::deserialize(deserializer)? {
            0 => Ok(Self::History),
            1 => Ok(Self::Visibility),
            other => Err(serde::de::Error::custom(format!(
                "unknown archival target {other}"
            ))),
        }
    }
}

/// Go `types.Memo`. `Fields` is `omitempty`, so a nil map is omitted.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Memo {
    #[serde(
        rename = "fields",
        default,
        skip_serializing_if = "Option::is_none",
        with = "base64_map"
    )]
    pub fields: Option<BTreeMap<String, Vec<u8>>>,
}

/// Go `archiver.ArchiveRequest`, the signal and continue-as-new carryover item.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveRequest {
    #[serde(rename = "DomainID")]
    pub domain_id: String,
    #[serde(rename = "DomainName")]
    pub domain_name: String,
    #[serde(rename = "WorkflowID")]
    pub workflow_id: String,
    #[serde(rename = "RunID")]
    pub run_id: String,

    // History archival.
    #[serde(rename = "ShardID")]
    pub shard_id: i64,
    #[serde(rename = "BranchToken", with = "base64_opt")]
    pub branch_token: Option<Vec<u8>>,
    #[serde(rename = "NextEventID")]
    pub next_event_id: i64,
    #[serde(rename = "CloseFailoverVersion")]
    pub close_failover_version: i64,
    /// Go keeps the field name `URI` for backward compatibility.
    #[serde(rename = "URI")]
    pub uri: String,

    // Visibility archival.
    #[serde(rename = "WorkflowTypeName")]
    pub workflow_type_name: String,
    #[serde(rename = "StartTimestamp")]
    pub start_timestamp: i64,
    #[serde(rename = "ExecutionTimestamp")]
    pub execution_timestamp: i64,
    #[serde(rename = "CloseTimestamp")]
    pub close_timestamp: i64,
    #[serde(rename = "CloseStatus")]
    pub close_status: CloseStatus,
    #[serde(rename = "HistoryLength")]
    pub history_length: i64,
    #[serde(rename = "Memo")]
    pub memo: Option<Memo>,
    #[serde(rename = "SearchAttributes", with = "base64_map")]
    pub search_attributes: Option<BTreeMap<String, Vec<u8>>>,
    #[serde(rename = "VisibilityURI")]
    pub visibility_uri: String,

    /// Archival targets: history and/or visibility.
    #[serde(rename = "Targets")]
    pub targets: Option<Vec<ArchivalTarget>>,
}

mod base64_opt {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        bytes: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match bytes {
            Some(bytes) => serializer
                .serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|value| {
                base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
    }
}

mod base64_map {
    use std::collections::BTreeMap;

    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        map: &Option<BTreeMap<String, Vec<u8>>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match map {
            Some(map) => {
                let encoded: BTreeMap<&String, String> = map
                    .iter()
                    .map(|(key, value)| {
                        (
                            key,
                            base64::engine::general_purpose::STANDARD.encode(value),
                        )
                    })
                    .collect();
                serializer.collect_map(encoded)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<BTreeMap<String, Vec<u8>>>, D::Error> {
        Option::<BTreeMap<String, String>>::deserialize(deserializer)?
            .map(|map| {
                map.into_iter()
                    .map(|(key, value)| {
                        base64::engine::general_purpose::STANDARD
                            .decode(value)
                            .map(|bytes| (key, bytes))
                            .map_err(serde::de::Error::custom)
                    })
                    .collect()
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_go_field_names_and_text_enums() {
        let request = ArchiveRequest {
            domain_id: "d".into(),
            branch_token: Some(vec![1, 2, 3]),
            close_status: CloseStatus::ContinuedAsNew,
            targets: Some(vec![ArchivalTarget::History, ArchivalTarget::Visibility]),
            ..Default::default()
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.starts_with(r#"{"DomainID":"d","DomainName":"","WorkflowID":"""#));
        assert!(json.contains(r#""BranchToken":"AQID""#));
        assert!(json.contains(r#""CloseStatus":"CONTINUED_AS_NEW""#));
        assert!(json.contains(r#""Memo":null"#));
        assert!(json.contains(r#""SearchAttributes":null"#));
        assert!(json.ends_with(r#""Targets":[0,1]}"#));
    }

    #[test]
    fn decodes_go_null_slices_and_upper_cases_close_status() {
        let json = r#"{"BranchToken":null,"CloseStatus":"timed_out","Targets":null}"#;
        let request: ArchiveRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.branch_token, None);
        assert_eq!(request.close_status, CloseStatus::TimedOut);
        assert_eq!(request.targets, None);
    }

    #[test]
    fn round_trips_memo_and_search_attribute_bytes() {
        let request = ArchiveRequest {
            memo: Some(Memo {
                fields: Some(BTreeMap::from([("memoKey1".into(), vec![1, 2, 3])])),
            }),
            search_attributes: Some(BTreeMap::from([("customKey1".into(), vec![4, 5, 6])])),
            ..Default::default()
        };
        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains(r#""Memo":{"fields":{"memoKey1":"AQID"}}"#));
        assert!(json.contains(r#""SearchAttributes":{"customKey1":"BAUG"}"#));
        let decoded: ArchiveRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn rejects_unknown_close_status_and_target() {
        assert!(serde_json::from_str::<CloseStatus>(r#""PAUSED""#).is_err());
        assert!(serde_json::from_str::<ArchivalTarget>("2").is_err());
    }
}
