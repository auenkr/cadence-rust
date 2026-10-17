//! Ringpop TChannel JSON wire models.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const ALIVE: &str = "alive";
pub const SUSPECT: &str = "suspect";
pub const FAULTY: &str = "faulty";
pub const LEAVE: &str = "leave";
pub const TOMBSTONE: &str = "tombstone";

pub const TIMESTAMP_ZERO: i64 = -62_135_596_800;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

impl Default for Timestamp {
    fn default() -> Self {
        Self(TIMESTAMP_ZERO)
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    T: Default + DeserializeOwned,
    D: serde::Deserializer<'de>,
{
    let value = Option::<T>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}

fn deserialize_labels<'de, D>(deserializer: D) -> Result<Option<BTreeMap<String, String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<BTreeMap<String, String>>::deserialize(deserializer)?;
    Ok(value.filter(|map| !map.is_empty()))
}

fn skip_empty_labels(value: &Option<BTreeMap<String, String>>) -> bool {
    value.as_ref().is_none_or(BTreeMap::is_empty)
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    #[serde(
        rename = "address",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub address: String,
    #[serde(
        rename = "status",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub status: String,
    #[serde(
        rename = "incarnationNumber",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub incarnation: i64,
    #[serde(
        rename = "labels",
        skip_serializing_if = "skip_empty_labels",
        default,
        deserialize_with = "deserialize_labels"
    )]
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    #[serde(
        rename = "source",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source: String,
    #[serde(
        rename = "sourceIncarnationNumber",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source_incarnation: i64,
    #[serde(
        rename = "address",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub address: String,
    #[serde(
        rename = "incarnationNumber",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub incarnation: i64,
    #[serde(
        rename = "status",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub status: String,
    #[serde(
        rename = "tombstone",
        skip_serializing_if = "is_false",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub tombstone: bool,
    #[serde(
        rename = "labels",
        skip_serializing_if = "skip_empty_labels",
        default,
        deserialize_with = "deserialize_labels"
    )]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(rename = "timestamp", default)]
    pub timestamp: Timestamp,
}

impl Change {
    pub fn validate_outgoing(mut self) -> Self {
        if self.status == TOMBSTONE {
            self.status = FAULTY.to_string();
            self.tombstone = true;
        }
        self
    }

    pub fn validate_incoming(mut self) -> Self {
        if self.status == FAULTY && self.tombstone {
            self.status = TOMBSTONE.to_string();
        }
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequest {
    #[serde(rename = "app", default, deserialize_with = "deserialize_null_default")]
    pub app: String,
    #[serde(
        rename = "source",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source: String,
    #[serde(
        rename = "incarnationNumber",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub incarnation: i64,
    #[serde(
        rename = "timeout",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub timeout: i64,
    #[serde(
        rename = "labels",
        skip_serializing_if = "skip_empty_labels",
        default,
        deserialize_with = "deserialize_labels"
    )]
    pub labels: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinResponse {
    #[serde(rename = "app", default, deserialize_with = "deserialize_null_default")]
    pub app: String,
    #[serde(
        rename = "coordinator",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub coordinator: String,
    #[serde(rename = "membership", default)]
    pub membership: Option<Vec<Change>>,
    #[serde(
        rename = "membershipChecksum",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub membership_checksum: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ping {
    #[serde(rename = "changes", default)]
    pub changes: Option<Vec<Change>>,
    #[serde(
        rename = "checksum",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub checksum: u32,
    #[serde(
        rename = "source",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source: String,
    #[serde(
        rename = "sourceIncarnationNumber",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source_incarnation: i64,
    #[serde(rename = "app", default, deserialize_with = "deserialize_null_default")]
    pub app: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingRequest {
    #[serde(
        rename = "source",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source: String,
    #[serde(
        rename = "sourceIncarnationNumber",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub source_incarnation: i64,
    #[serde(
        rename = "target",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub target: String,
    #[serde(
        rename = "checksum",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub checksum: u32,
    #[serde(rename = "changes", default)]
    pub changes: Option<Vec<Change>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingResponse {
    #[serde(
        rename = "pingStatus",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub ping_status: bool,
    #[serde(
        rename = "target",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub target: String,
    #[serde(rename = "changes", default)]
    pub changes: Option<Vec<Change>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationError {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub error_type: String,
    #[serde(
        rename = "message",
        default,
        deserialize_with = "deserialize_null_default"
    )]
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn fixture() -> Value {
        let path = format!(
            "{}/tests/fixtures/ringpop_wire.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).expect("fixture file");
        serde_json::from_str(&raw).expect("fixture json")
    }

    #[test]
    fn timestamp_zero_matches_go() {
        assert_eq!(Timestamp::default().0, TIMESTAMP_ZERO);
    }

    #[test]
    fn change_normalization_matches_go() {
        let outgoing = Change {
            status: TOMBSTONE.to_string(),
            ..Default::default()
        }
        .validate_outgoing();
        assert_eq!(outgoing.status, FAULTY);
        assert!(outgoing.tombstone);

        let incoming = Change {
            status: FAULTY.to_string(),
            tombstone: true,
            ..Default::default()
        }
        .validate_incoming();
        assert_eq!(incoming.status, TOMBSTONE);
    }

    #[test]
    fn nil_vs_empty_slices_preserved() {
        let nil_ping: Ping = Ping {
            changes: None,
            ..Default::default()
        };
        let empty_ping: Ping = Ping {
            changes: Some(vec![]),
            ..Default::default()
        };
        let nil_value = serde_json::to_value(&nil_ping).unwrap();
        let empty_value = serde_json::to_value(&empty_ping).unwrap();
        assert_eq!(nil_value.get("changes").unwrap(), &Value::Null);
        assert_eq!(empty_value.get("changes").unwrap(), &serde_json::json!([]));
    }

    #[test]
    fn go_wire_fixture_member() {
        let go = fixture();
        let rust = Member::default();
        assert_eq!(serde_json::to_value(&rust).unwrap(), go["member_default"]);
        let parsed: Member = serde_json::from_value(go["member_default"].clone()).unwrap();
        assert_eq!(parsed, rust);

        let rust = Member {
            address: "127.0.0.1:7933".into(),
            status: ALIVE.into(),
            incarnation: 1729123456,
            labels: Some({
                let mut m = BTreeMap::new();
                m.insert("__identity".into(), "host-a".into());
                m.insert("serviceName".into(), "matching".into());
                m.insert("tchannel".into(), "7933".into());
                m.insert("grpc".into(), "7833".into());
                m
            }),
        };
        assert_eq!(serde_json::to_value(&rust).unwrap(), go["member_labels"]);
        let parsed: Member = serde_json::from_value(go["member_labels"].clone()).unwrap();
        assert_eq!(parsed, rust);
    }

    #[test]
    fn go_wire_fixture_change() {
        let go = fixture();
        let rust = Change::default();
        assert_eq!(serde_json::to_value(&rust).unwrap(), go["change_default"]);
        let parsed: Change = serde_json::from_value(go["change_default"].clone()).unwrap();
        assert_eq!(parsed, rust);

        let rust = Change {
            source: "127.0.0.1:7934".into(),
            source_incarnation: 1729123400,
            address: "127.0.0.1:7933".into(),
            incarnation: 1729123456,
            status: TOMBSTONE.into(),
            tombstone: false,
            labels: Some({
                let mut m = BTreeMap::new();
                m.insert("serviceName".into(), "matching".into());
                m
            }),
            timestamp: Timestamp(1729123456),
        };
        assert_eq!(serde_json::to_value(&rust).unwrap(), go["change_tombstone"]);
        let parsed: Change = serde_json::from_value(go["change_tombstone"].clone()).unwrap();
        assert_eq!(parsed, rust);

        let rust = Change {
            source: "127.0.0.1:7933".into(),
            source_incarnation: 1729123456,
            address: "127.0.0.1:7933".into(),
            incarnation: 1729123456,
            status: ALIVE.into(),
            labels: Some({
                let mut m = BTreeMap::new();
                m.insert("serviceName".into(), "matching".into());
                m.insert("tchannel".into(), "7933".into());
                m.insert("grpc".into(), "7833".into());
                m
            }),
            timestamp: Timestamp(1729123456),
            ..Default::default()
        };
        assert_eq!(serde_json::to_value(&rust).unwrap(), go["change_labels"]);

        let rust = Change {
            source: "127.0.0.1:7933".into(),
            source_incarnation: 1729123456,
            address: "127.0.0.1:7933".into(),
            incarnation: 1729123456,
            status: FAULTY.into(),
            tombstone: true,
            timestamp: Timestamp(1729123456),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["change_faulty_tombstone_flag"]
        );
    }

    #[test]
    fn go_wire_fixture_join() {
        let go = fixture();
        let rust = JoinRequest::default();
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["join_request_default"]
        );
        let parsed: JoinRequest =
            serde_json::from_value(go["join_request_default"].clone()).unwrap();
        assert_eq!(parsed, rust);

        let rust = JoinRequest {
            app: "cadence".into(),
            source: "127.0.0.1:7933".into(),
            incarnation: 1729123456,
            timeout: 1000000000,
            labels: Some({
                let mut m = BTreeMap::new();
                m.insert("__identity".into(), "host-a".into());
                m.insert("serviceName".into(), "matching".into());
                m.insert("tchannel".into(), "7933".into());
                m.insert("grpc".into(), "7833".into());
                m
            }),
        };
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["join_request_labels"]
        );

        for (key, membership) in [
            ("join_response_nil_membership", None),
            ("join_response_empty_membership", Some(vec![])),
        ] {
            let rust = JoinResponse {
                app: "cadence".into(),
                coordinator: "127.0.0.1:7934".into(),
                membership,
                membership_checksum: 3735928559,
            };
            assert_eq!(serde_json::to_value(&rust).unwrap(), go[key]);
            let parsed: JoinResponse = serde_json::from_value(go[key].clone()).unwrap();
            assert_eq!(parsed, rust);
        }

        let rust = JoinResponse {
            app: "cadence".into(),
            coordinator: "127.0.0.1:7934".into(),
            membership: Some(vec![Change {
                source: "127.0.0.1:7934".into(),
                source_incarnation: 1729123400,
                address: "127.0.0.1:7934".into(),
                incarnation: 1729123400,
                status: ALIVE.into(),
                labels: Some({
                    let mut m = BTreeMap::new();
                    m.insert("__identity".into(), "host-b".into());
                    m.insert("serviceName".into(), "matching".into());
                    m.insert("tchannel".into(), "7934".into());
                    m.insert("grpc".into(), "7834".into());
                    m
                }),
                timestamp: Timestamp(1729123400),
                ..Default::default()
            }]),
            membership_checksum: 3735928559,
        };
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["join_response_populated"]
        );
    }

    #[test]
    fn go_wire_fixture_ping() {
        let go = fixture();
        for (key, changes) in [
            ("ping_nil_changes", None),
            ("ping_empty_changes", Some(vec![])),
        ] {
            let rust = Ping {
                changes,
                checksum: 3735928559,
                source: "127.0.0.1:7933".into(),
                source_incarnation: 1729123456,
                app: "cadence".into(),
            };
            assert_eq!(serde_json::to_value(&rust).unwrap(), go[key]);
            let parsed: Ping = serde_json::from_value(go[key].clone()).unwrap();
            assert_eq!(parsed, rust);
        }

        let rust = Ping {
            changes: Some(vec![Change {
                source: "127.0.0.1:7933".into(),
                source_incarnation: 1729123456,
                address: "127.0.0.1:7933".into(),
                incarnation: 1729123456,
                status: ALIVE.into(),
                labels: Some({
                    let mut m = BTreeMap::new();
                    m.insert("serviceName".into(), "matching".into());
                    m.insert("tchannel".into(), "7933".into());
                    m.insert("grpc".into(), "7833".into());
                    m
                }),
                timestamp: Timestamp(1729123456),
                ..Default::default()
            }]),
            checksum: 3735928559,
            source: "127.0.0.1:7933".into(),
            source_incarnation: 1729123456,
            app: "cadence".into(),
        };
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["ping_populated_changes"]
        );

        for (key, changes) in [
            ("ping_request_nil_changes", None),
            ("ping_request_empty_changes", Some(vec![])),
        ] {
            let rust = PingRequest {
                source: "127.0.0.1:7933".into(),
                source_incarnation: 1729123456,
                target: "127.0.0.1:7935".into(),
                checksum: 3735928559,
                changes,
            };
            assert_eq!(serde_json::to_value(&rust).unwrap(), go[key]);
            let parsed: PingRequest = serde_json::from_value(go[key].clone()).unwrap();
            assert_eq!(parsed, rust);
        }

        let rust = PingResponse::default();
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["ping_response_default"]
        );
        let parsed: PingResponse =
            serde_json::from_value(go["ping_response_default"].clone()).unwrap();
        assert_eq!(parsed, rust);

        let rust = PingResponse {
            ping_status: true,
            target: "127.0.0.1:7935".into(),
            changes: Some(vec![]),
        };
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["ping_response_populated"]
        );
    }

    #[test]
    fn go_wire_fixture_application_error() {
        let go = fixture();
        let rust = ApplicationError {
            error_type: "error".into(),
            message: "node is not ready to handle requests".into(),
        };
        assert_eq!(
            serde_json::to_value(&rust).unwrap(),
            go["application_error"]
        );
        let parsed: ApplicationError =
            serde_json::from_value(go["application_error"].clone()).unwrap();
        assert_eq!(parsed, rust);
    }

    #[test]
    fn empty_labels_serialize_as_omitted() {
        let go = fixture();
        let empty = BTreeMap::new();

        let member = Member {
            labels: Some(empty.clone()),
            ..Default::default()
        };
        assert_eq!(serde_json::to_value(&member).unwrap(), go["member_default"]);
        assert_eq!(
            serde_json::from_value::<Member>(go["member_default"].clone())
                .unwrap()
                .labels,
            None
        );

        let change = Change {
            labels: Some(empty.clone()),
            ..Default::default()
        };
        assert_eq!(serde_json::to_value(&change).unwrap(), go["change_default"]);

        let join = JoinRequest {
            labels: Some(empty),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&join).unwrap(),
            go["join_request_default"]
        );
    }

    #[test]
    fn timestamp_null_and_custom_unmarshal_match_go() {
        // Go's util.Timestamp.UnmarshalJSON requires an integer; null and strings error.
        assert!(serde_json::from_str::<Timestamp>("null").is_err());
        assert!(serde_json::from_str::<Timestamp>("\"1729123456\"").is_err());
        let parsed: Timestamp = serde_json::from_str("1729123456").unwrap();
        assert_eq!(parsed.0, 1729123456);
        // Missing field defaults to the zero Go timestamp.
        #[derive(serde::Deserialize)]
        struct Holder {
            #[serde(default)]
            timestamp: Timestamp,
        }
        let holder: Holder = serde_json::from_str("{}").unwrap();
        assert_eq!(holder.timestamp.0, TIMESTAMP_ZERO);
    }

    #[test]
    fn labels_empty_roundtrip_normalizes_to_none() {
        // Go unmarshals an empty/omitted label map into nil on the wire; Rust
        // normalizes empty maps to None so both sides omit empty labels.
        let input = serde_json::json!({"labels": {}});
        let parsed: Member = serde_json::from_value(input).unwrap();
        assert_eq!(parsed.labels, None);
        assert_eq!(serde_json::to_value(&parsed).unwrap().get("labels"), None);
    }
}
