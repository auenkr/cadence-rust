use cadence_common::ringpop_memberlist::Memberlist;
use cadence_common::ringpop_wire::{Change, Member, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    now_unix_seconds: i64,
    now_nanoseconds: i32,
    initial_local: OracleMember,
    initial_members: Vec<OracleMember>,
    input: Vec<OracleChange>,
    applied: Option<Vec<OracleChange>>,
    members: Vec<OracleMember>,
    checksum: u32,
    local: OracleMember,
}
// Keep Option<Map> as-is here: the oracle distinguishes null from an empty map.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct OracleMember {
    address: String,
    status: String,
    #[serde(rename = "incarnationNumber")]
    incarnation: i64,
    #[serde(default)]
    labels: Option<BTreeMap<String, String>>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct OracleChange {
    #[serde(default)]
    source: Option<String>,
    #[serde(default, rename = "sourceIncarnationNumber")]
    source_incarnation: Option<i64>,
    address: String,
    #[serde(rename = "incarnationNumber")]
    incarnation: i64,
    status: String,
    #[serde(default)]
    tombstone: Option<bool>,
    #[serde(default)]
    labels: Option<BTreeMap<String, String>>,
    timestamp: i64,
}
impl From<&OracleMember> for Member {
    fn from(m: &OracleMember) -> Self {
        Self {
            address: m.address.clone(),
            status: m.status.clone(),
            incarnation: m.incarnation,
            labels: m.labels.clone(),
        }
    }
}
impl From<&OracleChange> for Change {
    fn from(c: &OracleChange) -> Self {
        Self {
            source: c.source.clone().unwrap_or_default(),
            source_incarnation: c.source_incarnation.unwrap_or_default(),
            address: c.address.clone(),
            incarnation: c.incarnation,
            status: c.status.clone(),
            tombstone: c.tombstone.unwrap_or(false),
            labels: c.labels.clone(),
            timestamp: Timestamp(c.timestamp),
        }
    }
}
impl From<&Change> for OracleChange {
    fn from(c: &Change) -> Self {
        Self {
            source: Some(c.source.clone()),
            source_incarnation: Some(c.source_incarnation),
            address: c.address.clone(),
            incarnation: c.incarnation,
            status: c.status.clone(),
            tombstone: if c.tombstone { Some(true) } else { None },
            labels: c.labels.clone(),
            timestamp: c.timestamp.0,
        }
    }
}

#[test]
fn pinned_ringpop_memberlist_update_oracle() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/ringpop_memberlist_update.json")).unwrap();
    let mut mismatches = Vec::new();
    for case in fixture.cases {
        let local = Member::from(&case.initial_local);
        let remotes = case
            .initial_members
            .iter()
            .filter(|m| m.address != case.initial_local.address)
            .map(Member::from)
            .collect();
        let mut list = Memberlist::from_snapshot(local, remotes);
        let inputs: Vec<Change> = case.input.iter().map(Change::from).collect();
        let now_ms = case.now_unix_seconds * 1000 + i64::from(case.now_nanoseconds / 1_000_000);
        let result = list.update(&inputs, || now_ms, |_| 0);
        let actual_applied: Vec<OracleChange> =
            result.accepted.iter().map(OracleChange::from).collect();
        if actual_applied != case.applied.clone().unwrap_or_default() {
            mismatches.push(format!(
                "{} accepted changes: expected {}, got {}",
                case.name,
                case.applied.as_ref().map_or(0, Vec::len),
                actual_applied.len()
            ));
        }
        let mut actual_members: Vec<OracleMember> = list
            .members()
            .iter()
            .map(|m| OracleMember {
                address: m.address.clone(),
                status: m.status.clone(),
                incarnation: m.incarnation,
                labels: m.labels.clone(),
            })
            .collect();
        actual_members.sort_by(|a, b| a.address.cmp(&b.address));
        if actual_members != case.members {
            mismatches.push(format!("{} final members differ", case.name));
        }
        if list
            .member(&case.initial_local.address)
            .map(OracleMember::from_member)
            != Some(case.local.clone())
        {
            mismatches.push(format!("{} local member differs", case.name));
        }
        if list.checksum() != case.checksum {
            mismatches.push(format!(
                "{} checksum: expected {}, got {}",
                case.name,
                case.checksum,
                list.checksum()
            ));
        }
    }
    assert!(
        mismatches.is_empty(),
        "oracle mismatches ({}):\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
impl OracleMember {
    fn from_member(m: &Member) -> Self {
        Self {
            address: m.address.clone(),
            status: m.status.clone(),
            incarnation: m.incarnation,
            labels: m.labels.clone(),
        }
    }
}
