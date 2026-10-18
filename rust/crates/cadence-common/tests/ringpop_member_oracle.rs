use cadence_common::ringpop_member::{
    labels_checksum, member_checksum_string, memberlist_checksum, should_process_gossip,
};
use cadence_common::ringpop_wire::Member;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct Fixture {
    #[serde(rename = "primitiveCases")]
    primitive_cases: Vec<Case>,
    memberlists: Vec<Memberlist>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    old: Option<OracleMember>,
    gossip: OracleMember,
    #[serde(rename = "shouldProcessGossip")]
    process: bool,
    #[serde(rename = "labelChecksum")]
    label_checksum: i32,
    #[serde(rename = "checksumString")]
    checksum_string: String,
}
#[derive(Deserialize)]
struct Memberlist {
    name: String,
    members: Vec<OracleMember>,
    #[serde(rename = "computeChecksum")]
    checksum: u32,
}
#[derive(Deserialize)]
struct OracleMember {
    address: String,
    status: String,
    incarnation: i64,
    labels: Option<BTreeMap<String, String>>,
}
impl From<OracleMember> for Member {
    fn from(value: OracleMember) -> Self {
        Self {
            address: value.address,
            status: value.status,
            incarnation: value.incarnation,
            labels: value.labels.filter(|labels| !labels.is_empty()),
        }
    }
}

#[test]
fn pinned_ringpop_member_oracle() {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/ringpop_member_primitives.json")).unwrap();
    let mut failures = Vec::new();
    let mut order_checksums = Vec::new();
    for case in fixture.primitive_cases {
        let gossip: Member = case.gossip.into();
        let old = case.old.map(Member::from);
        if labels_checksum(gossip.labels.as_ref()) != case.label_checksum {
            failures.push(format!("{} label checksum", case.name));
        }
        if member_checksum_string(&gossip) != case.checksum_string {
            failures.push(format!("{} member string", case.name));
        }
        if should_process_gossip(old.as_ref(), &gossip) != case.process {
            failures.push(format!("{} process", case.name));
        }
    }
    for list in fixture.memberlists {
        let checksum = memberlist_checksum(
            &list
                .members
                .into_iter()
                .map(Member::from)
                .collect::<Vec<_>>(),
        );
        if checksum != list.checksum {
            failures.push(format!("{} aggregate checksum", list.name));
        }
        if list.name.starts_with("label_insertion_order_") {
            order_checksums.push(checksum);
        }
    }
    let ordered_a: BTreeMap<_, _> = [
        ("a".to_string(), "1".to_string()),
        ("b".into(), "2".into()),
        ("c".into(), "3".into()),
    ]
    .into();
    let ordered_b: BTreeMap<_, _> = [
        ("c".to_string(), "3".to_string()),
        ("b".into(), "2".into()),
        ("a".into(), "1".into()),
    ]
    .into();
    assert_eq!(
        labels_checksum(Some(&ordered_a)),
        labels_checksum(Some(&ordered_b))
    );
    assert_eq!(order_checksums.len(), 2);
    assert_eq!(order_checksums[0], order_checksums[1]);
    assert!(
        failures.is_empty(),
        "oracle mismatches: {}",
        failures.join(", ")
    );
}
