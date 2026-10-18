use cadence_common::ringpop_disseminator::Disseminator;
use cadence_common::ringpop_wire::{Change, Member, TOMBSTONE};
use serde_json::{json, Value};

fn change(
    address: &str,
    source: &str,
    source_incarnation: i64,
    incarnation: i64,
    status: &str,
) -> Change {
    Change {
        address: address.into(),
        source: source.into(),
        source_incarnation,
        incarnation,
        status: status.into(),
        ..Default::default()
    }
}

fn snapshot(d: &Disseminator) -> Value {
    json!({
        "changes": d.issue_sender(),
        "pending": d.len(),
        "counters": d.propagation_counts(),
    })
}

fn expected(fixture: &Value, path: &str) -> Value {
    fixture
        .pointer(path)
        .unwrap_or_else(|| panic!("missing oracle fixture {path}"))
        .clone()
}

fn normalize(mut changes: Vec<Change>) -> Vec<Change> {
    changes.sort_by(|a, b| a.address.cmp(&b.address));
    changes
}

#[test]
fn pinned_ringpop_dissemination_oracle() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/ringpop_disseminator.json")).unwrap();

    let mut d = Disseminator::new(3);
    d.record(change("peer", "s", 7, 2, "alive"));
    d.complete_sender(&d.issue_sender(), true);
    d.record(Change {
        source: "s2".into(),
        source_incarnation: 8,
        address: "peer".into(),
        incarnation: 3,
        status: "suspect".into(),
        timestamp: cadence_common::ringpop_wire::Timestamp(12),
        ..Default::default()
    });
    assert_eq!(snapshot(&d), expected(&fixture, "/recordChange"));

    // The Go callback is address based: replacing after snapshot still bumps the current entry.
    let mut d = Disseminator::new(3);
    d.record(change("peer", "old", 0, 1, "alive"));
    let issued = d.issue_sender();
    d.record(Change {
        source: "new".into(),
        address: "peer".into(),
        incarnation: 2,
        status: "suspect".into(),
        timestamp: cadence_common::ringpop_wire::Timestamp(19),
        ..Default::default()
    });
    d.complete_sender(&issued, true);
    assert_eq!(
        json!({"issued": issued, "afterCallback": snapshot(&d)}),
        expected(&fixture, "/senderCallbackReplacement")
    );

    d.record(change("failed", "peer", 0, 3, "alive"));
    let issued = d.issue_sender(); // delivery failure: callback omitted
    assert_eq!(
        json!({"issued": issued, "afterFailedDelivery": snapshot(&d)}),
        expected(&fixture, "/failedCallbackNotInvoked")
    );

    let mut d = Disseminator::new(3);
    d.record(change("repeat", "local", 41, 4, "alive"));
    let mut repeated = Vec::new();
    for _ in 0..4 {
        let issued = d.issue_sender();
        d.complete_sender(&issued, true);
        repeated.push(
            json!({"changes": issued, "pending": d.len(), "counters": d.propagation_counts()}),
        );
    }
    assert_eq!(
        json!(repeated),
        expected(&fixture, "/repeatedSuccessfulSends")
    );

    let local = Member {
        address: "local".into(),
        status: "alive".into(),
        incarnation: 41,
        labels: Some([("zone".into(), "west".into())].into()),
    };
    let members = vec![
        local.clone(),
        Member {
            address: "tomb".into(),
            status: TOMBSTONE.into(),
            incarnation: 6,
            ..Default::default()
        },
        Member {
            address: "other".into(),
            status: "alive".into(),
            incarnation: 8,
            ..Default::default()
        },
    ];
    let mut d = Disseminator::new(3);
    d.record(change("drop", "sender", 9, 1, "alive"));
    d.record(change("keep-incarnation", "sender", 10, 2, "alive"));
    d.record(change("keep-source", "other", 9, 3, "alive"));
    let (changes, full_sync) = d.issue_receiver("sender", 9, 0, 0, &members, "local", 41);
    assert_eq!(
        json!({"changes": normalize(changes), "state": snapshot(&d)}),
        expected(&fixture, "/receiverSourceIncarnationFilter")
    );
    assert!(!full_sync);
    d.clear();
    let (changes, full_sync) = d.issue_receiver("sender", 9, 123, 0, &members, "local", 41);
    assert_eq!(
        json!({"fullSync": full_sync, "changes": normalize(changes), "state": snapshot(&d)}),
        expected(&fixture, "/checksumMismatchFullSync")
    );

    // The Go memberlist count excludes local and non-pingable statuses.
    for (count, max_p) in [
        (0, 0),
        (1, 15),
        (8, 15),
        (9, 15),
        (10, 30),
        (98, 30),
        (99, 30),
        (100, 45),
        (999, 45),
    ] {
        let mut d = Disseminator::default();
        assert_eq!(
            d.adjust_max_propagations(count),
            max_p,
            "pingable member count {count}"
        );
    }
    assert_eq!(
        json!({"0": Disseminator::new(0).adjust_max_propagations(1), "1": Disseminator::new(1).adjust_max_propagations(1)}),
        expected(&fixture, "/maxPByFactorWithOnePingable")
    );

    let mut d = Disseminator::new(3);
    let equal_member = Member {
        address: "local".into(),
        status: "alive".into(),
        incarnation: 41,
        ..Default::default()
    };
    let equal_checksum =
        cadence_common::ringpop_member::memberlist_checksum(std::slice::from_ref(&equal_member));
    let (changes, full_sync) = d.issue_receiver(
        "sender",
        9,
        equal_checksum,
        equal_checksum,
        std::slice::from_ref(&equal_member),
        "local",
        41,
    );
    assert_eq!(
        json!({"changes": changes, "fullSync": full_sync, "state": snapshot(&d)}),
        expected(&fixture, "/checksumEqualNoDeltas")
    );

    let mut d = Disseminator::new(3);
    d.record(change("only", "sender", 9, 4, "alive"));
    let local_member = Member {
        address: "local".into(),
        status: "alive".into(),
        incarnation: 41,
        ..Default::default()
    };
    let (changes, full_sync) = d.issue_receiver(
        "sender",
        9,
        123,
        0,
        std::slice::from_ref(&local_member),
        "local",
        41,
    );
    assert_eq!(
        json!({"changes": changes, "fullSync": full_sync, "state": snapshot(&d)}),
        expected(&fixture, "/fullySenderFiltered")
    );
}
