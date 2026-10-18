use cadence_common::ringpop_disseminator::Disseminator;
use cadence_common::ringpop_wire::{Change, Member, TOMBSTONE};

fn change(address: &str, source: &str, status: &str) -> Change {
    Change {
        address: address.into(),
        source: source.into(),
        status: status.into(),
        ..Default::default()
    }
}

#[test]
fn replacement_sender_success_failure_and_bound() {
    let mut d = Disseminator::new(2);
    d.adjust_max_propagations(1); // ceil(log10(2)) * 2
    d.record(change("a", "x", "alive"));
    d.record(change("a", "y", "suspect"));
    let sent = d.issue_sender();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].source, "y");
    d.complete_sender(&sent, false);
    assert!(d.has_changes());
    d.record(change("a", "z", "faulty"));
    d.complete_sender(&sent, true);
    assert!(d.has_changes());
    let current = d.issue_sender();
    d.complete_sender(&current, true);
    assert!(!d.has_changes());
}

#[test]
fn receiver_suppresses_sender_and_selects_full_sync_only_without_deltas() {
    let mut d = Disseminator::new(15);
    d.record(change("a", "peer", "alive"));
    let member = Member {
        address: "a".into(),
        status: TOMBSTONE.into(),
        incarnation: 3,
        ..Default::default()
    };
    let (changes, full) = d.issue_receiver("peer", 0, 10, 11, &[member], "local", 7);
    assert!(full);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].status, "faulty");
    assert_eq!(changes[0].source, "local");
    assert_eq!(changes[0].source_incarnation, 7);
    assert!(changes[0].tombstone);
    assert!(d.has_changes()); // filtered delta was not charged
    let (changes, full) = d.issue_receiver("other", 0, 0, 0, &[], "local", 7);
    assert!(!full && changes.len() == 1);
    d.record(change("b", "origin", "alive"));
    let (changes, full) = d.issue_receiver("peer", 0, 2, 1, &[], "local", 7);
    assert!(!full && changes.len() == 1); // checksum mismatch is not enough while deltas are pending
}
