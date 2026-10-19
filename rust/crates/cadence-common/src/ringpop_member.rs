//! Deterministic Ringpop v0.10.0 member checksum and merge primitives.
//! These functions are deliberately pure; they do not implement SWIM lifecycle.
use crate::{
    membership::fingerprint32,
    ringpop_wire::{Member, ALIVE, FAULTY, LEAVE, SUSPECT, TOMBSTONE},
};
use std::collections::BTreeMap;

/// FarmHash32 checksum for a Ringpop label map (XOR of each encoded pair).
pub fn labels_checksum(labels: Option<&BTreeMap<String, String>>) -> i32 {
    let mut checksum = 0_u32;
    if let Some(labels) = labels {
        for (key, value) in labels {
            let mut bytes = Vec::with_capacity(key.len() + value.len() + 8);
            bytes.extend_from_slice(&(key.len() as i32).to_be_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
            checksum ^= fingerprint32(&bytes);
        }
    }
    checksum as i32
}

/// Append a member's Go-compatible checksum representation.
pub fn member_checksum_string(member: &Member) -> String {
    let mut result = format!("{}{}{}", member.address, member.status, member.incarnation);
    let labels = labels_checksum(member.labels.as_ref());
    if labels != 0 {
        result.push_str("#labels");
        result.push_str(&labels.to_string());
    }
    result
}

/// Compute Ringpop's aggregate memberlist checksum, excluding tombstones.
pub fn memberlist_checksum(members: &[Member]) -> u32 {
    let mut entries: Vec<_> = members
        .iter()
        .filter(|member| member.status != TOMBSTONE)
        .map(member_checksum_string)
        .collect();
    entries.sort();
    let mut input = String::new();
    for entry in entries {
        input.push_str(&entry);
        input.push(';');
    }
    fingerprint32(input.as_bytes())
}

pub fn state_precedence(status: &str) -> i32 {
    match status {
        ALIVE => 0,
        SUSPECT => 1,
        FAULTY => 2,
        LEAVE => 3,
        TOMBSTONE => 4,
        _ => -1,
    }
}

/// Match swim.shouldProcessGossip from the pinned Ringpop implementation.
pub fn should_process_gossip(old: Option<&Member>, gossip: &Member) -> bool {
    if gossip.status == TOMBSTONE && old.is_none() {
        return false;
    }
    let Some(old) = old else { return true };
    match gossip.incarnation.cmp(&old.incarnation) {
        std::cmp::Ordering::Greater => return true,
        std::cmp::Ordering::Less => return false,
        std::cmp::Ordering::Equal => {}
    }
    match state_precedence(&gossip.status).cmp(&state_precedence(&old.status)) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            labels_checksum(gossip.labels.as_ref()) > labels_checksum(old.labels.as_ref())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(status: &str, incarnation: i64) -> Member {
        Member {
            address: "node".into(),
            status: status.into(),
            incarnation,
            labels: None,
        }
    }

    #[test]
    fn merge_precedence_and_incarnation_rules() {
        let statuses = [
            "alive",
            "suspect",
            "faulty",
            "leave",
            "tombstone",
            "unknown",
        ];
        for old_status in statuses {
            for gossip_status in statuses {
                let old = member(old_status, 4);
                let gossip = member(gossip_status, 4);
                assert_eq!(
                    should_process_gossip(Some(&old), &gossip),
                    state_precedence(gossip_status) > state_precedence(old_status),
                    "{old_status} -> {gossip_status}"
                );
            }
        }
        let old = member("alive", 3);
        assert!(should_process_gossip(Some(&old), &member("alive", 4)));
        assert!(!should_process_gossip(Some(&old), &member("tombstone", 2)));
        assert!(!should_process_gossip(None, &member("tombstone", 4)));
        assert!(should_process_gossip(None, &member("mystery", 4)));
    }

    #[test]
    fn lower_precedence_rejects_gossip_even_with_higher_signed_label_checksum() {
        let mut old = member("suspect", 4);
        let mut gossip = member("alive", 4);
        let old_labels = BTreeMap::from([("k".to_string(), "old".to_string())]);
        old.labels = Some(old_labels.clone());
        let mut higher = None;
        for i in 0..100_000 {
            let labels = BTreeMap::from([("k".to_string(), format!("candidate-{i}"))]);
            if labels_checksum(Some(&labels)) > labels_checksum(Some(&old_labels)) {
                higher = Some(labels);
                break;
            }
        }
        gossip.labels = higher;
        assert!(labels_checksum(gossip.labels.as_ref()) > labels_checksum(Some(&old_labels)));
        assert!(!should_process_gossip(Some(&old), &gossip));
    }

    #[test]
    fn labels_are_utf8_byte_encoded_order_independent_and_signed() {
        let a = BTreeMap::from([("é".to_string(), "雪".to_string()), ("".into(), "v".into())]);
        let b = BTreeMap::from([("".into(), "v".into()), ("é".into(), "雪".into())]);
        assert_ne!(labels_checksum(None), labels_checksum(Some(&a)));
        assert_eq!(labels_checksum(Some(&a)), labels_checksum(Some(&b)));
        let mut found_negative = false;
        for i in 0..1000 {
            let labels = BTreeMap::from([("k".to_string(), format!("value-{i}"))]);
            if labels_checksum(Some(&labels)) < 0 {
                found_negative = true;
                break;
            }
        }
        assert!(found_negative);
        assert_eq!(
            labels_checksum(None),
            labels_checksum(Some(&BTreeMap::new()))
        );
    }

    #[test]
    fn aggregate_sorts_and_excludes_tombstones() {
        let a = member("alive", 1);
        let b = member("suspect", 2);
        let tombstone = member("tombstone", 99);
        assert_eq!(
            memberlist_checksum(&[a.clone(), b.clone()]),
            memberlist_checksum(&[b, a.clone(), tombstone])
        );
        assert_eq!(memberlist_checksum(&[]), fingerprint32(b""));
        assert_eq!(member_checksum_string(&a), "nodealive1");
    }
}
