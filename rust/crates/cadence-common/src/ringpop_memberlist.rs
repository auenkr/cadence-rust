//! Deterministic core of Ringpop v0.10.0 `swim.memberlist.Update`.
//!
//! Each call processes every input change in order. The caller supplies the
//! local incarnation value and insertion index, so this core invents neither
//! clock nor random-position behavior. Transport, events and dissemination
//! remain outside this slice.
use crate::ringpop_member::{memberlist_checksum, should_process_gossip};
use crate::ringpop_wire::{Change, Member};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberlistUpdate {
    pub accepted: Vec<Change>,
    pub checksum: u32,
    pub refuted_local: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Memberlist {
    local_address: String,
    members: Vec<Member>,
    by_address: BTreeMap<String, usize>,
    checksum: u32,
}

impl Memberlist {
    pub fn new(local: Member) -> Self {
        // Ringpop's constructor inserts the local member but leaves the cached
        // aggregate checksum at zero until its first membership mutation.
        Self::from_snapshot(local, vec![])
    }

    /// Construct a memberlist from an explicit initial snapshot. This is
    /// intended for deterministic oracle consumers and recovery fixtures.
    #[doc(hidden)]
    pub fn from_snapshot(local: Member, initial_remote_members: Vec<Member>) -> Self {
        let local_address = local.address.clone();
        let mut members = vec![local];
        members.extend(
            initial_remote_members
                .into_iter()
                .filter(|m| m.address != local_address),
        );
        let by_address = members
            .iter()
            .enumerate()
            .map(|(i, m)| (m.address.clone(), i))
            .collect();
        // Ringpop's manually constructed oracle snapshots begin with the
        // cached checksum at zero; Update recomputes it after accepted gossip.
        Self {
            local_address,
            members,
            by_address,
            checksum: 0,
        }
    }
    pub fn members(&self) -> &[Member] {
        &self.members
    }
    pub fn member(&self, address: &str) -> Option<&Member> {
        self.by_address.get(address).map(|i| &self.members[*i])
    }
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// Shuffle membership in place using the same descending Fisher-Yates
    /// bounds used by Go's `rand.Shuffle`. `random_below` receives an
    /// exclusive upper bound and must return a value within it.
    pub fn shuffle<R>(&mut self, mut random_below: R)
    where
        R: FnMut(usize) -> usize,
    {
        for index in (1..self.members.len()).rev() {
            let other = random_below(index + 1).min(index);
            self.members.swap(index, other);
        }
        self.reindex();
    }

    /// Remove a remote member, preserving the order of the remaining member
    /// slice as Go's `RemoveMember` does, then recompute the cached checksum.
    /// The local member is retained.
    pub fn remove_member(&mut self, address: &str) -> bool {
        if address == self.local_address {
            return false;
        }
        let Some(index) = self.by_address.get(address).copied() else {
            return false;
        };
        self.members.remove(index);
        self.reindex();
        self.checksum = memberlist_checksum(&self.members);
        true
    }

    fn reindex(&mut self) {
        self.by_address = self
            .members
            .iter()
            .enumerate()
            .map(|(index, member)| (member.address.clone(), index))
            .collect();
    }

    /// `next_incarnation` is called exactly once per accepted local refutation.
    /// `insert_position` is called once for each accepted unknown remote member
    /// with the current member count; its result is clamped to Go's
    /// `rand.Intn(member_count)` range, which excludes the end position.
    pub fn update<F, R>(
        &mut self,
        changes: &[Change],
        mut next_incarnation: F,
        mut insert_position: R,
    ) -> MemberlistUpdate
    where
        F: FnMut() -> i64,
        R: FnMut(usize) -> usize,
    {
        let mut accepted = Vec::new();
        let mut refuted_local = false;
        for incoming in changes {
            let mut change = incoming.clone().validate_incoming();
            let gossip = Member {
                address: change.address.clone(),
                status: change.status.clone(),
                incarnation: change.incarnation,
                labels: change.labels.clone(),
            };
            let old_index = self.by_address.get(&gossip.address).copied();
            let old = old_index.map(|i| &self.members[i]);
            if !should_process_gossip(old, &gossip) {
                continue;
            }
            if gossip.address == self.local_address {
                let local_index = old_index.expect("local member is always present");
                let local = &mut self.members[local_index];
                local.incarnation = next_incarnation();
                change = Change {
                    source: local.address.clone(),
                    source_incarnation: local.incarnation,
                    address: local.address.clone(),
                    incarnation: local.incarnation,
                    status: local.status.clone(),
                    tombstone: false,
                    labels: local.labels.clone(),
                    timestamp: Default::default(),
                };
                refuted_local = true;
            } else if let Some(index) = old_index {
                self.members[index] = gossip;
            } else {
                let index =
                    insert_position(self.members.len()).min(self.members.len().saturating_sub(1));
                self.members.insert(index, gossip.clone());
                for value in self.by_address.values_mut() {
                    if *value >= index {
                        *value += 1;
                    }
                }
                self.by_address.insert(gossip.address.clone(), index);
            }
            accepted.push(change);
        }
        if !accepted.is_empty() {
            self.checksum = memberlist_checksum(&self.members);
        }
        MemberlistUpdate {
            accepted,
            checksum: self.checksum,
            refuted_local,
        }
    }
}

/// Go-compatible memberlist iterator. It shuffles initially and whenever the
/// cursor crosses the end, then scans at most the membership size for a
/// nonlocal ALIVE/SUSPECT member.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberlistIter {
    current_index: isize,
    current_round: usize,
}

impl MemberlistIter {
    pub fn new<R>(memberlist: &mut Memberlist, random_below: R) -> Self
    where
        R: FnMut(usize) -> usize,
    {
        memberlist.shuffle(random_below);
        Self {
            current_index: -1,
            current_round: 0,
        }
    }

    pub fn current_round(&self) -> usize {
        self.current_round
    }

    pub fn next<R>(&mut self, memberlist: &mut Memberlist, mut random_below: R) -> Option<Member>
    where
        R: FnMut(usize) -> usize,
    {
        let max_to_visit = memberlist.members.len();
        let mut visited = std::collections::BTreeSet::new();
        while visited.len() < max_to_visit {
            self.current_index += 1;
            if self.current_index as usize >= memberlist.members.len() {
                self.current_index = 0;
                self.current_round += 1;
                memberlist.shuffle(&mut random_below);
            }
            let member = memberlist.members[self.current_index as usize].clone();
            visited.insert(member.address.clone());
            if member.address != memberlist.local_address
                && (member.status == crate::ringpop_wire::ALIVE
                    || member.status == crate::ringpop_wire::SUSPECT)
            {
                return Some(member);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ringpop_wire::{ALIVE, SUSPECT, TOMBSTONE};
    fn member(address: &str, status: &str, incarnation: i64) -> Member {
        Member {
            address: address.into(),
            status: status.into(),
            incarnation,
            labels: None,
        }
    }
    fn change(address: &str, status: &str, incarnation: i64) -> Change {
        Change {
            address: address.into(),
            status: status.into(),
            incarnation,
            ..Change::default()
        }
    }
    #[test]
    fn remote_acceptance_refutation_tombstone_and_checksum() {
        let mut list = Memberlist::new(member("self", ALIVE, 10));
        let old_sum = list.checksum();
        let result = list.update(
            &[
                change("new", ALIVE, 1),
                change("new", SUSPECT, 1),
                change("unknown", TOMBSTONE, 3),
                change("self", SUSPECT, 11),
            ],
            || 12,
            |_| 0,
        );
        assert_eq!(result.accepted.len(), 3);
        assert_eq!(list.member("new").unwrap().status, SUSPECT);
        assert!(list.member("unknown").is_none());
        assert_eq!(list.member("self").unwrap().incarnation, 12);
        assert!(result.refuted_local);
        assert_ne!(old_sum, result.checksum);
        assert_eq!(result.accepted[2].address, "self");
        assert_eq!(result.accepted[2].status, ALIVE);
    }

    #[test]
    fn update_processes_complete_batch_larger_than_sixty_four() {
        let mut list = Memberlist::new(member("self", ALIVE, 10));
        let changes: Vec<_> = (0..70)
            .map(|i| change(&format!("remote-{i}"), ALIVE, 1))
            .collect();
        let mut insertion_calls = 0;
        let result = list.update(
            &changes,
            || unreachable!("no local refutation expected"),
            |len| {
                insertion_calls += 1;
                len - 1
            },
        );

        assert_eq!(result.accepted.len(), 70);
        assert_eq!(insertion_calls, 70);
        assert_eq!(list.members().len(), 71);
        assert!(list.member("remote-69").is_some());
    }

    #[test]
    fn local_refutation_uses_new_incarnation_and_local_member_fields() {
        let mut local = member("self", ALIVE, 10);
        local.labels = Some([("zone".to_owned(), "west".to_owned())].into());
        let mut list = Memberlist::new(local);
        let mut incoming = change("self", SUSPECT, 11);
        incoming.source = "remote".into();
        incoming.source_incarnation = 55;
        incoming.timestamp = crate::ringpop_wire::Timestamp(1234);

        let mut clock_calls = 0;
        let result = list.update(
            &[incoming],
            || {
                clock_calls += 1;
                12
            },
            |_| unreachable!(),
        );
        assert_eq!(clock_calls, 1);
        let applied = &result.accepted[0];
        assert_eq!(applied.source, "self");
        assert_eq!(applied.source_incarnation, 12);
        assert_eq!(applied.address, "self");
        assert_eq!(applied.incarnation, 12);
        assert_eq!(applied.status, ALIVE);
        assert!(!applied.tombstone);
        assert_eq!(applied.labels, list.member("self").unwrap().labels);
        assert_eq!(applied.timestamp, Default::default());
        assert_eq!(list.member("self").unwrap().incarnation, 12);
    }

    #[test]
    fn insertion_position_clamps_to_go_random_range_excluding_end() {
        let mut list = Memberlist::new(member("self", ALIVE, 10));
        list.update(&[change("first", ALIVE, 1)], || unreachable!(), |_| 0);
        list.update(
            &[change("second", ALIVE, 1)],
            || unreachable!(),
            |_| usize::MAX,
        );

        assert_eq!(list.members()[0].address, "first");
        assert_eq!(list.members()[1].address, "second");
        assert_eq!(list.members()[2].address, "self");
    }

    #[test]
    fn memberlist_iterator_shuffles_and_visits_pingable_members_once_per_round() {
        let mut list = Memberlist::from_snapshot(
            member("self", ALIVE, 1),
            vec![
                member("alive", ALIVE, 1),
                member("suspect", SUSPECT, 1),
                member("faulty", "faulty", 1),
            ],
        );
        let mut iterator = MemberlistIter::new(&mut list, |_| 0);
        let first_round = [
            iterator.next(&mut list, |_| 0).unwrap().address,
            iterator.next(&mut list, |_| 0).unwrap().address,
        ];
        let mut sorted = first_round.clone();
        sorted.sort();
        assert_eq!(sorted, ["alive".to_owned(), "suspect".to_owned()]);
        assert!(matches!(
            iterator.next(&mut list, |_| 0).unwrap().address.as_str(),
            "alive" | "suspect"
        ));
        assert_eq!(iterator.current_round(), 1);
    }

    #[test]
    fn memberlist_iterator_returns_none_when_only_local_and_non_pingable_members_exist() {
        let mut list = Memberlist::from_snapshot(
            member("self", ALIVE, 1),
            vec![member("faulty", "faulty", 1)],
        );
        let mut iterator = MemberlistIter::new(&mut list, |_| 0);
        assert!(iterator.next(&mut list, |_| 0).is_none());
    }

    #[test]
    fn single_remote_iterator_round_matches_pinned_go_trace() {
        let mut list = Memberlist::from_snapshot(
            member("local:7933", ALIVE, 1),
            vec![member("127.0.0.1:9910", ALIVE, 1)],
        );
        let mut iterator = MemberlistIter::new(&mut list, |_| 0);
        let got = vec![
            iterator.next(&mut list, |_| 0).unwrap().address,
            iterator.next(&mut list, |_| 0).unwrap().address,
        ];
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_swim_runtime_oracle.json"
        ))
        .unwrap();
        assert_eq!(serde_json::to_value(got).unwrap(), oracle["iterator"]);
        assert_eq!(iterator.current_round(), 1);
    }

    #[test]
    fn removing_remote_member_reindexes_and_keeps_local() {
        let mut list = Memberlist::from_snapshot(
            member("self", ALIVE, 1),
            vec![member("first", ALIVE, 1), member("second", ALIVE, 1)],
        );
        assert!(list.remove_member("first"));
        assert!(list.member("first").is_none());
        assert!(list.member("self").is_some());
        assert!(list.member("second").is_some());
        assert!(!list.remove_member("self"));
        assert!(!list.remove_member("missing"));
    }
}
