//! Deterministic, transport-independent Ringpop v0.10.0 dissemination core.
//!
//! It owns the latest queued change per address and the Go piggyback bounds.
//! Probe scheduling, membership merge, and network delivery remain caller work.
use crate::ringpop_wire::{Change, Member};
use std::collections::BTreeMap;

pub const DEFAULT_PROPAGATION_FACTOR: usize = 15;

#[derive(Clone, Debug)]
struct Pending {
    change: Change,
    propagated: usize,
}

#[derive(Clone, Debug)]
pub struct Disseminator {
    changes: BTreeMap<String, Pending>,
    propagation_factor: usize,
    max_propagations: usize,
}

impl Default for Disseminator {
    fn default() -> Self {
        Self::new(DEFAULT_PROPAGATION_FACTOR)
    }
}

impl Disseminator {
    pub fn new(propagation_factor: usize) -> Self {
        Self {
            changes: BTreeMap::new(),
            propagation_factor,
            max_propagations: propagation_factor,
        }
    }

    /// Go: factor * ceil(log10(pingable_members + 1)).
    pub fn adjust_max_propagations(&mut self, pingable_members: usize) -> usize {
        let n = pingable_members.saturating_add(1);
        self.max_propagations = if n <= 1 {
            0
        } else {
            self.propagation_factor
                .saturating_mul((n as f64).log10().ceil() as usize)
        };
        self.max_propagations
    }
    pub fn max_propagations(&self) -> usize {
        self.max_propagations
    }
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }
    pub fn len(&self) -> usize {
        self.changes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }
    /// Read-only per-address propagation counters for parity diagnostics.
    pub fn propagation_counts(&self) -> BTreeMap<String, usize> {
        self.changes
            .iter()
            .map(|(address, pending)| (address.clone(), pending.propagated))
            .collect()
    }
    pub fn clear(&mut self) {
        self.changes.clear();
    }
    pub fn clear_address(&mut self, address: &str) {
        self.changes.remove(address);
    }
    pub fn record(&mut self, change: Change) {
        let address = change.address.clone();
        self.changes.insert(
            address,
            Pending {
                change,
                propagated: 0,
            },
        );
    }
    /// Snapshot for a sender. Counters are bumped only after successful delivery.
    pub fn issue_sender(&self) -> Vec<Change> {
        self.changes
            .values()
            .map(|p| p.change.clone().validate_outgoing())
            .collect()
    }
    pub fn complete_sender(&mut self, sent: &[Change], success: bool) {
        if !success {
            return;
        }
        for change in sent {
            if let Some(pending) = self.changes.get_mut(&change.address) {
                // Ringpop indexes its counter by address and bumps the current
                // entry, even when it was replaced after the sender snapshot.
                pending.propagated = pending.propagated.saturating_add(1);
                if pending.propagated >= self.max_propagations {
                    self.changes.remove(&change.address);
                }
            }
        }
    }
    /// Receiver responses count immediately, after suppressing changes last
    /// originated by this sender at the matching incarnation.
    #[allow(clippy::too_many_arguments)] // Mirrors Ringpop's receiver inputs plus local full-sync state.
    pub fn issue_receiver(
        &mut self,
        sender: &str,
        sender_incarnation: i64,
        sender_checksum: u32,
        local_checksum: u32,
        members: &[Member],
        local_address: &str,
        local_incarnation: i64,
    ) -> (Vec<Change>, bool) {
        let mut changes: Vec<_> = self
            .changes
            .values()
            .filter(|p| {
                !(p.change.source == sender && p.change.source_incarnation == sender_incarnation)
            })
            .map(|p| p.change.clone().validate_outgoing())
            .collect();
        let full_sync = changes.is_empty() && local_checksum != sender_checksum;
        if full_sync {
            changes = members
                .iter()
                .map(|m| {
                    Change {
                        source: local_address.to_owned(),
                        source_incarnation: local_incarnation,
                        address: m.address.clone(),
                        incarnation: m.incarnation,
                        status: m.status.clone(),
                        tombstone: false,
                        labels: m.labels.clone(),
                        timestamp: Default::default(),
                    }
                    .validate_outgoing()
                })
                .collect();
        } else {
            self.complete_sender(&changes, true);
        }
        (changes, full_sync)
    }
}
