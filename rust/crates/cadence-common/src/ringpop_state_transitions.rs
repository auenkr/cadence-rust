//! Deterministic scheduler for Ringpop's delayed membership transitions.
//!
//! The caller supplies monotonic nanoseconds and drains due actions. This keeps
//! scheduling semantics independent of a runtime while retaining the address
//! and incarnation captured by the Go callback.

use std::collections::BTreeMap;

pub const DEFAULT_SUSPECT_NS: i64 = 5_000_000_000;
pub const DEFAULT_FAULTY_NS: i64 = 86_400_000_000_000;
pub const DEFAULT_TOMBSTONE_NS: i64 = 60_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateTimeouts {
    pub suspect_ns: i64,
    pub faulty_ns: i64,
    pub tombstone_ns: i64,
}

impl Default for StateTimeouts {
    fn default() -> Self {
        Self {
            suspect_ns: DEFAULT_SUSPECT_NS,
            faulty_ns: DEFAULT_FAULTY_NS,
            tombstone_ns: DEFAULT_TOMBSTONE_NS,
        }
    }
}

impl StateTimeouts {
    /// Match Go's SelectDuration: a zero option selects the Ringpop default.
    pub fn with_go_defaults(self) -> Self {
        let defaults = Self::default();
        Self {
            suspect_ns: if self.suspect_ns == 0 {
                defaults.suspect_ns
            } else {
                self.suspect_ns
            },
            faulty_ns: if self.faulty_ns == 0 {
                defaults.faulty_ns
            } else {
                self.faulty_ns
            },
            tombstone_ns: if self.tombstone_ns == 0 {
                defaults.tombstone_ns
            } else {
                self.tombstone_ns
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum TransitionState {
    Suspect,
    Faulty,
    Tombstone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionAction {
    MakeFaulty,
    MakeTombstone,
    Evict,
}

impl TransitionState {
    fn timeout(self, timeouts: StateTimeouts) -> i64 {
        match self {
            Self::Suspect => timeouts.suspect_ns,
            Self::Faulty => timeouts.faulty_ns,
            Self::Tombstone => timeouts.tombstone_ns,
        }
    }

    fn action(self) -> TransitionAction {
        match self {
            Self::Suspect => TransitionAction::MakeFaulty,
            Self::Faulty => TransitionAction::MakeTombstone,
            Self::Tombstone => TransitionAction::Evict,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledTransition {
    pub address: String,
    pub incarnation: i64,
    pub state: TransitionState,
    pub deadline_ns: i64,
    pub generation: u64,
    /// Go leaves the fired timer in its address map until cancel/replacement.
    pub fired: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionActionRecord {
    pub address: String,
    pub incarnation: i64,
    pub action: TransitionAction,
    pub generation: u64,
}

#[derive(Clone, Debug)]
pub struct StateTransitionScheduler {
    local_address: String,
    timeouts: StateTimeouts,
    enabled: bool,
    next_generation: u64,
    timers: BTreeMap<String, ScheduledTransition>,
}

impl StateTransitionScheduler {
    pub fn new(local_address: impl Into<String>, timeouts: StateTimeouts) -> Self {
        Self {
            local_address: local_address.into(),
            timeouts: timeouts.with_go_defaults(),
            enabled: true,
            next_generation: 1,
            timers: BTreeMap::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
    pub fn timers(&self) -> &BTreeMap<String, ScheduledTransition> {
        &self.timers
    }

    /// Check a scheduled wakeup token before calling `advance` when the timer
    /// runtime can deliver obsolete wakeups after replacement or cancellation.
    pub fn is_current(&self, address: &str, generation: u64) -> bool {
        self.timers
            .get(address)
            .is_some_and(|timer| timer.generation == generation)
    }

    /// Returns false when Go's scheduler would ignore this operation.
    pub fn schedule(
        &mut self,
        address: impl Into<String>,
        incarnation: i64,
        state: TransitionState,
        now_ns: i64,
    ) -> bool {
        let address = address.into();
        if !self.enabled || address == self.local_address {
            return false;
        }
        if self
            .timers
            .get(&address)
            .is_some_and(|timer| timer.state == state)
        {
            return false;
        }
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let deadline_ns = now_ns.saturating_add(state.timeout(self.timeouts).max(0));
        self.timers.insert(
            address.clone(),
            ScheduledTransition {
                address,
                incarnation,
                state,
                deadline_ns,
                generation,
                fired: false,
            },
        );
        true
    }

    pub fn cancel(&mut self, address: &str) -> bool {
        self.timers.remove(address).is_some()
    }

    pub fn disable(&mut self) {
        self.enabled = false;
        self.timers.clear();
    }

    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Fires due timers once, retaining their map entries just as Go does.
    pub fn advance(&mut self, now_ns: i64) -> Vec<TransitionActionRecord> {
        let mut due: Vec<(i64, u64, String)> = self
            .timers
            .iter()
            .filter(|(_, timer)| !timer.fired && timer.deadline_ns <= now_ns)
            .map(|(address, timer)| (timer.deadline_ns, timer.generation, address.clone()))
            .collect();
        // Go's clock executes timers in deadline order. Generation provides a
        // deterministic tie order where Go permits callbacks to race.
        due.sort();
        let mut actions = Vec::with_capacity(due.len());
        for (_, _, address) in due {
            if let Some(timer) = self.timers.get_mut(&address) {
                timer.fired = true;
                actions.push(TransitionActionRecord {
                    address: timer.address.clone(),
                    incarnation: timer.incarnation,
                    action: timer.state.action(),
                    generation: timer.generation,
                });
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_subject_and_retains_fired_timer_until_canceled_or_replaced() {
        let mut s = StateTransitionScheduler::new("local", StateTimeouts::default());
        assert!(s.schedule("peer", 7, TransitionState::Suspect, 100));
        assert!(!s.schedule("peer", 8, TransitionState::Suspect, 200));
        let actions = s.advance(5_000_000_100);
        assert_eq!(actions[0].incarnation, 7);
        assert_eq!(actions[0].action, TransitionAction::MakeFaulty);
        assert!(s.timers()["peer"].fired);
        assert!(!s.schedule("peer", 9, TransitionState::Suspect, 5_000_000_101));
        assert!(s.schedule("peer", 9, TransitionState::Faulty, 5_000_000_101));
        assert_eq!(s.timers()["peer"].incarnation, 9);
    }

    #[test]
    fn cancellation_disable_enable_local_and_nonpositive_timeouts() {
        let mut s = StateTransitionScheduler::new(
            "local",
            StateTimeouts {
                suspect_ns: -1,
                faulty_ns: 1,
                tombstone_ns: 2,
            },
        );
        assert!(!s.schedule("local", 1, TransitionState::Suspect, 0));
        assert!(s.schedule("peer", 1, TransitionState::Suspect, 10));
        assert_eq!(s.advance(10)[0].address, "peer");
        assert!(s.advance(10).is_empty());
        assert!(s.cancel("peer"));
        s.schedule("other", 2, TransitionState::Faulty, 0);
        s.disable();
        assert!(!s.is_enabled());
        assert!(s.timers().is_empty());
        assert!(!s.schedule("other", 2, TransitionState::Suspect, 0));
        s.enable();
        assert!(s.schedule("other", 3, TransitionState::Suspect, 0));
    }

    #[test]
    fn pinned_go_oracle_trace_matches_core() {
        let Ok(path) = std::env::var("CADENCE_RINGPOP_TRANSITIONS_ORACLE") else {
            return;
        };
        let oracle: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let defaults = StateTimeouts::default();
        let mut s = StateTransitionScheduler::new(
            "local",
            StateTimeouts {
                suspect_ns: 0,
                faulty_ns: 0,
                tombstone_ns: 0,
            },
        );
        let local_ignored = !s.schedule("local", 1, TransitionState::Suspect, 0);
        let first = s.schedule("peer", 7, TransitionState::Suspect, 0);
        let repeated = !s.schedule("peer", 8, TransitionState::Suspect, 1);
        let fired = s.advance(defaults.suspect_ns);
        let fired_retained = s.timers()["peer"].fired;
        let peer_current_incarnation = 8;
        let go_status_after_fire = if fired[0].incarnation < peer_current_incarnation {
            "alive"
        } else {
            "faulty"
        };
        let mut same_inc = StateTransitionScheduler::new("local", StateTimeouts::default());
        same_inc.schedule("same-inc", 20, TransitionState::Suspect, 0);
        let same_inc_action = same_inc.advance(defaults.suspect_ns);
        let same_inc_fire_status = if same_inc_action[0].incarnation == 20 {
            "faulty"
        } else {
            "alive"
        };
        let replacement = s.schedule("peer", 9, TransitionState::Faulty, defaults.suspect_ns);
        let cancel_removed = s.cancel("peer");
        s.schedule("peer", 10, TransitionState::Tombstone, 0);
        s.disable();
        let disabled_cleared = s.timers().is_empty();
        s.enable();
        let reenabled = s.is_enabled();
        let mut canceled_scheduler =
            StateTransitionScheduler::new("local", StateTimeouts::default());
        canceled_scheduler.schedule("canceled", 30, TransitionState::Suspect, 0);
        canceled_scheduler.cancel("canceled");
        let cancel_prevented_fire = canceled_scheduler.advance(defaults.suspect_ns).is_empty();
        let mut disabled = StateTransitionScheduler::new("local", StateTimeouts::default());
        disabled.schedule("disabled", 40, TransitionState::Suspect, 0);
        disabled.disable();
        let disable_prevented_fire = disabled.advance(defaults.suspect_ns).is_empty();
        let mut negative = StateTransitionScheduler::new(
            "local",
            StateTimeouts {
                suspect_ns: -5,
                faulty_ns: 2,
                tombstone_ns: 2,
            },
        );
        negative.schedule("negative", 4, TransitionState::Suspect, 12);
        let negative_fired = if negative.advance(12).is_empty() {
            0
        } else {
            1
        };
        let mut ordered = StateTransitionScheduler::new(
            "local",
            StateTimeouts {
                suspect_ns: 5,
                faulty_ns: 10,
                tombstone_ns: 60,
            },
        );
        ordered.schedule("suspect-first", 1, TransitionState::Suspect, 0);
        ordered.schedule("faulty-second", 2, TransitionState::Faulty, 0);
        let due_in_deadline_order = ordered.advance(10);
        let actual = serde_json::json!({
            "defaults": [defaults.suspect_ns, defaults.faulty_ns, defaults.tombstone_ns],
            "localIgnored": local_ignored,
            "firstScheduled": first,
            "sameStateIgnored": repeated,
            "goStatusAfterFire": go_status_after_fire,
            "sameIncFireStatus": same_inc_fire_status,
            "firedAction": format!("{:?}", fired[0].action),
            "firedRetained": fired_retained,
            "replacementScheduled": replacement,
            "cancelRemoved": cancel_removed,
            "disableCleared": disabled_cleared,
            "reenabled": reenabled,
            "negativeFired": negative_fired,
            "cancelPreventedFire": cancel_prevented_fire,
            "disablePreventedFire": disable_prevented_fire,
            "deadlineOrder": due_in_deadline_order.iter().map(|action| action.address.as_str()).collect::<Vec<_>>()
        });
        assert_eq!(
            actual, oracle,
            "Rust scheduler trace differs from pinned Go output"
        );
    }
}
