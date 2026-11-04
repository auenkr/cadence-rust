//! `membership.Resolver` for the History service: a consistent-hash ring over
//! live `cadence-history` members (Go `common/membership` resolver `Lookup`,
//! `WhoAmI`, `Members`). Ring math is the shared Ringpop-compatible
//! `cadence_common::membership::Ring`; the member list is fed by the Ringpop
//! provider (`update_hosts`) so placement tracks joins/failures.
use std::sync::RwLock;

use cadence_common::membership::{Host, Ring};

pub struct MembershipResolver {
    who_am_i: String,
    ring: RwLock<Ring>,
}

impl MembershipResolver {
    pub fn new(who_am_i: impl Into<String>, hosts: Vec<Host>) -> Self {
        Self { who_am_i: who_am_i.into(), ring: RwLock::new(Ring::new(hosts)) }
    }
    /// Replace the member set (called on every membership change event).
    pub fn update_hosts(&self, hosts: Vec<Host>) {
        *self.ring.write().unwrap() = Ring::new(hosts);
    }
    pub fn who_am_i(&self) -> &str {
        &self.who_am_i
    }
    /// `Lookup(service, key)`: owner of `key`, or `None` on an empty ring.
    pub fn lookup(&self, key: &str) -> Option<Host> {
        self.ring.read().unwrap().lookup(key).cloned()
    }
    pub fn members(&self) -> Vec<Host> {
        self.ring.read().unwrap().hosts().to_vec()
    }
    /// True when this host owns `key` (shard ownership check in the controller).
    pub fn owns(&self, key: &str) -> bool {
        self.lookup(key).is_some_and(|h| h.identity() == self.who_am_i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn host(a: &str) -> Host {
        Host { address: a.into(), identity: a.into(), grpc_address: String::new() }
    }
    #[test]
    fn single_member_owns_everything() {
        let r = MembershipResolver::new("a:1", vec![host("a:1")]);
        assert!(r.owns("shard-1"));
        r.update_hosts(vec![host("b:1")]);
        assert!(!r.owns("shard-1"));
    }
}
