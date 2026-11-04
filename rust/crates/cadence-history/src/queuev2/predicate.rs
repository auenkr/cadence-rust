//! Port of queuev2/predicate.go. Go's interface + three structs is an enum here.

use std::collections::BTreeSet;

use super::deps::PersistenceTask;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Predicate {
    Universal,
    Empty,
    DomainId {
        domain_ids: BTreeSet<String>,
        is_exclusive: bool,
    },
}

impl Predicate {
    pub fn new_universal() -> Self {
        Predicate::Universal
    }
    pub fn new_empty() -> Self {
        Predicate::Empty
    }
    pub fn new_domain_id(domain_ids: &[String], is_exclusive: bool) -> Self {
        Predicate::DomainId {
            domain_ids: domain_ids.iter().cloned().collect(),
            is_exclusive,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Predicate::Universal => false,
            Predicate::Empty => true,
            Predicate::DomainId {
                domain_ids,
                is_exclusive,
            } => domain_ids.is_empty() && !*is_exclusive,
        }
    }
    pub fn check<T: PersistenceTask + ?Sized>(&self, task: &T) -> bool {
        match self {
            Predicate::Universal => true,
            Predicate::Empty => false,
            Predicate::DomainId {
                domain_ids,
                is_exclusive,
            } => {
                if domain_ids.contains(&task.get_domain_id()) {
                    !*is_exclusive
                } else {
                    *is_exclusive
                }
            }
        }
    }
    pub fn equals(&self, other: &Predicate) -> bool {
        self == other
    }
}
