//! Port of queuev2/predicate_operation.go

use std::collections::BTreeSet;

use super::predicate::Predicate;

type Set = BTreeSet<String>;

pub fn not(p: &Predicate) -> Predicate {
    match p {
        Predicate::Universal => Predicate::Empty,
        Predicate::Empty => Predicate::Universal,
        Predicate::DomainId {
            domain_ids,
            is_exclusive,
        } => {
            let is_exclusive = !*is_exclusive;
            if is_exclusive && domain_ids.is_empty() {
                return Predicate::Universal;
            }
            Predicate::DomainId {
                domain_ids: domain_ids.clone(),
                is_exclusive,
            }
        }
    }
}

fn mk(domain_ids: Set, is_exclusive: bool, if_empty: Predicate) -> Predicate {
    if domain_ids.is_empty() {
        return if_empty;
    }
    Predicate::DomainId {
        domain_ids,
        is_exclusive,
    }
}

pub fn and(p1: &Predicate, p2: &Predicate) -> Predicate {
    match p1 {
        Predicate::Universal => p2.clone(),
        Predicate::Empty => p1.clone(),
        Predicate::DomainId {
            domain_ids: d1,
            is_exclusive: e1,
        } => match p2 {
            Predicate::Universal => p1.clone(),
            Predicate::Empty => p2.clone(),
            Predicate::DomainId {
                domain_ids: d2,
                is_exclusive: e2,
            } => {
                if *e1 {
                    if *e2 {
                        return mk(union_string_set(d1, d2), true, Predicate::Empty);
                    }
                    return mk(diff_string_set(d2, d1), false, Predicate::Empty);
                }
                if *e2 {
                    return mk(diff_string_set(d1, d2), false, Predicate::Empty);
                }
                mk(intersect_string_set(d1, d2), false, Predicate::Empty)
            }
        },
    }
}

pub fn or(p1: &Predicate, p2: &Predicate) -> Predicate {
    match p1 {
        Predicate::Universal => p1.clone(),
        Predicate::Empty => p2.clone(),
        Predicate::DomainId {
            domain_ids: d1,
            is_exclusive: e1,
        } => match p2 {
            Predicate::Universal => p2.clone(),
            Predicate::Empty => p1.clone(),
            Predicate::DomainId {
                domain_ids: d2,
                is_exclusive: e2,
            } => {
                if *e1 {
                    if *e2 {
                        return mk(intersect_string_set(d1, d2), true, Predicate::Universal);
                    }
                    return mk(diff_string_set(d1, d2), true, Predicate::Universal);
                }
                if *e2 {
                    return mk(diff_string_set(d2, d1), true, Predicate::Universal);
                }
                mk(union_string_set(d1, d2), false, Predicate::Empty)
            }
        },
    }
}

fn union_string_set(a: &Set, b: &Set) -> Set {
    a.union(b).cloned().collect()
}
fn intersect_string_set(a: &Set, b: &Set) -> Set {
    a.intersection(b).cloned().collect()
}
fn diff_string_set(a: &Set, b: &Set) -> Set {
    a.difference(b).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_and_or_basics() {
        let a = Predicate::new_domain_id(&["a".into()], false);
        let b = Predicate::new_domain_id(&["b".into()], false);
        assert_eq!(not(&Predicate::Universal), Predicate::Empty);
        assert_eq!(and(&a, &b), Predicate::Empty);
        assert_eq!(or(&a, &not(&a)), Predicate::Universal);
        assert_eq!(
            or(&a, &b),
            Predicate::new_domain_id(&["a".into(), "b".into()], false)
        );
    }
}
