//! Port of `go/service/history/queue/domain_filter.go`.
use std::collections::HashSet;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DomainFilter {
    pub domain_ids: HashSet<String>,
    pub reverse_match: bool,
}

impl DomainFilter {
    pub fn new(domain_ids: HashSet<String>, reverse_match: bool) -> DomainFilter {
        DomainFilter { domain_ids, reverse_match }
    }

    pub fn filter(&self, domain_id: &str) -> bool {
        let ok = self.domain_ids.contains(domain_id);
        if self.reverse_match {
            !ok
        } else {
            ok
        }
    }

    pub fn include(&self, domain_ids: &HashSet<String>) -> DomainFilter {
        let mut f = self.copy();
        for id in domain_ids {
            if !f.reverse_match {
                f.domain_ids.insert(id.clone());
            } else {
                f.domain_ids.remove(id);
            }
        }
        f
    }

    pub fn exclude(&self, domain_ids: &HashSet<String>) -> DomainFilter {
        let mut f = self.copy();
        for id in domain_ids {
            if !f.reverse_match {
                f.domain_ids.remove(id);
            } else {
                f.domain_ids.insert(id.clone());
            }
        }
        f
    }

    pub fn merge(&self, f2: &DomainFilter) -> DomainFilter {
        if !self.reverse_match && !f2.reverse_match {
            let mut f = self.copy();
            for id in &f2.domain_ids {
                f.domain_ids.insert(id.clone());
            }
            return f;
        }
        if self.reverse_match && f2.reverse_match {
            let ids = self.domain_ids.iter().filter(|id| f2.domain_ids.contains(*id)).cloned().collect();
            return DomainFilter { domain_ids: ids, reverse_match: true };
        }
        let (mut filter, include) = if self.reverse_match {
            (self.copy(), &f2.domain_ids)
        } else {
            (f2.copy(), &self.domain_ids)
        };
        for id in include {
            filter.domain_ids.remove(id);
        }
        filter
    }

    pub fn copy(&self) -> DomainFilter {
        self.clone()
    }
}

/// Go `covertToDomainIDSet`.
pub fn convert_to_domain_id_set(domain_ids: &[String]) -> HashSet<String> {
    domain_ids.iter().cloned().collect()
}
