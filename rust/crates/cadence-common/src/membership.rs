//! Cadence membership placement: Ringpop's 100 FarmHash replica points.
//! Placement uses membership identities; RPC endpoints remain independent.
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Host {
    pub address: String,
    pub identity: String,
    pub grpc_address: String,
}
impl Host {
    pub fn identity(&self) -> &str {
        if self.identity.is_empty() {
            &self.address
        } else {
            &self.identity
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct Ring {
    hosts: Vec<Host>,
    points: Vec<(u32, String, usize, usize)>,
}
impl Ring {
    pub fn new(hosts: impl IntoIterator<Item = Host>) -> Self {
        // Ringpop membership is keyed by address, not by identity. Keep a
        // deterministic ordering, including collisions between legacy points.
        let mut hosts: Vec<_> = hosts.into_iter().collect();
        hosts.sort_by(|a, b| a.address.cmp(&b.address));
        hosts.dedup_by(|a, b| a.address == b.address);
        let mut points = Vec::with_capacity(hosts.len() * 100);
        for (host_index, host) in hosts.iter().enumerate() {
            for replica in 0..100 {
                let separator = if host.identity() == host.address {
                    ""
                } else {
                    "#"
                };
                let key = format!("{}{separator}{replica}", host.identity());
                points.push((
                    farmhash::fingerprint32(key.as_bytes()),
                    host.address.clone(),
                    replica,
                    host_index,
                ));
            }
        }
        points.sort();
        Self { hosts, points }
    }
    pub fn lookup(&self, key: &str) -> Option<&Host> {
        let hash = farmhash::fingerprint32(key.as_bytes());
        let start = self.points.partition_point(|point| point.0 < hash);
        self.points
            .get(start)
            .or_else(|| self.points.first())
            .map(|point| &self.hosts[point.3])
    }
    pub fn hosts(&self) -> &[Host] {
        &self.hosts
    }
    pub fn lookup_n(&self, key: &str, count: usize) -> Vec<&Host> {
        let mut result = Vec::new();
        if count == 0 || self.points.is_empty() {
            return result;
        }
        let hash = farmhash::fingerprint32(key.as_bytes());
        let start = self.points.partition_point(|point| point.0 < hash);
        let mut seen = HashSet::new();
        for point in self.points[start..].iter().chain(&self.points[..start]) {
            if seen.insert(point.3) {
                result.push(&self.hosts[point.3]);
                if result.len() == count {
                    break;
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matching_membership_go_ring_oracle() {
        let Ok(path) = std::env::var("CADENCE_MATCHING_RING_OUTPUT") else {
            return;
        };
        let cases: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let mut checked = 0;
        for case in cases.as_array().unwrap() {
            let hosts = case["hosts"].as_array().unwrap().iter().map(|host| Host {
                address: host["address"].as_str().unwrap().into(),
                identity: host["identity"].as_str().unwrap().into(),
                grpc_address: String::new(),
            });
            let ring = Ring::new(hosts);
            for query in case["queries"].as_array().unwrap() {
                let key = query["key"].as_str().unwrap();
                let expected: Vec<_> = query["owners"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect();
                let actual: Vec<_> = ring
                    .lookup_n(key, query["count"].as_u64().unwrap() as usize)
                    .iter()
                    .map(|h| h.address.as_str())
                    .collect();
                assert_eq!(actual, expected, "key={key:?}");
                if query["count"].as_u64().unwrap() == 1 {
                    assert_eq!(
                        ring.lookup(key).map(|h| h.address.as_str()),
                        expected.first().copied()
                    );
                }
                checked += 1;
            }
        }
        assert!(checked > 1000);
    }
}
