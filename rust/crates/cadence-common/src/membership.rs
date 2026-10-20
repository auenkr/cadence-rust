//! Cadence membership placement: Ringpop's 100 FarmHash replica points.
//! Placement uses membership identities; RPC endpoints remain independent.
use std::collections::HashSet;

/// Go `go-farm` Fingerprint32. Its 0–4 byte path sign-extends each byte as an
/// int8; the upstream Rust crate uses unsigned bytes on that path.
pub fn fingerprint32(bytes: &[u8]) -> u32 {
    if bytes.len() > 4 {
        return farmhash::fingerprint32(bytes);
    }
    const C1: u32 = 0xcc9e2d51;
    let mut b = 0_u32;
    let mut c = 9_u32;
    for byte in bytes {
        let signed = (*byte as i8 as i32) as u32;
        b = b.wrapping_mul(C1).wrapping_add(signed);
        c ^= b;
    }
    fn mur(mut a: u32, mut h: u32) -> u32 {
        a = a
            .wrapping_mul(0xcc9e2d51)
            .rotate_right(17)
            .wrapping_mul(0x1b873593);
        h ^= a;
        h = h.rotate_right(19);
        h.wrapping_mul(5).wrapping_add(0xe6546b64)
    }
    fn fmix(mut h: u32) -> u32 {
        h ^= h >> 16;
        h = h.wrapping_mul(0x85ebca6b);
        h ^= h >> 13;
        h = h.wrapping_mul(0xc2b2ae35);
        h ^ (h >> 16)
    }
    fmix(mur(b, mur(bytes.len() as u32, c)))
}

/// Go `dgryski/go-farm.Fingerprint64`, used to select the shard-distributor
/// onboarding percentage. The stable FarmHash fingerprint matches Go's
/// `Fingerprint64` implementation (not its platform-dependent `Hash64`).
pub fn fingerprint64(bytes: &[u8]) -> u64 {
    farmhash::fingerprint64(bytes)
}

#[cfg(test)]
mod fingerprint_tests {
    use super::{fingerprint32, Host, Ring};

    #[test]
    fn go_fingerprint32_matches_signed_short_byte_vectors() {
        for (bytes, expected) in [
            (&[][..], 3_696_677_242),
            (&[0x80][..], 801_663_339),
            (&[0xff][..], 495_582_926),
            (&[0xef, 0xbf, 0xbd][..], 4_143_110_158),
            (&[0x80, 0xff, 0xef, 0xbf][..], 740_356_336),
            (&[0xef, 0xbf, 0xbd, 0x01][..], 110_792_607),
            (&[0x80, 0x00, 0xff, 0x7f][..], 847_840_402),
        ] {
            assert_eq!(fingerprint32(bytes), expected, "input={bytes:02x?}");
        }
        assert_eq!(fingerprint32("�".as_bytes()), 4_143_110_158);
    }

    #[test]
    fn replacement_rune_has_go_ring_owner() {
        let ring = Ring::new([
            Host {
                address: "10.8.0.1:7934".into(),
                identity: "history-a".into(),
                grpc_address: String::new(),
            },
            Host {
                address: "10.8.0.2:7934".into(),
                identity: "history-b".into(),
                grpc_address: String::new(),
            },
            Host {
                address: "10.8.0.3:7934".into(),
                identity: "history-c".into(),
                grpc_address: String::new(),
            },
        ]);
        assert_eq!(ring.lookup("�").unwrap().address, "10.8.0.2:7934");
    }
}

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
                    fingerprint32(key.as_bytes()),
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
        let hash = fingerprint32(key.as_bytes());
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
        let hash = fingerprint32(key.as_bytes());
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
