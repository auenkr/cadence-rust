//! This module holds entities whose goal is to enable routing requests optimally,
//! that is, choosing a target node and a shard such that it is a replica for
//! given token.
//!
//! This includes:
//! - token representation,
//! - shard representation and shard computing logic,
//! - partitioners, which compute token based on a partition key,
//! - replica locator, which finds replicas (node + shard) for a given token.
//!

pub mod locator;
pub mod partitioner;
mod sharding;

pub use sharding::{InvalidShardAwarePortRange, Shard, ShardAwarePortRange, ShardCount, Sharder};
pub(crate) use sharding::{ShardInfo, ShardingError};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]

/// Token is a result of computing a hash of a primary key
///
/// It is basically an i64 with one caveat: i64::MIN is not
/// a valid token. It is used to represent infinity.
/// For this reason tokens are normalized - i64::MIN
/// is replaced with i64::MAX. See this fragment of
/// ScyllaDB code for more information:
/// <https://github.com/scylladb/scylladb/blob/4be70bfc2bc7f133cab492b4aac7bab9c790a48c/dht/token.hh#L32>
///
/// This struct is a wrapper over i64 that performs this normalization
/// when initialized using `new()` method.
pub struct Token {
    value: TokenValue,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum TokenValue { Murmur3(i64), Random(u128), Invalid }

impl Token {
    /// Creates a new token with given value, normalizing the value if necessary.
    #[inline]
    pub fn new(value: i64) -> Self {
        Self {
            value: TokenValue::Murmur3(if value == i64::MIN { i64::MAX } else { value }),
        }
    }

    /// Invalid Token - contains i64::MIN as value.
    ///
    /// This is (currently) only required by CDCPartitioner.
    /// See the following comment:
    /// https://github.com/scylladb/scylla-rust-driver/blob/049dc3546d24e45106fed0fdb985ec2511ab5192/scylla/src/transport/partitioner.rs#L312-L322
    pub(crate) const INVALID: Self = Token { value: TokenValue::Invalid };

    pub(crate) fn random(value: u128) -> Self { Self { value: TokenValue::Random(value) } }

    /// Parse the native RandomPartitioner token domain: the generated MD5
    /// tokens are non-negative signed 128-bit magnitudes; -1 is the ring sentinel.
    pub(crate) fn random_from_decimal(value: &str) -> Option<Self> {
        if value == "-1" { return Some(Self::random(u128::MAX)); }
        let magnitude = value.parse::<u128>().ok()?;
        (magnitude <= (1u128 << 127)).then(|| Self::random(magnitude))
    }

    /// Retrieves the value of the token.
    #[inline]
    pub fn value(&self) -> i64 {
        match self.value { TokenValue::Murmur3(value) => value, _ => panic!("RandomPartitioner token has no i64 value") }
    }

    pub(crate) fn random_decimal(&self) -> Option<String> {
        match self.value { TokenValue::Random(u128::MAX) => Some("-1".into()), TokenValue::Random(v) => Some(v.to_string()), _ => None }
    }
}

impl PartialOrd for Token { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for Token {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering::*;
        match (self.value, other.value) {
            (TokenValue::Murmur3(a), TokenValue::Murmur3(b)) => a.cmp(&b),
            (TokenValue::Random(u128::MAX), TokenValue::Random(_)) => if other.value == TokenValue::Random(u128::MAX) { Equal } else { Less },
            (TokenValue::Random(_), TokenValue::Random(u128::MAX)) => Greater,
            (TokenValue::Random(a), TokenValue::Random(b)) => a.cmp(&b),
            (TokenValue::Invalid, TokenValue::Invalid) => Equal,
            (TokenValue::Invalid, _) => Less, (_, TokenValue::Invalid) => Greater,
            (TokenValue::Murmur3(_), TokenValue::Random(_)) => Less,
            (TokenValue::Random(_), TokenValue::Murmur3(_)) => Greater,
        }
    }
}

#[derive(Debug, Clone)]
/// Describes the preferred location of nodes to contact when executing requests.
///
/// This preference influences the order in which nodes appear in load balancing
/// plans. Nodes matching the preference are considered "local" and are tried
/// first, while non-matching nodes are considered "remote".
#[non_exhaustive]
pub enum NodeLocationPreference {
    /// No location preference — all nodes are treated equally.
    Any,
    /// Prefer nodes located in the given datacenter.
    Datacenter(String),
    /// Prefer nodes located in the given datacenter and rack.
    ///
    /// Nodes in the specified rack of the specified datacenter are tried first,
    /// followed by other nodes in the same datacenter, and finally nodes in
    /// remote datacenters.
    DatacenterAndRack(String, String),
}

impl NodeLocationPreference {
    /// Returns the preferred datacenter, if any.
    pub fn datacenter(&self) -> Option<&str> {
        match self {
            Self::Any => None,
            Self::Datacenter(dc) | Self::DatacenterAndRack(dc, _) => Some(dc),
        }
    }

    /// Returns the preferred rack, if any.
    ///
    /// This is `Some` only for the [`DatacenterAndRack`](Self::DatacenterAndRack) variant.
    pub fn rack(&self) -> Option<&str> {
        match self {
            Self::Any | Self::Datacenter(_) => None,
            Self::DatacenterAndRack(_, rack) => Some(rack),
        }
    }
}
