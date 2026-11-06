//! Port of go/service/history/lookup/lookup.go.
use cadence_common::membership::Host;

use crate::engine::deps::{HResult, HistoryError};
use crate::wiring::resources::membership::MembershipResolver;

/// Go `membership.ErrInsufficientHosts`.
pub const ERR_INSUFFICIENT_HOSTS: &str = "Not enough hosts to serve the request";

/// Go `string(rune(shardID))`: the ring key is the UTF-8 encoding of the shard id as a
/// code point (invalid code points -- negatives, surrogates, > U+10FFFF -- become U+FFFD).
pub fn shard_key(shard_id: i32) -> String {
    char::from_u32(shard_id as u32).unwrap_or('\u{FFFD}').to_string()
}

/// Go `HistoryServerByShardID`: `resolver.Lookup(service.History, string(rune(shardID)))`.
pub fn history_server_by_shard_id(resolver: &MembershipResolver, shard_id: i32) -> HResult<Host> {
    resolver
        .lookup(&shard_key(shard_id))
        .ok_or_else(|| HistoryError::internal(ERR_INSUFFICIENT_HOSTS))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(a: &str) -> Host {
        Host { address: a.into(), identity: a.into(), grpc_address: String::new() }
    }

    #[test]
    fn key_is_rune_encoding() {
        assert_eq!(shard_key(0), "\0");
        assert_eq!(shard_key(65), "A");
        assert_eq!(shard_key(0x4e16), "世");
        assert_eq!(shard_key(-1), "\u{FFFD}");
        assert_eq!(shard_key(0xD800), "\u{FFFD}");
        assert_eq!(shard_key(0x110000), "\u{FFFD}");
    }

    #[test]
    fn lookup_uses_single_member_and_errors_on_empty_ring() {
        let r = MembershipResolver::new("a:1", vec![host("a:1")]);
        assert_eq!(history_server_by_shard_id(&r, 3).unwrap().address, "a:1");
        r.update_hosts(vec![]);
        assert!(history_server_by_shard_id(&r, 3).is_err());
    }
}
