//! History shard ownership and range-ID acquisition.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: u32,
    pub range_id: i64,
    pub owner: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ShardError {
    #[error("shard {shard_id} ownership lost to {owner}")]
    ShardOwnershipLost { shard_id: u32, owner: String },
    #[error("shard store error: {0}")]
    Store(String),
}

/// Minimum persistence contract used by the controller. SQL stores can
/// implement the same compare-and-swap update without changing the controller.
pub trait ShardStore: Send + Sync {
    fn get_shard(&self, shard_id: u32) -> Result<Option<ShardInfo>, ShardError>;
    fn create_shard(&self, shard_id: u32, owner: &str) -> Result<ShardInfo, ShardError>;
    fn update_shard(&self, shard_id: u32, prev_range_id: i64, owner: &str) -> Result<ShardInfo, ShardError>;
}

#[derive(Default)]
pub struct InMemoryShardStore(Mutex<BTreeMap<u32, ShardInfo>>);
impl ShardStore for InMemoryShardStore {
    fn get_shard(&self, id: u32) -> Result<Option<ShardInfo>, ShardError> {
        Ok(self.0.lock().map_err(|e| ShardError::Store(e.to_string()))?.get(&id).cloned())
    }
    fn create_shard(&self, id: u32, owner: &str) -> Result<ShardInfo, ShardError> {
        let mut s = self.0.lock().map_err(|e| ShardError::Store(e.to_string()))?;
        if let Some(existing) = s.get(&id) { return Ok(existing.clone()); }
        let value = ShardInfo { shard_id: id, range_id: 1, owner: owner.to_owned() };
        s.insert(id, value.clone()); Ok(value)
    }
    fn update_shard(&self, id: u32, prev: i64, owner: &str) -> Result<ShardInfo, ShardError> {
        let mut s = self.0.lock().map_err(|e| ShardError::Store(e.to_string()))?;
        let shard = s.get_mut(&id).ok_or_else(|| ShardError::Store(format!("shard {id} does not exist")))?;
        if shard.range_id != prev { return Err(ShardError::ShardOwnershipLost { shard_id: id, owner: shard.owner.clone() }); }
        shard.range_id += 1; shard.owner = owner.to_owned(); Ok(shard.clone())
    }
}

/// Owns the shards assigned to this host by the current live membership ring.
/// Membership uses the shared Cadence Ring implementation (100 FarmHash
/// virtual points), so placement stays consistent with Go/Ringpop.
pub struct ShardController {
    pub num_history_shards: u32,
    host: String,
    store: Arc<dyn ShardStore>,
    open: Mutex<BTreeSet<u32>>,
}

impl ShardController {
    pub fn new(num_history_shards: u32, host: impl Into<String>, store: Arc<dyn ShardStore>) -> Self {
        Self { num_history_shards, host: host.into(), store, open: Mutex::new(BTreeSet::new()) }
    }

    pub fn workflow_id_to_shard(workflow_id: &str, num_history_shards: u32) -> u32 {
        let n = num_history_shards.max(1);
        cadence_common::membership::fingerprint32(workflow_id.as_bytes()) % n
    }

    pub fn desired_owner(shard_id: u32, members: &[cadence_common::membership::Host]) -> Option<String> {
        cadence_common::membership::Ring::new(members.iter().cloned()).lookup(&shard_id.to_string()).map(|h| h.identity().to_owned())
    }

    /// Return the address Go places in `ShardOwnershipLostError.Owner`.
    /// Cadence clients resolve this named TChannel address to the peer's gRPC
    /// address when gRPC is enabled, so returning the membership identity here
    /// breaks redirects for multi-address hosts.
    pub fn desired_owner_address(shard_id: u32, members: &[cadence_common::membership::Host]) -> Option<String> {
        cadence_common::membership::Ring::new(members.iter().cloned())
            .lookup(&shard_id.to_string())
            .map(|host| host.address.clone())
    }

    pub fn acquire(&self, shard_id: u32, members: &[cadence_common::membership::Host]) -> Result<Option<ShardInfo>, ShardError> {
        if shard_id >= self.num_history_shards { return Err(ShardError::Store(format!("shard id {shard_id} exceeds shard count {}", self.num_history_shards))); }
        let Some(owner) = Self::desired_owner(shard_id, members) else { return Ok(None); };
        if owner != self.host { self.close(shard_id); return Ok(None); }
        let acquired = match self.store.get_shard(shard_id)? {
            None => self.store.create_shard(shard_id, &self.host)?,
            Some(existing) if existing.owner == self.host => existing,
            Some(existing) => self.store.update_shard(shard_id, existing.range_id, &self.host)?,
        };
        self.open.lock().map_err(|e| ShardError::Store(e.to_string()))?.insert(shard_id);
        Ok(Some(acquired))
    }

    pub fn check_ownership(&self, shard_id: u32, members: &[cadence_common::membership::Host]) -> Result<(), ShardError> {
        let identity = Self::desired_owner(shard_id, members).unwrap_or_default();
        if identity != self.host || !self.open.lock().map_err(|e| ShardError::Store(e.to_string()))?.contains(&shard_id) {
            let owner = Self::desired_owner_address(shard_id, members).unwrap_or_default();
            return Err(ShardError::ShardOwnershipLost { shard_id, owner });
        }
        Ok(())
    }
    pub fn close(&self, shard_id: u32) { if let Ok(mut open) = self.open.lock() { open.remove(&shard_id); } }
    pub fn owned_shards(&self) -> Vec<u32> { self.open.lock().map(|x| x.iter().copied().collect()).unwrap_or_default() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_common::membership::Host;
    fn hosts() -> Vec<Host> { vec![Host { address:"a".into(), identity:"a".into(), grpc_address:String::new() }, Host { address:"b".into(), identity:"b".into(), grpc_address:String::new() }] }
    #[test]
    fn go_workflow_shard_vectors() {
        assert_eq!(ShardController::workflow_id_to_shard("",1000),242);
        assert_eq!(ShardController::workflow_id_to_shard("workflowId",1000),580);
        assert_eq!(ShardController::workflow_id_to_shard("",4096), 378);
    }
    #[test]
    fn acquires_then_steals_and_closes_when_ownership_moves() {
        let ring = hosts();
        let id = (0..100).find(|id| ShardController::desired_owner(*id,&ring).as_deref()==Some("a")).unwrap();
        let store = Arc::new(InMemoryShardStore::default());
        let a = ShardController::new(100,"a",store.clone());
        let first = a.acquire(id,&ring).unwrap().unwrap();
        assert_eq!(first.range_id,1);
        let next_ring = vec![ring[1].clone()];
        let b = ShardController::new(100,"b",store);
        a.acquire(id,&next_ring).unwrap();
        assert!(a.owned_shards().is_empty());
        let second=b.acquire(id,&next_ring).unwrap().unwrap();
        assert!(second.range_id>first.range_id);
    }

    #[test]
    fn ownership_lost_redirect_contains_go_named_tchannel_address() {
        let members = vec![Host {
            address: "history-b:7934".into(),
            identity: "history-b".into(),
            grpc_address: "history-b:7834".into(),
        }];
        let id = 7;
        let controller = ShardController::new(100, "history-a", Arc::new(InMemoryShardStore::default()));
        let err = controller.check_ownership(id, &members).unwrap_err();
        assert_eq!(err, ShardError::ShardOwnershipLost { shard_id: id, owner: "history-b:7934".into() });
    }
}
