//! In-flight matching queue acknowledgements (Go common/messaging/ackManager.go).
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Debug)]
struct State {
    outstanding: BTreeMap<i64, bool>,
    read_level: i64,
    ack_level: i64,
    backlog: i64,
}

#[derive(Debug)]
pub struct AckManager(Mutex<State>);

impl Default for AckManager {
    fn default() -> Self {
        Self(Mutex::new(State {
            outstanding: BTreeMap::new(),
            read_level: -1,
            ack_level: -1,
            backlog: 0,
        }))
    }
}

impl AckManager {
    pub fn read_item(&self, id: i64) -> Result<(), String> {
        let mut state = self.0.lock().expect("ack manager lock");
        // Go increments before validation, including rejected duplicate reads.
        state.backlog = state.backlog.wrapping_add(1);
        if state.read_level >= id {
            return Err(format!("next item ID is less than or equal to current read level. itemID {id}, readLevel {}", state.read_level));
        }
        if state.outstanding.contains_key(&id) {
            return Err(format!(
                "already present in outstanding items but hasn't added itemID:{id}"
            ));
        }
        state.read_level = id;
        if state.ack_level == -1 {
            state.ack_level = id.wrapping_sub(1);
        }
        state.outstanding.insert(id, false);
        Ok(())
    }

    pub fn ack_item(&self, id: i64) -> i64 {
        let mut state = self.0.lock().expect("ack manager lock");
        if let Some(false) = state.outstanding.get(&id) {
            state.outstanding.insert(id, true);
            state.backlog = state.backlog.wrapping_sub(1);
        }
        if state.read_level <= state.ack_level {
            return state.ack_level;
        }
        // Gaps are legal in matching queues. Iterate registered IDs instead of
        // scanning potentially enormous unused ranges, preserving the watermark.
        let completed: Vec<_> = state
            .outstanding
            .range((
                std::ops::Bound::Excluded(state.ack_level),
                std::ops::Bound::Included(state.read_level),
            ))
            .take_while(|(_, done)| **done)
            .map(|(id, _)| *id)
            .collect();
        for id in completed {
            state.ack_level = id;
            state.outstanding.remove(&id);
        }
        state.ack_level
    }

    pub fn set_read_level(&self, level: i64) {
        self.0.lock().expect("ack manager lock").read_level = level;
    }
    pub fn set_ack_level(&self, level: i64) {
        let mut state = self.0.lock().expect("ack manager lock");
        state.ack_level = state.ack_level.max(level);
        state.read_level = state.read_level.max(level);
    }
    /// (read level, ack level, backlog count), observed under one lock.
    pub fn levels(&self) -> (i64, i64, i64) {
        let state = self.0.lock().expect("ack manager lock");
        (state.read_level, state.ack_level, state.backlog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matching_ack_go_fixtures() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../../../../tests/fixtures/matching-ack.json"))
                .unwrap();
        for case in cases.as_array().unwrap() {
            let manager = AckManager::default();
            for (step, result) in case["steps"]
                .as_array()
                .unwrap()
                .iter()
                .zip(case["results"].as_array().unwrap())
            {
                let id = step["id"].as_i64().unwrap();
                let mut error = String::new();
                match step["op"].as_str().unwrap() {
                    "read" => error = manager.read_item(id).err().unwrap_or_default(),
                    "ack" => {
                        manager.ack_item(id);
                    }
                    "setRead" => manager.set_read_level(id),
                    "setAck" => manager.set_ack_level(id),
                    other => panic!("unknown operation {other}"),
                }
                assert_eq!(error, result["error"].as_str().unwrap());
                let (read, ack, backlog) = manager.levels();
                assert_eq!(
                    serde_json::json!([read, ack, backlog]),
                    result["levels"],
                    "{step}"
                );
            }
        }
    }
}
