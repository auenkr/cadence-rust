//! History branch/version algorithms from Go common/persistence/versionHistory.go.
//! Endpoint items encode inclusive event ranges sharing a failover version.
use cadence_api::{
    errors::BadRequestError,
    types::{VersionHistories, VersionHistory, VersionHistoryItem},
};

type Result<T> = std::result::Result<T, BadRequestError>;

pub const EMPTY_VERSION: i64 = -24;

pub fn new_item(event_id: i64, version: i64) -> VersionHistoryItem {
    assert!(
        event_id >= 0 && (version >= 0 || version == EMPTY_VERSION),
        "invalid version history item event ID: {event_id}, version: {version}"
    );
    VersionHistoryItem { event_id, version }
}

pub fn new_history(branch_token: Vec<u8>, items: &[VersionHistoryItem]) -> VersionHistory {
    let mut history = VersionHistory {
        branch_token,
        items: Vec::new(),
    };
    for item in items {
        let item = new_item(item.event_id, item.version);
        if let Err(error) = history.add_or_update(&item) {
            panic!("unable to initialize version history: {}", error.message);
        }
    }
    history
}

pub trait VersionHistoryExt {
    fn add_or_update(&mut self, item: &VersionHistoryItem) -> Result<()>;
    fn contains_item(&self, item: &VersionHistoryItem) -> bool;
    fn find_lca(&self, remote: &VersionHistory) -> Result<VersionHistoryItem>;
    fn duplicate_until_lca(&self, item: &VersionHistoryItem) -> Result<VersionHistory>;
    fn is_lca_appendable(&self, item: &VersionHistoryItem) -> bool;
    fn first_item(&self) -> Result<VersionHistoryItem>;
    fn last_item(&self) -> Result<VersionHistoryItem>;
    fn event_version(&self, event_id: i64) -> Result<i64>;
}

impl VersionHistoryExt for VersionHistory {
    fn add_or_update(&mut self, item: &VersionHistoryItem) -> Result<()> {
        if let Some(last) = self.items.last_mut() {
            if item.version < last.version {
                return Err(BadRequestError::new(format!(
                    "cannot update version history with a lower version {}. Last version: {}",
                    item.version, last.version
                )));
            }
            if item.event_id <= last.event_id {
                return Err(BadRequestError::new(format!(
                    "cannot add version history with a lower event id {}. Last event id: {}",
                    item.event_id, last.event_id
                )));
            }
            if item.version == last.version {
                last.event_id = item.event_id;
                return Ok(());
            }
        }
        self.items.push(new_item(item.event_id, item.version));
        Ok(())
    }

    fn contains_item(&self, item: &VersionHistoryItem) -> bool {
        let mut previous = 0;
        for current in &self.items {
            if item.version == current.version {
                return previous < item.event_id && item.event_id <= current.event_id;
            }
            if item.version < current.version {
                return false;
            }
            previous = current.event_id;
        }
        false
    }

    fn find_lca(&self, remote: &VersionHistory) -> Result<VersionHistoryItem> {
        let (mut local, mut other) = (self.items.len(), remote.items.len());
        while local > 0 && other > 0 {
            let a = &self.items[local - 1];
            let b = &remote.items[other - 1];
            match a.version.cmp(&b.version) {
                std::cmp::Ordering::Equal => {
                    return Ok(if a.event_id > b.event_id { b } else { a }.clone())
                }
                std::cmp::Ordering::Greater => local -= 1,
                std::cmp::Ordering::Less => other -= 1,
            }
        }
        Err(BadRequestError::new(
            "version history is malformed. No joint point found.",
        ))
    }

    fn duplicate_until_lca(&self, lca: &VersionHistoryItem) -> Result<VersionHistory> {
        let mut history = VersionHistory::default();
        for item in &self.items {
            if item.version < lca.version {
                history.add_or_update(item)?;
            } else if item.version == lca.version && lca.event_id <= item.event_id {
                history.add_or_update(lca)?;
                return Ok(history);
            } else {
                break;
            }
        }
        Err(BadRequestError::new(
            "version history does not contains the LCA item.",
        ))
    }

    fn is_lca_appendable(&self, item: &VersionHistoryItem) -> bool {
        assert!(!self.items.is_empty(), "version history not initialized");
        self.items.last() == Some(item)
    }

    fn first_item(&self) -> Result<VersionHistoryItem> {
        self.items
            .first()
            .cloned()
            .ok_or_else(|| BadRequestError::new("version history is empty."))
    }

    fn last_item(&self) -> Result<VersionHistoryItem> {
        self.items
            .last()
            .cloned()
            .ok_or_else(|| BadRequestError::new("version history is empty"))
    }

    fn event_version(&self, event_id: i64) -> Result<i64> {
        let last = self.last_item()?;
        if event_id >= 1 && event_id <= last.event_id {
            if let Some(item) = self.items.iter().find(|item| event_id <= item.event_id) {
                return Ok(item.version);
            }
        }
        Err(BadRequestError::new("input event ID is not in range."))
    }
}

pub trait VersionHistoriesExt {
    fn history(&self, index: i32) -> Result<&VersionHistory>;
    fn current_history(&self) -> Result<&VersionHistory>;
    fn add_history(&mut self, history: Option<&VersionHistory>) -> Result<(bool, usize)>;
    fn find_lca_history(
        &self,
        incoming: &VersionHistory,
    ) -> Result<(usize, Option<VersionHistoryItem>)>;
    fn find_first_by_item(&self, item: &VersionHistoryItem) -> Result<(usize, &VersionHistory)>;
    fn is_rebuilt(&self) -> Result<bool>;
    fn set_current_index(&mut self, index: i32) -> Result<()>;
}

impl VersionHistoriesExt for VersionHistories {
    fn history(&self, index: i32) -> Result<&VersionHistory> {
        usize::try_from(index)
            .ok()
            .and_then(|i| self.histories.get(i))
            .ok_or_else(|| {
                BadRequestError::new(format!(
                    "getting branch index: {index}, available branch count: {}",
                    self.histories.len()
                ))
            })
    }

    fn current_history(&self) -> Result<&VersionHistory> {
        self.history(self.current_version_history_index)
    }

    fn add_history(&mut self, incoming: Option<&VersionHistory>) -> Result<(bool, usize)> {
        let incoming =
            incoming.ok_or_else(|| BadRequestError::new("version histories is null."))?;
        let first = incoming.first_item()?;
        let current = self.current_history()?;
        if first.version != current.first_item()?.version {
            return Err(BadRequestError::new(
                "version history first item does not match.",
            ));
        }
        let current_version = current.last_item()?.version;
        let history = new_history(incoming.branch_token.clone(), &incoming.items);
        let changed = history.last_item()?.version > current_version;
        self.histories.push(history);
        let index = self.histories.len() - 1;
        if changed {
            self.current_version_history_index = index as i32;
        }
        Ok((changed, index))
    }

    fn find_lca_history(
        &self,
        incoming: &VersionHistory,
    ) -> Result<(usize, Option<VersionHistoryItem>)> {
        let mut result: Option<(usize, usize, VersionHistoryItem)> = None;
        for (index, history) in self.histories.iter().enumerate() {
            let item = history.find_lca(incoming)?;
            if result.as_ref().is_none_or(|(_, length, previous)| {
                item.event_id > previous.event_id
                    || (item.event_id == previous.event_id && history.items.len() < *length)
            }) {
                result = Some((index, history.items.len(), item));
            }
        }
        Ok(result
            .map(|(index, _, item)| (index, Some(item)))
            .unwrap_or((0, None)))
    }

    fn find_first_by_item(&self, item: &VersionHistoryItem) -> Result<(usize, &VersionHistory)> {
        self.histories
            .iter()
            .enumerate()
            .find(|(_, h)| h.contains_item(item))
            .ok_or_else(|| BadRequestError::new("version histories does not contains given item."))
    }

    fn is_rebuilt(&self) -> Result<bool> {
        let current = self.current_history()?.last_item()?.version;
        for history in &self.histories {
            if history.last_item()?.version > current {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn set_current_index(&mut self, index: i32) -> Result<()> {
        if index < 0 || index as usize >= self.histories.len() {
            return Err(BadRequestError::new("invalid current branch index."));
        }
        self.current_version_history_index = index;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::{json, Value};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Case {
        name: String,
        op: String,
        histories: Vec<Vec<[i64; 2]>>,
        #[serde(default)]
        current: i32,
        #[serde(default)]
        branch: usize,
        #[serde(default)]
        index: i32,
        #[serde(default)]
        item: [i64; 2],
        #[serde(default)]
        remote: Vec<[i64; 2]>,
        #[serde(default, rename = "eventID")]
        event_id: i64,
        expected: Value,
    }
    fn history(items: &[[i64; 2]]) -> VersionHistory {
        new_history(
            b"branch".to_vec(),
            &items
                .iter()
                .map(|v| new_item(v[0], v[1]))
                .collect::<Vec<_>>(),
        )
    }
    fn pair(item: VersionHistoryItem) -> Value {
        json!([item.event_id, item.version])
    }
    fn pairs(history: &VersionHistory) -> Value {
        json!(history
            .items
            .iter()
            .map(|i| [i.event_id, i.version])
            .collect::<Vec<_>>())
    }
    fn evaluate(c: &Case) -> Result<Value> {
        let mut all = VersionHistories {
            current_version_history_index: c.current,
            histories: c.histories.iter().map(|v| history(v)).collect(),
        };
        Ok(match c.op.as_str() {
            "new_item" => pair(new_item(c.item[0], c.item[1])),
            "add" => {
                all.histories[c.branch].add_or_update(&new_item(c.item[0], c.item[1]))?;
                pairs(&all.histories[c.branch])
            }
            "contains" => {
                json!(all.histories[c.branch].contains_item(&new_item(c.item[0], c.item[1])))
            }
            "lca" => pair(all.histories[c.branch].find_lca(&history(&c.remote))?),
            "duplicate" => {
                let h =
                    all.histories[c.branch].duplicate_until_lca(&new_item(c.item[0], c.item[1]))?;
                json!({"items": pairs(&h), "tokenLength": h.branch_token.len()})
            }
            "appendable" => {
                json!(all.histories[c.branch].is_lca_appendable(&new_item(c.item[0], c.item[1])))
            }
            "first" => pair(all.histories[c.branch].first_item()?),
            "last" => pair(all.histories[c.branch].last_item()?),
            "event_version" => json!(all.histories[c.branch].event_version(c.event_id)?),
            "get" => {
                all.history(c.index)?;
                json!(c.index)
            }
            "add_history" | "add_null" => {
                let incoming = history(&c.remote);
                let (changed, index) = all.add_history(if c.op == "add_null" {
                    None
                } else {
                    Some(&incoming)
                })?;
                json!({"changed": changed, "index": index, "current": all.current_version_history_index,
                    "histories": all.histories.iter().map(pairs).collect::<Vec<_>>()})
            }
            "lca_history" => {
                let (index, item) = all.find_lca_history(&history(&c.remote))?;
                json!({"index": index, "item": item.map(pair)})
            }
            "find_first" => json!(all.find_first_by_item(&new_item(c.item[0], c.item[1]))?.0),
            "rebuilt" => json!(all.is_rebuilt()?),
            "set_current" => {
                all.set_current_index(c.index)?;
                json!(all.current_version_history_index)
            }
            op => panic!("unknown oracle operation {op}"),
        })
    }
    #[test]
    fn version_history_go_fixtures() {
        let cases: Vec<Case> = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/version-history.json"
        ))
        .unwrap();
        for case in cases {
            let actual = match std::panic::catch_unwind(|| evaluate(&case)) {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => json!({"error": error.message}),
                Err(payload) => json!({"panic": payload.downcast_ref::<String>().cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string())).unwrap()}),
            };
            assert_eq!(actual, case.expected, "{}", case.name);
        }
    }
}
