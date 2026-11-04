//! Go encoding/gob cursor for sqlHistoryStore.GetAllHistoryTreeBranches.
use crate::history_contract::PersistenceError;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct HistoryTreeCursor {
    pub shard_id: i32,
    pub tree_id: Vec<u8>,
    pub branch_id: Vec<u8>,
}

fn uint(out: &mut Vec<u8>, value: u64) {
    if value < 128 { out.push(value as u8); return; }
    let bytes = value.to_be_bytes();
    let offset = bytes.iter().position(|b| *b != 0).unwrap_or(7);
    out.push((-(8 - offset as i16)) as u8);
    out.extend_from_slice(&bytes[offset..]);
}
fn int(out: &mut Vec<u8>, value: i64) { uint(out, ((value as u64) << 1) ^ ((value >> 63) as u64)); }
fn bytes(out: &mut Vec<u8>, value: &[u8]) { uint(out, value.len() as u64); out.extend_from_slice(value); }
fn message(out: &mut Vec<u8>, body: &[u8]) { bytes(out, body); }

pub fn encode(cursor: &HistoryTreeCursor) -> Vec<u8> {
    // wireType.StructT -> structType{CommonType, []fieldType}.
    let mut ty = Vec::new();
    int(&mut ty, -64);
    ty.extend_from_slice(&[3, 1, 1]);
    bytes(&mut ty, b"historyTreePageToken");
    ty.push(1); int(&mut ty, 64); ty.push(0);
    ty.push(1); uint(&mut ty, 3);
    for (name, id) in [(b"ShardID".as_slice(), 2), (b"TreeID".as_slice(), 5), (b"BranchID".as_slice(), 5)] {
        ty.push(1); bytes(&mut ty, name); ty.push(1); int(&mut ty, id); ty.push(0);
    }
    ty.extend_from_slice(&[0, 0]);
    let mut out = Vec::new(); message(&mut out, &ty);
    let mut body = Vec::new(); int(&mut body, 64);
    let mut previous = -1_i32;
    if cursor.shard_id != 0 { uint(&mut body, 1); int(&mut body, i64::from(cursor.shard_id)); previous = 0; }
    if !cursor.tree_id.is_empty() { uint(&mut body, (1 - previous) as u64); bytes(&mut body, &cursor.tree_id); previous = 1; }
    if !cursor.branch_id.is_empty() { uint(&mut body, (2 - previous) as u64); bytes(&mut body, &cursor.branch_id); }
    body.push(0); message(&mut out, &body); out
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn uint(&mut self) -> Option<u64> {
        let (&first, rest) = self.0.split_first()?; self.0 = rest;
        if first < 128 { return Some(u64::from(first)); }
        let n = usize::try_from(-(first as i8 as i16)).ok()?;
        if n > 8 || self.0.len() < n { return None; }
        let mut value = 0;
        for b in &self.0[..n] { value = (value << 8) | u64::from(*b); }
        self.0 = &self.0[n..]; Some(value)
    }
    fn int(&mut self) -> Option<i64> { let v = self.uint()?; Some(((v >> 1) as i64) ^ -((v & 1) as i64)) }
    fn bytes(&mut self) -> Option<&'a [u8]> { let n = usize::try_from(self.uint()?).ok()?; let value = self.0.get(..n)?; self.0 = &self.0[n..]; Some(value) }
}

pub fn decode(token: &[u8]) -> Result<HistoryTreeCursor, PersistenceError> {
    if token.is_empty() { return Ok(HistoryTreeCursor::default()); }
    let parse = || -> Option<HistoryTreeCursor> {
        let mut stream = Reader(token);
        while !stream.0.is_empty() {
            let mut msg = Reader(stream.bytes()?);
            if msg.int()? < 0 { continue; } // Go may assign any user type ID.
            let mut cursor = HistoryTreeCursor::default(); let mut field = -1_i64;
            loop {
                let delta = msg.uint()?;
                if delta == 0 { break; }
                field = field.checked_add(i64::try_from(delta).ok()?)?;
                match field {
                    0 => cursor.shard_id = i32::try_from(msg.int()?).ok()?,
                    1 => cursor.tree_id = msg.bytes()?.to_vec(),
                    2 => cursor.branch_id = msg.bytes()?.to_vec(),
                    _ => return None,
                }
            }
            if !msg.0.is_empty() || !stream.0.is_empty() { return None; }
            if (!cursor.tree_id.is_empty() && cursor.tree_id.len() != 16) || (!cursor.branch_id.is_empty() && cursor.branch_id.len() != 16) { return None; }
            return Some(cursor);
        }
        None
    };
    parse().ok_or_else(|| PersistenceError::Internal("unable to decode next page token".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gob_cursor_handles_zero_fields_and_multibyte_integers() {
        for shard_id in [0, 1, 256, -1] {
            let cursor = HistoryTreeCursor { shard_id, tree_id: vec![1;16], branch_id: vec![2;16] };
            assert_eq!(decode(&encode(&cursor)).unwrap(), cursor);
        }
        assert_eq!(decode(&encode(&HistoryTreeCursor::default())).unwrap(), HistoryTreeCursor::default());
        assert!(decode(&[255]).is_err());
    }
}
