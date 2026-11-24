//! Go persistence's version-0 ThriftRW history-batch format.

use cadence_api::enums::EncodingType;
use cadence_api::service_types::{History, HistoryEvent};
use cadence_api::types::DataBlob;

use crate::value::{read_struct, write_struct};
use crate::{Error, FromThrift, Reader, Result, ToThrift, Writer};

/// `HistoryEvent.timestamp` is Unix nanoseconds in memory and on the wire (Go parity).
pub fn encode(events: Vec<HistoryEvent>) -> DataBlob {
    encode_slice(&events)
}

/// Borrowed-batch variant of [`encode`]; produces identical bytes without taking ownership.
pub fn encode_slice(events: &[HistoryEvent]) -> DataBlob {
    let mut writer = Writer::new();
    writer.write_u8(0x59);
    // Preserve the element type even for an empty batch. Generic empty lists
    // otherwise have no element from which to infer their Thrift type.
    writer.write_field_begin(crate::TType::List, 10);
    writer.write_list_begin(crate::TType::Struct, events.len());
    for event in events {
        write_struct(&mut writer, &event.to_thrift());
    }
    writer.write_field_stop();
    DataBlob {
        encoding_type: Some(EncodingType::ThriftRW),
        data: writer.into_bytes(),
    }
}

pub fn decode(blob: &DataBlob) -> Result<Vec<HistoryEvent>> {
    if blob.encoding_type != Some(EncodingType::ThriftRW) {
        return Err(Error::Invalid("expected ThriftRW history encoding".into()));
    }
    let mut reader = Reader::new(&blob.data);
    if reader.read_u8()? != 0x59 {
        return Err(Error::Invalid("invalid ThriftRW history version".into()));
    }
    let history = History::from_thrift(&read_struct(&mut reader)?)?;
    if reader.remaining() != 0 {
        return Err(Error::Invalid("trailing history blob bytes".into()));
    }
    Ok(history.events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_encode_matches_owned_encode() {
        let events: Vec<HistoryEvent> = (1..4)
            .map(|i| HistoryEvent { event_id: i, timestamp: 1_000 * i, version: 7, task_id: i, ..Default::default() })
            .collect();
        let owned = encode(events.clone());
        let borrowed = encode_slice(&events);
        assert_eq!(owned.data, borrowed.data);
        assert_eq!(owned.encoding_type, borrowed.encoding_type);
        assert_eq!(encode(vec![]).data, encode_slice(&[]).data);
        assert_eq!(decode(&borrowed).unwrap().len(), 3);
    }
}
