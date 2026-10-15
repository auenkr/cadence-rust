//! Go persistence's version-0 ThriftRW history-batch format.

use cadence_api::enums::EncodingType;
use cadence_api::service_types::{History, HistoryEvent};
use cadence_api::types::DataBlob;

use crate::value::{read_struct, write_struct};
use crate::{Error, FromThrift, Reader, Result, ToThrift, Writer};

/// The converter translates internal millisecond timestamps to wire nanoseconds.
pub fn encode(events: Vec<HistoryEvent>) -> DataBlob {
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
    let mut history = History::from_thrift(&read_struct(&mut reader)?)?;
    if reader.remaining() != 0 {
        return Err(Error::Invalid("trailing history blob bytes".into()));
    }
    for event in &mut history.events {
        event.timestamp = event.timestamp.map(|nanos| nanos / 1_000_000);
    }
    Ok(history.events)
}
