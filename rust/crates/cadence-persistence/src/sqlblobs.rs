//! ThriftRW codec shared by History's SQL `sqlblobs` values.
//!
//! `sqlblobs.thrift` structs are encoded with the binary protocol without a
//! Thrift message envelope or version byte. The column's `data_encoding`
//! carries the codec choice; `thriftrw_snappy` is a raw Snappy block around
//! the exact same struct bytes. Struct-specific field mapping belongs at the
//! contract boundary, while this module owns the byte-compatible wire layer.

use cadence_thrift::{value::{read_struct, write_value, Struct}, Error, Reader, Result, TType, Writer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    ThriftRW,
    ThriftRWSnappy,
}

impl Encoding {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "thriftrw" => Ok(Self::ThriftRW),
            "thriftrw_snappy" => Ok(Self::ThriftRWSnappy),
            _ => Err(Error::Invalid(format!("invalid encoding type: {value}"))),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThriftRW => "thriftrw",
            Self::ThriftRWSnappy => "thriftrw_snappy",
        }
    }

    pub fn encode(self, value: &Struct) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        for (id, field) in &value.fields {
            let ty = value_type(field);
            writer.write_field_begin(ty, *id);
            write_value(&mut writer, field);
        }
        writer.write_field_stop();
        let bytes = writer.into_bytes();
        match self {
            Self::ThriftRW => Ok(bytes),
            Self::ThriftRWSnappy => snap::raw::Encoder::new()
                .compress_vec(&bytes)
                .map_err(|error| Error::Invalid(format!("snappy encode: {error}"))),
        }
    }

    pub fn decode(self, data: &[u8]) -> Result<Struct> {
        let uncompressed;
        let bytes = match self {
            Self::ThriftRW => data,
            Self::ThriftRWSnappy => {
                uncompressed = snap::raw::Decoder::new()
                    .decompress_vec(data)
                    .map_err(|error| Error::Invalid(format!("snappy decode: {error}")))?;
                &uncompressed
            }
        };
        let mut reader = Reader::new(bytes);
        let value = read_struct(&mut reader)?;
        if reader.remaining() != 0 {
            return Err(Error::Invalid("trailing bytes after sqlblobs struct".into()));
        }
        Ok(value)
    }
}

fn value_type(value: &cadence_thrift::Value) -> TType {
    use cadence_thrift::Value;
    match value {
        Value::Bool(_) => TType::Bool,
        Value::I8(_) => TType::Byte,
        Value::I16(_) => TType::I16,
        Value::I32(_) => TType::I32,
        Value::I64(_) => TType::I64,
        Value::F64(_) => TType::Double,
        Value::Bytes(_) => TType::String,
        Value::List(_) => TType::List,
        Value::Map(_) => TType::Map,
        Value::Set(_) => TType::Set,
        Value::Struct(_) => TType::Struct,
    }
}

/// Names of the persisted SQL structs handled by the History serializer.
pub const HISTORY_STRUCTS: &[&str] = &[
    "ShardInfo",
    "WorkflowExecutionInfo",
    "ActivityInfo",
    "ChildExecutionInfo",
    "SignalInfo",
    "RequestCancelInfo",
    "TimerInfo",
    "TransferTaskInfo",
    "TimerTaskInfo",
    "ReplicationTaskInfo",
    "CrossClusterTaskInfo",
    "HistoryTreeInfo",
    "HistoryBranchRange",
];

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_thrift::Value;

    // Go sqlblobs.TaskInfo emits these exact field bytes for scheduleID=7.
    const TASK_INFO_SCHEDULE_ID_7: &[u8] = &[8, 0, 13, 0, 0, 0, 0, 0, 0, 0, 7, 0];

    #[test]
    fn raw_thriftrw_golden_round_trip() {
        let mut value = Struct::default();
        value.fields.push((13, Value::I64(7)));
        let encoded = Encoding::ThriftRW.encode(&value).unwrap();
        assert_eq!(encoded, TASK_INFO_SCHEDULE_ID_7);
        assert_eq!(Encoding::ThriftRW.decode(&encoded).unwrap(), value);
    }

    #[test]
    fn snappy_wraps_the_same_struct_and_encoding_names_match_go() {
        let mut value = Struct::default();
        value.fields.push((13, Value::I64(7)));
        let packed = Encoding::ThriftRWSnappy.encode(&value).unwrap();
        assert_eq!(Encoding::ThriftRWSnappy.decode(&packed).unwrap(), value);
        assert_eq!(Encoding::parse("thriftrw").unwrap().as_str(), "thriftrw");
        assert_eq!(Encoding::parse("thriftrw_snappy").unwrap().as_str(), "thriftrw_snappy");
        assert!(Encoding::parse("json").is_err());
    }
}
