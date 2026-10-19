//! Native outbound YARPC/TChannel Thrift calls used by Matching's History
//! client when Go's `EnableGRPCOutbound` selector chooses TChannel.
//!
//! The five operation adapters are intentionally colocated with the History
//! client; this module owns only the TChannel/Thrift wire boundary and never
//! owns retries or operation deadlines.

use cadence_thrift::{protocol::Reader, value::Struct, TType, Value};
use std::time::Duration;
use tonic::{Code, Status};

const HISTORY_SERVICE: &str = "cadence-history";

#[derive(Clone, Debug)]
pub(crate) struct TChannelHistoryClient {
    address: std::sync::Arc<str>,
}

impl TChannelHistoryClient {
    pub(crate) fn new(address: impl Into<std::sync::Arc<str>>) -> Self {
        Self {
            address: address.into(),
        }
    }

    /// `arg0` is YARPC's fully-qualified Thrift method, `arg1` is binary
    /// metadata, and `arg2` is the raw Thrift argument/result struct. This is
    /// not a thrift RPC envelope: YARPC carries the method in arg0.
    pub(crate) async fn call(
        &self,
        method: &'static str,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<Struct, Status> {
        let method = format!("History::{method}");
        let headers = vec![
            ("$rpc$-caller".to_owned(), "internal".to_owned()),
            ("$rpc$-service".to_owned(), HISTORY_SERVICE.to_owned()),
        ];
        let response = crate::tchannel_json_client::call_thrift(
            &self.address,
            HISTORY_SERVICE,
            &method,
            &headers,
            &body,
            timeout,
        )
        .await
        .map_err(|error| match error {
            crate::tchannel_json_client::JsonCallError::Application(message) => {
                Status::unknown(format!("History TChannel application error: {message}"))
            }
            crate::tchannel_json_client::JsonCallError::Transport(error) => {
                if error.to_string().contains("deadline exceeded") {
                    Status::deadline_exceeded(error.to_string())
                } else {
                    Status::unavailable(error.to_string())
                }
            }
        })?;
        if response.application_error {
            return Err(application_error(&response.headers, &response.body));
        }
        decode_struct(&response.body)
            .map_err(|error| Status::internal(format!("invalid History Thrift reply: {error}")))
    }
}

fn application_error(headers: &[(String, String)], body: &[u8]) -> Status {
    let name = headers
        .iter()
        .find(|(key, _)| key == "$rpc$-application-error-name")
        .map(|(_, value)| value.as_str())
        .unwrap_or("UnknownHistoryError");
    let details = headers
        .iter()
        .find(|(key, _)| key == "$rpc$-application-error-details")
        .map(|(_, value)| value.as_str())
        .unwrap_or("History returned a Thrift application error.");
    // Typed Cadence exception translation is performed by the operation
    // adapters after decoding the exception schema. Keep the class and raw
    // payload visible here until that mapping is applied.
    let _ = body;
    Status::new(
        Code::Unknown,
        format!("{name}: {details}"),
    )
}

pub(crate) fn encode_struct(value: &Struct) -> Vec<u8> {
    let mut writer = cadence_thrift::protocol::Writer::new();
    write_struct(&mut writer, value);
    writer.into_bytes()
}

pub(crate) fn decode_struct(bytes: &[u8]) -> cadence_thrift::Result<Struct> {
    let mut reader = Reader::new(bytes);
    let value = read_struct(&mut reader)?;
    if reader.remaining() != 0 {
        return Err(cadence_thrift::Error::Invalid(
            "trailing bytes after Thrift struct".into(),
        ));
    }
    Ok(value)
}

fn read_struct(reader: &mut Reader<'_>) -> cadence_thrift::Result<Struct> {
    let mut fields = Vec::new();
    while let Some((ty, id)) = reader.read_field_begin()? {
        fields.push((id, read_value(reader, ty)?));
    }
    Ok(Struct { fields })
}

fn read_value(reader: &mut Reader<'_>, ty: TType) -> cadence_thrift::Result<Value> {
    match ty {
        TType::Bool => Ok(Value::Bool(reader.read_bool()?)),
        TType::Byte => Ok(Value::I8(reader.read_u8()? as i8)),
        TType::I16 => Ok(Value::I16(reader.read_i16()?)),
        TType::I32 => Ok(Value::I32(reader.read_i32()?)),
        TType::I64 => Ok(Value::I64(reader.read_i64()?)),
        TType::Double => Ok(Value::F64(reader.read_f64()?)),
        TType::String => Ok(Value::Bytes(reader.read_bytes()?.to_vec())),
        TType::Struct => Ok(Value::Struct(read_struct(reader)?)),
        TType::List | TType::Set => {
            let (element_ty, count) = reader.read_list_begin()?;
            let values = (0..count)
                .map(|_| read_value(reader, element_ty))
                .collect::<cadence_thrift::Result<Vec<_>>>()?;
            if ty == TType::List {
                Ok(Value::List(values))
            } else {
                Ok(Value::Set(values))
            }
        }
        TType::Map => {
            let (key_ty, value_ty, count) = reader.read_map_begin()?;
            let values = (0..count)
                .map(|_| Ok((read_value(reader, key_ty)?, read_value(reader, value_ty)?)))
                .collect::<cadence_thrift::Result<Vec<_>>>()?;
            Ok(Value::Map(values))
        }
        TType::Stop => Err(cadence_thrift::Error::Invalid(
            "STOP is not a value type".into(),
        )),
    }
}

fn write_struct(writer: &mut cadence_thrift::protocol::Writer, value: &Struct) {
    for (id, field) in &value.fields {
        let ty = value_type(field);
        writer.write_field_begin(ty, *id);
        write_value(writer, field);
    }
    writer.write_field_stop();
}

fn value_type(value: &Value) -> TType {
    match value {
        Value::Bool(_) => TType::Bool,
        Value::I8(_) => TType::Byte,
        Value::I16(_) => TType::I16,
        Value::I32(_) => TType::I32,
        Value::I64(_) => TType::I64,
        Value::F64(_) => TType::Double,
        Value::Bytes(_) => TType::String,
        Value::Struct(_) => TType::Struct,
        Value::Map(_) => TType::Map,
        Value::Set(_) => TType::Set,
        Value::List(_) => TType::List,
    }
}

fn write_value(writer: &mut cadence_thrift::protocol::Writer, value: &Value) {
    match value {
        Value::Bool(value) => writer.write_bool(*value),
        Value::I8(value) => writer.write_u8(*value as u8),
        Value::I16(value) => writer.write_i16(*value),
        Value::I32(value) => writer.write_i32(*value),
        Value::I64(value) => writer.write_i64(*value),
        Value::F64(value) => writer.write_f64(*value),
        Value::Bytes(value) => writer.write_bytes(value),
        Value::Struct(value) => write_struct(writer, value),
        Value::List(values) | Value::Set(values) => {
            let element_type = values.first().map(value_type).unwrap_or(TType::Stop);
            writer.write_list_begin(element_type, values.len());
            for value in values {
                write_value(writer, value);
            }
        }
        Value::Map(values) => {
            let (key_type, value_type) = values
                .first()
                .map(|(key, value)| (value_type(key), value_type(value)))
                .unwrap_or((TType::Stop, TType::Stop));
            writer.write_map_begin(key_type, value_type, values.len());
            for (key, value) in values {
                write_value(writer, key);
                write_value(writer, value);
            }
        }
    }
}

/// Local typed representation used by schema adapters. Unlike the shared
/// `cadence_thrift::Value`, containers retain their declared element/key/value
/// types even when empty (Thrift's binary protocol still writes those bytes).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TypedValue {
    Bool(bool), I8(i8), I16(i16), I32(i32), I64(i64), F64(f64),
    Bytes(Vec<u8>), Struct(TypedStruct),
    List(TType, Vec<TypedValue>), Set(TType, Vec<TypedValue>),
    Map(TType, TType, Vec<(TypedValue, TypedValue)>),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TypedStruct { pub fields: Vec<(i16, TypedValue)> }

impl TypedValue {
    fn ty(&self) -> TType { match self {
        Self::Bool(_) => TType::Bool, Self::I8(_) => TType::Byte,
        Self::I16(_) => TType::I16, Self::I32(_) => TType::I32,
        Self::I64(_) => TType::I64, Self::F64(_) => TType::Double,
        Self::Bytes(_) => TType::String, Self::Struct(_) => TType::Struct,
        Self::List(..) => TType::List, Self::Set(..) => TType::Set,
        Self::Map(..) => TType::Map,
    }}
}

pub(crate) fn decode_typed_struct(bytes: &[u8]) -> cadence_thrift::Result<TypedStruct> {
    let mut reader = Reader::new(bytes);
    let value = read_typed_struct(&mut reader)?;
    if reader.remaining() != 0 {
        return Err(cadence_thrift::Error::Invalid("trailing bytes after Thrift struct".into()));
    }
    Ok(value)
}

pub(crate) fn encode_typed_struct(value: &TypedStruct) -> Vec<u8> {
    let mut writer = cadence_thrift::protocol::Writer::new();
    write_typed_struct(&mut writer, value);
    writer.into_bytes()
}

fn read_typed_struct(r: &mut Reader<'_>) -> cadence_thrift::Result<TypedStruct> {
    let mut fields = Vec::new();
    while let Some((ty, id)) = r.read_field_begin()? { fields.push((id, read_typed_value(r, ty)?)); }
    Ok(TypedStruct { fields })
}

fn read_typed_value(r: &mut Reader<'_>, ty: TType) -> cadence_thrift::Result<TypedValue> {
    Ok(match ty {
        TType::Bool => TypedValue::Bool(r.read_bool()?),
        TType::Byte => TypedValue::I8(r.read_u8()? as i8),
        TType::I16 => TypedValue::I16(r.read_i16()?), TType::I32 => TypedValue::I32(r.read_i32()?),
        TType::I64 => TypedValue::I64(r.read_i64()?), TType::Double => TypedValue::F64(r.read_f64()?),
        TType::String => TypedValue::Bytes(r.read_bytes()?.to_vec()),
        TType::Struct => TypedValue::Struct(read_typed_struct(r)?),
        TType::List | TType::Set => {
            let (element, n) = r.read_list_begin()?;
            let mut values = Vec::with_capacity(n);
            for _ in 0..n { values.push(read_typed_value(r, element)?); }
            if ty == TType::List { TypedValue::List(element, values) } else { TypedValue::Set(element, values) }
        }
        TType::Map => {
            let (key, val, n) = r.read_map_begin()?;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n { entries.push((read_typed_value(r, key)?, read_typed_value(r, val)?)); }
            TypedValue::Map(key, val, entries)
        }
        TType::Stop => return Err(cadence_thrift::Error::Invalid("STOP is not a value type".into())),
    })
}

fn write_typed_struct(w: &mut cadence_thrift::protocol::Writer, s: &TypedStruct) {
    for (id, value) in &s.fields { w.write_field_begin(value.ty(), *id); write_typed_value(w, value); }
    w.write_field_stop();
}

fn write_typed_value(w: &mut cadence_thrift::protocol::Writer, v: &TypedValue) {
    match v {
        TypedValue::Bool(x) => w.write_bool(*x), TypedValue::I8(x) => w.write_u8(*x as u8),
        TypedValue::I16(x) => w.write_i16(*x), TypedValue::I32(x) => w.write_i32(*x),
        TypedValue::I64(x) => w.write_i64(*x), TypedValue::F64(x) => w.write_f64(*x),
        TypedValue::Bytes(x) => w.write_bytes(x), TypedValue::Struct(x) => write_typed_struct(w, x),
        TypedValue::List(ty, values) | TypedValue::Set(ty, values) => {
            w.write_list_begin(*ty, values.len()); for v in values { write_typed_value(w, v); }
        }
        TypedValue::Map(key, val, entries) => {
            w.write_map_begin(*key, *val, entries.len());
            for (k, v) in entries { write_typed_value(w, k); write_typed_value(w, v); }
        }
    }
}
