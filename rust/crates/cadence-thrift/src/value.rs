//! Generic Thrift value model.
//!
//! A decoded struct is a list of `(field_id, Value)` pairs; each API struct
//! converts to/from this form with its IDL field IDs. Unknown fields are
//! preserved on decode and skipped (never re-encoded), keeping forward
//! compatibility with newer IDL revisions.

use crate::protocol::{Error, Reader, Result, TType, Writer};
use cadence_api::GoString;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Set(Vec<Value>),
    Struct(Struct),
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Struct {
    pub fields: Vec<(i16, Value)>,
}

impl Struct {
    #[must_use]
    pub fn get(&self, id: i16) -> Option<&Value> {
        self.fields.iter().find(|(i, _)| *i == id).map(|(_, v)| v)
    }

    // -- getters used by generated-style converters -------------------------

    /// Decode Go's byte-valued Thrift string without UTF-8 validation.
    pub fn opt_go_string(&self, id: i16) -> Result<Option<GoString>> {
        match self.get(id) {
            None => Ok(None),
            Some(value) => GoString::from_value(value).map(Some),
        }
    }

    pub fn req_go_string(&self, id: i16, default: &[u8]) -> Result<GoString> {
        Ok(self.opt_go_string(id)?.unwrap_or_else(|| GoString::from(default)))
    }

    /// Map keys retain raw bytes and duplicate keys are last-wins, like Go.
    pub fn opt_map_go_string<T>(&self, id: i16) -> Result<Option<std::collections::HashMap<GoString, T>>>
    where
        T: FromValue,
    {
        match self.get(id) {
            None => Ok(None),
            Some(Value::Map(items)) => {
                let mut map = std::collections::HashMap::with_capacity(items.len());
                for (key, value) in items {
                    map.insert(GoString::from_value(key)?, T::from_value(value)?);
                }
                Ok(Some(map))
            }
            Some(value) => Err(Error::Invalid(format!("field {id}: expected map, got {value:?}"))),
        }
    }

    pub fn opt_string(&self, id: i16) -> Result<Option<String>> {
        match self.get(id) {
            None => Ok(None),
            Some(Value::Bytes(b)) => Ok(Some(
                String::from_utf8(b.clone())
                    .map_err(|e| Error::Invalid(format!("field {id}: {e}")))?,
            )),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected string, got {v:?}"
            ))),
        }
    }

    pub fn req_string(&self, id: i16, default: &str) -> Result<String> {
        Ok(self.opt_string(id)?.unwrap_or_else(|| default.to_string()))
    }

    pub fn opt_bytes(&self, id: i16) -> Result<Option<Vec<u8>>> {
        match self.get(id) {
            None => Ok(None),
            Some(Value::Bytes(b)) => Ok(Some(b.clone())),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected binary, got {v:?}"
            ))),
        }
    }

    pub fn bytes_or_empty(&self, id: i16) -> Result<Vec<u8>> {
        Ok(self.opt_bytes(id)?.unwrap_or_default())
    }

    pub fn opt_bool(&self, id: i16) -> Result<Option<bool>> {
        match self.get(id) {
            None => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected bool, got {v:?}"
            ))),
        }
    }

    pub fn bool_or_false(&self, id: i16) -> Result<bool> {
        Ok(self.opt_bool(id)?.unwrap_or(false))
    }

    pub fn opt_i32(&self, id: i16) -> Result<Option<i32>> {
        match self.get(id) {
            None => Ok(None),
            Some(Value::I32(v)) => Ok(Some(*v)),
            Some(Value::I16(v)) => Ok(Some(i32::from(*v))),
            Some(Value::I8(v)) => Ok(Some(i32::from(*v))),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected i32, got {v:?}"
            ))),
        }
    }

    pub fn opt_i64(&self, id: i16) -> Result<Option<i64>> {
        match self.get(id) {
            None => Ok(None),
            Some(Value::I64(v)) => Ok(Some(*v)),
            Some(Value::I32(v)) => Ok(Some(i64::from(*v))),
            Some(Value::I16(v)) => Ok(Some(i64::from(*v))),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected i64, got {v:?}"
            ))),
        }
    }

    pub fn i64_or_zero(&self, id: i16) -> Result<i64> {
        Ok(self.opt_i64(id)?.unwrap_or(0))
    }

    pub fn opt_f64(&self, id: i16) -> Result<Option<f64>> {
        match self.get(id) {
            None => Ok(None),
            Some(Value::F64(v)) => Ok(Some(*v)),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected double, got {v:?}"
            ))),
        }
    }

    pub fn opt_enum<E>(&self, id: i16) -> Result<Option<E>>
    where
        E: FromI32,
    {
        match self.get(id) {
            None => Ok(None),
            Some(Value::I32(v)) => {
                Ok(Some(E::from_i32(*v).ok_or_else(|| {
                    Error::Invalid(format!("field {id}: unknown enum value {v}"))
                })?))
            }
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected enum, got {v:?}"
            ))),
        }
    }

    pub fn opt_struct<T>(&self, id: i16) -> Result<Option<T>>
    where
        T: FromThrift,
    {
        match self.get(id) {
            None => Ok(None),
            Some(Value::Struct(s)) => Ok(Some(T::from_thrift(s)?)),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected struct, got {v:?}"
            ))),
        }
    }

    pub fn opt_list<T>(&self, id: i16) -> Result<Option<Vec<T>>>
    where
        T: FromValue,
    {
        match self.get(id) {
            None => Ok(None),
            Some(Value::List(items)) => items
                .iter()
                .map(T::from_value)
                .collect::<Result<Vec<_>>>()
                .map(Some),
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected list, got {v:?}"
            ))),
        }
    }

    pub fn opt_map_str<T>(&self, id: i16) -> Result<Option<std::collections::HashMap<String, T>>>
    where
        T: FromValue,
    {
        match self.get(id) {
            None => Ok(None),
            Some(Value::Map(items)) => {
                let mut map = std::collections::HashMap::with_capacity(items.len());
                for (k, v) in items {
                    let key = match k {
                        Value::Bytes(b) => String::from_utf8(b.clone())
                            .map_err(|e| Error::Invalid(format!("field {id}: {e}")))?,
                        _ => {
                            return Err(Error::Invalid(format!(
                                "field {id}: expected string map key, got {k:?}"
                            )));
                        }
                    };
                    map.insert(key, T::from_value(v)?);
                }
                Ok(Some(map))
            }
            Some(v) => Err(Error::Invalid(format!(
                "field {id}: expected map, got {v:?}"
            ))),
        }
    }
}

/// Convert a decoded value into a Rust type (for list/map elements).
pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self>;
}

impl FromValue for GoString {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bytes(bytes) => Ok(Self::from(bytes.clone())),
            _ => Err(Error::Invalid(format!("expected string, got {value:?}"))),
        }
    }
}

impl FromValue for String {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Bytes(b) => Ok(String::from_utf8(b.clone())?),
            _ => Err(Error::Invalid(format!("expected string, got {v:?}"))),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Bytes(b) => Ok(b.clone()),
            _ => Err(Error::Invalid(format!("expected binary, got {v:?}"))),
        }
    }
}

impl FromValue for i32 {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::I32(x) => Ok(*x),
            Value::I16(x) => Ok(i32::from(*x)),
            Value::I8(x) => Ok(i32::from(*x)),
            _ => Err(Error::Invalid(format!("expected i32, got {v:?}"))),
        }
    }
}

impl<T: FromThrift> FromValue for T {
    fn from_value(v: &Value) -> Result<Self> {
        match v {
            Value::Struct(s) => T::from_thrift(s),
            _ => Err(Error::Invalid(format!("expected struct, got {v:?}"))),
        }
    }
}

/// Thrift enums: integer newtypes on the wire.
pub trait FromI32: Sized {
    fn from_i32(v: i32) -> Option<Self>;
    fn value(self) -> i32;
}

/// Structs convertible to/from the generic value model.
pub trait FromThrift: Sized {
    fn from_thrift(s: &Struct) -> Result<Self>;
}

pub trait ToThrift {
    fn to_thrift(&self) -> Struct;
}

// ---------------------------------------------------------------------------
// Encoding values
// ---------------------------------------------------------------------------

/// Field builders: each pushes `(id, Value)` in IDL order when present.
#[derive(Debug, Default)]
pub struct Fields {
    pub fields: Vec<(i16, Value)>,
}

impl Fields {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ostr(&mut self, id: i16, v: &Option<String>) {
        if let Some(s) = v {
            self.fields.push((id, Value::Bytes(s.as_bytes().to_vec())));
        }
    }

    pub fn sstr(&mut self, id: i16, v: &str) {
        if !v.is_empty() {
            self.fields.push((id, Value::Bytes(v.as_bytes().to_vec())));
        }
    }

    pub fn obytes(&mut self, id: i16, v: &Option<Vec<u8>>) {
        if let Some(b) = v {
            self.fields.push((id, Value::Bytes(b.clone())));
        }
    }

    pub fn sbytes(&mut self, id: i16, v: &[u8]) {
        if !v.is_empty() {
            self.fields.push((id, Value::Bytes(v.to_vec())));
        }
    }

    pub fn obool(&mut self, id: i16, v: bool) {
        if v {
            self.fields.push((id, Value::Bool(true)));
        }
    }

    pub fn oi32(&mut self, id: i16, v: Option<i32>) {
        if let Some(x) = v {
            self.fields.push((id, Value::I32(x)));
        }
    }

    pub fn oi64(&mut self, id: i16, v: Option<i64>) {
        if let Some(x) = v {
            self.fields.push((id, Value::I64(x)));
        }
    }

    pub fn of64(&mut self, id: i16, v: Option<f64>) {
        if let Some(x) = v {
            self.fields.push((id, Value::F64(x)));
        }
    }

    pub fn oenum<E>(&mut self, id: i16, v: Option<E>)
    where
        E: FromI32,
    {
        if let Some(e) = v {
            self.fields.push((id, Value::I32(e.value())));
        }
    }

    pub fn ostruct<T>(&mut self, id: i16, v: Option<&T>)
    where
        T: ToThrift,
    {
        if let Some(t) = v {
            self.fields.push((id, Value::Struct(t.to_thrift())));
        }
    }

    pub fn olist<T>(&mut self, id: i16, v: &[T])
    where
        T: ToValue,
    {
        if !v.is_empty() {
            self.fields
                .push((id, Value::List(v.iter().map(T::to_value).collect())));
        }
    }

    pub fn omap_str_bytes(
        &mut self,
        id: i16,
        v: &Option<std::collections::HashMap<String, Vec<u8>>>,
    ) {
        if let Some(m) = v {
            if m.is_empty() {
                return;
            }
            let mut items: Vec<(Value, Value)> = m
                .iter()
                .map(|(k, b)| (Value::Bytes(k.as_bytes().to_vec()), Value::Bytes(b.clone())))
                .collect();
            items.sort_by(|a, b| match (a, b) {
                ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            self.fields.push((id, Value::Map(items)));
        }
    }

    pub fn omap_str_str(&mut self, id: i16, v: &Option<std::collections::HashMap<String, String>>) {
        if let Some(m) = v {
            if m.is_empty() {
                return;
            }
            let mut items: Vec<(Value, Value)> = m
                .iter()
                .map(|(k, s)| {
                    (
                        Value::Bytes(k.as_bytes().to_vec()),
                        Value::Bytes(s.as_bytes().to_vec()),
                    )
                })
                .collect();
            items.sort_by(|a, b| match (a, b) {
                ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            self.fields.push((id, Value::Map(items)));
        }
    }

    pub fn omap_str_struct<T>(&mut self, id: i16, v: &Option<std::collections::HashMap<String, T>>)
    where
        T: ToThrift,
    {
        if let Some(m) = v {
            if m.is_empty() {
                return;
            }
            let mut items: Vec<(Value, Value)> = m
                .iter()
                .map(|(k, t)| {
                    (
                        Value::Bytes(k.as_bytes().to_vec()),
                        Value::Struct(t.to_thrift()),
                    )
                })
                .collect();
            items.sort_by(|a, b| match (a, b) {
                ((Value::Bytes(x), _), (Value::Bytes(y), _)) => x.cmp(y),
                _ => std::cmp::Ordering::Equal,
            });
            self.fields.push((id, Value::Map(items)));
        }
    }

    #[must_use]
    pub fn finish(self) -> Struct {
        Struct {
            fields: self.fields,
        }
    }
}

/// Convert a Rust value into a Thrift value (for list elements).
pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl ToValue for GoString {
    fn to_value(&self) -> Value {
        Value::Bytes(self.as_bytes().to_vec())
    }
}

impl ToValue for String {
    fn to_value(&self) -> Value {
        Value::Bytes(self.as_bytes().to_vec())
    }
}

impl ToValue for Vec<u8> {
    fn to_value(&self) -> Value {
        Value::Bytes(self.clone())
    }
}

impl ToValue for i32 {
    fn to_value(&self) -> Value {
        Value::I32(*self)
    }
}

impl<T: ToThrift> ToValue for T {
    fn to_value(&self) -> Value {
        Value::Struct(self.to_thrift())
    }
}

// ---------------------------------------------------------------------------
// Wire read/write of values
// ---------------------------------------------------------------------------

fn wire_type(v: &Value) -> TType {
    match v {
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

pub fn write_value(w: &mut Writer, v: &Value) {
    match v {
        Value::Bool(b) => w.write_bool(*b),
        Value::I8(x) => w.write_u8(*x as u8),
        Value::I16(x) => w.write_i16(*x),
        Value::I32(x) => w.write_i32(*x),
        Value::I64(x) => w.write_i64(*x),
        Value::F64(x) => w.write_f64(*x),
        Value::Bytes(b) => w.write_bytes(b),
        Value::List(items) => {
            let elem = items.first().map(wire_type).unwrap_or(TType::Stop);
            w.write_list_begin(elem, items.len());
            for item in items {
                write_value(w, item);
            }
        }
        Value::Set(items) => {
            let elem = items.first().map(wire_type).unwrap_or(TType::Stop);
            w.write_list_begin(elem, items.len());
            for item in items {
                write_value(w, item);
            }
        }
        Value::Map(items) => {
            let (kt, vt) = items
                .first()
                .map(|(k, v)| (wire_type(k), wire_type(v)))
                .unwrap_or((TType::Stop, TType::Stop));
            w.write_map_begin(kt, vt, items.len());
            for (k, v) in items {
                write_value(w, k);
                write_value(w, v);
            }
        }
        Value::Struct(s) => write_struct(w, s),
    }
}

pub fn write_struct(w: &mut Writer, s: &Struct) {
    for (id, v) in &s.fields {
        w.write_field_begin(wire_type(v), *id);
        write_value(w, v);
    }
    w.write_field_stop();
}

pub fn read_value(r: &mut Reader<'_>, ty: TType) -> Result<Value> {
    match ty {
        TType::Stop => Err(Error::Invalid("unexpected STOP value".to_string())),
        TType::Bool => Ok(Value::Bool(r.read_bool()?)),
        TType::Byte => Ok(Value::I8(r.read_u8()? as i8)),
        TType::Double => Ok(Value::F64(r.read_f64()?)),
        TType::I16 => Ok(Value::I16(r.read_i16()?)),
        TType::I32 => Ok(Value::I32(r.read_i32()?)),
        TType::I64 => Ok(Value::I64(r.read_i64()?)),
        TType::String => Ok(Value::Bytes(r.read_bytes()?.to_vec())),
        TType::Struct => Ok(Value::Struct(read_struct(r)?)),
        TType::List | TType::Set => {
            let (ety, len) = r.read_list_begin()?;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(read_value(r, ety)?);
            }
            Ok(if ty == TType::List {
                Value::List(items)
            } else {
                Value::Set(items)
            })
        }
        TType::Map => {
            let (kt, vt, len) = r.read_map_begin()?;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                let k = read_value(r, kt)?;
                let v = read_value(r, vt)?;
                items.push((k, v));
            }
            Ok(Value::Map(items))
        }
    }
}

pub fn read_struct(r: &mut Reader<'_>) -> Result<Struct> {
    let mut fields = Vec::new();
    while let Some((ty, id)) = r.read_field_begin()? {
        fields.push((id, read_value(r, ty)?));
    }
    Ok(Struct { fields })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_string_wire_roundtrip_preserves_invalid_utf8_and_nul() {
        let raw = GoString::from(vec![0xff, 0, 0xc0, 0xaf, 0xfe]);
        let original = Struct { fields: vec![(10, raw.to_value())] };
        let mut writer = Writer::new();
        write_struct(&mut writer, &original);
        let bytes = writer.into_bytes();
        let mut reader = Reader::new(&bytes);
        let decoded = read_struct(&mut reader).unwrap();
        assert_eq!(decoded.req_go_string(10, b"").unwrap(), raw);
        assert!(decoded.req_string(10, "").is_err());
        assert_eq!(decoded.opt_go_string(20).unwrap(), None);
        assert_eq!(decoded.req_go_string(20, &[0xfe]).unwrap().as_bytes(), &[0xfe]);
        assert_eq!(reader.remaining(), 0);
        let mut writer = Writer::new();
        write_struct(&mut writer, &decoded);
        assert_eq!(writer.into_bytes(), bytes);
    }

    #[test]
    fn go_string_map_keys_are_lossless_and_duplicate_keys_are_last_wins() {
        let value = Struct { fields: vec![(10, Value::Map(vec![
            (Value::Bytes(vec![0xff]), Value::Bytes(vec![1])),
            (Value::Bytes(vec![0xfe]), Value::Bytes(vec![2])),
            (Value::Bytes(vec![0xff]), Value::Bytes(vec![3])),
            (Value::Bytes("\u{fffd}".as_bytes().to_vec()), Value::Bytes(vec![4])),
        ]))] };
        let map = value.opt_map_go_string::<GoString>(10).unwrap().unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map.get([0xff_u8].as_slice()).unwrap().as_bytes(), &[3]);
        assert_eq!(map.get([0xfe_u8].as_slice()).unwrap().as_bytes(), &[2]);
        assert_eq!(map.get("\u{fffd}".as_bytes()).unwrap().as_bytes(), &[4]);
    }

    #[test]
    fn struct_round_trip() {
        let mut f = Fields::new();
        f.sstr(10, "domain");
        f.oi32(20, Some(5));
        f.oi32(30, None);
        f.sbytes(40, b"payload");
        let s = f.finish();
        let mut w = Writer::new();
        write_struct(&mut w, &s);
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        let back = read_struct(&mut r).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.req_string(10, "").unwrap(), "domain");
        assert_eq!(back.opt_i32(20).unwrap(), Some(5));
        assert_eq!(back.opt_i32(30).unwrap(), None);
        assert_eq!(back.bytes_or_empty(40).unwrap(), b"payload");
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn empty_containers_encode_cleanly() {
        // Empty list/map with unknown elem types still round-trips.
        let mut w = Writer::new();
        write_value(&mut w, &Value::List(vec![]));
        write_value(&mut w, &Value::Map(vec![]));
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        assert_eq!(
            read_value(&mut r, TType::List).unwrap(),
            Value::List(vec![])
        );
        assert_eq!(read_value(&mut r, TType::Map).unwrap(), Value::Map(vec![]));
    }
}
