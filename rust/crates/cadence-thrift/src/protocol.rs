//! Thrift binary protocol codec (Apache-compatible, strict envelopes).
//!
//! Implements the same wire format as `go.uber.org/thriftrw/protocol/binary`,
//! which is what Cadence's YARPC Thrift handlers speak. Field IDs come from
//! the IDL (`go/.gen/.../rawIDL`); JSON names are irrelevant on this path.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unexpected end of buffer")]
    Truncated,
    #[error("invalid thrift data: {0}")]
    Invalid(String),
    #[error("unsupported strict version {0:#x}")]
    BadVersion(i32),
    #[error("unknown message type {0}")]
    BadMessageType(i32),
    #[error("utf-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Strict-mode version marker (`0x80010000`).
pub const VERSION_1: i32 = -2147418112;
const VERSION_MASK: i32 = -65536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum MessageType {
    Call = 1,
    Reply = 2,
    Exception = 3,
    Oneway = 4,
}

impl MessageType {
    fn from_i32(v: i32) -> Result<Self> {
        match v {
            1 => Ok(Self::Call),
            2 => Ok(Self::Reply),
            3 => Ok(Self::Exception),
            4 => Ok(Self::Oneway),
            _ => Err(Error::BadMessageType(v)),
        }
    }
}

/// Thrift field/container type bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TType {
    Stop = 0,
    Bool = 2,
    Byte = 3,
    Double = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    String = 11,
    Struct = 12,
    Map = 13,
    Set = 14,
    List = 15,
}

impl TType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Stop),
            2 => Ok(Self::Bool),
            3 => Ok(Self::Byte),
            4 => Ok(Self::Double),
            6 => Ok(Self::I16),
            8 => Ok(Self::I32),
            10 => Ok(Self::I64),
            11 => Ok(Self::String),
            12 => Ok(Self::Struct),
            13 => Ok(Self::Map),
            14 => Ok(Self::Set),
            15 => Ok(Self::List),
            _ => Err(Error::Invalid(format!("unknown ttype {v}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Truncated);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_i16(&mut self) -> Result<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn read_f64(&mut self) -> Result<f64> {
        let b = self.take(8)?;
        Ok(f64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_i32()?;
        if len < 0 {
            return Err(Error::Invalid(format!("negative length {len}")));
        }
        self.take(len as usize)
    }

    pub fn read_string(&mut self) -> Result<String> {
        Ok(String::from_utf8(self.read_bytes()?.to_vec())?)
    }

    /// Returns `(name, type, seqid)`, accepting strict and non-strict forms.
    pub fn read_message_begin(&mut self) -> Result<(String, MessageType, i32)> {
        let first = self.read_i32()?;
        if first < 0 {
            if first & VERSION_MASK != VERSION_1 {
                return Err(Error::BadVersion(first));
            }
            let ty = MessageType::from_i32(first & 0xff)?;
            let name = self.read_string()?;
            let seqid = self.read_i32()?;
            Ok((name, ty, seqid))
        } else {
            if first < 0 {
                return Err(Error::Invalid("negative name length".to_string()));
            }
            let name = String::from_utf8(self.take(first as usize)?.to_vec())?;
            let ty = MessageType::from_i32(i32::from(self.read_u8()?))?;
            let seqid = self.read_i32()?;
            Ok((name, ty, seqid))
        }
    }

    /// Returns `None` on STOP.
    pub fn read_field_begin(&mut self) -> Result<Option<(TType, i16)>> {
        let ty = TType::from_u8(self.read_u8()?)?;
        if ty == TType::Stop {
            return Ok(None);
        }
        let id = self.read_i16()?;
        Ok(Some((ty, id)))
    }

    pub fn read_list_begin(&mut self) -> Result<(TType, usize)> {
        let ty = TType::from_u8(self.read_u8()?)?;
        let len = self.read_i32()?;
        if len < 0 {
            return Err(Error::Invalid(format!("negative list length {len}")));
        }
        Ok((ty, len as usize))
    }

    pub fn read_map_begin(&mut self) -> Result<(TType, TType, usize)> {
        let kt = TType::from_u8(self.read_u8()?)?;
        let vt = TType::from_u8(self.read_u8()?)?;
        let len = self.read_i32()?;
        if len < 0 {
            return Err(Error::Invalid(format!("negative map length {len}")));
        }
        Ok((kt, vt, len as usize))
    }

    /// Skip one value of the given type (unknown fields stay forward-compatible).
    pub fn skip(&mut self, ty: TType) -> Result<()> {
        match ty {
            TType::Stop => Ok(()),
            TType::Bool | TType::Byte => {
                self.take(1)?;
                Ok(())
            }
            TType::Double | TType::I64 => {
                self.take(8)?;
                Ok(())
            }
            TType::I16 => {
                self.take(2)?;
                Ok(())
            }
            TType::I32 => {
                self.take(4)?;
                Ok(())
            }
            TType::String => {
                self.read_bytes()?;
                Ok(())
            }
            TType::Struct => {
                while let Some((fty, _)) = self.read_field_begin()? {
                    self.skip(fty)?;
                }
                Ok(())
            }
            TType::List | TType::Set => {
                let (ety, len) = self.read_list_begin()?;
                for _ in 0..len {
                    self.skip(ety)?;
                }
                Ok(())
            }
            TType::Map => {
                let (kt, vt, len) = self.read_map_begin()?;
                for _ in 0..len {
                    self.skip(kt)?;
                    self.skip(vt)?;
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(u8::from(v));
    }

    pub fn write_i16(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn write_bytes(&mut self, v: &[u8]) {
        self.write_i32(v.len() as i32);
        self.buf.extend_from_slice(v);
    }

    pub fn write_string(&mut self, v: &str) {
        self.write_bytes(v.as_bytes());
    }

    /// Strict message envelope, like thriftrw's binary protocol.
    pub fn write_message_begin(&mut self, name: &str, ty: MessageType, seqid: i32) {
        self.write_i32(VERSION_1 | (ty as i32));
        self.write_string(name);
        self.write_i32(seqid);
    }

    pub fn write_field_begin(&mut self, ty: TType, id: i16) {
        self.write_u8(ty as u8);
        self.write_i16(id);
    }

    pub fn write_field_stop(&mut self) {
        self.write_u8(TType::Stop as u8);
    }

    pub fn write_list_begin(&mut self, elem: TType, len: usize) {
        self.write_u8(elem as u8);
        self.write_i32(len as i32);
    }

    pub fn write_map_begin(&mut self, key: TType, val: TType, len: usize) {
        self.write_u8(key as u8);
        self.write_u8(val as u8);
        self.write_i32(len as i32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_envelope_round_trip() {
        let mut w = Writer::new();
        w.write_message_begin("StartWorkflowExecution", MessageType::Call, 7);
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        let (name, ty, seq) = r.read_message_begin().unwrap();
        assert_eq!(name, "StartWorkflowExecution");
        assert_eq!(ty, MessageType::Call);
        assert_eq!(seq, 7);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn primitives_round_trip() {
        let mut w = Writer::new();
        w.write_bool(true);
        w.write_i16(-5);
        w.write_i32(123456);
        w.write_i64(-9876543210);
        w.write_f64(2.5);
        w.write_string("héllo");
        w.write_bytes(b"\x00\xff binary");
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        assert!(r.read_bool().unwrap());
        assert_eq!(r.read_i16().unwrap(), -5);
        assert_eq!(r.read_i32().unwrap(), 123456);
        assert_eq!(r.read_i64().unwrap(), -9876543210);
        assert_eq!(r.read_f64().unwrap(), 2.5);
        assert_eq!(r.read_string().unwrap(), "héllo");
        assert_eq!(r.read_bytes().unwrap(), b"\x00\xff binary");
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn truncated_is_an_error() {
        let mut r = Reader::new(b"\x00\x01");
        assert!(matches!(r.read_i32(), Err(Error::Truncated)));
    }

    #[test]
    fn skip_unknown_struct_fields() {
        // struct { 99: i32 5, 1: string "a", STOP }
        let mut w = Writer::new();
        w.write_field_begin(TType::I32, 99);
        w.write_i32(5);
        w.write_field_begin(TType::String, 1);
        w.write_string("a");
        w.write_field_stop();
        let bytes = w.into_bytes();
        let mut r = Reader::new(&bytes);
        let mut seen = Vec::new();
        while let Some((ty, id)) = r.read_field_begin().unwrap() {
            if id == 1 {
                seen.push(r.read_string().unwrap());
            } else {
                r.skip(ty).unwrap();
            }
        }
        assert_eq!(seen, vec!["a".to_string()]);
    }
}
