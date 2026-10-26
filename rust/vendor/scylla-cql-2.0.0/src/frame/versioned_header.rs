//! Version-aware native protocol frame headers. This foundation deliberately does not
//! enable v2/v3 session negotiation in the Scylla driver.
use std::io::{self, Read};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum CqlProtocolVersion { V2 = 2, V3 = 3, V4 = 4 }

impl CqlProtocolVersion {
    pub const fn header_len(self) -> usize { if matches!(self, Self::V2) { 8 } else { 9 } }
    pub fn from_wire(v: u8) -> io::Result<(Self, Direction)> {
        let direction = if v & 0x80 == 0 { Direction::Request } else { Direction::Response };
        let version = match v & 0x7f { 2 => Self::V2, 3 => Self::V3, 4 => Self::V4,
            x => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("unsupported CQL protocol {x}"))) };
        Ok((version, direction))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Direction { Request, Response }

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Header { pub version: CqlProtocolVersion, pub direction: Direction, pub flags: u8, pub stream: i16, pub opcode: u8, pub length: u32 }

impl Header {
    pub fn encode(self) -> io::Result<Vec<u8>> {
        validate_flags(self.version, self.flags)?;
        validate_length(self.length)?;
        if self.version == CqlProtocolVersion::V2 && !(-128..=127).contains(&self.stream) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "v2 stream must fit signed i8"));
        }
        let mut out = Vec::with_capacity(self.version.header_len());
        out.push(self.version as u8 | if self.direction == Direction::Response { 0x80 } else { 0 });
        out.push(self.flags);
        if self.version == CqlProtocolVersion::V2 { out.push(self.stream as i8 as u8); }
        else { out.extend_from_slice(&self.stream.to_be_bytes()); }
        out.push(self.opcode);
        out.extend_from_slice(&self.length.to_be_bytes());
        Ok(out)
    }
    pub fn decode<R: Read>(reader: &mut R, expected: Direction) -> io::Result<Self> {
        let mut prefix = [0; 2]; reader.read_exact(&mut prefix)?;
        let (version, direction) = CqlProtocolVersion::from_wire(prefix[0])?;
        if direction != expected { return Err(io::Error::new(io::ErrorKind::InvalidData, "frame direction mismatch")); }
        validate_flags(version, prefix[1])?;
        let mut tail = [0; 7];
        let n = if version == CqlProtocolVersion::V2 { 6 } else { 7 };
        reader.read_exact(&mut tail[..n])?;
        let (stream, opcode, len_at) = if version == CqlProtocolVersion::V2 { (tail[0] as i8 as i16, tail[1], 2) }
            else { (i16::from_be_bytes([tail[0], tail[1]]), tail[2], 3) };
        let length = u32::from_be_bytes(tail[len_at..len_at+4].try_into().unwrap());
        validate_length(length)?;
        Ok(Self { version, direction, flags: prefix[1], stream, opcode, length })
    }
}

fn validate_length(length: u32) -> io::Result<()> {
    if length > i32::MAX as u32 {
        Err(io::Error::new(io::ErrorKind::InvalidData, "negative CQL frame length"))
    } else {
        Ok(())
    }
}

fn validate_flags(version: CqlProtocolVersion, flags: u8) -> io::Result<()> {
    let allowed = match version { CqlProtocolVersion::V2 => 0x03, CqlProtocolVersion::V3 => 0x03, CqlProtocolVersion::V4 => 0x0f };
    if flags & !allowed != 0 { Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported frame flags")) } else { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn signed_stream_boundaries_round_trip() {
        for (v, streams) in [(CqlProtocolVersion::V2, vec![-128, -1, 0, 127]), (CqlProtocolVersion::V3, vec![-32768, -1, 0, 32767]), (CqlProtocolVersion::V4, vec![-32768, -1, 0, 32767])] {
            for stream in streams { let h = Header { version:v,direction:Direction::Response,flags:0,stream,opcode:8,length:0x12345678 }; let bytes=h.encode().unwrap(); assert_eq!(bytes.len(),v.header_len()); assert_eq!(Header::decode(&mut bytes.as_slice(),Direction::Response).unwrap(),h); }
        }
        let h=Header{version:CqlProtocolVersion::V2,direction:Direction::Request,flags:0,stream:128,opcode:7,length:0}; assert!(h.encode().is_err());
    }
    #[test]
    fn negative_wire_lengths_are_rejected() {
        for version in [CqlProtocolVersion::V2, CqlProtocolVersion::V3, CqlProtocolVersion::V4] {
            let header = Header { version, direction: Direction::Response,
                flags: 0, stream: 0, opcode: 8, length: 0 };
            let mut bytes = header.encode().unwrap();
            let offset = bytes.len() - 4;
            bytes[offset..].copy_from_slice(&u32::MAX.to_be_bytes());
            assert!(Header::decode(&mut bytes.as_slice(), Direction::Response).is_err());
            assert!(Header { length: u32::MAX, ..header }.encode().is_err());
        }
    }
    #[test]
    fn custom_payload_flag_requires_v4() {
        let header = Header { version: CqlProtocolVersion::V3,
            direction: Direction::Response, flags: 0x04,
            stream: 0, opcode: 8, length: 0 };
        assert!(header.encode().is_err());
        assert!(Header { version: CqlProtocolVersion::V4, ..header }.encode().is_ok());
    }
    #[test] fn fragmented_read_and_direction_flags_are_checked() {
        struct Fragment<'a>(&'a [u8]); impl Read for Fragment<'_> { fn read(&mut self,b:&mut [u8])->io::Result<usize>{ if self.0.is_empty(){return Ok(0)} let n=1.min(b.len()); b[..n].copy_from_slice(&self.0[..n]); self.0=&self.0[n..]; Ok(n) } }
        let h=Header{version:CqlProtocolVersion::V3,direction:Direction::Response,flags:0,stream:-2,opcode:2,length:9}; let b=h.encode().unwrap(); assert_eq!(Header::decode(&mut Fragment(&b),Direction::Response).unwrap(),h);
        assert!(Header::decode(&mut b.as_slice(),Direction::Request).is_err()); let mut bad=b; bad[1]=0x80; assert!(Header::decode(&mut bad.as_slice(),Direction::Response).is_err());
    }
}
