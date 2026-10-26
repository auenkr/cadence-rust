//! Collection payload codec for Cassandra native protocol v2 and later.
//!
//! A collection payload consists of a count followed by length-prefixed
//! elements. Protocol v2 uses unsigned 16-bit count and length fields;
//! protocol v3 and later use signed 32-bit fields. This is deliberately a
//! standalone boundary: the frame/body layer must select the negotiated
//! protocol and call these functions for collection cells in both directions.

use std::fmt;

/// Native protocol version relevant to collection payload widths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionProtocol {
    /// Cassandra native protocol v2 (short collection sizes).
    V2,
    /// Cassandra native protocol v3 or newer (int collection sizes).
    V3OrNewer,
}

/// Shape of a collection payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectionKind {
    /// List or set: each element is a byte string.
    List,
    /// Set: each element is a byte string, using the same layout as a list.
    Set,
    /// Map: alternating key and value byte strings.
    Map,
}

/// Failure to encode or decode a collection payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionCodecError {
    /// Count or item length exceeds the selected protocol's representable size.
    SizeOverflow,
    /// Payload ended before a complete field was available.
    Truncated,
    /// A v3+ field contained a negative size.
    NegativeSize,
    /// A map payload declared an odd number of key/value elements.
    OddMapElementCount,
    /// Trailing bytes remained after the declared collection was decoded.
    TrailingBytes,
}

impl fmt::Display for CollectionCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SizeOverflow => f.write_str("CQL collection count or item exceeds protocol limit"),
            Self::Truncated => f.write_str("truncated CQL collection payload"),
            Self::NegativeSize => f.write_str("negative CQL collection size"),
            Self::OddMapElementCount => f.write_str("CQL map has an odd element count"),
            Self::TrailingBytes => f.write_str("trailing bytes in CQL collection payload"),
        }
    }
}

impl std::error::Error for CollectionCodecError {}

/// Encodes one list/set or map payload. Map values must alternate key, value.
/// Nested collection values are supplied as their already encoded payloads.
pub fn encode_collection(
    protocol: CollectionProtocol,
    kind: CollectionKind,
    elements: &[&[u8]],
) -> Result<Vec<u8>, CollectionCodecError> {
    if kind == CollectionKind::Map && elements.len() % 2 != 0 {
        return Err(CollectionCodecError::OddMapElementCount);
    }
    let mut out = Vec::new();
    let count = if kind == CollectionKind::Map {
        elements.len() / 2
    } else {
        elements.len()
    };
    write_size(protocol, count, &mut out)?;
    for element in elements {
        write_size(protocol, element.len(), &mut out)?;
        out.extend_from_slice(element);
    }
    Ok(out)
}

/// Decodes one list/set or map payload, returning borrowed element bytes.
pub fn decode_collection<'a>(
    protocol: CollectionProtocol,
    kind: CollectionKind,
    payload: &'a [u8],
) -> Result<Vec<&'a [u8]>, CollectionCodecError> {
    let mut cursor = 0;
    let count = read_size(protocol, payload, &mut cursor)?;
    let count = if kind == CollectionKind::Map {
        count.checked_mul(2).ok_or(CollectionCodecError::SizeOverflow)?
    } else {
        count
    };
    // Every entry needs at least a length prefix. Bound allocation by the
    // available payload before trusting a server-provided collection count.
    let width = if protocol == CollectionProtocol::V2 { 2 } else { 4 };
    if count > payload.len().saturating_sub(cursor) / width {
        return Err(CollectionCodecError::Truncated);
    }
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_size(protocol, payload, &mut cursor)?;
        let end = cursor.checked_add(len).ok_or(CollectionCodecError::Truncated)?;
        let bytes = payload.get(cursor..end).ok_or(CollectionCodecError::Truncated)?;
        elements.push(bytes);
        cursor = end;
    }
    if cursor != payload.len() {
        return Err(CollectionCodecError::TrailingBytes);
    }
    Ok(elements)
}

fn write_size(protocol: CollectionProtocol, n: usize, out: &mut Vec<u8>) -> Result<(), CollectionCodecError> {
    match protocol {
        CollectionProtocol::V2 => out.extend_from_slice(&u16::try_from(n).map_err(|_| CollectionCodecError::SizeOverflow)?.to_be_bytes()),
        CollectionProtocol::V3OrNewer => out.extend_from_slice(&i32::try_from(n).map_err(|_| CollectionCodecError::SizeOverflow)?.to_be_bytes()),
    }
    Ok(())
}

fn read_size(protocol: CollectionProtocol, payload: &[u8], cursor: &mut usize) -> Result<usize, CollectionCodecError> {
    match protocol {
        CollectionProtocol::V2 => {
            let end = cursor.checked_add(2).ok_or(CollectionCodecError::Truncated)?;
            let raw: [u8; 2] = payload.get(*cursor..end).ok_or(CollectionCodecError::Truncated)?.try_into().map_err(|_| CollectionCodecError::Truncated)?;
            *cursor = end;
            Ok(u16::from_be_bytes(raw) as usize)
        }
        CollectionProtocol::V3OrNewer => {
            let end = cursor.checked_add(4).ok_or(CollectionCodecError::Truncated)?;
            let raw: [u8; 4] = payload.get(*cursor..end).ok_or(CollectionCodecError::Truncated)?.try_into().map_err(|_| CollectionCodecError::Truncated)?;
            *cursor = end;
            usize::try_from(i32::from_be_bytes(raw)).map_err(|_| CollectionCodecError::NegativeSize)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(p: CollectionProtocol, k: CollectionKind, items: &[&[u8]], expected: &[u8]) {
        let encoded = encode_collection(p, k, items).unwrap();
        assert_eq!(encoded, expected);
        assert_eq!(decode_collection(p, k, &encoded).unwrap(), items);
    }

    #[test]
    fn protocol_v2_bytes_cover_empty_multiple_and_nested_list_set_map() {
        roundtrip(CollectionProtocol::V2, CollectionKind::List, &[], &[0, 0]);
        roundtrip(CollectionProtocol::V2, CollectionKind::List, &[b""], &[0, 1, 0, 0]);
        roundtrip(CollectionProtocol::V2, CollectionKind::List, &[b"a", b"bc"], &[0, 2, 0, 1, b'a', 0, 2, b'b', b'c']);
        roundtrip(CollectionProtocol::V2, CollectionKind::Map, &[], &[0, 0]);
        roundtrip(CollectionProtocol::V2, CollectionKind::Map, &[b"k", b"v", b"x", b"y"], &[0, 2, 0, 1, b'k', 0, 1, b'v', 0, 1, b'x', 0, 1, b'y']);
        roundtrip(CollectionProtocol::V2, CollectionKind::Set, &[b"a", b"b"], &[0, 2, 0, 1, b'a', 0, 1, b'b']);
        let nested = encode_collection(CollectionProtocol::V2, CollectionKind::List, &[b"z"]).unwrap();
        roundtrip(CollectionProtocol::V2, CollectionKind::Set, &[&nested], &[0, 1, 0, 5, 0, 1, 0, 1, b'z']);
        roundtrip(CollectionProtocol::V2, CollectionKind::Map, &[b"k", &nested], &[0, 1, 0, 1, b'k', 0, 5, 0, 1, 0, 1, b'z']);
    }

    #[test]
    fn protocol_v3_bytes_cover_empty_multiple_and_nested_list_map() {
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::List, &[], &[0, 0, 0, 0]);
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::List, &[b""], &[0, 0, 0, 1, 0, 0, 0, 0]);
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::List, &[b"a", b"bc"], &[0, 0, 0, 2, 0, 0, 0, 1, b'a', 0, 0, 0, 2, b'b', b'c']);
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::Map, &[], &[0, 0, 0, 0]);
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::Map, &[b"k", b"v"], &[0, 0, 0, 1, 0, 0, 0, 1, b'k', 0, 0, 0, 1, b'v']);
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::Set, &[b"a", b"b"], &[0, 0, 0, 2, 0, 0, 0, 1, b'a', 0, 0, 0, 1, b'b']);
        let nested = encode_collection(CollectionProtocol::V3OrNewer, CollectionKind::List, &[b"z"]).unwrap();
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::Set, &[&nested], &[0, 0, 0, 1, 0, 0, 0, 9, 0, 0, 0, 1, 0, 0, 0, 1, b'z']);
        roundtrip(CollectionProtocol::V3OrNewer, CollectionKind::Map, &[b"k", &nested], &[0, 0, 0, 1, 0, 0, 0, 1, b'k', 0, 0, 0, 9, 0, 0, 0, 1, 0, 0, 0, 1, b'z']);
    }

    #[test]
    fn rejects_overflow_malformed_map_negative_and_trailing_bytes() {
        let too_big = vec![b'x'; u16::MAX as usize + 1];
        assert_eq!(encode_collection(CollectionProtocol::V2, CollectionKind::List, &[&too_big]), Err(CollectionCodecError::SizeOverflow));
        assert_eq!(encode_collection(CollectionProtocol::V2, CollectionKind::List, &vec![b""; u16::MAX as usize + 1]), Err(CollectionCodecError::SizeOverflow));
        assert_eq!(encode_collection(CollectionProtocol::V2, CollectionKind::Map, &[b"k"]), Err(CollectionCodecError::OddMapElementCount));
        assert_eq!(decode_collection(CollectionProtocol::V2, CollectionKind::List, &[0]), Err(CollectionCodecError::Truncated));
        assert_eq!(decode_collection(CollectionProtocol::V3OrNewer, CollectionKind::List, &(-1i32).to_be_bytes()), Err(CollectionCodecError::NegativeSize));
        assert_eq!(decode_collection(CollectionProtocol::V2, CollectionKind::List, &[0, 0, 99]), Err(CollectionCodecError::TrailingBytes));
    }
}
