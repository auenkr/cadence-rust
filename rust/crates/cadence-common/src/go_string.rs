//! Go strings are byte sequences. UTF-8 is a transport constraint, not an
//! invariant of a Thrift string or a Matching identifier.

use std::borrow::Borrow;
use std::str::Utf8Error;

/// Lossless representation for Go `string` values on Matching's Thrift path.
///
/// Equality, ordering and hashing operate on the original bytes. In particular,
/// invalid UTF-8 never becomes the Unicode replacement character. Callers must
/// explicitly validate UTF-8 when crossing a protobuf string boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoString(Vec<u8>);

impl GoString {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn as_utf8(&self) -> Result<&str, Utf8Error> {
        std::str::from_utf8(&self.0)
    }

    pub fn into_utf8(self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.0)
    }

    /// Go's `len(string)` counts bytes, including malformed UTF-8 bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for GoString {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for GoString {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<String> for GoString {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl From<&str> for GoString {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

impl AsRef<[u8]> for GoString {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for GoString {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::GoString;
    use std::collections::{BTreeSet, HashMap};

    #[test]
    fn go_string_preserves_every_byte_and_requires_explicit_utf8_validation() {
        let bytes: Vec<u8> = (0..=255).collect();
        let value = GoString::from(bytes.clone());
        assert_eq!(value.len(), 256);
        assert!(value.as_utf8().is_err());
        let error = value.into_utf8().unwrap_err();
        assert_eq!(error.into_bytes(), bytes);
        assert_eq!(GoString::from("é").len(), 2);
        assert_eq!(GoString::from("é").as_utf8().unwrap(), "é");
    }

    #[test]
    fn go_string_invalid_bytes_never_alias_replacement_character_or_each_other() {
        let values = [
            GoString::from(vec![0xff]),
            GoString::from(vec![0xfe]),
            GoString::from("\u{fffd}"),
            GoString::from(vec![0]),
            GoString::default(),
        ];
        let mut map = HashMap::new();
        for (index, value) in values.into_iter().enumerate() {
            map.insert(value, index);
        }
        assert_eq!(map.len(), 5);
        assert_eq!(map.get([0xff].as_slice()), Some(&0));
        assert_eq!(map.get([0xfe].as_slice()), Some(&1));
        assert_eq!(map.get("\u{fffd}".as_bytes()), Some(&2));
        let ordered: BTreeSet<_> = map.into_keys().collect();
        let bytes: Vec<_> = ordered.into_iter().map(GoString::into_bytes).collect();
        assert_eq!(
            bytes,
            vec![
                vec![],
                vec![0],
                vec![0xef, 0xbf, 0xbd],
                vec![0xfe],
                vec![0xff]
            ]
        );
    }
}
