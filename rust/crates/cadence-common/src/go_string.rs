//! Go strings are byte sequences. UTF-8 is not an invariant of a Matching
//! identifier, including on the pinned Go Thrift and gogo protobuf paths.

use std::borrow::Borrow;
use std::str::Utf8Error;

/// Lossless representation for Go `string` values on Matching's Thrift path.
///
/// Equality, ordering and hashing operate on the original bytes. In particular,
/// invalid UTF-8 never becomes the Unicode replacement character. Conversion
/// to Rust's public protobuf `String` fields requires explicit UTF-8 validation;
/// native Go-compatible transport paths must preserve the original bytes.
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

    /// Quote these bytes as a Go `encoding/json` string value.
    ///
    /// Go replaces each invalid UTF-8 byte separately before JSON escaping.
    /// This returns the quoted JSON text (including its surrounding quotes);
    /// it does not define serde serialization for `GoString`.
    pub fn go_json_string(&self) -> String {
        let mut out = String::with_capacity(self.0.len() + 2);
        out.push('"');
        let mut offset = 0;
        while offset < self.0.len() {
            let first = self.0[offset];
            let width = match first {
                0x00..=0x7f => 1,
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => 0,
            };
            let decoded = (width != 0)
                .then(|| self.0.get(offset..offset + width))
                .flatten()
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .and_then(|s| s.chars().next());
            if let Some(ch) = decoded {
                push_go_json_char(&mut out, ch);
                offset += width;
            } else {
                // Go emits an escape for invalid bytes but writes a valid
                // UTF-8 replacement rune literally. Preserve that distinction.
                out.push_str("\\ufffd");
                offset += 1;
            }
        }
        out.push('"');
        out
    }
}

fn push_go_json_char(out: &mut String, ch: char) {
    match ch {
        '"' => out.push_str("\\\""),
        '\\' => out.push_str("\\\\"),
        '\u{0008}' => out.push_str("\\b"),
        '\u{000c}' => out.push_str("\\f"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        '<' => out.push_str("\\u003c"),
        '>' => out.push_str("\\u003e"),
        '&' => out.push_str("\\u0026"),
        '\u{2028}' => out.push_str("\\u2028"),
        '\u{2029}' => out.push_str("\\u2029"),
        c if c <= '\u{001f}' => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let n = c as usize;
            out.push_str("\\u00");
            out.push(HEX[n >> 4] as char);
            out.push(HEX[n & 0xf] as char);
        }
        c => out.push(c),
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

    fn check_json(bytes: &[u8], expected: &str) {
        assert_eq!(GoString::from(bytes).go_json_string(), expected);
    }

    #[test]
    fn go_json_string_matches_encoding_json_for_malformed_sequences() {
        check_json(&[0xff], "\"\\ufffd\"");
        check_json(&[0xe1, 0x80, 0xff], "\"\\ufffd\\ufffd\\ufffd\"");
        check_json(&[0xc0, 0xaf], "\"\\ufffd\\ufffd\"");
        check_json(&[0xe2, 0x82], "\"\\ufffd\\ufffd\"");
        check_json(&[0xf0, 0x28, 0x8c, 0x28], "\"\\ufffd(\\ufffd(\"");
        check_json(&[0xef, 0xbf, 0xbd], "\"�\"");
    }

    #[test]
    fn go_json_string_matches_encoding_json_escaping() {
        let bytes: Vec<u8> = (0..=255).collect();
        let value = GoString::from(bytes);
        let quoted = value.go_json_string();
        let parsed: String = serde_json::from_str(&quoted).unwrap();
        assert_eq!(parsed.chars().count(), 256);
        assert_eq!(
            parsed.chars().take(128).collect::<String>().as_bytes(),
            &(0u8..=127).collect::<Vec<_>>()
        );
        check_json(b"<>&", "\"\\u003c\\u003e\\u0026\"");
        check_json("\u{2028}\u{2029}".as_bytes(), "\"\\u2028\\u2029\"");
        check_json("é雪🙂".as_bytes(), "\"é雪🙂\"");
        check_json(
            b"\0\x08\x0c\n\r\t\"\\",
            "\"\\u0000\\b\\f\\n\\r\\t\\\"\\\\\"",
        );
    }

    #[test]
    fn go_json_string_matches_configured_go_oracle_fixture() {
        let Ok(path) = std::env::var("MATCHING_GO_RAW_STRING_ORACLE") else {
            return;
        };
        let fixture = std::fs::read_to_string(path).unwrap();
        let rows: Vec<serde_json::Value> = serde_json::from_str(&fixture).unwrap();
        assert!(!rows.is_empty(), "Go raw string oracle fixture was empty");
        for row in rows {
            let hex = row["hex"].as_str().expect("oracle row hex");
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>();
            let want = row["jsonText"].as_str().expect("oracle row jsonText");
            assert_eq!(
                GoString::from(bytes).go_json_string(),
                want,
                "oracle input {hex}"
            );
        }
    }

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
