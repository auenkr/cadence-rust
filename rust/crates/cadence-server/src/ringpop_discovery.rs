//! Go-compatible static-host and JSON-file Ringpop seed discovery.
//! DNS and DNS-SRV modes remain deliberately unimplemented here.

use crate::config::{BootstrapMode, RingpopConfig};
use serde::de::{SeqAccess, Visitor};
use std::fmt;
use std::{fs, path::PathBuf};

pub trait DiscoverProvider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error(transparent)] Io(#[from] std::io::Error),
    #[error("JSON syntax error: {0}")]
    JsonSyntax(serde_json::Error),
    #[error("JSON value has the wrong type: {0}")]
    JsonType(serde_json::Error),
    #[error("unknown bootstrap mode {0:?}")]
    UnsupportedMode(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticHosts { hosts: Vec<String> }

impl StaticHosts {
    pub fn new(hosts: impl IntoIterator<Item = String>) -> Self { Self { hosts: hosts.into_iter().collect() } }
}

impl DiscoverProvider for StaticHosts {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> { Ok(self.hosts.clone()) }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonFile { path: PathBuf }

impl JsonFile {
    pub fn new(path: impl Into<PathBuf>) -> Self { Self { path: path.into() } }
    pub fn path(&self) -> &std::path::Path { &self.path }
}

impl DiscoverProvider for JsonFile {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        let data = fs::read(&self.path)?;
        // encoding/json unmarshals JSON null into a nil []string, and null
        // array elements into empty strings. serde's Vec<String> rejects both.
        // Go's encoding/json replaces malformed UTF-8 inside JSON strings
        // with U+FFFD. Keep malformed bytes outside strings intact so they
        // remain syntax errors instead of being reinterpreted as string data.
        let normalized = replace_invalid_utf8_in_strings(&data);
        // encoding/json scans the whole document before decoding its target,
        // so a syntax error later in the document takes precedence over an
        // earlier target-type mismatch. Validate the raw JSON first; RawValue
        // does not parse numeric tokens into bounded machine numbers.
        serde_json::from_slice::<Box<serde_json::value::RawValue>>(&normalized)
            .map_err(DiscoveryError::JsonSyntax)?;
        if normalized
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| byte == b'-' || byte.is_ascii_digit())
        {
            // Avoid parsing a root number into serde_json's bounded f64/i64
            // representation. Go sees a valid number and reports that the
            // target []string has the wrong type, even for huge exponents.
            return Err(DiscoveryError::JsonType(
                <serde_json::Error as serde::de::Error>::custom(
                    "expected array or null, got number",
                ),
            ));
        }
        serde_json::from_slice::<GoStringList>(&normalized)
            .map(|hosts| hosts.0)
            .map_err(|error| {
                if error.is_data() {
                    DiscoveryError::JsonType(error)
                } else {
                    DiscoveryError::JsonSyntax(error)
                }
            })
    }
}

fn replace_invalid_utf8_in_strings(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut in_string = false;
    let mut escaped = false;
    let mut index = 0;

    while index < input.len() {
        let byte = input[index];
        if in_string && !escaped && byte == b'\\' {
            if let Some(first) = json_unicode_escape(input, index) {
                if (0xD800..=0xDBFF).contains(&first) {
                    if let Some(second) = json_unicode_escape(input, index + 6) {
                        if (0xDC00..=0xDFFF).contains(&second) {
                            output.extend_from_slice(&input[index..index + 12]);
                            index += 12;
                            continue;
                        }
                    }
                    output.extend_from_slice(br"\uFFFD");
                    index += 6;
                    continue;
                }
                if (0xDC00..=0xDFFF).contains(&first) {
                    output.extend_from_slice(br"\uFFFD");
                    index += 6;
                    continue;
                }
            }
        }
        if byte < 0x80 {
            output.push(byte);
            index += 1;
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
            } else if byte == b'"' {
                in_string = true;
            }
            continue;
        }

        let width = match byte {
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => 1,
        };
        let candidate_end = index.saturating_add(width).min(input.len());
        let sequence = &input[index..candidate_end];
        if width > 1 && sequence.len() == width && std::str::from_utf8(sequence).is_ok() {
            output.extend_from_slice(sequence);
            index = candidate_end;
        } else if in_string && !escaped {
            output.extend_from_slice("�".as_bytes());
            // encoding/json consumes one invalid leading byte at a time, so
            // malformed continuation and truncated sequences can produce
            // more than one replacement character.
            index += std::str::from_utf8(sequence)
                .err()
                .and_then(|error| error.error_len())
                .unwrap_or(1);
        } else {
            output.extend_from_slice(sequence);
            index = candidate_end;
        }
    }
    output
}

fn json_unicode_escape(input: &[u8], start: usize) -> Option<u16> {
    let escape = input.get(start..start + 6)?;
    if escape[0] != b'\\' || escape[1] != b'u' {
        return None;
    }
    let mut value = 0u16;
    for byte in &escape[2..] {
        value = (value << 4) | match byte {
            b'0'..=b'9' => (byte - b'0') as u16,
            b'a'..=b'f' => (byte - b'a' + 10) as u16,
            b'A'..=b'F' => (byte - b'A' + 10) as u16,
            _ => return None,
        };
    }
    Some(value)
}

struct GoStringList(Vec<String>);

impl<'de> serde::Deserialize<'de> for GoStringList {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ListVisitor;
        impl<'de> Visitor<'de> for ListVisitor {
            type Value = GoStringList;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON array of strings or null")
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(GoStringList(Vec::new()))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut hosts = Vec::new();
                let mut first_type_error = None;
                while let Some(raw) = seq.next_element::<Box<serde_json::value::RawValue>>()? {
                    match raw.get() {
                        "null" => hosts.push(String::new()),
                        raw if raw.starts_with('"') => {
                            hosts.push(serde_json::from_str(raw).map_err(serde::de::Error::custom)?);
                        }
                        raw => {
                            first_type_error.get_or_insert_with(|| {
                                format!("expected string or null, got {}", json_token_type(raw))
                            });
                        }
                    }
                }
                if let Some(error) = first_type_error {
                    return Err(serde::de::Error::custom(error));
                }
                Ok(GoStringList(hosts))
            }

            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self::Value, E> {
                Err(E::custom("expected array or null, got boolean"))
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self::Value, E> { Err(E::custom("expected array or null, got number")) }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self::Value, E> { Err(E::custom("expected array or null, got number")) }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self::Value, E> { Err(E::custom("expected array or null, got number")) }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> { Err(E::custom("expected array or null, got string")) }
            fn visit_string<E: serde::de::Error>(self, _: String) -> Result<Self::Value, E> { Err(E::custom("expected array or null, got string")) }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, _: A) -> Result<Self::Value, A::Error> { Err(serde::de::Error::custom("expected array or null, got object")) }
    }

        deserializer.deserialize_any(ListVisitor)
    }
}

fn json_token_type(raw: &str) -> &'static str {
    match raw.as_bytes().first() {
        Some(b'"') => "string", Some(b'[') => "array", Some(b'{') => "object",
        Some(b't' | b'f') => "boolean", Some(b'n') => "null", _ => "number",
    }
}

pub enum Provider { Static(StaticHosts), File(JsonFile) }

impl Provider {
    pub fn from_config(config: &RingpopConfig) -> Result<Self, DiscoveryError> {
        match config.bootstrap_mode {
            BootstrapMode::Hosts => Ok(Self::Static(StaticHosts::new(config.bootstrap_hosts.clone()))),
            BootstrapMode::File => Ok(Self::File(JsonFile::new(config.bootstrap_file.clone()))),
            BootstrapMode::Dns => Err(DiscoveryError::UnsupportedMode("dns".into())),
            BootstrapMode::DnsSrv => Err(DiscoveryError::UnsupportedMode("dns-srv".into())),
            BootstrapMode::Custom => Err(DiscoveryError::UnsupportedMode("custom".into())),
            BootstrapMode::None => Err(DiscoveryError::UnsupportedMode("0".into())),
        }
    }
}

impl DiscoverProvider for Provider {
    fn hosts(&self) -> Result<Vec<String>, DiscoveryError> {
        match self { Self::Static(provider) => provider.hosts(), Self::File(provider) => provider.hosts() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn static_hosts_preserve_order_duplicates_and_strings() {
        let p = StaticHosts::new([" host:1 ".to_string(), "host:1".into(), "raw".into()]);
        assert_eq!(p.hosts().unwrap(), [" host:1 ", "host:1", "raw"]);
        assert_eq!(p.hosts().unwrap(), [" host:1 ", "host:1", "raw"]);
    }

    #[test]
    fn file_reads_each_time_and_preserves_array_contents() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(br#"["a:1","a:1"," raw "]"#).unwrap();
        let p = JsonFile::new(f.path());
        assert_eq!(p.hosts().unwrap(), ["a:1", "a:1", " raw "]);
        fs::write(f.path(), br#"["updated:2"]"#).unwrap();
        assert_eq!(p.hosts().unwrap(), ["updated:2"]);
        fs::write(f.path(), br#"["a","#).unwrap();
        assert!(matches!(p.hosts(), Err(DiscoveryError::JsonSyntax(_))));
        fs::write(f.path(), b"{}").unwrap();
        assert!(matches!(p.hosts(), Err(DiscoveryError::JsonType(_))));
    }

    #[test]
    fn file_matches_go_string_replacement_and_number_type_errors() {
        let f = tempfile::NamedTempFile::new().unwrap();
        // Go encoding/json replaces malformed UTF-8 bytes and escaped lone
        // UTF-16 surrogates in JSON strings with U+FFFD.
        fs::write(f.path(), b"[\"\xff\",\"\\ud800\"]").unwrap();
        assert_eq!(JsonFile::new(f.path()).hosts().unwrap(), ["�", "�"]);

        // Even an exponent outside f64's range is valid JSON syntax. Go
        // reports a target-type error when decoding it into []string.
        fs::write(f.path(), b"[1e100000]").unwrap();
        assert!(matches!(JsonFile::new(f.path()).hosts(), Err(DiscoveryError::JsonType(_))));
    }

    #[test]
    fn malformed_utf8_outside_strings_remains_json_syntax_error() {
        let f = tempfile::NamedTempFile::new().unwrap();
        fs::write(f.path(), b"[\xff]").unwrap();
        assert!(matches!(JsonFile::new(f.path()).hosts(), Err(DiscoveryError::JsonSyntax(_))));
    }

    #[test]
    fn missing_file_is_io_error_and_config_selects_supported_provider() {
        assert!(matches!(JsonFile::new("/does/not/exist").hosts(), Err(DiscoveryError::Io(_))));
        let cfg = crate::config::Config::from_yaml("ringpop: {name: matching, bootstrapMode: hosts, bootstrapHosts: [' x ', 'x']}").unwrap();
        let provider = Provider::from_config(&cfg.ringpop).unwrap();
        assert_eq!(provider.hosts().unwrap(), [" x ", "x"]);
    }

    #[test]
    fn matches_actual_go_provider_oracle_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("../../../../tests/fixtures/ringpop_bootstrap_provider.json")).unwrap();
        let inputs: serde_json::Value = serde_json::from_str(include_str!("../../../../tests/fixtures/ringpop_bootstrap_provider_inputs.json")).unwrap();
        assert_eq!(inputs["fileInputs"], fixture["fileInputs"]);
        let mixed_mode = crate::config::Config::from_yaml("ringpop: {name: x, bootstrapMode: HoStS}").unwrap();
        assert_eq!(mixed_mode.ringpop.bootstrap_mode as i32, fixture["bootstrapModes"]["mixed"]["mode"].as_i64().unwrap() as i32);
        let upper_mode = crate::config::Config::from_yaml("ringpop: {name: x, bootstrapMode: DNS-SRV}").unwrap();
        assert_eq!(upper_mode.ringpop.bootstrap_mode as i32, fixture["bootstrapModes"]["upper"]["mode"].as_i64().unwrap() as i32);
        let unknown_mode = crate::config::Config::from_yaml("ringpop: {name: x, bootstrapMode: WeIrD}").unwrap_err().to_string();
        assert!(unknown_mode.contains(fixture["bootstrapModes"]["unknown"]["error"].as_str().unwrap()));
        for (raw, expected) in inputs["fileInputs"].as_array().unwrap().iter().zip(fixture["fileObservations"].as_array().unwrap()) {
            let f = tempfile::NamedTempFile::new().unwrap();
            fs::write(f.path(), raw.as_str().unwrap()).unwrap();
            let actual = JsonFile::new(f.path()).hosts();
            match expected["errorType"].as_str().unwrap() {
                "" => assert_eq!(actual.unwrap(), expected["hosts"].as_array().map_or_else(Vec::new, |values| values.iter().map(|value| value.as_str().unwrap().to_owned()).collect()), "raw input must match Go provider result"),
                "*json.SyntaxError" => assert!(matches!(actual, Err(DiscoveryError::JsonSyntax(_))), "raw input {raw:?}: {actual:?}"),
                "*json.UnmarshalTypeError" => assert!(matches!(actual, Err(DiscoveryError::JsonType(_))), "raw input {raw:?}: {actual:?}"),
                other => panic!("unhandled Go error category {other}"),
            }
        }
        for (name, raw) in [
            ("invalidUtf8InString", b"[\"\xff\"]".as_slice()),
            ("invalidUtf8ContinuationInString", b"[\"\xe2(\xa1\"]".as_slice()),
            ("escapedLoneSurrogate", br#"["\ud800"]"#.as_slice()),
            ("invalidUtf8OutsideString", b"[\xff]".as_slice()),
            ("largeExponent", b"[1e100000]".as_slice()),
            ("largeInteger", b"[999999999999999999999999999999999999999999999999999999]".as_slice()),
            ("topLevelLargeExponent", b"1e100000".as_slice()),
            ("topLevelLargeInteger", b"999999999999999999999999999999999999999999999999999999".as_slice()),
        ] {
            let f = tempfile::NamedTempFile::new().unwrap();
            fs::write(f.path(), raw).unwrap();
            let actual = JsonFile::new(f.path()).hosts();
            let expected = &fixture["byteObservations"][name];
            match expected["errorType"].as_str().unwrap() {
                "" => assert_eq!(actual.unwrap(), expected["hosts"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_owned()).collect::<Vec<_>>()),
                "*json.SyntaxError" => assert!(matches!(actual, Err(DiscoveryError::JsonSyntax(_))), "{name}: {actual:?}"),
                "*json.UnmarshalTypeError" => assert!(matches!(actual, Err(DiscoveryError::JsonType(_))), "{name}: {actual:?}"),
                other => panic!("unhandled Go byte-input error category {other}"),
            }
        }
        let empty = tempfile::NamedTempFile::new().unwrap();
        fs::write(empty.path(), b"[]").unwrap();
        assert!(JsonFile::new(empty.path()).hosts().unwrap().is_empty());
        let static_hosts: Vec<String> = serde_json::from_value(fixture["static"].clone()).unwrap();
        assert_eq!(StaticHosts::new(static_hosts).hosts().unwrap(), serde_json::from_value::<Vec<String>>(fixture["static"].clone()).unwrap());
        let file_hosts: Vec<String> = serde_json::from_value(fixture["file"].clone()).unwrap();
        let f = tempfile::NamedTempFile::new().unwrap();
        fs::write(f.path(), serde_json::to_vec(&file_hosts).unwrap()).unwrap();
        let file = JsonFile::new(f.path());
        assert_eq!(file.hosts().unwrap(), serde_json::from_value::<Vec<String>>(fixture["file"].clone()).unwrap());
        let updated: Vec<String> = serde_json::from_value(fixture["fileAfterUpdate"].clone()).unwrap();
        fs::write(f.path(), serde_json::to_vec(&updated).unwrap()).unwrap();
        assert_eq!(file.hosts().unwrap(), updated);
        fs::write(f.path(), b"{}").unwrap();
        assert!(matches!(file.hosts(), Err(DiscoveryError::JsonType(_))));
        let malformed_error = fixture["malformedErrorType"].as_str().unwrap();
        assert_eq!(malformed_error, "*json.SyntaxError");
        fs::write(f.path(), br#"["a","#).unwrap();
        assert!(matches!(file.hosts(), Err(DiscoveryError::JsonSyntax(_))));
        let file_config = RingpopConfig { bootstrap_mode: BootstrapMode::File, bootstrap_file: f.path().to_string_lossy().into_owned(), ..Default::default() };
        assert!(matches!(Provider::from_config(&file_config).unwrap(), Provider::File(_)));
        let cfg = crate::config::Config::from_yaml("ringpop: {name: matching, bootstrapMode: hosts, bootstrapHosts: [' x ', x, x]}").unwrap();
        assert_eq!(cfg.ringpop.bootstrap_hosts, serde_json::from_value::<Vec<String>>(fixture["configHosts"].clone()).unwrap());
        let mut cfg = cfg;
        cfg.validate_ringpop().unwrap();
        assert_eq!(cfg.ringpop.max_join_duration_nanos, fixture["configMaxJoinDurationNanos"].as_i64().unwrap());
        let config_errors = &fixture["configErrors"];
        let mut missing_name = crate::config::RingpopConfig::default();
        assert_eq!(missing_name.validate().unwrap_err().to_string(), config_errors["missingName"]);
        let mut unknown = crate::config::RingpopConfig { name: "x".into(), ..Default::default() };
        assert_eq!(unknown.validate().unwrap_err().to_string(), config_errors["unknownMode"]);
        let mut missing_file = crate::config::RingpopConfig { name: "x".into(), bootstrap_mode: BootstrapMode::File, ..Default::default() };
        assert_eq!(missing_file.validate().unwrap_err().to_string(), config_errors["missingFile"]);
        let mut missing_hosts = crate::config::RingpopConfig { name: "x".into(), bootstrap_mode: BootstrapMode::Hosts, ..Default::default() };
        assert_eq!(missing_hosts.validate().unwrap_err().to_string(), config_errors["missingHosts"]);
        let mut custom = crate::config::RingpopConfig { name: "x".into(), bootstrap_mode: BootstrapMode::Custom, ..Default::default() };
        assert_eq!(custom.validate().unwrap_err().to_string(), config_errors["custom"]);
        for (key, source) in [("max", "9223372036854775807ns"), ("min", "-9223372036854775808ns"), ("fraction", "1.000000001s"), ("preciseThirdHour", "0.333333333333333333h"), ("preciseAlmostHour", "0.999999999999999999h")] {
            let config: crate::config::Config = crate::config::Config::from_yaml(&format!("ringpop: {{name: x, bootstrapMode: hosts, bootstrapHosts: [x], maxJoinDuration: {source}}}")).unwrap();
            assert_eq!(config.ringpop.max_join_duration_nanos, fixture["durations"][key].as_i64().unwrap());
        }
    }
}
