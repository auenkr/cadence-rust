//! Base64 helpers matching Go `encoding/json` `[]byte` semantics:
//! byte slices serialize as base64 strings.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;

/// Encode a byte slice using Go `encoding/json`'s standard base64 spelling.
pub fn encode_bytes(value: &[u8]) -> String {
    B64.encode(value)
}

pub fn serialize_bytes<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&B64.encode(v))
}

pub fn deserialize_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    let s = String::deserialize(d)?;
    B64.decode(s.as_bytes()).map_err(serde::de::Error::custom)
}

pub fn serialize_opt_bytes<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
    match v {
        Some(b) => s.serialize_str(&B64.encode(b)),
        None => s.serialize_none(),
    }
}

#[allow(dead_code)]
pub fn deserialize_opt_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
    Option::<String>::deserialize(d)?.map(|text| {
        B64.decode(text.as_bytes()).map_err(serde::de::Error::custom)
    }).transpose()
}

pub fn serialize_b64_map<S: Serializer>(
    v: &Option<HashMap<String, Vec<u8>>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match v {
        None => s.serialize_none(),
        Some(m) => {
            let enc: HashMap<&str, String> = m
                .iter()
                .map(|(k, val)| (k.as_str(), B64.encode(val)))
                .collect();
            enc.serialize(s)
        }
    }
}

pub fn deserialize_b64_map<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<HashMap<String, Vec<u8>>>, D::Error> {
    let m = Option::<HashMap<String, String>>::deserialize(d)?;
    m.map(|inner| {
        inner
            .into_iter()
            .map(|(k, v)| {
                B64.decode(v.as_bytes())
                    .map(|b| (k, b))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    })
    .transpose()
}
