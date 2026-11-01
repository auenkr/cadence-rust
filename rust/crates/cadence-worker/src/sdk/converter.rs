//! Go `DefaultDataConverter` compatible encoding: every value is JSON followed
//! by a newline; multiple arguments are concatenated.

use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    let mut data = serde_json::to_vec(value)?;
    data.push(b'\n');
    Ok(data)
}

/// Encode several values back to back (Go `encodeArgs`).
pub fn encode_args(values: &[serde_json::Value]) -> Result<Vec<u8>, serde_json::Error> {
    let mut out = Vec::new();
    for v in values {
        out.extend(encode(v)?);
    }
    Ok(out)
}

/// Decode the first value in `data`.
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T, serde_json::Error> {
    let mut stream = serde_json::Deserializer::from_slice(data).into_iter::<T>();
    match stream.next() {
        Some(r) => r,
        None => serde_json::from_slice::<T>(b""),
    }
}

/// Decode every JSON value in `data`.
pub fn decode_args(data: &[u8]) -> Result<Vec<serde_json::Value>, serde_json::Error> {
    serde_json::Deserializer::from_slice(data)
        .into_iter::<serde_json::Value>()
        .collect()
}
