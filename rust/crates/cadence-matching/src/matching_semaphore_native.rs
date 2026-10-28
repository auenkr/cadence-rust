//! Byte exact Matching semaphore identifiers for native Go-compatible routes.
use cadence_common::go_string::GoString;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdentifierBytes {
    pub domain_id: GoString,
    pub semaphore_name: GoString,
    pub bucket: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifierError { MissingDomain, MissingName, NegativeBucket(i32) }

impl IdentifierBytes {
    pub fn new(domain_id: impl Into<GoString>, semaphore_name: impl Into<GoString>, bucket: i32) -> Result<Self, IdentifierError> {
        let value = Self { domain_id: domain_id.into(), semaphore_name: semaphore_name.into(), bucket };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), IdentifierError> {
        if self.domain_id.is_empty() { return Err(IdentifierError::MissingDomain); }
        if self.semaphore_name.is_empty() { return Err(IdentifierError::MissingName); }
        if self.bucket < 0 { return Err(IdentifierError::NegativeBucket(self.bucket)); }
        Ok(())
    }
    /// Go `common/semaphore.RingKey`: raw `domain_name_bucket` bytes.
    pub fn ring_key(&self) -> GoString {
        let mut bytes = Vec::with_capacity(self.domain_id.len() + self.semaphore_name.len() + 16);
        bytes.extend_from_slice(self.domain_id.as_bytes()); bytes.push(b'_');
        bytes.extend_from_slice(self.semaphore_name.as_bytes()); bytes.push(b'_');
        bytes.extend_from_slice(self.bucket.to_string().as_bytes());
        GoString::from(bytes)
    }
    pub fn to_log_string(&self) -> GoString {
        let mut bytes = Vec::with_capacity(self.domain_id.len() + self.semaphore_name.len() + 12);
        bytes.extend_from_slice(self.domain_id.as_bytes()); bytes.push(b'/');
        bytes.extend_from_slice(self.semaphore_name.as_bytes()); bytes.push(b'/');
        bytes.extend_from_slice(self.bucket.to_string().as_bytes());
        GoString::from(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn validation_and_ring_keys_preserve_raw_name_bytes() {
        assert_eq!(IdentifierBytes::new(b"d".to_vec(), b"\xff_\xef\xbf\xbd".to_vec(), 4).unwrap().ring_key().as_bytes(), b"d_\xff_\xef\xbf\xbd_4");
        assert_eq!(IdentifierBytes::new("", "s", 0), Err(IdentifierError::MissingDomain));
        assert_eq!(IdentifierBytes::new("d", "", 0), Err(IdentifierError::MissingName));
        assert_eq!(IdentifierBytes::new("d", "s", -1), Err(IdentifierError::NegativeBucket(-1)));
        let raw = IdentifierBytes::new("d", vec![0xfe], 1).unwrap();
        let replacement = IdentifierBytes::new("d", "�", 1).unwrap();
        assert_ne!(raw.ring_key(), replacement.ring_key());
        use std::hash::{Hash, Hasher};
        let mut a = std::collections::hash_map::DefaultHasher::new(); raw.ring_key().hash(&mut a);
        let mut b = std::collections::hash_map::DefaultHasher::new(); replacement.ring_key().hash(&mut b);
        assert_ne!(a.finish(), b.finish());
    }
}
