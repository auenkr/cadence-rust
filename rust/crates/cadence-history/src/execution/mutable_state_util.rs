//! Pure mutable-state helpers corresponding to `mutable_state_util.go`.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResetPoint { pub binary_checksum: String, pub expiring_time_unix_nanos: i64, pub resettable: bool }
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BadBinary { pub reason: String }

/// Keep the newest reset points and their corresponding binary checksums,
/// reserving a slot for the current run exactly as Go's `trimBinaryChecksums`.
pub fn trim_binary_checksums(mut recent_checksums: Vec<String>, mut points: Vec<ResetPoint>, max_reset_points: usize) -> (Vec<String>, Vec<ResetPoint>) {
    if points.len() >= max_reset_points {
        let start = points.len().saturating_sub(max_reset_points).saturating_add(1);
        points = points.into_iter().skip(start).collect();
        recent_checksums = recent_checksums.into_iter().skip(start).collect();
    }
    (recent_checksums, points)
}

/// Return the first valid reset point backed by a bad-binary entry.
pub fn find_auto_reset_point<'a>(
    now_unix_nanos: i64,
    bad_binaries: &'a std::collections::BTreeMap<String, BadBinary>,
    reset_points: &'a [ResetPoint],
) -> Option<(&'a str, &'a ResetPoint)> {
    reset_points.iter().find_map(|point| {
        if !point.resettable || (point.expiring_time_unix_nanos > 0 && now_unix_nanos > point.expiring_time_unix_nanos) { return None; }
        bad_binaries.get(&point.binary_checksum).map(|binary| (binary.reason.as_str(), point))
    })
}

pub trait DomainLookup {
    type Error;
    fn domain_id_to_name(&self, id: &str) -> Result<String, Self::Error>;
    fn domain_name_to_id(&self, name: &str) -> Result<String, Self::Error>;
}

/// Resolve child domain name with the same legacy-field fallback order as Go.
pub fn child_domain_name<L: DomainLookup>(domain_id: &str, deprecated_domain_name: &str, parent_domain_name: &str, lookup: &L) -> Result<String, L::Error> {
    if !domain_id.is_empty() { return lookup.domain_id_to_name(domain_id); }
    if !deprecated_domain_name.is_empty() { return Ok(deprecated_domain_name.to_owned()); }
    Ok(parent_domain_name.to_owned())
}

pub fn child_domain_id<L: DomainLookup>(domain_id: &str, deprecated_domain_name: &str, parent_domain_id: &str, lookup: &L) -> Result<String, L::Error> {
    if !domain_id.is_empty() { return Ok(domain_id.to_owned()); }
    if !deprecated_domain_name.is_empty() { return lookup.domain_name_to_id(deprecated_domain_name); }
    Ok(parent_domain_id.to_owned())
}
