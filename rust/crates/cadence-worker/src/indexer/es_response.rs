//! Bulk-response classification from Go `indexer/esProcessor.go`.

/// Go `isResponseSuccess`: 2xx, plus 409 (version conflict) and 404 (not found),
/// which count as done so the message is acknowledged.
pub fn is_response_success(status: i32) -> bool {
    (200..300).contains(&status) || status == 409 || status == 404
}

/// Go `isResponseRetriable`. These statuses stay queued and are retried until they
/// succeed: 408, 429, 500, 503 and 507.
pub fn is_response_retriable(status: i32) -> bool {
    matches!(status, 408 | 429 | 500 | 503 | 507)
}

/// Go `isDeleteRequest`. A delete is one bulk source part, which holds only the
/// action metadata; index and update requests have two parts.
pub fn is_delete_request(source_parts: usize) -> bool {
    source_parts == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_covers_2xx_and_conflict_or_not_found() {
        for status in 200..300 {
            assert!(is_response_success(status), "{status}");
        }
        for status in [409, 404] {
            assert!(is_response_success(status), "{status}");
        }
        for status in [100, 199, 300, 400, 500, 408, 429, 503, 507] {
            assert!(!is_response_success(status), "{status}");
        }
    }

    #[test]
    fn retriable_statuses_match_go_list() {
        for status in [408, 429, 500, 503, 507] {
            assert!(is_response_retriable(status), "{status}");
        }
        for status in [400, 404, 409, 200] {
            assert!(!is_response_retriable(status), "{status}");
        }
    }

    #[test]
    fn delete_is_a_single_source_part() {
        assert!(is_delete_request(1));
        assert!(!is_delete_request(2));
        assert!(!is_delete_request(0));
    }
}
