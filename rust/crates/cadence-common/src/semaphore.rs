//! Semaphore owner-ID codec ported from `go/common/semaphore/owner.go`.
//!
//! An owner identifies one hold: a workflow run plus the history event that
//! started the acquire. The canonical string form is
//! `<byteLen(WorkflowID)> ":" <WorkflowID> ":" <RunID> ":" <HoldID>`; the
//! length prefix is what makes the encoding splittable when the workflow ID
//! itself contains separators. One hold has exactly one byte form, so the
//! parser only accepts the spellings the encoder produces.

const SEPARATOR: char = ':';

/// Identifies one semaphore hold: a specific run and which of its holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Owner {
    pub workflow_id: String,
    pub run_id: String,
    pub hold_id: i64,
}

impl Owner {
    /// Canonical owner_id string. Cannot fail.
    pub fn encode(&self) -> String {
        format!(
            "{}{}{}{}{}{}{}",
            self.workflow_id.len(),
            SEPARATOR,
            self.workflow_id,
            SEPARATOR,
            self.run_id,
            SEPARATOR,
            self.hold_id
        )
    }
}

/// Split an owner_id back into its three ids. Only the canonical spellings
/// produced by [`Owner::encode`] are accepted.
pub fn parse_owner(owner_id: &str) -> Result<Owner, String> {
    let err = |msg: String| Err(format!("owner_id {owner_id:?} {msg}"));
    let Some(sep) = owner_id.find(SEPARATOR) else {
        return err("has no length prefix".to_string());
    };
    let prefix = &owner_id[..sep];
    let length: usize = match prefix.parse() {
        Ok(length) => length,
        Err(_) => return err(format!("has an invalid length prefix {prefix:?}")),
    };
    if prefix != length.to_string() {
        return err(format!("has a non-canonical length prefix {prefix:?}"));
    }
    let rest = &owner_id[sep + 1..];
    // The declared length must fit in rest, with a byte to spare for the
    // separator. Lengths near usize::MAX must not wrap past this guard.
    if length.saturating_add(1) > rest.len() {
        return err(format!(
            "is shorter than its length prefix {length} claims"
        ));
    }
    let workflow_id = rest[..length].to_string();
    let tail = &rest[length..];
    let Some(tail) = tail.strip_prefix(SEPARATOR) else {
        return err(format!(
            "has no separator after its {length}-byte workflow id"
        ));
    };
    // From the right: HoldID is decimal, so the last separator always sits
    // in front of it; the run id is whatever lies between.
    let Some(mid) = tail.rfind(SEPARATOR) else {
        return err("has no separator between run id and hold id".to_string());
    };
    let run_id = tail[..mid].to_string();
    let hold_text = &tail[mid + 1..];
    let hold_id: i64 = match hold_text.parse() {
        Ok(hold_id) => hold_id,
        Err(_) => return err(format!("has an invalid hold id {hold_text:?}")),
    };
    if hold_text != hold_id.to_string() {
        return err(format!("has a non-canonical hold id {hold_text:?}"));
    }
    Ok(Owner {
        workflow_id,
        run_id,
        hold_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ported from go/common/semaphore/owner_test.go TestOwnerStringRoundTrip.
    #[test]
    fn owner_string_round_trip_matches_go() {
        let cases = [
            (
                Owner {
                    workflow_id: "wf-1".into(),
                    run_id: "run-1".into(),
                    hold_id: 7,
                },
                "4:wf-1:run-1:7",
            ),
            (
                Owner {
                    workflow_id: "a:b".into(),
                    run_id: "run-1".into(),
                    hold_id: 7,
                },
                "3:a:b:run-1:7",
            ),
            (
                Owner {
                    workflow_id: ":::".into(),
                    run_id: "run-1".into(),
                    hold_id: 7,
                },
                "3:::::run-1:7",
            ),
            (
                Owner {
                    workflow_id: ":wf:".into(),
                    run_id: "run-1".into(),
                    hold_id: 1,
                },
                "4::wf::run-1:1",
            ),
            (
                Owner {
                    workflow_id: "12:xy".into(),
                    run_id: "run-1".into(),
                    hold_id: 2,
                },
                "5:12:xy:run-1:2",
            ),
            (
                Owner {
                    workflow_id: "".into(),
                    run_id: "run-1".into(),
                    hold_id: 3,
                },
                "0::run-1:3",
            ),
            (
                Owner {
                    workflow_id: "wf".into(),
                    run_id: "run:1".into(),
                    hold_id: 7,
                },
                "2:wf:run:1:7",
            ),
            (
                Owner {
                    workflow_id: "a:b".into(),
                    run_id: ":c:".into(),
                    hold_id: -9,
                },
                "3:a:b::c::-9",
            ),
            (
                Owner {
                    workflow_id: "wf".into(),
                    run_id: "3f2504e0-4f89-11d3-9a0c-0305e82c3301".into(),
                    hold_id: 12345,
                },
                "2:wf:3f2504e0-4f89-11d3-9a0c-0305e82c3301:12345",
            ),
            (Owner::default(), "0:::0"),
            (
                Owner {
                    workflow_id: "wf".into(),
                    run_id: "run-1".into(),
                    hold_id: 0,
                },
                "2:wf:run-1:0",
            ),
            (
                Owner {
                    workflow_id: "wf".into(),
                    run_id: "run-1".into(),
                    hold_id: i64::MAX,
                },
                "2:wf:run-1:9223372036854775807",
            ),
            (
                Owner {
                    workflow_id: "wf".into(),
                    run_id: "run-1".into(),
                    hold_id: i64::MIN,
                },
                "2:wf:run-1:-9223372036854775808",
            ),
        ];
        for (owner, want) in cases {
            let got = owner.encode();
            assert_eq!(got, want, "encoding must be byte-for-byte stable");
            assert_eq!(parse_owner(&got).unwrap(), owner, "must round-trip");
        }
    }

    // Ported from go/common/semaphore/owner_test.go
    // TestParseOwnerRejectsMalformed.
    #[test]
    fn parse_owner_rejects_malformed() {
        for id in [
            "",
            "nonsense",
            "x:wf:run-1:1",
            "-1:wf:run-1:1",
            "004:wf-1:run-1:1",
            "+4:wf-1:run-1:1",
            "99:wf:run-1:1",
            "9223372036854775807:wf:run:1",
            "9223372036854775806:wf:run:1",
            "99999999999999999999:wf:run:1",
            "2:wf",
            "2:wfrun-1:1",
            "2:wf:run-1",
            "2:wf:run-1:abc",
            "2:wf:run-1:007",
            "2:wf:run-1:9223372036854775808",
            "2:wf:run-1:1:",
        ] {
            assert!(parse_owner(id).is_err(), "{id:?} must be rejected");
        }
    }

    // Byte lengths, not rune counts: a parser slicing by runes passes ASCII
    // cases and comes apart on multibyte workflow IDs.
    #[test]
    fn length_prefix_counts_bytes() {
        let owner = Owner {
            workflow_id: "é".into(),
            run_id: "r".into(),
            hold_id: 1,
        };
        assert_eq!(owner.encode(), "2:é:r:1");
        assert_eq!(parse_owner("2:é:r:1").unwrap(), owner);
    }
}
