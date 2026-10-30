//! Byte-preserving Cassandra semaphore adapter for native Matching paths.
//!
//! CQL text identifiers are bound from their original GoString bytes. Cassandra
//! validates `text` as UTF-8; values returned by it are therefore copied from
//! the driver's validated text bytes without normalization.

use super::CassandraSemaphorePersistence;
use crate::{
    semaphores::{
        AcquireError, GrantOutcome, SemaphoreError, SemaphoreMetadata, DEFAULT_BUCKET_SIZE,
        MAX_BUCKET_SIZE, SEMAPHORE_SCAN_PAGE_SIZE_MESSAGE,
    },
    semaphores_native::{SemaphoreBucketPageBytes, SemaphoreCallContext, SemaphorePersistenceBytes, SemaphoreRowBytes},
};
use async_trait::async_trait;
use cadence_common::go_string::GoString;
use chrono::{DateTime, Utc};
use scylla::{
    cluster::metadata::{ColumnType, NativeType},
    response::PagingState,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
        SerializationError,
    },
    statement::Statement,
};
use std::{fmt, ops::ControlFlow};
use uuid::Uuid;

const TOKEN: i32 = 1;
const OWNER: i32 = 2;
const OWNER_TOKEN: i32 = -1;
const OWNER_NONE: &str = "0::30000000-0000-f000-f000-000000000000:-1";
const FREE: &str = "0::30000000-0000-f000-f000-000000000000:-2";
const META_INSERT: &str = "INSERT INTO semaphore_metadata (domain_id, semaphore_name, size, bucket_size, created_time) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS";
const META_GET: &str =
    "SELECT size, bucket_size FROM semaphore_metadata WHERE domain_id = ? AND semaphore_name = ?";
const BUCKET_GET: &str = "SELECT type, token_id, owner_id, holder, held_token FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ?";
const TOKEN_GET: &str = "SELECT holder FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ?";
const OWNER_GET: &str = "SELECT held_token FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ?";
const GRANT: &str = "UPDATE semaphore_tokens SET holder = ?, updated_time = ? WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ? IF holder IN (?, null)";
const INSERT_OWNER: &str = "INSERT INTO semaphore_tokens (domain_id, semaphore_name, bucket, type, token_id, owner_id, holder, held_token, updated_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";
const RELEASE: &str = "UPDATE semaphore_tokens SET holder = ?, updated_time = ? WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ? IF holder = ?";
const DELETE_OWNER: &str = "DELETE FROM semaphore_tokens WHERE domain_id = ? AND semaphore_name = ? AND bucket = ? AND type = ? AND token_id = ? AND owner_id = ?";

#[derive(Debug)]
struct RawText(Vec<u8>);
#[derive(Debug)]
struct SerError;
impl fmt::Display for SerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("raw CQL text serialization error")
    }
}
impl std::error::Error for SerError {}
impl SerializeValue for RawText {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        if !matches!(
            typ,
            ColumnType::Native(NativeType::Text | NativeType::Ascii)
        ) {
            return Err(SerializationError::new(SerError));
        }
        writer
            .set_value(&self.0)
            .map_err(|_| SerializationError::new(SerError))
    }
}
fn raw(value: &GoString) -> RawText {
    RawText(value.as_bytes().to_vec())
}
fn domain(value: &GoString) -> Result<Uuid, AcquireError> {
    let text = std::str::from_utf8(value.as_bytes())
        .map_err(|e| AcquireError::Invalid(format!("invalid domainID bytes: {e}")))?;
    Uuid::parse_str(text)
        .map_err(|e| AcquireError::Invalid(format!("invalid domainID {text:?}: {e}")))
}
fn internal(e: impl fmt::Display) -> AcquireError {
    AcquireError::Internal(e.to_string())
}
fn missing(s: String) -> AcquireError {
    AcquireError::NotFound(cadence_api::errors::EntityNotExistsError::new(s))
}

impl CassandraSemaphorePersistence {
    async fn owner_token_bytes(
        &self,
        domain: Uuid,
        name: &GoString,
        bucket: i32,
        owner: &GoString,
    ) -> Result<i32, AcquireError> {
        let res = self
            .session
            .query_unpaged(
                OWNER_GET,
                (domain, raw(name), bucket, OWNER, OWNER_TOKEN, raw(owner)),
            )
            .await
            .map_err(internal)?;
        res.into_rows_result()
            .map_err(internal)?
            .maybe_first_row::<(i32,)>()
            .map_err(internal)?
            .map(|(v,)| v)
            .ok_or_else(|| {
                missing(format!(
                    "semaphore owner {} does not hold a token in bucket {bucket}",
                    owner.go_quoted_string()
                ))
            })
    }
    async fn holder_bytes(
        &self,
        domain: Uuid,
        name: &GoString,
        bucket: i32,
        token: i32,
    ) -> Result<GoString, AcquireError> {
        let res = self
            .session
            .query_unpaged(TOKEN_GET, (domain, raw(name), bucket, TOKEN, token))
            .await
            .map_err(internal)?;
        let value = res
            .into_rows_result()
            .map_err(internal)?
            .maybe_first_row::<(Option<String>,)>()
            .map_err(internal)?
            .ok_or_else(|| {
                missing(format!(
                    "semaphore token {token} does not exist in bucket {bucket}"
                ))
            })?
            .0;
        let bytes = value.filter(|v| v != FREE).unwrap_or_default().into_bytes();
        Ok(GoString::from(bytes))
    }
}

// Dropping a Scylla request future stops waiting for the response, but cannot
// undo a request already sent to Cassandra. A cancelled LWT is ambiguous, as
// with gocql's WithContext; never retry it here. Poll the result first when
// both become ready so a received applied result is retained by the manager.
async fn caller_io<T>(
    context: &SemaphoreCallContext,
    operation: impl std::future::Future<Output = Result<T, AcquireError>>,
) -> Result<T, AcquireError> {
    tokio::select! {
        biased;
        result = operation => result,
        _ = context.wait() => Err(AcquireError::Internal("context canceled".into())),
    }
}

#[async_trait]
impl SemaphorePersistenceBytes for CassandraSemaphorePersistence {
    async fn create_semaphore(
        &self,
        domain_id: &GoString,
        name: &GoString,
        size: i32,
        bucket_size: i32,
    ) -> Result<SemaphoreMetadata, SemaphoreError> {
        if size <= 0 {
            return Err(SemaphoreError::Invalid(format!(
                "Size must be positive, got {size}"
            )));
        }
        if !(0..=MAX_BUCKET_SIZE).contains(&bucket_size) {
            return Err(SemaphoreError::Invalid(format!(
                "BucketSize must not exceed {MAX_BUCKET_SIZE}, got {bucket_size}"
            )));
        }
        let did = domain(domain_id).map_err(|e| SemaphoreError::Invalid(e.to_string()))?;
        let bs = if bucket_size == 0 {
            DEFAULT_BUCKET_SIZE
        } else {
            bucket_size
        };
        let now: DateTime<Utc> = Utc::now();
        let res = self
            .session
            .query_unpaged(META_INSERT, (did, raw(name), size, bs, now))
            .await
            .map_err(|e| SemaphoreError::Internal(e.to_string()))?;
        let rows = res
            .into_rows_result()
            .map_err(|e| SemaphoreError::Internal(e.to_string()))?;
        // Cassandra's successful INSERT IF NOT EXISTS response contains only
        // `[applied]`. On a conflict it appends the existing row columns. Do
        // not decode the success shape as though nullable existing-row columns
        // were always present: Scylla checks tuple arity against result
        // metadata before decoding values.
        let applied = match rows.column_specs().len() {
            1 => rows
                .maybe_first_row::<(bool,)>()
                .map_err(|e| SemaphoreError::Internal(e.to_string()))?
                .map(|r| r.0)
                .unwrap_or(false),
            6 => rows
                .maybe_first_row::<(
                    bool,
                    Option<Uuid>,
                    Option<String>,
                    Option<i32>,
                    Option<i32>,
                    Option<DateTime<Utc>>,
                )>()
                .map_err(|e| SemaphoreError::Internal(e.to_string()))?
                .map(|r| r.0)
                .unwrap_or(false),
            columns => {
                return Err(SemaphoreError::Internal(format!(
                    "unexpected semaphore metadata LWT result shape: {columns} columns"
                )))
            }
        };
        if !applied {
            return Err(SemaphoreError::Exists(format!(
                "semaphore {} already exists",
                name.go_quoted_string()
            )));
        }
        Ok(SemaphoreMetadata {
            size,
            bucket_size: bs,
        })
    }
    async fn get_semaphore_metadata(
        &self,
        domain_id: &GoString,
        name: &GoString,
    ) -> Result<SemaphoreMetadata, AcquireError> {
        let did = domain(domain_id)?;
        let res = self
            .session
            .query_unpaged(META_GET, (did, raw(name)))
            .await
            .map_err(internal)?;
        res.into_rows_result()
            .map_err(internal)?
            .maybe_first_row::<(i32, i32)>()
            .map_err(internal)?
            .map(|(size, bucket_size)| SemaphoreMetadata { size, bucket_size })
            .ok_or_else(|| {
                missing(format!(
                    "semaphore {} does not exist in domain {did}",
                    name.go_quoted_string()
                ))
            })
    }
    async fn scan_semaphore_bucket(
        &self,
        domain_id: &GoString,
        name: &GoString,
        bucket: i32,
        page_size: usize,
        next: Option<&[u8]>,
    ) -> Result<SemaphoreBucketPageBytes, AcquireError> {
        let did = domain(domain_id)?;
        let page_size = i32::try_from(page_size)
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| AcquireError::Invalid(SEMAPHORE_SCAN_PAGE_SIZE_MESSAGE.into()))?;
        let mut st = Statement::new(BUCKET_GET);
        st.set_page_size(page_size);
        let state = next
            .map(|b| PagingState::new_from_raw_bytes(b.to_vec()))
            .unwrap_or_else(PagingState::start);
        let (res, paging) = self
            .session
            .query_single_page(st, (did, raw(name), bucket), state)
            .await
            .map_err(internal)?;
        let next_page_token = match paging.into_paging_control_flow() {
            ControlFlow::Continue(s) => s.as_bytes_slice().map(|bytes| bytes.to_vec()),
            ControlFlow::Break(()) => None,
        };
        let rows = res.into_rows_result().map_err(internal)?;
        let mut out = Vec::new();
        for row in rows
            .rows::<(i32, i32, Option<String>, Option<String>, Option<i32>)>()
            .map_err(internal)?
        {
            let (ty, token, owner, holder, held) = row.map_err(internal)?;
            match ty {
                TOKEN => out.push(SemaphoreRowBytes::Token {
                    token_id: token,
                    holder: GoString::from(
                        holder
                            .filter(|h| h != FREE)
                            .unwrap_or_default()
                            .into_bytes(),
                    ),
                }),
                OWNER => {
                    if let (Some(owner), Some(held_token)) = (owner, held) {
                        out.push(SemaphoreRowBytes::Owner {
                            owner_id: GoString::from(owner.into_bytes()),
                            held_token,
                        });
                    }
                }
                _ => out.push(SemaphoreRowBytes::Unknown),
            }
        }
        Ok(SemaphoreBucketPageBytes {
            rows: out,
            next_page_token,
        })
    }
    async fn get_semaphore_ownership_by_token(
        &self,
        d: &GoString,
        n: &GoString,
        b: i32,
        t: i32,
    ) -> Result<GoString, AcquireError> {
        self.holder_bytes(domain(d)?, n, b, t).await
    }
    async fn get_semaphore_ownership_by_owner(
        &self,
        d: &GoString,
        n: &GoString,
        b: i32,
        o: &GoString,
    ) -> Result<i32, AcquireError> {
        self.owner_token_bytes(domain(d)?, n, b, o).await
    }
    async fn grant_semaphore_token(
        &self,
        d: &GoString,
        n: &GoString,
        b: i32,
        t: i32,
        o: &GoString,
    ) -> Result<GrantOutcome, AcquireError> {
        let did = domain(d)?;
        match self.owner_token_bytes(did, n, b, o).await {
            Ok(existing) => return Ok(GrantOutcome::AlreadyHeld(existing)),
            Err(AcquireError::NotFound(_)) => (),
            Err(e) => return Err(e),
        }
        let mut batch =
            scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Logged);
        batch.append_statement(GRANT);
        batch.append_statement(INSERT_OWNER);
        let now: DateTime<Utc> = Utc::now();
        let result = self
            .session
            .batch(
                &batch,
                (
                    (
                        raw(o),
                        now,
                        did,
                        raw(n),
                        b,
                        TOKEN,
                        t,
                        RawText(OWNER_NONE.as_bytes().to_vec()),
                        RawText(FREE.as_bytes().to_vec()),
                    ),
                    (
                        did,
                        raw(n),
                        b,
                        OWNER,
                        OWNER_TOKEN,
                        raw(o),
                        Option::<String>::None,
                        t,
                        now,
                    ),
                ),
            )
            .await
            .map_err(internal)?;
        let rows = result.into_rows_result().map_err(internal)?;
        // A successful conditional batch may report only `[applied]`; a
        // failed batch reports the conditional row. The latter metadata shape
        // is that of the first (UPDATE ... IF holder ...) statement.
        let applied = match rows.column_specs().len() {
            1 => rows
                .maybe_first_row::<(bool,)>()
                .map_err(internal)?
                .map(|r| r.0)
                .unwrap_or(false),
            8 => rows
                .maybe_first_row::<(
                    bool,
                    Option<Uuid>,
                    Option<String>,
                    Option<i32>,
                    Option<i32>,
                    Option<i32>,
                    Option<String>,
                    Option<String>,
                )>()
                .map_err(internal)?
                .map(|r| r.0)
                .unwrap_or(false),
            columns => {
                return Err(AcquireError::Internal(format!(
                    "unexpected semaphore grant LWT result shape: {columns} columns"
                )))
            }
        };
        if applied {
            return Ok(GrantOutcome::Applied);
        }
        match self.owner_token_bytes(did, n, b, o).await {
            Ok(held) => Ok(GrantOutcome::AlreadyHeld(held)),
            Err(AcquireError::NotFound(_)) => Ok(GrantOutcome::SlotTaken),
            Err(e) => Err(e),
        }
    }
    async fn confirm_with_context(
        &self,
        d: &GoString,
        n: &GoString,
        b: i32,
        t: i32,
        context: &SemaphoreCallContext,
    ) -> Result<GoString, AcquireError> {
        caller_io(context, self.get_semaphore_ownership_by_token(d, n, b, t)).await
    }

    async fn grant_with_context(
        &self,
        d: &GoString,
        n: &GoString,
        b: i32,
        t: i32,
        o: &GoString,
        context: &SemaphoreCallContext,
    ) -> Result<GrantOutcome, AcquireError> {
        caller_io(context, self.grant_semaphore_token(d, n, b, t, o)).await
    }

    async fn release_semaphore_token(
        &self,
        d: &GoString,
        n: &GoString,
        b: i32,
        t: i32,
        o: &GoString,
    ) -> Result<bool, AcquireError> {
        let did = domain(d)?;
        if self.holder_bytes(did, n, b, t).await?.as_bytes() != o.as_bytes() {
            return Ok(false);
        }
        let mut batch =
            scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Logged);
        batch.append_statement(RELEASE);
        batch.append_statement(DELETE_OWNER);
        let now: DateTime<Utc> = Utc::now();
        let result = self
            .session
            .batch(
                &batch,
                (
                    (
                        RawText(FREE.as_bytes().to_vec()),
                        now,
                        did,
                        raw(n),
                        b,
                        TOKEN,
                        t,
                        RawText(OWNER_NONE.as_bytes().to_vec()),
                        raw(o),
                    ),
                    (did, raw(n), b, OWNER, OWNER_TOKEN, raw(o)),
                ),
            )
            .await
            .map_err(internal)?;
        let rows = result.into_rows_result().map_err(internal)?;
        let applied = match rows.column_specs().len() {
            1 => rows
                .maybe_first_row::<(bool,)>()
                .map_err(internal)?
                .map(|r| r.0)
                .unwrap_or(false),
            8 => rows
                .maybe_first_row::<(
                    bool,
                    Option<Uuid>,
                    Option<String>,
                    Option<i32>,
                    Option<i32>,
                    Option<i32>,
                    Option<String>,
                    Option<String>,
                )>()
                .map_err(internal)?
                .map(|r| r.0)
                .unwrap_or(false),
            columns => {
                return Err(AcquireError::Internal(format!(
                    "unexpected semaphore release LWT result shape: {columns} columns"
                )))
            }
        };
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scylla::serialize::writers::CellWriter;

    #[tokio::test]
    async fn caller_io_cancels_pending_and_keeps_ready_result() {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let context = SemaphoreCallContext { cancelled: Some(rx), ..Default::default() };
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let signal = entered.clone();
        let pending = async move {
            signal.notify_one();
            std::future::pending::<Result<(), AcquireError>>().await
        };
        let task = tokio::spawn(async move { caller_io(&context, pending).await });
        entered.notified().await;
        tx.send(true).unwrap();
        assert!(matches!(task.await.unwrap(), Err(AcquireError::Internal(_))));

        let (_tx, rx) = tokio::sync::watch::channel(true);
        let context = SemaphoreCallContext { cancelled: Some(rx), ..Default::default() };
        let result = caller_io(&context, async { Ok(GrantOutcome::Applied) }).await.unwrap();
        assert!(matches!(result, GrantOutcome::Applied));
    }

    #[test]
    fn raw_text_binding_keeps_go_string_bytes_exact() {
        let typ = ColumnType::Native(NativeType::Text);
        for bytes in [&b"\xff"[..], "�".as_bytes(), "e\u{301}".as_bytes(), b""] {
            let mut encoded = Vec::new();
            RawText(bytes.to_vec())
                .serialize(&typ, CellWriter::new(&mut encoded))
                .unwrap();
            assert_eq!(&encoded[..4], &(bytes.len() as i32).to_be_bytes());
            assert_eq!(&encoded[4..], bytes);
        }
        let mut encoded = Vec::new();
        assert!(RawText(vec![0xff])
            .serialize(
                &ColumnType::Native(NativeType::BigInt),
                CellWriter::new(&mut encoded)
            )
            .is_err());
    }

    #[test]
    fn go_string_values_round_trip_bytes_at_adapter_boundary() {
        let input = GoString::from(vec![b'a', 0xff, 0]);
        let bound = raw(&input);
        assert_eq!(bound.0, input.as_bytes());
        let read = GoString::from(vec![0xc3, 0xa9]);
        assert_eq!(read.as_bytes(), &[0xc3, 0xa9]);
    }
}
