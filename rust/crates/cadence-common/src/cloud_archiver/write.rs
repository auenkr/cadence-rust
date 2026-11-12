//! Archive (write) path of the Go `s3store` and `gcloud` archivers.
//!
//! Mirrors `go/common/archiver/s3store/{historyArchiver,visibilityArchiver,util}.go` `Archive`
//! and `go/common/archiver/gcloud/{historyArchiver,visibilityArchiver,util}.go` `Archive`
//! (uploads through `connector.Client.Upload`/`Exist`). History batches come from the same
//! [`HistoryBatchSource`] the filestore archiver uses; they are re-split into blobs of the Go
//! `targetHistoryBlobSize` (2MB of JSON) exactly like `archiver.HistoryIterator`.

use serde_json::{json, Value};

use super::{
    gcs_hash, gcs_history_filename, gcs_object, gcs_validate_uri,
    s3_bucket, s3_history_key, s3_key_path, s3_soft_validate_uri, s3_visibility_prefix,
    ObjectStore, StoreError, StoreErrorKind, ERR_BUCKET_NOT_EXISTS,
};
use crate::archiver::{
    validate_history_request, validate_visibility_request, ArchiveHistoryRequest,
    ArchiveVisibilityRequest, ArchiverError, ArchiverUri, HistoryBatchSource,
};

/// Go `targetHistoryBlobSize` (s3store / archiver iterator): 2MB.
pub const TARGET_HISTORY_BLOB_SIZE: usize = 2 * 1024 * 1024;
/// Go gcloud `errUploadNonRetriable`.
pub const GCS_ERR_UPLOAD_NON_RETRIABLE: &str = "upload non-retriable error";
/// Go gcloud `errRetriable`.
pub const GCS_ERR_RETRIABLE: &str = "retriable error";

const S3_INDEX_CLOSE: &str = "closeTimeout";
const S3_INDEX_START: &str = "startTimeout";
const S3_PRIMARY_TYPE: &str = "workflowTypeName";
const S3_PRIMARY_ID: &str = "workflowID";
const GCS_INDEX_CLOSE: &str = "closeTimeout";
const GCS_INDEX_START: &str = "startTimeout";

/// Go `time.Unix(0, ts).UTC().Format(time.RFC3339)`.
fn rfc3339_utc(ts: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ts)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// Go `archiver.historyIterator.readHistoryBatches`: a blob is closed as soon as its estimated
/// JSON size reaches `target`; the last blob is the one flagged `isLast`.
pub fn split_history_blobs(batches: Vec<Value>, target: usize) -> Vec<Vec<Value>> {
    let mut blobs = Vec::new();
    let mut cur = Vec::new();
    let mut size = 0_usize;
    for batch in batches {
        size += serde_json::to_vec(&batch).map_or(0, |v| v.len());
        cur.push(batch);
        if size >= target {
            blobs.push(std::mem::take(&mut cur));
            size = 0;
        }
    }
    if !cur.is_empty() {
        blobs.push(cur);
    }
    blobs
}

fn event_i64(batch: &Value, last: bool, field: &str) -> i64 {
    let events = batch.get("events").and_then(Value::as_array);
    let ev = events.and_then(|e| if last { e.last() } else { e.first() });
    ev.and_then(|e| e.get(field)).and_then(Value::as_i64).unwrap_or(0)
}

/// Go `archiver.HistoryBlob` (`header` + `body`) as JSON, matching the Go struct tags.
fn history_blob(request: &ArchiveHistoryRequest, batches: &[Value], is_last: bool) -> Value {
    let (first, last) = (&batches[0], &batches[batches.len() - 1]);
    let event_count: usize = batches
        .iter()
        .map(|b| b.get("events").and_then(Value::as_array).map_or(0, Vec::len))
        .sum();
    json!({
        "header": {
            "domain_name": request.domain_name,
            "domain_id": request.domain_id,
            "workflow_id": request.workflow_id,
            "run_id": request.run_id,
            "is_last": is_last,
            "first_failover_version": event_i64(first, false, "version"),
            "last_failover_version": event_i64(last, true, "version"),
            "first_event_id": event_i64(first, false, "eventId"),
            "last_event_id": event_i64(last, true, "eventId"),
            "event_count": event_count,
        },
        "body": batches,
    })
}

fn encode(v: &impl serde::Serialize) -> Result<Vec<u8>, ArchiverError> {
    serde_json::to_vec(v).map_err(|e| ArchiverError::Other(e.to_string()))
}

// ---------------------------------------------------------------------- S3

/// Go s3store `upload`: `NoSuchBucket` becomes a `BadRequestError`.
fn s3_upload(
    store: &dyn ObjectStore,
    bucket: &str,
    key: &str,
    data: &[u8],
) -> Result<(), ArchiverError> {
    store.put_object(bucket, key, data).map_err(|e| match e.kind {
        StoreErrorKind::NoSuchBucket => ArchiverError::BadRequest(ERR_BUCKET_NOT_EXISTS.to_owned()),
        _ => ArchiverError::Other(e.message),
    })
}

/// Go s3store `historyArchiver.Archive` (one `HistoryBlob` object per key `.../<version>/<batchIdx>`).
pub fn s3_archive_history(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
) -> Result<(), ArchiverError> {
    s3_soft_validate_uri(uri).map_err(ArchiverError::Other)?;
    validate_history_request(request)?;
    let Some(batches) = source.read_all(request)? else {
        return Ok(()); // history gone (duplicate archival signal): skipped like Go
    };
    let bucket = s3_bucket(uri, region).map_err(ArchiverError::Other)?;
    let path = s3_key_path(uri);
    let blobs = split_history_blobs(batches, TARGET_HISTORY_BLOB_SIZE);
    let count = blobs.len();
    for (idx, blob) in blobs.iter().enumerate() {
        let encoded = encode(&history_blob(request, blob, idx + 1 == count))?;
        let key = s3_history_key(
            &path,
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            request.close_failover_version,
            idx as i64,
        );
        // Go keyExists: HeadObject; NotFound = false.
        let exists = store
            .head_object(&bucket, &key)
            .map_err(|e| ArchiverError::Other(e.message))?;
        if !exists {
            s3_upload(store, &bucket, &key, &encoded)?;
        }
    }
    Ok(())
}

/// Go s3store `visibilityArchiver.Archive`: the record is uploaded under four index keys.
pub fn s3_archive_visibility(
    store: &dyn ObjectStore,
    region: &str,
    uri: &ArchiverUri,
    request: &ArchiveVisibilityRequest,
) -> Result<(), ArchiverError> {
    s3_soft_validate_uri(uri).map_err(ArchiverError::Other)?;
    validate_visibility_request(request)?;
    let encoded = encode(request)?;
    let path = s3_key_path(uri);
    let indexes = [
        (S3_PRIMARY_TYPE, &request.workflow_type_name, S3_INDEX_CLOSE, request.close_timestamp),
        (S3_PRIMARY_TYPE, &request.workflow_type_name, S3_INDEX_START, request.start_timestamp),
        (S3_PRIMARY_ID, &request.workflow_id, S3_INDEX_CLOSE, request.close_timestamp),
        (S3_PRIMARY_ID, &request.workflow_id, S3_INDEX_START, request.start_timestamp),
    ];
    for (primary, value, secondary, ts) in indexes {
        // Go constructTimestampIndex.
        let key = format!(
            "{}/{}/{}",
            s3_visibility_prefix(&path, &request.domain_id, primary, value, secondary),
            rfc3339_utc(ts),
            request.run_id
        );
        let bucket = s3_bucket(uri, region).map_err(ArchiverError::Other)?;
        s3_upload(store, &bucket, &key, &encoded)?;
    }
    Ok(())
}

// --------------------------------------------------------------------- GCS

/// Go gcloud `constructVisibilityFilename`.
fn gcs_visibility_filename(req: &ArchiveVisibilityRequest, tag: &str, ts: i64) -> String {
    format!(
        "{}/{tag}_{}_{}_{}_{}.visibility",
        req.domain_id,
        rfc3339_utc(ts),
        gcs_hash(&req.workflow_type_name),
        gcs_hash(&req.workflow_id),
        gcs_hash(&req.run_id)
    )
}

fn gcs_upload(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    file: &str,
    data: &[u8],
) -> Result<(), StoreError> {
    store.put_object(&uri.host, &gcs_object(uri, file), data)
}

/// Go gcloud `historyArchiver.Archive`. Like Go without a progress manager (inline archival),
/// every blob is written to part 0 (`CurrentPageNumber` is only advanced by the progress
/// manager) and `Exist` checks the bare file name (no sink path), so it effectively never skips.
pub fn gcs_archive_history(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &ArchiveHistoryRequest,
    source: &dyn HistoryBatchSource,
) -> Result<(), ArchiverError> {
    let non_retriable = || ArchiverError::Other(GCS_ERR_UPLOAD_NON_RETRIABLE.to_owned());
    gcs_validate_uri(store, uri).map_err(|_| non_retriable())?;
    validate_history_request(request).map_err(|_| non_retriable())?;
    let Some(batches) = source.read_all(request)? else {
        return Ok(());
    };
    for blob in split_history_blobs(batches, TARGET_HISTORY_BLOB_SIZE) {
        let encoded = encode(&blob)?; // Go encodes historyBlob.Body only
        let filename = gcs_history_filename(
            &request.domain_id,
            &request.workflow_id,
            &request.run_id,
            request.close_failover_version,
            0,
        );
        // Go: `exist, _ := Exist(ctx, URI, filename)`; errors count as not existing.
        let exist = store.head_object(&uri.host, &filename).unwrap_or(false);
        if !exist {
            gcs_upload(store, uri, &filename, &encoded)
                .map_err(|e| ArchiverError::Other(e.message))?;
        }
    }
    Ok(())
}

/// Go gcloud `visibilityArchiver.Archive`: close-time record then start-time record.
pub fn gcs_archive_visibility(
    store: &dyn ObjectStore,
    uri: &ArchiverUri,
    request: &ArchiveVisibilityRequest,
) -> Result<(), ArchiverError> {
    gcs_validate_uri(store, uri)?;
    validate_visibility_request(request)?;
    let encoded = encode(request)?;
    for (tag, ts) in [
        (GCS_INDEX_CLOSE, request.close_timestamp),
        (GCS_INDEX_START, request.start_timestamp),
    ] {
        let filename = gcs_visibility_filename(request, tag, ts);
        gcs_upload(store, uri, &filename, &encoded)
            .map_err(|_| ArchiverError::Other(GCS_ERR_RETRIABLE.to_owned()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archiver::parse_uri;
    use crate::cloud_archiver::{ListPage, GCS_ERR_BUCKET_NOT_EXIST};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Mock {
        bucket: String,
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        fail_put: bool,
    }

    impl ObjectStore for Mock {
        fn head_bucket(&self, bucket: &str) -> Result<(), StoreError> {
            if bucket == self.bucket {
                Ok(())
            } else {
                Err(StoreError::new(StoreErrorKind::NotFound, "NotFound"))
            }
        }
        fn get_object(&self, _b: &str, key: &str) -> Result<Vec<u8>, StoreError> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StoreError::new(StoreErrorKind::NoSuchKey, "no key"))
        }
        fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(), StoreError> {
            if self.fail_put {
                return Err(StoreError::new(StoreErrorKind::Other, "put failed"));
            }
            if bucket != self.bucket {
                return Err(StoreError::new(StoreErrorKind::NoSuchBucket, "no bucket"));
            }
            self.objects.lock().unwrap().insert(key.to_owned(), data.to_vec());
            Ok(())
        }
        fn head_object(&self, _b: &str, key: &str) -> Result<bool, StoreError> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
        fn list(
            &self,
            _b: &str,
            prefix: &str,
            _d: Option<&str>,
            _m: Option<usize>,
            _t: Option<&str>,
        ) -> Result<ListPage, StoreError> {
            Ok(ListPage {
                keys: self
                    .objects
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|k| k.starts_with(prefix))
                    .cloned()
                    .collect(),
                ..Default::default()
            })
        }
    }

    struct Src(Option<Vec<Value>>);
    impl HistoryBatchSource for Src {
        fn read_all(&self, _r: &ArchiveHistoryRequest) -> Result<Option<Vec<Value>>, ArchiverError> {
            Ok(self.0.clone())
        }
    }

    fn hist_req() -> ArchiveHistoryRequest {
        ArchiveHistoryRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            branch_token: vec![1],
            next_event_id: 4,
            close_failover_version: 7,
        }
    }

    fn vis_req() -> ArchiveVisibilityRequest {
        ArchiveVisibilityRequest {
            domain_id: "d".into(),
            domain_name: "dn".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            workflow_type_name: "t".into(),
            start_timestamp: 1_580_819_141_000_000_000,
            close_timestamp: 1_580_819_200_000_000_000,
            ..Default::default()
        }
    }

    fn batches() -> Vec<Value> {
        vec![
            json!({"events":[{"eventId":1,"version":7},{"eventId":2,"version":7}]}),
            json!({"events":[{"eventId":3,"version":7}]}),
        ]
    }

    #[test]
    fn split_blobs_closes_at_target_size() {
        let b = batches();
        let one = serde_json::to_vec(&b[0]).unwrap().len();
        assert_eq!(split_history_blobs(b.clone(), usize::MAX).len(), 1);
        let blobs = split_history_blobs(b.clone(), one);
        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].len(), 1);
        assert!(split_history_blobs(vec![], 1).is_empty());
    }

    #[test]
    fn s3_history_archive_writes_blob_and_roundtrips() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/prefix").unwrap();
        s3_archive_history(&store, "us-east-1", &uri, &hist_req(), &Src(Some(batches()))).unwrap();
        let key = s3_history_key("/prefix", "d", "w", "r", 7, 0);
        let blob: Value = serde_json::from_slice(&store.objects.lock().unwrap()[&key]).unwrap();
        assert_eq!(blob["header"]["is_last"], json!(true));
        assert_eq!(blob["header"]["event_count"], json!(3));
        assert_eq!(blob["header"]["first_event_id"], json!(1));
        assert_eq!(blob["header"]["last_event_id"], json!(3));
        assert_eq!(blob["header"]["first_failover_version"], json!(7));
        assert_eq!(blob["body"], json!(batches()));
        let got = crate::cloud_archiver::s3_get_history(
            &store,
            "us-east-1",
            &uri,
            &crate::archiver::GetHistoryRequest {
                domain_id: "d".into(),
                workflow_id: "w".into(),
                run_id: "r".into(),
                close_failover_version: Some(7),
                next_page_token: vec![],
                page_size: 10,
            },
        )
        .unwrap();
        assert_eq!(got.history_batches, batches());
    }

    #[test]
    fn s3_history_existing_key_is_not_overwritten_and_gone_history_skips() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/prefix").unwrap();
        let key = s3_history_key("/prefix", "d", "w", "r", 7, 0);
        store.objects.lock().unwrap().insert(key.clone(), b"old".to_vec());
        s3_archive_history(&store, "r", &uri, &hist_req(), &Src(Some(batches()))).unwrap();
        assert_eq!(store.objects.lock().unwrap()[&key], b"old".to_vec());
        let empty = Mock { bucket: "b".into(), ..Default::default() };
        s3_archive_history(&empty, "r", &uri, &hist_req(), &Src(None)).unwrap();
        assert!(empty.objects.lock().unwrap().is_empty());
    }

    #[test]
    fn s3_history_errors_like_go() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let bad = parse_uri("file:///tmp/x").unwrap();
        assert!(s3_archive_history(&store, "r", &bad, &hist_req(), &Src(None)).is_err());
        let uri = parse_uri("s3://b/p").unwrap();
        let mut r = hist_req();
        r.domain_id.clear();
        assert!(s3_archive_history(&store, "r", &uri, &r, &Src(None)).is_err());
        let missing = parse_uri("s3://other/p").unwrap();
        let e = s3_archive_history(&store, "r", &missing, &hist_req(), &Src(Some(batches())))
            .unwrap_err();
        assert_eq!(e, ArchiverError::BadRequest(ERR_BUCKET_NOT_EXISTS.into()));
        let failing = Mock { bucket: "b".into(), fail_put: true, ..Default::default() };
        let e = s3_archive_history(&failing, "r", &uri, &hist_req(), &Src(Some(batches())))
            .unwrap_err();
        assert_eq!(e.message(), "put failed");
    }

    #[test]
    fn s3_visibility_archive_writes_four_indexes() {
        let store = Mock { bucket: "b".into(), ..Default::default() };
        let uri = parse_uri("s3://b/prefix").unwrap();
        s3_archive_visibility(&store, "r", &uri, &vis_req()).unwrap();
        let keys: Vec<String> = store.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            keys,
            vec![
                "prefix/d/visibility/workflowID/w/closeTimeout/2020-02-04T12:26:40Z/r",
                "prefix/d/visibility/workflowID/w/startTimeout/2020-02-04T12:25:41Z/r",
                "prefix/d/visibility/workflowTypeName/t/closeTimeout/2020-02-04T12:26:40Z/r",
                "prefix/d/visibility/workflowTypeName/t/startTimeout/2020-02-04T12:25:41Z/r",
            ]
        );
        let mut bad = vis_req();
        bad.close_timestamp = 0;
        assert!(s3_archive_visibility(&store, "r", &uri, &bad).is_err());
    }

    #[test]
    fn gcs_filename_matches_go_util_test() {
        let req = ArchiveVisibilityRequest {
            domain_id: "domainID".into(),
            workflow_type_name: "workflowTypeName".into(),
            workflow_id: "workflowID".into(),
            run_id: "runID".into(),
            ..Default::default()
        };
        assert_eq!(
            gcs_visibility_filename(&req, "startTimeout", 1_472_313_624_305),
            "domainID/startTimeout_1970-01-01T00:24:32Z_4346151385925082125_8344541402884576509_131521284625246243.visibility"
        );
    }

    #[test]
    fn gcs_history_and_visibility_archive() {
        let store = Mock { bucket: "bkt".into(), ..Default::default() };
        let uri = parse_uri("gs://bkt/cadence_archival/dev").unwrap();
        gcs_archive_history(&store, &uri, &hist_req(), &Src(Some(batches()))).unwrap();
        gcs_archive_visibility(&store, &uri, &vis_req()).unwrap();
        let keys: Vec<String> = store.objects.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|k| k.starts_with("cadence_archival/dev/")));
        let got = crate::cloud_archiver::gcs_get_history(
            &store,
            &uri,
            &crate::archiver::GetHistoryRequest {
                domain_id: "d".into(),
                workflow_id: "w".into(),
                run_id: "r".into(),
                close_failover_version: Some(7),
                next_page_token: vec![],
                page_size: 10,
            },
        )
        .unwrap();
        assert_eq!(got.history_batches, batches());
    }

    #[test]
    fn gcs_errors_like_go() {
        let store = Mock { bucket: "bkt".into(), ..Default::default() };
        let missing = parse_uri("gs://nope/p").unwrap();
        let e = gcs_archive_history(&store, &missing, &hist_req(), &Src(None)).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_UPLOAD_NON_RETRIABLE);
        let e = gcs_archive_visibility(&store, &missing, &vis_req()).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_BUCKET_NOT_EXIST);
        let uri = parse_uri("gs://bkt/p").unwrap();
        let failing = Mock { bucket: "bkt".into(), fail_put: true, ..Default::default() };
        let e = gcs_archive_visibility(&failing, &uri, &vis_req()).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_RETRIABLE);
        let e = gcs_archive_history(&failing, &uri, &hist_req(), &Src(Some(batches()))).unwrap_err();
        assert_eq!(e.message(), "put failed");
        let mut r = hist_req();
        r.run_id.clear();
        let e = gcs_archive_history(&store, &uri, &r, &Src(None)).unwrap_err();
        assert_eq!(e.message(), GCS_ERR_UPLOAD_NON_RETRIABLE);
    }
}
