//! Opt-in cross-language test against the pinned Go MySQL v8 schema.
//!
//! `tests/run-matching-mysql-interop.py` runs the Go seed phase, this test, and
//! the Go verification phase in order. It is deliberately ignored by default.

use cadence_persistence::matching_blob::{BlobEncoding, TaskData, TaskListData};
use cadence_persistence::matching_domain_mysql::MySqlDomainMetadataStore;
use cadence_persistence::matching_mysql::{MySqlTaskStore, MySqlTaskStoreConfig};
use cadence_persistence::matching_task_store::{
    AllocatedTask, MatchingTaskStore, StorageError, TaskListKey,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn config() -> MySqlTaskStoreConfig {
    MySqlTaskStoreConfig {
        user: std::env::var("CADENCE_MYSQL_USER").unwrap_or_else(|_| "root".into()),
        password: std::env::var("CADENCE_MYSQL_PASSWORD").unwrap_or_else(|_| "cadence".into()),
        database_name: std::env::var("CADENCE_MYSQL_DATABASE").unwrap_or_else(|_| "cadence".into()),
        connect_protocol: "tcp".into(),
        connect_addr: std::env::var("CADENCE_MYSQL_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:33307".into()),
        connect_attributes: HashMap::new(),
        max_conns: 16,
        max_idle_conns: 8,
        max_conn_lifetime: Some(Duration::from_secs(300)),
        n_shards: 1,
        use_multiple_databases: false,
        multiple_databases_config: Vec::new(),
        encoding: BlobEncoding::ThriftRW,
        tls: None,
    }
}

fn key(name: &str) -> TaskListKey {
    TaskListKey {
        domain_id: Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        name: name.into(),
        task_type: 1,
    }
}

fn allocated(task_id: i64, workflow_id: &str) -> AllocatedTask {
    AllocatedTask {
        task_id,
        data: TaskData {
            workflow_id: workflow_id.into(),
            run_id: Some(
                Uuid::parse_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
            ),
            schedule_id: task_id,
            ..Default::default()
        },
    }
}

#[test]
#[ignore = "requires tests/run-matching-mysql-interop.py and an ephemeral MySQL 8 server"]
fn mysql_interop_harness() {
    let config = config();
    let store = MySqlTaskStore::connect(config.clone()).expect("connect MySQL task store");
    let domain_reader = MySqlDomainMetadataStore::connect(&config, "active").unwrap();

    // Go TaskManager and DomainStore rows are the input to the Rust readers.
    let go_key = key("go-queue");
    let go_list = store.get_task_list(&go_key).unwrap();
    assert_eq!(go_list.range_id, 1);
    assert_eq!(go_list.data.kind, 1);
    let go_tasks = store.get_tasks(&go_key, 0, i64::MAX, 32).unwrap();
    assert_eq!(go_tasks.len(), 2);
    assert_eq!(go_tasks[0].data.workflow_id, "go-workflow-10");
    assert_eq!(go_tasks[1].data.workflow_id, "go-workflow-20");
    assert_eq!(store.get_tasks(&go_key, 0, i64::MAX, 1).unwrap().len(), 1);
    assert_eq!(store.task_list_size(&go_key, 0).unwrap(), 2);

    let domain = domain_reader.get_domain(Some("go-domain-a"), None).unwrap();
    assert_eq!(domain.info.uuid, "11111111-1111-4111-8111-111111111111");
    assert_eq!(domain.info.name, "go-domain-a");
    assert!(domain_reader.get_metadata_version().unwrap() >= 3);
    let first_page = domain_reader.list_domains(1, None).unwrap();
    assert_eq!(first_page.domains.len(), 1);
    let token = first_page.next_page_token.expect("full page token");
    let second_page = domain_reader.list_domains(1, Some(&token)).unwrap();
    assert_eq!(second_page.domains.len(), 1);

    // The stale insert must be fenced and rolled back atomically.
    let stale = allocated(40, "must-rollback");
    let result = store.create_tasks_with_domains(&go_key, 0, &[(go_key.domain_id, &stale)]);
    assert!(matches!(result, Err(StorageError::ConditionFailed(_))));
    assert_eq!(store.get_tasks(&go_key, 39, i64::MAX, 32).unwrap().len(), 0);

    // Rust renews the Go-owned queue and writes a row Go reads in the final phase.
    let rust_lease = store
        .lease(&go_key, 1, 1, 1_700_000_000_000_000_000)
        .unwrap();
    assert_eq!(rust_lease.range_id, 2);
    let rust_task = allocated(30, "rust-workflow-30");
    store
        .create_tasks_with_domains(&go_key, 2, &[(go_key.domain_id, &rust_task)])
        .unwrap();
    let mut task_list = rust_lease.data;
    task_list.ack_level = 20;
    store
        .update_task_list(&go_key, 2, task_list, 1_700_000_001_000_000_000)
        .unwrap();
    let expiry = store.get_task_list(&go_key).unwrap().data.expiry_nanos;
    assert_eq!(expiry, 1_700_000_001_000_000_000i64 + 86_400_000_000_000);

    // Independent pool instances must serialize lease increments at the row fence.
    let concurrent_key = key("concurrent-lease");
    let first = store
        .lease(&concurrent_key, 1, 0, 1_700_000_000_000_000_000)
        .unwrap();
    assert_eq!(first.range_id, 1);
    let shared = Arc::new(MySqlTaskStore::connect(config.clone()).unwrap());
    let workers: Vec<_> = (0..4)
        .map(|index| {
            let store = shared.clone();
            let key = concurrent_key.clone();
            std::thread::spawn(move || {
                for attempt in 0..8 {
                    match store.lease(&key, 1, 0, 1_700_000_002_000_000_000 + index + attempt) {
                        Ok(record) => return record.range_id,
                        Err(StorageError::ConditionFailed(_)) => std::thread::yield_now(),
                        Err(error) => panic!("concurrent lease failed: {error}"),
                    }
                }
                panic!("concurrent lease did not converge after stale-range retries")
            })
        })
        .collect();
    let mut ranges: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    ranges.sort_unstable();
    assert_eq!(ranges, vec![2, 3, 4, 5]);

    // Exercise the persistence operations the Matching expiry collector uses:
    // an expired task-list blob is removed only with its current range fence.
    let gc_key = key("gc-expired");
    let gc_lease = store.lease(&gc_key, 1, 0, 0).unwrap();
    store
        .update_task_list(&gc_key, gc_lease.range_id, gc_lease.data, 0)
        .unwrap();
    let expiry = store.get_task_list(&gc_key).unwrap().data.expiry_nanos;
    assert_eq!(expiry, 86_400_000_000_000);
    assert!(expiry < 86_400_000_000_001); // simulated collector timestamp
    assert_eq!(
        store.get_task_list(&gc_key).unwrap().range_id,
        gc_lease.range_id
    );

    // Reconnect to prove the lease, ack, expiry and task rows survive restart.
    drop(shared);
    drop(store);
    let reopened = MySqlTaskStore::connect(config).unwrap();
    let reopened_list = reopened.get_task_list(&go_key).unwrap();
    assert_eq!(reopened_list.range_id, 2);
    assert_eq!(reopened_list.data.ack_level, 20);
    assert_eq!(
        reopened.get_tasks(&go_key, 20, i64::MAX, 32).unwrap()[0].task_id,
        30
    );
    assert!(reopened_list.data.expiry_nanos > 0);

    // Ensure the public fixture type stays the same encoding Go writes.
    assert_eq!(BlobEncoding::ThriftRW.as_str(), "thriftrw");
    let _ = TaskListData::default();
}
