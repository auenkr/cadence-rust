// matching-cassandra-protocol-oracle exercises the Go-pinned gocql client
// against a real Cassandra server using an explicitly selected CQL protocol.
// It is backend interoperability evidence, not a wire mock or Rust test.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// Keep the field names and types aligned with the six-field task UDT in
// go/schema/cassandra/cadence/schema.cql.
type matchingTask struct {
	DomainID        gocql.UUID        `cql:"domain_id"`
	WorkflowID      string            `cql:"workflow_id"`
	RunID           gocql.UUID        `cql:"run_id"`
	ScheduleID      int64             `cql:"schedule_id"`
	CreatedTime     time.Time         `cql:"created_time"`
	PartitionConfig map[string]string `cql:"partition_config"`
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	version, err := strconv.Atoi(os.Getenv("CASSANDRA_PROTO_VERSION"))
	must(err)
	if version != 2 && version != 3 && version != 4 {
		must(fmt.Errorf("oracle supports only requested protocols 2, 3 and 4, got %d", version))
	}
	host := os.Getenv("CASSANDRA_HOST")
	keyspace := os.Getenv("CASSANDRA_KEYSPACE")
	if host == "" || keyspace == "" {
		must(fmt.Errorf("CASSANDRA_HOST and CASSANDRA_KEYSPACE are required"))
	}
	cluster := gocql.NewCluster(host)
	cluster.Port = 9042
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = version
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 0}
	session, err := cluster.CreateSession()
	must(err)
	defer session.Close()

	step := func(name string) { fmt.Printf("RUN %s\n", name) }
	// DDL makes the server return a schema-change result and emits the schema
	// event gocql's control connection subscribes to during startup.
	step("schema-create")
	must(session.Query(`CREATE TABLE IF NOT EXISTS matching_protocol_oracle (id bigint PRIMARY KEY, payload blob)`).Exec())
	step("task-udt-schema-create")
	must(session.Query(`CREATE TYPE matching_protocol_task (domain_id uuid, workflow_id text, run_id uuid, schedule_id bigint, created_time timestamp, partition_config map<text, text>)`).Exec())
	must(session.Query(`CREATE TABLE matching_protocol_tasks (id bigint PRIMARY KEY, task frozen<matching_protocol_task>)`).Exec())
	task := matchingTask{
		DomainID: gocql.UUID{0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66, 0x77, 0x77, 0x88, 0x88},
		WorkflowID: "matching-udt-workflow",
		RunID: gocql.UUID{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
		ScheduleID: 123456789,
		CreatedTime: time.Date(2026, time.October, 27, 12, 34, 56, 789000000, time.UTC),
		PartitionConfig: map[string]string{"partition": "blue", "zone": "west"},
	}
	step("task-udt-prepare-insert")
	step("task-udt-insert")
	err = session.Query(`INSERT INTO matching_protocol_tasks (id, task) VALUES (?, ?)`, int64(7001), task).Exec()
	if version == 2 {
		const expected = "UDT are not available on protocols less than 3, please update config"
		if err == nil || !strings.Contains(err.Error(), expected) {
			must(fmt.Errorf("protocol v2 task UDT expected gocql ErrorUDTUnavailable %q, got %v", expected, err))
		}
		fmt.Printf("EXPECTED_CLIENT_REJECTION task_udt=%q\n", err)
	} else {
		must(err)
		step("task-udt-select")
		var got matchingTask
		must(session.Query(`SELECT task FROM matching_protocol_tasks WHERE id = ?`, int64(7001)).Scan(&got))
		if got.DomainID != task.DomainID || got.WorkflowID != task.WorkflowID || got.RunID != task.RunID || got.ScheduleID != task.ScheduleID || !got.CreatedTime.Equal(task.CreatedTime) || len(got.PartitionConfig) != len(task.PartitionConfig) || got.PartitionConfig["partition"] != "blue" || got.PartitionConfig["zone"] != "west" {
			must(fmt.Errorf("task UDT roundtrip mismatch: got %#v", got))
		}
	}

	// These are the Matching persistence queue statements/schema shape: an
	// idempotent enqueue, ordered/ranged reads, deletes, count, and a batch.
	const queueType = 91
	step("queue-cas-enqueue")
	_, err = session.Query(`INSERT INTO queue (queue_type, message_id, message_payload, created_time) VALUES (?, ?, ?, ?) IF NOT EXISTS`, queueType, int64(1001), []byte("protocol-oracle"), time.Now().UTC()).MapScanCAS(map[string]interface{}{})
	must(err)
	var gotID int64
	var payload []byte
	step("queue-range-select")
	must(session.Query(`SELECT message_id, message_payload FROM queue WHERE queue_type = ? AND message_id > ? LIMIT ?`, queueType, int64(1000), 10).Scan(&gotID, &payload))
	if gotID != 1001 || string(payload) != "protocol-oracle" {
		must(fmt.Errorf("queue read mismatch: id=%d payload=%q", gotID, payload))
	}
	var count int64
	step("queue-count")
	must(session.Query(`SELECT COUNT(1) AS count FROM queue WHERE queue_type = ?`, queueType).Scan(&count))
	if count != 1 {
		must(fmt.Errorf("queue count mismatch: %d", count))
	}
	batch := session.NewBatch(gocql.LoggedBatch)
	step("queue-batch")
	batch.Query(`INSERT INTO queue (queue_type, message_id, message_payload, created_time) VALUES (?, ?, ?, ?)`, queueType, int64(1002), []byte("batch"), time.Now().UTC())
	must(session.ExecuteBatch(batch))
	step("queue-range-delete")
	rangeDeleteErr := session.Query(`DELETE FROM queue WHERE queue_type = ? AND message_id > ? AND message_id <= ?`, queueType, int64(1001), int64(1002)).Exec()
	expectReject := os.Getenv("CASSANDRA_EXPECT_RANGE_DELETE_REJECTION") == "true"
	if expectReject && rangeDeleteErr == nil {
		must(fmt.Errorf("server unexpectedly accepted Matching strict-range DELETE"))
	}
	if expectReject && !strings.Contains(rangeDeleteErr.Error(), "Invalid operator") {
		must(fmt.Errorf("expected Cassandra unsupported-operator rejection, got: %w", rangeDeleteErr))
	}
	if !expectReject {
		must(rangeDeleteErr)
	}
	if expectReject {
		fmt.Printf("EXPECTED_SERVER_REJECTION range_delete=%q\n", rangeDeleteErr)
		step("queue-point-delete-batch-row-cleanup")
		must(session.Query(`DELETE FROM queue WHERE queue_type = ? AND message_id = ?`, queueType, int64(1002)).Exec())
	}
	step("queue-point-delete-cas-row")
	must(session.Query(`DELETE FROM queue WHERE queue_type = ? AND message_id = ?`, queueType, int64(1001)).Exec())
	step("server-error")
	if err := session.Query(`SELECT definitely_missing FROM queue WHERE queue_type = ?`, queueType).Exec(); err == nil {
		must(fmt.Errorf("expected server query error, got nil"))
	}
	step("schema-drop")
	must(session.Query(`DROP TABLE matching_protocol_oracle`).Exec())
	step("task-udt-schema-drop")
	must(session.Query(`DROP TABLE matching_protocol_tasks`).Exec())
	must(session.Query(`DROP TYPE matching_protocol_task`).Exec())
	fmt.Printf("PASS gocql=%s protocol=%d server=%s operations=schema-change,event-subscription,cas,query,result,prepared,batch,range-delete,point-delete,error,task-udt range_delete_rejected=%t unset_value=not-used\n", "v0.0.0-20211015133455-b225f9b53fa1", version, os.Getenv("CASSANDRA_SERVER_VERSION"), expectReject)
}
