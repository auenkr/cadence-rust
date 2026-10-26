// matching-cassandra-protocol-oracle exercises the Go-pinned gocql client
// against a real Cassandra server using an explicitly selected CQL protocol.
// It is backend interoperability evidence, not a wire mock or Rust test.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	version, err := strconv.Atoi(os.Getenv("CASSANDRA_PROTO_VERSION"))
	must(err)
	if version != 2 && version != 3 {
		must(fmt.Errorf("oracle supports only requested protocols 2 and 3, got %d", version))
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
	// Cassandra 2.1/2.2 reject a strict lower-bound DELETE on a primary-key
	// component; use the inclusive boundary for this protocol/backend probe.
	must(session.Query(`DELETE FROM queue WHERE queue_type = ? AND message_id >= ? AND message_id <= ?`, queueType, int64(1002), int64(1002)).Exec())
	step("queue-point-delete")
	must(session.Query(`DELETE FROM queue WHERE queue_type = ? AND message_id = ?`, queueType, int64(1001)).Exec())
	step("server-error")
	if err := session.Query(`SELECT definitely_missing FROM queue WHERE queue_type = ?`, queueType).Exec(); err == nil {
		must(fmt.Errorf("expected server query error, got nil"))
	}
	step("schema-drop")
	must(session.Query(`DROP TABLE matching_protocol_oracle`).Exec())
	fmt.Printf("PASS gocql=%s protocol=%d server=%s operations=schema-change,event-subscription,cas,query,result,prepared,batch,delete,error unset_value=not-used\n", "v0.0.0-20211015133455-b225f9b53fa1", version, os.Getenv("CASSANDRA_SERVER_VERSION"))
}
