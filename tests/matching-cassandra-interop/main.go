// Command matching-cassandra-interop seeds a Go Cassandra task-list row and
// verifies task/list state written by the Rust persistence adapter.
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func connect() *gocql.Session {
	hosts := strings.Split(required("CADENCE_CASSANDRA_CONTACT_POINTS"), ",")
	port := 9042
	if value := os.Getenv("CADENCE_CASSANDRA_PORT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			log.Fatal(err)
		}
		port = parsed
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = port
	cluster.Keyspace = required("CADENCE_CASSANDRA_KEYSPACE")
	cluster.Consistency = gocql.Quorum
	if user := os.Getenv("CADENCE_CASSANDRA_USER"); user != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: user,
			Password: os.Getenv("CADENCE_CASSANDRA_PASSWORD"),
		}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return session
}

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "seed" && os.Args[1] != "verify" && os.Args[1] != "observe-task" && os.Args[1] != "wait-empty" && os.Args[1] != "verify-semaphore") {
		log.Fatal("usage: go run ./tests/matching-cassandra-interop {seed|verify|observe-task|wait-empty|verify-semaphore}")
	}
	if os.Args[1] == "observe-task" || os.Args[1] == "wait-empty" {
		if len(os.Args) != 5 {
			log.Fatal("task observer expects domain-id task-list timeout-seconds")
		}
		observeTask(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
		return
	}
	if os.Args[1] == "verify-semaphore" {
		if len(os.Args) != 4 {
			log.Fatal("semaphore observer expects domain-id semaphore-name")
		}
		semaphore(os.Args[2], os.Args[3])
		return
	}
	domainID := required("CADENCE_CASSANDRA_TEST_DOMAIN_ID")
	parsedDomainID, err := gocql.ParseUUID(domainID)
	if err != nil {
		log.Fatal(err)
	}
	name := required("CADENCE_CASSANDRA_TEST_TASK_LIST")
	session := connect()
	defer session.Close()
	if os.Args[1] == "seed" {
		seed(session, parsedDomainID, domainID, name)
	} else {
		verify(session, parsedDomainID, name)
	}
}

func observeTask(command, domainID, name, timeoutText string) {
	domainUUID, err := gocql.ParseUUID(domainID)
	if err != nil {
		log.Fatal(err)
	}
	timeout, err := time.ParseDuration(timeoutText + "s")
	if err != nil {
		log.Fatal(err)
	}
	session := connect()
	defer session.Close()
	deadline := time.Now().Add(timeout)
	for {
		var rangeID int64
		err := session.Query("SELECT range_id FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=1 AND task_id=-12345", domainUUID, name, 1).Scan(&rangeID)
		if err != nil && err != gocql.ErrNotFound {
			log.Fatal(err)
		}
		iter := session.Query("SELECT task_id FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=0", domainUUID, name, 1).Iter()
		count := 0
		var taskID int64
		for iter.Scan(&taskID) { count++ }
		if err := iter.Close(); err != nil { log.Fatal(err) }
		if (command == "observe-task" && count > 0) || (command == "wait-empty" && count == 0) {
			fmt.Printf("{\"domain_id\":%q,\"task_list\":%q,\"queued_rows\":%d,\"range_id\":%d}\n", domainID, name, count, rangeID)
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("Cassandra task condition %s timed out: rows=%d range=%d", command, count, rangeID)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func semaphore(domainID, name string) {
	domainUUID, err := gocql.ParseUUID(domainID)
	if err != nil {
		log.Fatal(err)
	}
	session := connect()
	defer session.Close()
	iter := session.Query("SELECT owner_id,held_token FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=1", domainUUID, name).Iter()
	owners := map[string]int{}
	var owner string
	var token int
	for iter.Scan(&owner, &token) { owners[owner] = token }
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	if len(owners) != 1 {
		log.Fatalf("Cassandra semaphore owner rows=%d, want exactly one: %#v", len(owners), owners)
	}
	for owner, token := range owners {
		fmt.Printf("{\"domain_id\":%q,\"semaphore\":%q,\"owner_id\":%q,\"token_id\":%d}\n", domainID, name, owner, token)
	}
}

func seed(session *gocql.Session, domainUUID gocql.UUID, domainID, name string) {
	if err := session.Query("DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=?", domainUUID, name, 1).Exec(); err != nil {
		log.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	list := fmt.Sprintf("{domain_id:%s,name:'%s',type:1,ack_level:0,kind:0,last_updated:'%s',adaptive_partition_config:null}",
		domainID, strings.ReplaceAll(name, "'", "''"), now.Format("2006-01-02 15:04:05.000-0700"))
	query := fmt.Sprintf("INSERT INTO tasks (domain_id,task_list_name,task_list_type,type,task_id,range_id,task_list,created_time) VALUES (?,?,?,1,-12345,41,%s,?) IF NOT EXISTS", list)
	applied, err := session.Query(query, domainUUID, name, 1, now).ScanCAS()
	if err != nil {
		log.Fatal(err)
	}
	if !applied {
		log.Fatal("task-list sentinel already exists after seed cleanup")
	}
	log.Printf("Go seeded task-list range 41: domain=%s list=%s", domainID, name)
}

func verify(session *gocql.Session, domainID gocql.UUID, name string) {
	var rangeID int64
	if err := session.Query("SELECT range_id FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=1 AND task_id=-12345", domainID, name, 1).Scan(&rangeID); err != nil {
		log.Fatal(err)
	}
	if rangeID != 42 {
		log.Fatalf("Rust lease range = %d, want 42", rangeID)
	}
	var taskID int64
	var task map[string]interface{}
	if err := session.Query("SELECT task_id,task FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=0 AND task_id=101", domainID, name, 1).Scan(&taskID, &task); err != nil {
		log.Fatal(err)
	}
	if taskID != 101 || task["workflow_id"] != "rust-cassandra-interop" || task["schedule_id"] != int64(23) {
		log.Fatalf("unexpected Rust task row: id=%d task=%#v", taskID, task)
	}
	log.Printf("Go verified Rust range=%d task=%#v", rangeID, task)
}
