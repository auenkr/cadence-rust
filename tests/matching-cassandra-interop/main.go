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
	if len(os.Args) != 2 || (os.Args[1] != "seed" && os.Args[1] != "verify") {
		log.Fatal("usage: go run ./tests/matching-cassandra-interop {seed|verify}")
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
