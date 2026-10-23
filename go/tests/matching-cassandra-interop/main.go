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

// These values are pinned to common/persistence.SemaphoreRowTypeOwner and
// the owner-row token_id sentinel in the Go Cassandra adapter. This helper
// lives outside the Go module, so it cannot import those source constants.
const (
	// The tasks table has its own row-type encoding and is unrelated to
	// semaphore_tokens' row types below.
	tasksTaskRowType         = 0
	tasksTaskListRowType     = 1
	decisionTaskListType     = 1
	// Go persistence.TaskListTypeDecision (iota 0); the seed/verify fixture above uses its own value.
	liveDecisionTaskListType = 0
	semaphoreOwnerRowType     = 2
	semaphoreTokenRowType     = 1
	semaphoreOwnerTokenIDNone = -1
	semaphoreTokenOwnerNone   = "0::30000000-0000-f000-f000-000000000000:-1"
	semaphoreFreeSentinel     = "0::30000000-0000-f000-f000-000000000000:-2"
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
	if len(os.Args) < 2 || (os.Args[1] != "seed" && os.Args[1] != "verify" && os.Args[1] != "observe-task" && os.Args[1] != "wait-empty" && os.Args[1] != "verify-semaphore" && os.Args[1] != "release-semaphore" && os.Args[1] != "verify-semaphore-free") {
		log.Fatal("usage: go run ./tests/matching-cassandra-interop {seed|verify|observe-task|wait-empty|verify-semaphore|release-semaphore|verify-semaphore-free}")
	}
	if os.Args[1] == "observe-task" || os.Args[1] == "wait-empty" {
		if len(os.Args) != 5 {
			log.Fatal("task observer expects domain-id task-list timeout-seconds")
		}
		observeTask(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
		return
	}
	if os.Args[1] == "verify-semaphore" || os.Args[1] == "release-semaphore" || os.Args[1] == "verify-semaphore-free" {
		if len(os.Args) != 6 {
			log.Fatal("semaphore command expects domain-id semaphore-name owner-id token-id")
		}
		semaphore(os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5])
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
		err := session.Query("SELECT range_id FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=-12345", domainUUID, name, liveDecisionTaskListType, tasksTaskListRowType).Scan(&rangeID)
		if err != nil && err != gocql.ErrNotFound {
			log.Fatal(err)
		}
		iter := session.Query("SELECT task_id FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=?", domainUUID, name, liveDecisionTaskListType, tasksTaskRowType).Iter()
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

func semaphore(command, domainID, name, ownerID, expectedTokenText string) {
	domainUUID, err := gocql.ParseUUID(domainID)
	if err != nil {
		log.Fatal(err)
	}
	expectedToken, err := strconv.Atoi(expectedTokenText)
	if err != nil || expectedToken <= 0 {
		log.Fatalf("expected semaphore token must be positive: %q", expectedTokenText)
	}
	session := connect()
	defer session.Close()
	if command == "release-semaphore" {
		// Storage fixture only: reproduce Go CDB.ReleaseSemaphoreToken's conditional
		// logged batch after requiring the exact live owner/token rows and version.
		var currentHolder string
		var tokenVersion, ownerVersion time.Time
		if err := session.Query("SELECT holder,updated_time FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=?",
			domainUUID, name, semaphoreTokenRowType, expectedToken, semaphoreTokenOwnerNone).Scan(&currentHolder, &tokenVersion); err != nil {
			log.Fatalf("release fixture requires exact token row: %v", err)
		}
		var currentOwner string
		var currentToken int
		if err := session.Query("SELECT owner_id,held_token,updated_time FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=?",
			domainUUID, name, semaphoreOwnerRowType, semaphoreOwnerTokenIDNone, ownerID).Scan(&currentOwner, &currentToken, &ownerVersion); err != nil {
			log.Fatalf("release fixture requires exact owner row: %v", err)
		}
		if currentHolder != ownerID || currentOwner != ownerID || currentToken != expectedToken || !tokenVersion.Equal(ownerVersion) {
			log.Fatalf("release fixture precondition mismatch: holder=%q owner=%q held_token=%d token_version=%s owner_version=%s",
				currentHolder, currentOwner, currentToken, tokenVersion.UTC().Format(time.RFC3339Nano), ownerVersion.UTC().Format(time.RFC3339Nano))
		}
		// This is not a Matching/SDK release API call. It mirrors Go persistence's
		// actual conditional logged batch and preserves its holder guard.
		batch := session.NewBatch(gocql.LoggedBatch)
		batch.Query("UPDATE semaphore_tokens SET holder=?,updated_time=? WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=? IF holder=?",
			semaphoreFreeSentinel, time.Now().UTC(), domainUUID, name, semaphoreTokenRowType, expectedToken, semaphoreTokenOwnerNone, ownerID)
		batch.Query("DELETE FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=?",
			domainUUID, name, semaphoreOwnerRowType, semaphoreOwnerTokenIDNone, ownerID)
		previous := make(map[string]interface{})
		applied, iter, err := session.MapExecuteBatchCAS(batch, previous)
		if iter != nil { _ = iter.Close() }
		if err != nil { log.Fatal(err) }
		if !applied { log.Fatalf("Go persistence release batch did not apply for owner %q token %d", ownerID, expectedToken) }
		fmt.Printf("{\"released_owner_id\":%q,\"released_token_id\":%d,\"precondition_version\":%q,\"operation\":\"storage fixture mirroring Go CDB.ReleaseSemaphoreToken conditional logged batch\"}\n",
			ownerID, expectedToken, tokenVersion.UTC().Format(time.RFC3339Nano))
		return
	}
	if command == "verify-semaphore-free" {
		var holder string
		err := session.Query("SELECT holder FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=?",
			domainUUID, name, semaphoreTokenRowType, expectedToken, semaphoreTokenOwnerNone).Scan(&holder)
		if err != nil { log.Fatalf("Cassandra token row missing after release: %v", err) }
		if holder != semaphoreFreeSentinel { log.Fatalf("Cassandra token holder after release=%q, want free sentinel", holder) }
		var heldToken int
		err = session.Query("SELECT held_token FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=?",
			domainUUID, name, semaphoreOwnerRowType, semaphoreOwnerTokenIDNone, ownerID).Scan(&heldToken)
		if err == nil { log.Fatalf("Cassandra owner row remains after release with held token %d", heldToken) }
		if err != gocql.ErrNotFound { log.Fatalf("reading released owner row: %v", err) }
		fmt.Printf("{\"released_owner_absent\":true,\"token_id\":%d,\"holder_free\":true}\n", expectedToken)
		return
	}
	var owner string
	var token int
	query := "SELECT owner_id,held_token FROM semaphore_tokens WHERE domain_id=? AND semaphore_name=? AND bucket=0 AND type=? AND token_id=? AND owner_id=?"
	if err := session.Query(query, domainUUID, name, semaphoreOwnerRowType, semaphoreOwnerTokenIDNone, ownerID).Scan(&owner, &token); err != nil {
		log.Fatalf("Cassandra semaphore owner row missing: %v", err)
	}
	if token <= 0 || token != expectedToken {
		log.Fatalf("Cassandra semaphore owner token=%d, want positive token %d for owner %q", token, expectedToken, ownerID)
	}
	fmt.Printf("{\"domain_id\":%q,\"semaphore\":%q,\"owner_id\":%q,\"token_id\":%d}\n", domainID, name, owner, token)
}

func seed(session *gocql.Session, domainUUID gocql.UUID, domainID, name string) {
	if err := session.Query("DELETE FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=?", domainUUID, name, decisionTaskListType).Exec(); err != nil {
		log.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	list := fmt.Sprintf("{domain_id:%s,name:'%s',type:%d,ack_level:0,kind:0,last_updated:'%s',adaptive_partition_config:null}",
		domainID, strings.ReplaceAll(name, "'", "''"), decisionTaskListType, now.Format("2006-01-02 15:04:05.000-0700"))
	query := fmt.Sprintf("INSERT INTO tasks (domain_id,task_list_name,task_list_type,type,task_id,range_id,task_list,created_time) VALUES (?,?,?,%d,-12345,41,%s,?) IF NOT EXISTS", tasksTaskListRowType, list)
	applied, err := session.Query(query, domainUUID, name, decisionTaskListType, now).ScanCAS()
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
	if err := session.Query("SELECT range_id FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=-12345", domainID, name, decisionTaskListType, tasksTaskListRowType).Scan(&rangeID); err != nil {
		log.Fatal(err)
	}
	if rangeID != 42 {
		log.Fatalf("Rust lease range = %d, want 42", rangeID)
	}
	var taskID int64
	var task map[string]interface{}
	if err := session.Query("SELECT task_id,task FROM tasks WHERE domain_id=? AND task_list_name=? AND task_list_type=? AND type=? AND task_id=101", domainID, name, decisionTaskListType, tasksTaskRowType).Scan(&taskID, &task); err != nil {
		log.Fatal(err)
	}
	if taskID != 101 || task["workflow_id"] != "rust-cassandra-interop" || task["schedule_id"] != int64(23) {
		log.Fatalf("unexpected Rust task row: id=%d task=%#v", taskID, task)
	}
	log.Printf("Go verified Rust range=%d task=%#v", rangeID, task)
}
