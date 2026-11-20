// Run on CI to obtain a fresh-encoder fixture for SQL domain audit pagination.
package main

import (
	"encoding/gob"
	"os"
	"time"
)

type domainAuditLogPageToken struct {
	CreatedTime time.Time `json:"created_time"`
	EventID     string    `json:"event_id"`
}

func main() {
	token := domainAuditLogPageToken{
		CreatedTime: time.Unix(1700000000, 123456789).UTC(),
		EventID:     "018bcfe5-687b-7000-8000-000000000000",
	}
	if err := gob.NewEncoder(os.Stdout).Encode(token); err != nil {
		panic(err)
	}
}
