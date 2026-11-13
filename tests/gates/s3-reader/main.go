// Command s3-reader reads archived history / visibility back with the Go s3store archivers, to prove Go parses blobs
// written by Rust History. Copied into the go module at build time (like gcs-seed).
// Usage: s3-reader history  ENDPOINT URI DOMAIN_ID WORKFLOW_ID RUN_ID   -> prints event count and types
//
//	s3-reader visibility ENDPOINT URI DOMAIN_ID QUERY             -> prints one workflowID per line
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/s3store"
	"github.com/uber/cadence/common/log"
)

func fail(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: s3-reader history|visibility ENDPOINT URI DOMAIN_ID [WORKFLOW_ID RUN_ID | QUERY]")
		os.Exit(2)
	}
	endpoint := os.Args[2]
	cfg := &s3store.Config{Region: "us-east-1", Endpoint: &endpoint, S3ForcePathStyle: true}
	uri, err := archiver.NewURI(os.Args[3])
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "history":
		a, err := s3store.NewHistoryArchiver(&archiver.HistoryBootstrapContainer{Logger: log.NewNoop()}, cfg)
		if err != nil {
			fail(err)
		}
		var token []byte
		total := 0
		for {
			resp, err := a.Get(ctx, uri, &archiver.GetHistoryRequest{
				DomainID: os.Args[4], WorkflowID: os.Args[5], RunID: os.Args[6], PageSize: 1000, NextPageToken: token})
			if err != nil {
				fail(err)
			}
			for _, b := range resp.HistoryBatches {
				for _, e := range b.Events {
					total++
					fmt.Println("EVENT", e.ID, e.EventType.String())
				}
			}
			token = resp.NextPageToken
			if len(token) == 0 {
				break
			}
		}
		fmt.Println("TOTAL", total)
	case "visibility":
		a, err := s3store.NewVisibilityArchiver(&archiver.VisibilityBootstrapContainer{Logger: log.NewNoop()}, cfg)
		if err != nil {
			fail(err)
		}
		var token []byte
		for {
			resp, err := a.Query(ctx, uri, &archiver.QueryVisibilityRequest{
				DomainID: os.Args[4], PageSize: 100, Query: os.Args[5], NextPageToken: token})
			if err != nil {
				fail(err)
			}
			for _, e := range resp.Executions {
				fmt.Println("WORKFLOW", e.Execution.WorkflowID)
			}
			token = resp.NextPageToken
			if len(token) == 0 {
				break
			}
		}
	default:
		fail(fmt.Errorf("unknown mode %s", os.Args[1]))
	}
}
