// Command gcs-seed prints the Go gstorage history object name for a run (mirrors
// common/archiver/gcloud constructHistoryFilenameMultipart). Usage: gcs-seed domainID workflowID runID version part
package main

import (
	"fmt"
	"os"

	"github.com/dgryski/go-farm"
)

func hash(s string) string {
	if s == "" {
		return ""
	}
	return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
}

func main() {
	if len(os.Args) != 6 {
		fmt.Fprintln(os.Stderr, "usage: gcs-seed domainID workflowID runID version part")
		os.Exit(2)
	}
	fmt.Printf("%s%s%s_%s_%s.history\n", hash(os.Args[1]), hash(os.Args[2]), hash(os.Args[3]), os.Args[4], os.Args[5])
}
