// Command configstore-seed writes one dynamic-config snapshot into a Go MySQL
// schema's cluster_config table exactly as Go's configStoreManager does:
// DynamicConfigBlob serialized with the payload serializer (thriftrw), row_type
// DynamicConfig, stored version negated, values JSON-encoded.
//
// Usage: configstore-seed USER:PASS@HOST:PORT DATABASE '{"name":<json value>,...}'
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: configstore-seed USER:PASS@HOST:PORT DATABASE JSON")
		os.Exit(2)
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(os.Args[3]), &values); err != nil {
		fmt.Fprintln(os.Stderr, "bad json:", err)
		os.Exit(2)
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	jsonEncoding := types.EncodingTypeJSON
	blob := &types.DynamicConfigBlob{SchemaVersion: 1}
	for _, name := range names {
		blob.Entries = append(blob.Entries, &types.DynamicConfigEntry{
			Name: name,
			Values: []*types.DynamicConfigValue{{
				Value: &types.DataBlob{EncodingType: &jsonEncoding, Data: values[name]},
			}},
		})
	}
	data, err := persistence.NewPayloadSerializer().SerializeDynamicConfigBlob(blob, constants.EncodingTypeThriftRW)
	if err != nil {
		fmt.Fprintln(os.Stderr, "serialize:", err)
		os.Exit(1)
	}
	if os.Getenv("SEED_DRY") != "" {
		fmt.Printf("%x\n", data.Data)
		return
	}
	var userPass, hostPort string
	for i := len(os.Args[1]) - 1; i >= 0; i-- {
		if os.Args[1][i] == '@' {
			userPass, hostPort = os.Args[1][:i], os.Args[1][i+1:]
			break
		}
	}
	db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(%s)/%s?parseTime=true", userPass, hostPort, os.Args[2]))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()
	const version = 1
	_, err = db.Exec("INSERT INTO cluster_config (row_type, version, timestamp, data, data_encoding) VALUES(?, ?, ?, ?, ?)",
		int(persistence.DynamicConfig), -version, time.Now().UTC(), data.Data, string(data.Encoding))
	if err != nil {
		fmt.Fprintln(os.Stderr, "insert:", err)
		os.Exit(1)
	}
	fmt.Printf("seeded %d entries version=%d encoding=%s bytes=%d\n", len(names), version, data.Encoding, len(data.Data))
}
