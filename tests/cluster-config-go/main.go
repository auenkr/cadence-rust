// Calls Go's real cluster configuration defaults and validation.
package main

import (
	"bytes"
	"encoding/json"
	"github.com/uber/cadence/common/config"
	"go.uber.org/multierr"
	"os"
	"sort"
)

type Case struct {
	Name     string                       `json:"name"`
	Input    *config.ClusterGroupMetadata `json:"input"`
	Fill     bool                         `json:"fillDefaults"`
	Expected interface{}                  `json:"expected"`
}

func clusters(values map[string]config.ClusterInformation) interface{} {
	out := map[string]interface{}{}
	for name, c := range values {
		out[name] = map[string]interface{}{"enabled": c.Enabled, "initialFailoverVersion": c.InitialFailoverVersion, "newInitialFailoverVersion": c.NewInitialFailoverVersion, "rpcName": c.RPCName, "rpcAddress": c.RPCAddress, "rpcTransport": c.RPCTransport}
	}
	return out
}
func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var cases []Case
	if err := json.Unmarshal(data, &cases); err != nil {
		panic(err)
	}
	// Retain raw inputs: defaulting mutates Go structs/maps in-place.
	var originals []map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&originals); err != nil {
		panic(err)
	}
	for i, c := range cases {
		if c.Fill && c.Input != nil {
			c.Input.FillDefaults()
		}
		errors := []string{}
		for _, e := range multierr.Errors(c.Input.Validate()) {
			errors = append(errors, e.Error())
		}
		sort.Strings(errors)
		var normalized interface{}
		if c.Input != nil {
			v := c.Input
			normalized = map[string]interface{}{"failoverVersionIncrement": v.FailoverVersionIncrement, "primaryClusterName": v.PrimaryClusterName, "masterClusterName": v.MasterClusterName, "currentClusterName": v.CurrentClusterName, "clusterGroup": clusters(v.ClusterGroup), "clusterInformation": clusters(v.ClusterInformation)}
		}
		originals[i]["expected"] = map[string]interface{}{"errors": errors, "config": normalized}
	}
	if err := json.NewEncoder(os.Stdout).Encode(originals); err != nil {
		panic(err)
	}
}
