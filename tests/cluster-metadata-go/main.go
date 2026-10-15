// Differential oracle calls the original cluster.Metadata, never a Go server.
package main

import (
	"encoding/json"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"os"
)

type Case struct {
	Name     string      `json:"name"`
	Op       string      `json:"op"`
	Target   string      `json:"target"`
	Current  int64       `json:"current"`
	Other    int64       `json:"other"`
	UseNew   bool        `json:"useNew"`
	Expected interface{} `json:"expected"`
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
	newVersion := int64(3)
	group := config.ClusterGroupMetadata{FailoverVersionIncrement: 10, PrimaryClusterName: "a", CurrentClusterName: "a", ClusterGroup: map[string]config.ClusterInformation{
		"a":        {Enabled: true, InitialFailoverVersion: 0, NewInitialFailoverVersion: &newVersion},
		"b":        {Enabled: true, InitialFailoverVersion: 2},
		"disabled": {Enabled: false, InitialFailoverVersion: 4},
	}}
	for i := range cases {
		c := &cases[i]
		m := cluster.NewMetadata(group, func(string) bool { return c.UseNew }, metrics.NewNoopMetricsClient(), log.NewNoop())
		switch c.Op {
		case "next":
			c.Expected = m.GetNextFailoverVersion(c.Target, c.Current, "domain")
		case "resolve":
			value, err := m.ClusterNameForFailoverVersion(c.Current)
			if err != nil {
				c.Expected = map[string]string{"error": err.Error()}
			} else {
				c.Expected = value
			}
		case "same":
			c.Expected = m.IsVersionFromSameCluster(c.Current, c.Other)
		case "sets":
			c.Expected = map[string]interface{}{"primary": m.IsPrimaryCluster(), "enabled": len(m.GetEnabledClusterInfo()), "remote": len(m.GetRemoteClusterInfo()), "all": len(m.GetAllClusterInfo())}
		default:
			panic(c.Op)
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(cases); err != nil {
		panic(err)
	}
}
