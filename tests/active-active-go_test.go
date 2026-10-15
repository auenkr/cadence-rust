// Injected by Go's overlay into the original domain package.
package domain

import (
	"encoding/json"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"os"
	"testing"
)

type rustActive struct {
	Scopes map[string]types.ClusterAttributeScope `json:"activeClustersByClusterAttribute"`
}

func rustToActive(value *rustActive) *types.ActiveClusters {
	if value == nil {
		return nil
	}
	return &types.ActiveClusters{AttributeScopes: value.Scopes}
}
func rustNormalizeActive(value *types.ActiveClusters) interface{} {
	if value == nil {
		return nil
	}
	var scopes map[string]interface{}
	if value.AttributeScopes != nil {
		scopes = map[string]interface{}{}
		for name, scope := range value.AttributeScopes {
			var attributes map[string]interface{}
			if scope.ClusterAttributes != nil {
				attributes = map[string]interface{}{}
				for key, info := range scope.ClusterAttributes {
					attributes[key] = map[string]interface{}{"activeClusterName": info.ActiveClusterName, "failoverVersion": info.FailoverVersion}
				}
			}
			scopes[name] = map[string]interface{}{"attributes": attributes}
		}
	}
	return map[string]interface{}{"scopes": scopes}
}
func TestRustActiveActiveFixtures(t *testing.T) {
	data, err := os.ReadFile(os.Getenv("CADENCE_ACTIVE_ACTIVE_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name     string      `json:"name"`
		Op       string      `json:"op"`
		Global   bool        `json:"global"`
		UseNew   bool        `json:"useNew"`
		Existing *rustActive `json:"existing"`
		Incoming *rustActive `json:"incoming"`
		Target   *string     `json:"target"`
	}
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	var originals []map[string]json.RawMessage
	if err = json.Unmarshal(data, &originals); err != nil {
		t.Fatal(err)
	}
	newOffset := int64(3)
	for index, c := range cases {
		meta := cluster.NewMetadata(config.ClusterGroupMetadata{FailoverVersionIncrement: 10, PrimaryClusterName: "a", CurrentClusterName: "a", ClusterGroup: map[string]config.ClusterInformation{
			"a": {Enabled: true, InitialFailoverVersion: 0, NewInitialFailoverVersion: &newOffset}, "b": {Enabled: true, InitialFailoverVersion: 2}, "disabled": {Enabled: false, InitialFailoverVersion: 4},
		}}, func(string) bool { return c.UseNew }, metrics.NewNoopMetricsClient(), log.NewNoop())
		handler := handlerImpl{clusterMetadata: meta, domainAttrValidator: newAttrValidator(meta, 0), logger: log.NewNoop()}
		existing, incoming := rustToActive(c.Existing), rustToActive(c.Incoming)
		var result *types.ActiveClusters
		var changed, configChanged, activeChanged bool
		var err error
		target := "a"
		switch c.Op {
		case "register":
			result, err = handler.activeClustersFromRegisterRequest(&types.RegisterDomainRequest{IsGlobalDomain: c.Global, ActiveClusters: incoming})
		case "merge":
			result, changed = mergeActiveActiveScopes(existing, incoming)
		case "build":
			result, changed = handler.buildActiveActiveClusterScopesFromUpdateRequest(&types.UpdateDomainRequest{ActiveClusters: incoming}, &p.DomainReplicationConfig{ActiveClusters: existing}, "domain")
		case "validate":
			err = handler.domainAttrValidator.validateActiveActiveDomainReplicationConfig(incoming)
		case "update":
			var cfg *p.DomainReplicationConfig
			cfg, configChanged, activeChanged, err = handler.updateReplicationConfig("domain", &p.DomainReplicationConfig{ActiveClusterName: "a", ActiveClusters: existing}, &types.UpdateDomainRequest{ActiveClusterName: c.Target, ActiveClusters: incoming})
			if err == nil {
				result = cfg.ActiveClusters
				target = cfg.ActiveClusterName
			}
		default:
			t.Fatal(c.Op)
		}
		message := ""
		if err != nil {
			message = err.Error()
		}
		expected := map[string]interface{}{"active": rustNormalizeActive(result), "changed": changed, "configChanged": configChanged, "activeChanged": activeChanged, "target": target, "error": message}
		encoded, err := json.Marshal(expected)
		if err != nil {
			t.Fatal(err)
		}
		originals[index]["expected"] = encoded
	}
	encoded, err := json.MarshalIndent(originals, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if output := os.Getenv("CADENCE_ACTIVE_ACTIVE_OUTPUT"); output != "" {
		if err = os.WriteFile(output, append(encoded, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Fatal("output path missing")
	}
}
