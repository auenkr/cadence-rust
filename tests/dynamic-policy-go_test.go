// Injected into the real configstore package; reads actual client and Collection.
package configstore

import (
	"encoding/json"
	dc "github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/types"
	"os"
	"testing"
)

func TestRustDynamicPolicyFixtures(t *testing.T) {
	data, err := os.ReadFile(os.Getenv("CADENCE_DYNAMIC_POLICY_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name   string                      `json:"name"`
		Key    string                      `json:"key"`
		Domain string                      `json:"domain"`
		Values []*types.DynamicConfigValue `json:"values"`
	}
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	var original []map[string]json.RawMessage
	if err = json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	for i, c := range cases {
		var name string
		switch c.Key {
		case "cooldown":
			name = dynamicproperties.FrontendFailoverCoolDown.String()
		case "min":
			name = dynamicproperties.MinRetentionDays.String()
		case "bad":
			name = dynamicproperties.FrontendMaxBadBinaries.String()
		case "history":
			name = dynamicproperties.FrontendFailoverHistoryMaxSize.String()
		default:
			t.Fatal(c.Key)
		}
		client := &configStoreClient{}
		client.values.Store(cacheEntry{dcEntries: map[string]*types.DynamicConfigEntry{name: {Name: name, Values: c.Values}}})
		collection := dc.NewCollection(client, log.NewNoop())
		var value int64
		switch c.Key {
		case "cooldown":
			value = int64(collection.GetDurationPropertyFilteredByDomain(dynamicproperties.FrontendFailoverCoolDown)(c.Domain))
		case "min":
			value = int64(collection.GetIntProperty(dynamicproperties.MinRetentionDays)())
		case "bad":
			value = int64(collection.GetIntPropertyFilteredByDomain(dynamicproperties.FrontendMaxBadBinaries)(c.Domain))
		case "history":
			value = int64(collection.GetIntPropertyFilteredByDomain(dynamicproperties.FrontendFailoverHistoryMaxSize)(c.Domain))
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		original[i]["expected"] = encoded
	}
	encoded, err := json.MarshalIndent(original, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_DYNAMIC_POLICY_OUTPUT"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
