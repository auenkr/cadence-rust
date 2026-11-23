package dynamicconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
)

func oracleClient(t *testing.T, content string) (Client, chan struct{}, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dynamic.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	client, err := NewFileBasedClient(&FileBasedClientConfig{Filepath: path, PollInterval: time.Second}, log.NewNoop(), done)
	if err != nil {
		close(done)
		t.Fatal(err)
	}
	t.Cleanup(func() { close(done) })
	return client, done, path
}

func TestFileBasedOracleOrderedSubsetAndLastFallback(t *testing.T) {
	client, _, _ := oracleClient(t, `testGetIntPropertyKey:
- value: 10
- value: 20
  constraints: {domainName: sample}
- value: 30
  constraints: {domainName: sample, taskListName: queue}
- value: 40
`)
	value, err := client.GetIntValue(dynamicproperties.TestGetIntPropertyKey, nil)
	if err != nil || value != 40 {
		t.Fatalf("last unfiltered value = %d, %v; want 40", value, err)
	}
	filters := map[dynamicproperties.Filter]interface{}{
		dynamicproperties.DomainName: "sample",
		dynamicproperties.TaskListName: "queue",
	}
	value, err = client.GetIntValue(dynamicproperties.TestGetIntPropertyKey, filters)
	if err != nil || value != 20 {
		t.Fatalf("first matching subset value = %d, %v; want 20", value, err)
	}
}

func TestFileBasedOracleNullMissingAndIntegerType(t *testing.T) {
	client, _, _ := oracleClient(t, `testGetIntPropertyKey:
- value: 1.0
`)
	value, err := client.GetIntValue(dynamicproperties.TestGetIntPropertyKey, nil)
	if err == nil || value != dynamicproperties.TestGetIntPropertyKey.DefaultInt() {
		t.Fatalf("float YAML value result = %d, %v; want default plus type error", value, err)
	}
	client, _, _ = oracleClient(t, `testGetBoolPropertyKey:
- constraints: null
- value: null
`)
	valueAny, err := client.GetValue(dynamicproperties.TestGetBoolPropertyKey)
	if err != nil || valueAny != nil {
		t.Fatalf("explicit null value result = %#v, %v; want nil without error", valueAny, err)
	}
	for _, content := range []string{"null\n", "emptyValues: null\n"} {
		path := filepath.Join(t.TempDir(), "empty.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		client, err := NewFileBasedClient(&FileBasedClientConfig{Filepath: path}, log.NewNoop(), done)
		if err != nil || client == nil {
			t.Fatalf("empty config %q failed construction: %v", content, err)
		}
		close(done)
	}
}

func TestFileBasedOracleValueMapConversionAndConstraintNonconversion(t *testing.T) {
	for _, key := range []string{"1", "true"} {
		path := filepath.Join(t.TempDir(), "bad-value-map.yaml")
		if err := os.WriteFile(path, []byte("testGetBoolPropertyKey:\n- value: {"+key+": value}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		client, err := NewFileBasedClient(&FileBasedClientConfig{Filepath: path}, log.NewNoop(), make(chan struct{}))
		if err == nil || client != nil {
			t.Fatalf("value map with key %s accepted; want recursive conversion error", key)
		}
	}
	client, _, _ := oracleClient(t, `testGetBoolPropertyKey:
- value: false
- value: true
  constraints:
    domainName: {1: nested key}
- value: true
`)
	value, err := client.GetBoolValue(dynamicproperties.TestGetBoolPropertyKey, nil)
	if err != nil || value != true {
		t.Fatalf("nonmatching nested constraint result = %v, %v; want fallback true", value, err)
	}
}

func TestFileBasedOraclePollReload(t *testing.T) {
	client, _, path := oracleClient(t, `testGetIntPropertyKey:
- value: 1
`)
	if err := os.WriteFile(path, []byte("testGetIntPropertyKey:\n- value: 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(7 * time.Second)
	for {
		value, err := client.GetIntValue(dynamicproperties.TestGetIntPropertyKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		if value == 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("filebased client did not reload; last value %d", value)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
