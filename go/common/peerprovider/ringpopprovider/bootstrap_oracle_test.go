package ringpopprovider

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/peerprovider/ringpopprovider/config"
	"gopkg.in/yaml.v2"
)

// This test is an executable oracle over the pinned Ringpop discovery
// providers selected by Cadence's production factory.
func TestBootstrapProviderActualOracle(t *testing.T) {
	cases := []struct {
		name  string
		mode  config.BootstrapMode
		hosts []string
	}{
		{"empty", config.BootstrapModeHosts, []string{}},
		{"duplicate-preserving", config.BootstrapModeHosts, []string{" a:1 ", "a:1", "a:1", "[::1]:2"}},
	}
	for _, tc := range cases {
		provider, err := newDiscoveryProvider(&config.Config{BootstrapMode: tc.mode, BootstrapHosts: tc.hosts}, testlogger.New(t))
		if err != nil {
			t.Fatal(err)
		}
		got, err := provider.Hosts()
		if err != nil {
			t.Fatal(err)
		}
		if !equalStringSlices(got, tc.hosts) {
			t.Fatalf("%s hosts: %#v", tc.name, got)
		}
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "hosts.json")
	if err := os.WriteFile(path, []byte(`[]`), 0600); err != nil {
		t.Fatal(err)
	}
	emptyProvider, err := newDiscoveryProvider(&config.Config{BootstrapMode: config.BootstrapModeFile, BootstrapFile: path}, testlogger.New(t))
	if err != nil {
		t.Fatal(err)
	}
	empty, err := emptyProvider.Hosts()
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty file hosts %v, error %v", empty, err)
	}
	if err := os.WriteFile(path, []byte(`["first:1","first:1"," raw "]`), 0600); err != nil {
		t.Fatal(err)
	}
	provider, err := newDiscoveryProvider(&config.Config{BootstrapMode: config.BootstrapModeFile, BootstrapFile: path}, testlogger.New(t))
	if err != nil {
		t.Fatal(err)
	}
	first, err := provider.Hosts()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`["updated:2"]`), 0600); err != nil {
		t.Fatal(err)
	}
	updated, err := provider.Hosts()
	if err != nil {
		t.Fatal(err)
	}
	missing, err := newDiscoveryProvider(&config.Config{BootstrapMode: config.BootstrapModeFile, BootstrapFile: filepath.Join(dir, "missing")}, testlogger.New(t))
	if err != nil {
		t.Fatal(err)
	}
	_, missingErr := missing.Hosts()
	if missingErr == nil {
		t.Fatal("missing file unexpectedly succeeded")
	}
	if err := os.WriteFile(path, []byte(`["a",`), 0600); err != nil {
		t.Fatal(err)
	}
	_, syntaxErr := provider.Hosts()
	if syntaxErr == nil {
		t.Fatal("malformed JSON unexpectedly succeeded")
	}
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, objectErr := provider.Hosts()
	if objectErr == nil {
		t.Fatal("JSON object unexpectedly decoded as []string")
	}

	var parsed config.Config
	if err := yaml.Unmarshal([]byte("name: matching\nbootstrapMode: hosts\nbootstrapHosts: [' x ', x, x]\n"), &parsed); err != nil {
		t.Fatal(err)
	}
	if err := parsed.Validate(); err != nil {
		t.Fatal(err)
	}
	durations := map[string]int64{}
	for key, value := range map[string]string{"max": "9223372036854775807ns", "min": "-9223372036854775808ns", "fraction": "1.000000001s"} {
		var durationConfig config.Config
		text := fmt.Sprintf("name: x\nbootstrapMode: hosts\nbootstrapHosts: [x]\nmaxJoinDuration: %s\n", value)
		if err := yaml.Unmarshal([]byte(text), &durationConfig); err != nil {
			t.Fatalf("Go duration %s: %v", key, err)
		}
		if err := durationConfig.Validate(); err != nil {
			t.Fatal(err)
		}
		durations[key] = int64(durationConfig.MaxJoinDuration)
	}
	configErrors := map[string]string{}
	for name, candidate := range map[string]config.Config{
		"missingName":  {},
		"unknownMode":  {Name: "x"},
		"missingFile":  {Name: "x", BootstrapMode: config.BootstrapModeFile},
		"missingHosts": {Name: "x", BootstrapMode: config.BootstrapModeHosts},
		"custom":       {Name: "x", BootstrapMode: config.BootstrapModeCustom},
	} {
		if err := candidate.Validate(); err != nil {
			configErrors[name] = err.Error()
		}
	}
	result := map[string]any{"static": cases[1].hosts, "emptyFile": empty, "file": first, "fileAfterUpdate": updated,
		"missingErrorType": fmt.Sprintf("%T", missingErr), "malformedErrorType": fmt.Sprintf("%T", syntaxErr), "objectErrorType": fmt.Sprintf("%T", objectErr),
		"configHosts": parsed.BootstrapHosts, "configMode": int(parsed.BootstrapMode), "configMaxJoinDurationNanos": int64(parsed.MaxJoinDuration), "configErrors": configErrors, "durations": durations}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("BOOTSTRAP_PROVIDER_ORACLE=%s\n", b)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
