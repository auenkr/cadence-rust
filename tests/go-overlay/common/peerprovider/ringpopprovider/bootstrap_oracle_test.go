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
	emptyProvider, err := newDiscoveryProvider(&config.Config{BootstrapMode: config.BootstrapModeFile, BootstrapFile: path}, testlogger.New(t))
	if err != nil {
		t.Fatal(err)
	}
	inputBytes, err := os.ReadFile("../../../../tests/fixtures/ringpop_bootstrap_provider_inputs.json")
	if err != nil {
		t.Fatal(err)
	}
	var inputs struct {
		FileInputs []string `json:"fileInputs"`
	}
	if err := json.Unmarshal(inputBytes, &inputs); err != nil {
		t.Fatal(err)
	}
	type observation struct {
		Hosts     []string `json:"hosts"`
		ErrorType string   `json:"errorType"`
	}
	fileObservations := make([]observation, 0, len(inputs.FileInputs))
	for _, raw := range inputs.FileInputs {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		hosts, readErr := emptyProvider.Hosts()
		if readErr != nil {
			fileObservations = append(fileObservations, observation{ErrorType: fmt.Sprintf("%T", readErr)})
		} else {
			fileObservations = append(fileObservations, observation{Hosts: hosts})
		}
	}
	byteCases := []struct {
		name string
		data []byte
	}{
		{"invalidUtf8InString", []byte{'[', '"', 0xff, '"', ']'}},
		{"invalidUtf8ContinuationInString", []byte{'[', '"', 0xe2, '(', 0xa1, '"', ']'}},
		{"escapedLoneSurrogate", []byte(`["\ud800"]`)},
		{"invalidUtf8OutsideString", []byte{'[', 0xff, ']'}},
		{"largeExponent", []byte(`[1e100000]`)},
		{"largeInteger", []byte(`[999999999999999999999999999999999999999999999999999999]`)},
		{"topLevelLargeExponent", []byte(`1e100000`)},
		{"topLevelLargeInteger", []byte(`999999999999999999999999999999999999999999999999999999`)},
	}
	byteObservations := make(map[string]observation, len(byteCases))
	for _, tc := range byteCases {
		if err := os.WriteFile(path, tc.data, 0600); err != nil {
			t.Fatal(err)
		}
		hosts, readErr := emptyProvider.Hosts()
		if readErr != nil {
			byteObservations[tc.name] = observation{ErrorType: fmt.Sprintf("%T", readErr)}
		} else {
			byteObservations[tc.name] = observation{Hosts: hosts}
		}
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
	for key, value := range map[string]string{"max": "9223372036854775807ns", "min": "-9223372036854775808ns", "fraction": "1.000000001s", "preciseThirdHour": "0.333333333333333333h", "preciseAlmostHour": "0.999999999999999999h"} {
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
	bootstrapModes := map[string]any{}
	for key, value := range map[string]string{"mixed": "HoStS", "upper": "DNS-SRV", "unknown": "WeIrD"} {
		var modeConfig config.Config
		err := yaml.Unmarshal([]byte("name: x\nbootstrapMode: "+value+"\n"), &modeConfig)
		if err != nil {
			bootstrapModes[key] = map[string]any{"error": err.Error()}
		} else {
			bootstrapModes[key] = map[string]any{"mode": int(modeConfig.BootstrapMode)}
		}
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
	result := map[string]any{"static": cases[1].hosts, "fileInputs": inputs.FileInputs, "fileObservations": fileObservations, "byteObservations": byteObservations, "file": first, "fileAfterUpdate": updated,
		"missingErrorType": fmt.Sprintf("%T", missingErr), "malformedErrorType": fmt.Sprintf("%T", syntaxErr), "objectErrorType": fmt.Sprintf("%T", objectErr),
		"configHosts": parsed.BootstrapHosts, "configMode": int(parsed.BootstrapMode), "configMaxJoinDurationNanos": int64(parsed.MaxJoinDuration), "configErrors": configErrors, "durations": durations, "bootstrapModes": bootstrapModes}
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
