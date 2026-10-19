package ringpopprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
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

type dnsOracleResolver struct {
	hostAnswers map[string][][]string
	hostErrors  map[string]error
	hostCalls   []string
	srvAnswers map[string][]*net.SRV
	srvErrors  map[string]error
	srvCalls   []map[string]string
}

func (r *dnsOracleResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	r.hostCalls = append(r.hostCalls, host)
	if err := r.hostErrors[host]; err != nil {
		return nil, err
	}
	answers := r.hostAnswers[host]
	if len(answers) == 0 {
		return nil, fmt.Errorf("Host was not resolved: %s", host)
	}
	answerIndex := 0
	for _, prior := range r.hostCalls[:len(r.hostCalls)-1] {
		if prior == host {
			answerIndex++
		}
	}
	if answerIndex >= len(answers) {
		answerIndex = len(answers) - 1
	}
	return answers[answerIndex], nil
}

func (r *dnsOracleResolver) LookupSRV(_ context.Context, service, proto, name string) (string, []*net.SRV, error) {
	r.srvCalls = append(r.srvCalls, map[string]string{"service": service, "proto": proto, "name": name})
	if err := r.srvErrors[service]; err != nil {
		return "", nil, err
	}
	answers, ok := r.srvAnswers[service]
	if !ok {
		return "", nil, fmt.Errorf("Host was not resolved: %s", service)
	}
	return "oracle", answers, nil
}

func TestDiscoveryDNSActualOracle(t *testing.T) {
	logger := testlogger.New(t)
	result := map[string]any{}
	resolver := &dnsOracleResolver{hostAnswers: map[string][][]string{
		"seed.example.net": {{"10.0.0.1", "2001:db8::1", "10.0.0.1"}},
	},}
	provider := newDNSProvider([]string{"seed.example.net:1111", "seed.example.net:2222", "seed.example.net:1111"}, resolver, logger)
	addresses, err := provider.Hosts()
	if err != nil { t.Fatal(err) }
	sort.Strings(addresses)
	result["dnsSuccess"] = map[string]any{"addresses": addresses, "hostCallCount": len(resolver.hostCalls)}

	resolver = &dnsOracleResolver{hostAnswers: map[string][][]string{"2001:db8::1": {{"2001:db8::1"}}}}
	provider = newDNSProvider([]string{"[2001:db8::1]:0007"}, resolver, logger)
	addresses, err = provider.Hosts()
	if err != nil { t.Fatal(err) }
	result["dnsIpv6Seed"] = addresses

	notFound := &net.DNSError{Err: "no such host", Name: "missing.example.net", IsNotFound: true}
	resolver = &dnsOracleResolver{hostErrors: map[string]error{"missing.example.net": notFound}}
	provider = newDNSProvider([]string{"missing.example.net:1", "missing.example.net:2"}, resolver, logger)
	addresses, err = provider.Hosts()
	if err != nil { t.Fatal(err) }
	result["dnsNotFound"] = map[string]any{"addresses": addresses, "hostCallCount": len(resolver.hostCalls)}

	resolver = &dnsOracleResolver{hostAnswers: map[string][][]string{"empty.example.net": {nil}}}
	provider = newDNSProvider([]string{"empty.example.net:1", "empty.example.net:2"}, resolver, logger)
	addresses, err = provider.Hosts()
	if err != nil { t.Fatal(err) }
	result["dnsEmptySuccess"] = map[string]any{"addresses": addresses, "hostCallCount": len(resolver.hostCalls)}

	resolver = &dnsOracleResolver{hostAnswers: map[string][][]string{"again.example.net": {{"10.1.0.1"}, {"10.1.0.2"}}}}
	provider = newDNSProvider([]string{"again.example.net:99"}, resolver, logger)
	first, err := provider.Hosts(); if err != nil { t.Fatal(err) }
	second, err := provider.Hosts(); if err != nil { t.Fatal(err) }
	result["dnsAcrossCalls"] = map[string]any{"first": first, "second": second, "hostCallCount": len(resolver.hostCalls)}

	resolver = &dnsOracleResolver{hostErrors: map[string]error{"bad.example.net": fmt.Errorf("temporary resolver failure")}}
	provider = newDNSProvider([]string{"bad.example.net:8"}, resolver, logger)
	_, err = provider.Hosts()
	if err == nil { t.Fatal("expected non-not-found DNS failure") }
	result["dnsOtherError"] = err.Error()

	resolver = &dnsOracleResolver{srvAnswers: map[string][]*net.SRV{"api": {
		{Target: "v4.example.net.", Port: 7755, Priority: 1, Weight: 1},
		{Target: "missing.example.net.", Port: 7000, Priority: 1, Weight: 1},
		{Target: "v6.example.net.", Port: 7566, Priority: 2, Weight: 1},
	}}, hostAnswers: map[string][][]string{
		"v4.example.net.": {{"10.0.0.1", "10.0.0.2"}},
		"v6.example.net.": {{"2001:db8::2"}},
	}, hostErrors: map[string]error{"missing.example.net.": notFound}}
	providerSRV := newDNSSRVProvider([]string{"api.example.net."}, resolver, logger)
	addresses, err = providerSRV.Hosts()
	if err != nil { t.Fatal(err) }
	result["srvSuccess"] = map[string]any{"addresses": addresses, "srvCalls": resolver.srvCalls}

	resolver = &dnsOracleResolver{srvErrors: map[string]error{"missing": notFound}}
	providerSRV = newDNSSRVProvider([]string{"missing.example.net"}, resolver, logger)
	_, err = providerSRV.Hosts()
	if err == nil { t.Fatal("expected SRV lookup failure") }
	result["srvLookupError"] = err.Error()

	providerSRV = newDNSSRVProvider([]string{"short.name"}, &dnsOracleResolver{}, logger)
	_, err = providerSRV.Hosts()
	if err == nil { t.Fatal("expected malformed SRV seed") }
	result["srvMalformedSeed"] = err.Error()

	resolver = &dnsOracleResolver{srvAnswers: map[string][]*net.SRV{"api": {
		{Target: "good.example.net.", Port: 7001}, {Target: "bad.example.net.", Port: 7002},
	}}, hostAnswers: map[string][][]string{"good.example.net.": {{"10.2.0.1"}}},
		hostErrors: map[string]error{"bad.example.net.": fmt.Errorf("temporary target failure")}}
	providerSRV = newDNSSRVProvider([]string{"api.example.net"}, resolver, logger)
	_, err = providerSRV.Hosts()
	if err == nil { t.Fatal("expected target lookup failure") }
	result["srvTargetError"] = err.Error()

	resolver = &dnsOracleResolver{srvAnswers: map[string][]*net.SRV{
		"api": {{Target: "one.example.", Port: 80}},
	}, hostAnswers: map[string][][]string{
		"one.example.": {{"10.3.0.1"}}, "two.example.": {{"10.3.0.2"}},
	}}
	providerSRV = newDNSSRVProvider([]string{"api.one.example", "api.two.example"}, resolver, logger)
	addresses, err = providerSRV.Hosts()
	if err != nil { t.Fatal(err) }
	allowed := equalStringSlices(addresses, []string{"10.3.0.1:80", "10.3.0.1:80"}) || equalStringSlices(addresses, []string{"10.3.0.2:80", "10.3.0.2:80"})
	if !allowed || len(resolver.srvCalls) != 1 { t.Fatalf("service-only cache semantics: hosts=%v calls=%v", addresses, resolver.srvCalls) }
	result["srvServiceOnlyCacheValidated"] = true

	b, err := json.Marshal(result)
	if err != nil { t.Fatal(err) }
	fmt.Printf("DNS_PROVIDER_ORACLE=%s\n", b)
}

// TestSystemLookupHostOracle supplies this machine's actual Go resolver
// results to the Rust/Linux resolver parity test. The result is deliberately
// not a checked-in fixture because NSS, /etc/hosts, and interface names vary.
func TestSystemLookupHostOracle(t *testing.T) {
	localhost, err := net.DefaultResolver.LookupHost(context.Background(), "localhost")
	if err != nil {
		t.Fatal(err)
	}
	literal := "2001:0db8::1"
	literalResult, err := net.DefaultResolver.LookupHost(context.Background(), literal)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string][]string{"localhost": localhost, "literal": literalResult}
	b, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("SYSTEM_LOOKUP_HOST_ORACLE=%s\n", b)
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
