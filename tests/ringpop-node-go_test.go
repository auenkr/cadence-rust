package ringpopprovider

import (
	"os"
	"testing"
	"time"

	"github.com/uber/tchannel-go"
	channeljson "github.com/uber/tchannel-go/json"
)

func TestRustRealRingpopNode(t *testing.T) {
	addr := os.Getenv("CADENCE_RINGPOP_TCHANNEL")
	ch, err := tchannel.NewChannel("rust-ringpop-node-probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	client := channeljson.NewClient(ch, "ringpop", &channeljson.ClientOptions{HostPort: addr})
	ctx, cancel := channeljson.NewContext(5 * time.Second)
	defer cancel()
	var join map[string]interface{}
	if err := client.Call(ctx, "/protocol/join", map[string]interface{}{"app": "cadence", "source": "127.0.0.2:7933"}, &join); err != nil {
		t.Fatalf("real join: %v", err)
	}
	if join["coordinator"] != "127.0.0.1:7934" {
		t.Fatalf("wrong coordinator: %#v", join)
	}
	// Go's encoding/json leaves absent and explicit null scalar fields at their
	// zero values. Exercise the actual TChannel decoder with both forms.
	var zeroValueJoin map[string]interface{}
	if err := client.Call(ctx, "/protocol/join", map[string]interface{}{"app": "cadence", "source": nil, "incarnationNumber": nil}, &zeroValueJoin); err != nil {
		t.Fatalf("join with Go zero-valued null fields: %v", err)
	}
	var ping map[string]interface{}
	err = client.Call(ctx, "/protocol/ping", map[string]interface{}{"app": "cadence", "source": "remote", "changes": nil}, &ping)
	appErr, ok := err.(channeljson.ErrApplication)
	if !ok || appErr["message"] != "node is not ready to handle requests" {
		t.Fatalf("pre-ready ping: %T %v", err, err)
	}
	readyFile := os.Getenv("RINGPOP_NODE_READY_FILE")
	if readyFile == "" {
		t.Fatal("RINGPOP_NODE_READY_FILE is required")
	}
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0600); err != nil {
		t.Fatal(err)
	}
	pingRequest := map[string]interface{}{"app": "cadence", "source": "127.0.0.2:7933", "sourceIncarnationNumber": int64(200), "checksum": uint32(0), "changes": []interface{}{map[string]interface{}{"source": "127.0.0.2:7933", "sourceIncarnationNumber": int64(200), "address": "127.0.0.2:7933", "incarnationNumber": int64(200), "status": "alive"}}}
	for attempt := 0; attempt < 100; attempt++ {
		err = client.Call(ctx, "/protocol/ping", pingRequest, &ping)
		if err == nil {
			break
		}
		appErr, ok := err.(channeljson.ErrApplication)
		if !ok || appErr["message"] != "node is not ready to handle requests" {
			t.Fatalf("ready ping: %T %v", err, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("node did not become ready after test signal: %v", err)
	}
	for i := 0; i < 16; i++ {
		if i == 0 {
			continue
		}
		if err := client.Call(ctx, "/protocol/ping", pingRequest, &ping); err != nil {
			t.Fatalf("ready ping %d: %v", i, err)
		}
	}
	if ping["checksum"] == nil {
		t.Fatalf("ping response missing checksum: %#v", ping)
	}
	changes, ok := ping["changes"].([]interface{})
	if !ok || len(changes) != 2 {
		t.Fatalf("expected full membership sync after delta drain, got %#v", ping["changes"])
	}
	for _, tc := range []struct {
		method  string
		body    interface{}
		message string
	}{
		{"/protocol/join", map[string]interface{}{"app": "wrong", "source": "remote"}, "A node tried joining a different app cluster. The expected app, cadence, did not match the actual app ,wrong"},
		{"/protocol/join", map[string]interface{}{"app": "cadence", "source": "127.0.0.1:7934"}, "A node tried joining a cluster by attempting to join itself. The node ,127.0.0.1:7934, must join someone else."},
		{"/protocol/ping", map[string]interface{}{"app": "wrong"}, "Pinged ringpop has a different app name"},
		{"/protocol/ping", map[string]interface{}{"app": nil}, "Pinged ringpop requires app name"},
	} {
		var out interface{}
		err := client.Call(ctx, tc.method, tc.body, &out)
		got, ok := err.(channeljson.ErrApplication)
		if !ok || got["message"] != tc.message {
			t.Errorf("%s app error: %T %v", tc.method, err, err)
		}
	}
	var out interface{}
	err = client.Call(ctx, "/protocol/join", "malformed-struct", &out)
	if err == nil {
		t.Fatal("malformed JSON did not return system error")
	}
	if _, ok := err.(channeljson.ErrApplication); ok {
		t.Fatalf("malformed JSON returned application error: %v", err)
	}
	err = client.Call(ctx, "join", map[string]interface{}{}, &out)
	if err == nil {
		t.Fatal("bare alias unexpectedly accepted")
	}
	if _, ok := err.(channeljson.ErrApplication); ok {
		t.Fatalf("bare alias returned application error: %v", err)
	}
	err = client.Call(ctx, "/protocol/unknown", map[string]interface{}{}, &out)
	if err == nil {
		t.Fatal("unknown method unexpectedly accepted")
	}
	if _, ok := err.(channeljson.ErrApplication); ok {
		t.Fatalf("unknown method returned application error: %v", err)
	}
}
