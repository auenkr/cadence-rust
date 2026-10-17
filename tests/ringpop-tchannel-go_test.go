package ringpopprovider

import (
	"os"
	"testing"
	"time"

	"github.com/uber/tchannel-go"
	channeljson "github.com/uber/tchannel-go/json"
)

func TestRustRingpopTChannel(t *testing.T) {
	addr := os.Getenv("CADENCE_RINGPOP_TCHANNEL")
	if addr == "" {
		t.Fatal("CADENCE_RINGPOP_TCHANNEL is required")
	}

	ch, err := tchannel.NewChannel("rust-ringpop-probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	client := channeljson.NewClient(ch, "ringpop", &channeljson.ClientOptions{HostPort: addr})
	ctx, cancel := channeljson.NewContext(5 * time.Second)
	defer cancel()

	// Join
	joinReq := map[string]interface{}{
		"app":               "cadence",
		"source":            "127.0.0.1:7933",
		"incarnationNumber": int64(1729123456),
		"timeout":           int64(1000000000),
		"labels": map[string]string{
			"__identity":  "host-a",
			"serviceName": "matching",
			"tchannel":    "7933",
			"grpc":        "7833",
		},
	}
	var joinResp map[string]interface{}
	if err := client.Call(ctx, "/protocol/join", joinReq, &joinResp); err != nil {
		t.Fatalf("join: %v", err)
	}
	if joinResp["app"] != "cadence" {
		t.Fatalf("unexpected join app: %#v", joinResp["app"])
	}
	if joinResp["coordinator"] != "127.0.0.1:7934" {
		t.Fatalf("unexpected join coordinator: %#v", joinResp["coordinator"])
	}
	if _, ok := joinResp["membership"].([]interface{}); !ok {
		t.Fatalf("unexpected join membership: %#v", joinResp["membership"])
	}

	// Ping
	pingReq := map[string]interface{}{
		"changes":                 []interface{}{},
		"checksum":                uint32(3735928559),
		"source":                  "127.0.0.1:7933",
		"sourceIncarnationNumber": int64(1729123456),
		"app":                     "cadence",
	}
	var pingResp map[string]interface{}
	if err := client.Call(ctx, "/protocol/ping", pingReq, &pingResp); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if pingResp["app"] != "cadence" {
		t.Fatalf("unexpected ping app: %#v", pingResp["app"])
	}
	if pingResp["source"] != "127.0.0.1:7933" || pingResp["checksum"] != float64(3735928559) {
		t.Fatalf("ping payload was not echoed: %#v", pingResp)
	}

	// Ping-req
	pingReqReq := map[string]interface{}{
		"source":                  "127.0.0.1:7933",
		"sourceIncarnationNumber": int64(1729123456),
		"target":                  "127.0.0.1:7935",
		"checksum":                uint32(3735928559),
		"changes":                 []interface{}{},
	}
	var pingReqResp map[string]interface{}
	if err := client.Call(ctx, "/protocol/ping-req", pingReqReq, &pingReqResp); err != nil {
		t.Fatalf("ping-req: %v", err)
	}
	if pingReqResp["pingStatus"] != true {
		t.Fatalf("unexpected pingStatus: %#v", pingReqResp["pingStatus"])
	}
	if pingReqResp["target"] != "127.0.0.1:7935" {
		t.Fatalf("unexpected ping-req target: %#v", pingReqResp["target"])
	}

	// Application error: ping with empty app
	badPing := map[string]interface{}{
		"changes":                 []interface{}{},
		"checksum":                uint32(0),
		"source":                  "127.0.0.1:7933",
		"sourceIncarnationNumber": int64(1729123456),
		"app":                     "",
	}
	var badPingResp map[string]interface{}
	err = client.Call(ctx, "/protocol/ping", badPing, &badPingResp)
	if err == nil {
		t.Fatalf("expected application error, got %#v", badPingResp)
	}
	// err is json.ErrApplication
	appErr, ok := err.(channeljson.ErrApplication)
	if !ok {
		t.Fatalf("expected ErrApplication, got %T: %v", err, err)
	}
	if appErr["message"] != "Pinged ringpop requires app name" {
		t.Fatalf("unexpected app error: %#v", appErr)
	}

	// Malformed request: send a JSON string body where an object is expected.
	// Use raw tchannel? json client marshals string as JSON string; server handler
	// fails to parse into struct -> system error.
	var malformedResp map[string]interface{}
	err = client.Call(ctx, "/protocol/join", "not-an-object", &malformedResp)
	if err == nil {
		t.Fatalf("expected system error for malformed request")
	}

	// Unknown method
	var unknownMethodResp map[string]interface{}
	err = client.Call(ctx, "/protocol/does-not-exist", joinReq, &unknownMethodResp)
	if err == nil {
		t.Fatalf("expected system error for unknown method")
	}

	// Unknown service
	badClient := channeljson.NewClient(ch, "ringpop-does-not-exist", &channeljson.ClientOptions{HostPort: addr})
	var unknownServiceResp map[string]interface{}
	err = badClient.Call(ctx, "/protocol/join", joinReq, &unknownServiceResp)
	if err == nil {
		t.Fatalf("expected system error for unknown service")
	}

}
