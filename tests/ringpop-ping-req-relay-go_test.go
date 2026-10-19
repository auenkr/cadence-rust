package swim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/uber/ringpop-go/discovery/statichosts"
	"github.com/uber/tchannel-go"
	tchanneljson "github.com/uber/tchannel-go/json"
)

// A real Go requester sends PING-REQ through the Rust production handler to a
// real pinned Go Ringpop target. The Rust process is the only relay responder.
func TestGoRequesterRustRelayGoTarget(t *testing.T) {
	bin := os.Getenv("RINGPOP_PING_REQ_RELAY_PROBE")
	if bin == "" {
		t.Fatal("RINGPOP_PING_REQ_RELAY_PROBE required")
	}
	targetChannel, err := tchannel.NewChannel("cadence", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer targetChannel.Close()
	if err = targetChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	targetAddress := targetChannel.PeerInfo().HostPort
	target := NewNode("cadence", targetAddress, targetChannel.GetSubChannel("ringpop"), nil)
	defer target.Destroy()
	if _, err = target.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(targetAddress)}); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relayAddress := listener.Addr().String()
	_ = listener.Close()
	stateFile := t.TempDir() + "/state.json"
	cmd := exec.Command(bin, relayAddress)
	cmd.Env = append(os.Environ(), "RINGPOP_RELAY_STATE_FILE="+stateFile)
	if err = cmd.Start(); err != nil {
		t.Fatalf("start Rust relay: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, e := net.DialTimeout("tcp", relayAddress, 50*time.Millisecond)
		if e == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Rust relay did not listen at %s", relayAddress)
		}
		time.Sleep(20 * time.Millisecond)
	}

	requesterChannel, err := tchannel.NewChannel("cadence", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer requesterChannel.Close()
	if err = requesterChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	requesterAddress := requesterChannel.PeerInfo().HostPort
	requester := NewNode("cadence", requesterAddress, requesterChannel.GetSubChannel("ringpop"), nil)
	defer requester.Destroy()
	if _, err = requester.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(targetAddress)}); err != nil {
		t.Fatal(err)
	}

	response, err := newPingRequestSender(requester, relayAddress, targetAddress, 4*time.Second).SendPingRequest()
	if err != nil {
		t.Fatalf("Go requester -> Rust relay: %v", err)
	}
	if !response.Ok || response.Target != targetAddress {
		t.Fatalf("unexpected response: %+v", response)
	}
	if _, ok := requester.memberlist.Member(targetAddress); !ok {
		t.Fatal("requester did not merge the Go target change returned by the Rust relay")
	}
	if _, ok := target.memberlist.Member(requesterAddress); !ok {
		t.Fatal("Go target did not receive the requester's change through the Rust relay")
	}

	// ArgReadJSON above establishes that null becomes a zero-value request. The
	// pinned ready Go handler then panics when sendPing asks TChannel for an
	// empty target peer. Recover the source-level behavior here instead of
	// crashing the shared test process with a network call.
	goNullCtx, goNullCancel := tchanneljson.NewContext(time.Second)
	var goNullPanic any
	func() {
		defer func() { goNullPanic = recover() }()
		_, _ = target.pingRequestHandler(goNullCtx, &pingRequest{})
	}()
	goNullCancel()
	if goNullPanic == nil || !strings.Contains(fmt.Sprint(goNullPanic), "blank hostPort") {
		t.Fatalf("ready Go PING-REQ null handler panic = %v, want blank hostPort", goNullPanic)
	}
	rustNullClient := tchanneljson.NewClient(requesterChannel, "ringpop", &tchanneljson.ClientOptions{HostPort: relayAddress})
	rustNullCtx, rustNullCancel := tchanneljson.NewContext(time.Second)
	var rustNullResponse pingResponse
	rustNullErr := rustNullClient.Call(rustNullCtx, "/protocol/ping-req", nil, &rustNullResponse)
	rustNullCancel()
	if rustNullErr != nil || rustNullResponse.Ok || rustNullResponse.Target != "" {
		t.Fatalf("ready Rust PING-REQ null contract: response=%+v error=%v", rustNullResponse, rustNullErr)
	}

	// An unready Go Ringpop node still decodes null into a zero-value request,
	// then rejects it at the readiness gate.
	unreadyChannel, err := tchannel.NewChannel("cadence", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer unreadyChannel.Close()
	unreadyAddress := "127.0.0.1:0"
	unready := NewNode("cadence", unreadyAddress, unreadyChannel.GetSubChannel("ringpop"), nil)
	defer unready.Destroy()
	if err = unreadyChannel.ListenAndServe(unreadyAddress); err != nil {
		t.Fatal(err)
	}
	unreadyClient := tchanneljson.NewClient(requesterChannel, "ringpop", &tchanneljson.ClientOptions{HostPort: unreadyChannel.PeerInfo().HostPort})
	unreadyCtx, unreadyCancel := tchanneljson.NewContext(time.Second)
	var unreadyResponse pingResponse
	unreadyErr := unreadyClient.Call(unreadyCtx, "/protocol/ping-req", nil, &unreadyResponse)
	unreadyCancel()
	if unreadyErr == nil || !strings.Contains(unreadyErr.Error(), "node is not ready to handle requests") {
		t.Fatalf("unready Go PING-REQ null error = %v, want not-ready application error", unreadyErr)
	}
	// A second Go requester asks the Rust relay to ping a closed address. The
	// relay must return pingStatus=false, retain the requester's membership
	// change, and omit any change for the unreachable target.
	failedChannel, err := tchannel.NewChannel("cadence", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer failedChannel.Close()
	if err = failedChannel.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	failedRequesterAddress := failedChannel.PeerInfo().HostPort
	failedRequester := NewNode("cadence", failedRequesterAddress, failedChannel.GetSubChannel("ringpop"), nil)
	defer failedRequester.Destroy()
	if _, err = failedRequester.Bootstrap(&BootstrapOptions{DiscoverProvider: statichosts.New(targetAddress)}); err != nil {
		t.Fatal(err)
	}
	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadTarget := deadListener.Addr().String()
	_ = deadListener.Close()
	failedResponse, err := newPingRequestSender(failedRequester, relayAddress, deadTarget, 4*time.Second).SendPingRequest()
	if err != nil {
		t.Fatalf("Go requester -> Rust relay with unreachable target: %v", err)
	}
	if failedResponse.Ok || failedResponse.Target != deadTarget {
		t.Fatalf("unexpected failed-target response: %+v", failedResponse)
	}
	var counts struct {
		PingReq       int               `json:"ping_req"`
		Members       []string          `json:"members"`
		Dissemination map[string]uint64 `json:"dissemination"`
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(stateFile)
		_ = json.Unmarshal(data, &counts)
		if counts.PingReq == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if counts.PingReq != 3 {
		t.Fatalf("Rust handler count = %d, want 3", counts.PingReq)
	}
	if !containsString(counts.Members, failedRequesterAddress) {
		t.Fatalf("Rust relay did not retain failed requester's change: %v", counts.Members)
	}
	if containsString(counts.Members, deadTarget) {
		t.Fatalf("Rust relay merged an unreachable target: %v", counts.Members)
	}
	if _, ok := counts.Dissemination[failedRequesterAddress]; !ok {
		t.Fatalf("failed requester change was not queued for dissemination: %v", counts.Dissemination)
	}
}

func TestGoTChannelArgReadJSONRelayBodies(t *testing.T) {
	for _, test := range []struct {
		name      string
		body      []byte
		want      error
		wantError bool
	}{
		{name: "empty"},
		{name: "null", body: []byte("null")},
		{name: "one value and trailing JSON", body: []byte("{} {}")},
		{name: "whitespace only", body: []byte("  "), want: io.EOF, wantError: true},
		{name: "malformed field", body: []byte(`{"target":7}`), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var request pingRequest
			err := tchannel.NewArgReader(io.NopCloser(bytes.NewReader(test.body)), nil).ReadJSON(&request)
			if test.want != nil {
				if err == nil || !errors.Is(err, test.want) {
					t.Fatalf("ReadJSON error = %v, want %v", err, test.want)
				}
				return
			}
			if test.wantError {
				if err == nil {
					t.Fatal("ReadJSON unexpectedly succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadJSON: %v", err)
			}
		})
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
