package membership

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	ringswim "github.com/uber/ringpop-go/swim"
	ringutil "github.com/uber/ringpop-go/util"
)

// Mirrored structs below reproduce the JSON tags of Ringpop's unexported SWIM
// wire structs exactly. Source citations:
//   Change:        ringpop-go/swim/member.go:299-310
//   joinRequest:   ringpop-go/swim/join_sender.go:58-64
//   joinResponse:   ringpop-go/swim/join_handler.go:27-32
//   ping:          ringpop-go/swim/ping_sender.go:35-41
//   pingRequest:   ringpop-go/swim/ping_request_sender.go:35-41
//   pingResponse:  ringpop-go/swim/ping_request_handler.go:26-30
// util.Timestamp is mirrored as int64 because it marshals as Unix seconds
// (ringpop-go/util/util.go:270-289). Exported Member uses the original type.
//
// Limitation: unexported Ringpop structs are mirrored rather than imported, so
// this generator verifies JSON tags/types but cannot call unexported methods.

type wireChange struct {
	Source            string            `json:"source"`
	SourceIncarnation int64             `json:"sourceIncarnationNumber"`
	Address           string            `json:"address"`
	Incarnation       int64             `json:"incarnationNumber"`
	Status            string            `json:"status"`
	Tombstone         bool              `json:"tombstone,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Timestamp         ringutil.Timestamp `json:"timestamp"`
}

type wireJoinRequest struct {
	App         string            `json:"app"`
	Source      string            `json:"source"`
	Incarnation int64             `json:"incarnationNumber"`
	Timeout     int64             `json:"timeout"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type wireJoinResponse struct {
	App         string       `json:"app"`
	Coordinator string       `json:"coordinator"`
	Membership  []wireChange `json:"membership"`
	Checksum    uint32       `json:"membershipChecksum"`
}

type wirePing struct {
	Changes           []wireChange `json:"changes"`
	Checksum          uint32       `json:"checksum"`
	Source            string       `json:"source"`
	SourceIncarnation int64        `json:"sourceIncarnationNumber"`
	App               string       `json:"app"`
}

type wirePingRequest struct {
	Source            string       `json:"source"`
	SourceIncarnation int64        `json:"sourceIncarnationNumber"`
	Target            string       `json:"target"`
	Checksum          uint32       `json:"checksum"`
	Changes           []wireChange `json:"changes"`
}

type wirePingResponse struct {
	Ok      bool         `json:"pingStatus"`
	Target  string       `json:"target"`
	Changes []wireChange `json:"changes"`
}

type wireApplicationError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

func TestRustRingpopWireFixtures(t *testing.T) {
	out := os.Getenv("CADENCE_RINGPOP_WIRE_OUTPUT")
	if out == "" {
		t.Fatal("CADENCE_RINGPOP_WIRE_OUTPUT is required")
	}

	cases := map[string]interface{}{}

	// Member uses original exported type (ringpop-go/swim/member.go:62-67).
	cases["member_default"] = &ringswim.Member{}
	cases["member_labels"] = &ringswim.Member{
		Address:     "127.0.0.1:7933",
		Status:      ringswim.Alive,
		Incarnation: 1729123456,
		Labels:      ringswim.LabelMap{"__identity": "host-a", "serviceName": "matching", "tchannel": "7933", "grpc": "7833"},
	}

	ts := ringutil.Timestamp(time.Unix(1729123456, 0))
	cases["change_default"] = &wireChange{}
	cases["change_tombstone"] = &wireChange{
		Source:            "127.0.0.1:7934",
		SourceIncarnation: 1729123400,
		Address:           "127.0.0.1:7933",
		Incarnation:       1729123456,
		Status:            ringswim.Tombstone,
		Tombstone:         false,
		Labels:            map[string]string{"serviceName": "matching"},
		Timestamp:         ts,
	}
	cases["change_labels"] = &wireChange{
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		Address:           "127.0.0.1:7933",
		Incarnation:       1729123456,
		Status:            ringswim.Alive,
		Labels:            map[string]string{"serviceName": "matching", "tchannel": "7933", "grpc": "7833"},
		Timestamp:         ts,
	}
	cases["change_faulty_tombstone_flag"] = &wireChange{
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		Address:           "127.0.0.1:7933",
		Incarnation:       1729123456,
		Status:            ringswim.Faulty,
		Tombstone:         true,
		Timestamp:         ts,
	}

	cases["join_request_default"] = &wireJoinRequest{}
	cases["join_request_labels"] = &wireJoinRequest{
		App:         "cadence",
		Source:      "127.0.0.1:7933",
		Incarnation: 1729123456,
		Timeout:     1000000000,
		Labels:      map[string]string{"__identity": "host-a", "serviceName": "matching", "tchannel": "7933", "grpc": "7833"},
	}

	cases["join_response_nil_membership"] = &wireJoinResponse{
		App:         "cadence",
		Coordinator: "127.0.0.1:7934",
		Membership:  nil,
		Checksum:    3735928559,
	}
	cases["join_response_empty_membership"] = &wireJoinResponse{
		App:         "cadence",
		Coordinator: "127.0.0.1:7934",
		Membership:  []wireChange{},
		Checksum:    3735928559,
	}
	cases["join_response_populated"] = &wireJoinResponse{
		App:         "cadence",
		Coordinator: "127.0.0.1:7934",
		Membership: []wireChange{{
			Source:            "127.0.0.1:7934",
			SourceIncarnation: 1729123400,
			Address:           "127.0.0.1:7934",
			Incarnation:       1729123400,
			Status:            ringswim.Alive,
			Labels:            map[string]string{"__identity": "host-b", "serviceName": "matching", "tchannel": "7934", "grpc": "7834"},
			Timestamp:         ringutil.Timestamp(time.Unix(1729123400, 0)),
		}},
		Checksum: 3735928559,
	}

	cases["ping_nil_changes"] = &wirePing{
		Changes:           nil,
		Checksum:          3735928559,
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		App:               "cadence",
	}
	cases["ping_empty_changes"] = &wirePing{
		Changes:           []wireChange{},
		Checksum:          3735928559,
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		App:               "cadence",
	}
	cases["ping_populated_changes"] = &wirePing{
		Changes: []wireChange{{
			Source:            "127.0.0.1:7933",
			SourceIncarnation: 1729123456,
			Address:           "127.0.0.1:7933",
			Incarnation:       1729123456,
			Status:            ringswim.Alive,
			Labels:            map[string]string{"serviceName": "matching", "tchannel": "7933", "grpc": "7833"},
			Timestamp:         ts,
		}},
		Checksum:          3735928559,
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		App:               "cadence",
	}

	cases["ping_request_nil_changes"] = &wirePingRequest{
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		Target:            "127.0.0.1:7935",
		Checksum:          3735928559,
		Changes:           nil,
	}
	cases["ping_request_empty_changes"] = &wirePingRequest{
		Source:            "127.0.0.1:7933",
		SourceIncarnation: 1729123456,
		Target:            "127.0.0.1:7935",
		Checksum:          3735928559,
		Changes:           []wireChange{},
	}

	cases["ping_response_default"] = &wirePingResponse{
		Ok:      false,
		Target:  "",
		Changes: nil,
	}
	cases["ping_response_populated"] = &wirePingResponse{
		Ok:      true,
		Target:  "127.0.0.1:7935",
		Changes: []wireChange{},
	}

	cases["application_error"] = &wireApplicationError{
		Type:    "error",
		Message: "node is not ready to handle requests",
	}

	data, err := json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
