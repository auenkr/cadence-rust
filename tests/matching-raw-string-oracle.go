// Original Go byte-string/hash/JSON behavior for Matching boundary tests.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"unicode/utf8"

	farm "github.com/dgryski/go-farm"
	"github.com/gogo/protobuf/proto"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	"github.com/uber/cadence/common"
	"go.uber.org/yarpc/encoding/protobuf"
)

// Build wire strings independently of the generated marshal implementation so
// inbound acceptance is observable even when outbound marshal rejects a value.
func wireString(field uint64, raw []byte) []byte {
	wire := binary.AppendUvarint(nil, field<<3|2)
	wire = binary.AppendUvarint(wire, uint64(len(raw)))
	return append(wire, raw...)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func protobufCase(message proto.Message, fresh func() proto.Message, fields func(proto.Message) []string, wire []byte) map[string]interface{} {
	// This is the codec used by the pinned generated Matching YARPC client,
	// including its gogo Buffer.Marshal path rather than protobuf/v2.
	codec, ok := protobuf.GetCodecForEncoding(protobuf.Encoding)
	if !ok {
		panic("missing pinned YARPC protobuf codec")
	}
	encoded, release, marshalErr := codec.Marshal(message)
	defer release()
	decoded := fresh()
	unmarshalErr := codec.Unmarshal(wire, decoded)
	row := map[string]interface{}{
		"marshalError":   errorText(marshalErr),
		"marshalHex":     hex.EncodeToString(encoded),
		"inputHex":       hex.EncodeToString(wire),
		"unmarshalError": errorText(unmarshalErr),
	}
	if unmarshalErr == nil {
		row["decodedHex"] = fields(decoded)
	}
	if marshalErr == nil {
		roundtrip := fresh()
		err := codec.Unmarshal(encoded, roundtrip)
		row["roundtripError"] = errorText(err)
		if err == nil {
			row["roundtripHex"] = fields(roundtrip)
		}
	}
	return row
}

func protobufCases(raw []byte) map[string]interface{} {
	value := string(raw)
	add := func() proto.Message { return &matchingv1.AddDecisionTaskRequest{} }
	hexString := func(value string) string { return hex.EncodeToString([]byte(value)) }
	return map[string]interface{}{
		"domainId": protobufCase(&matchingv1.AddDecisionTaskRequest{DomainId: value}, add,
			func(m proto.Message) []string {
				return []string{hexString(m.(*matchingv1.AddDecisionTaskRequest).DomainId)}
			},
			wireString(1, raw)),
		"workflowId": protobufCase(&matchingv1.AddDecisionTaskRequest{WorkflowExecution: &apiv1.WorkflowExecution{WorkflowId: value}}, add,
			func(m proto.Message) []string {
				return []string{hexString(m.(*matchingv1.AddDecisionTaskRequest).GetWorkflowExecution().GetWorkflowId())}
			},
			wireString(2, wireString(1, raw))),
		"partitionMap": protobufCase(&matchingv1.AddDecisionTaskRequest{PartitionConfig: map[string]string{value: value}}, add,
			func(m proto.Message) []string {
				entries := m.(*matchingv1.AddDecisionTaskRequest).PartitionConfig
				if len(entries) != 1 {
					panic("partition map entry lost")
				}
				for key, value := range entries {
					return []string{hexString(key), hexString(value)}
				}
				panic("unreachable")
			}, wireString(8, append(wireString(1, raw), wireString(2, raw)...))),
		"pollIdentity": protobufCase(&matchingv1.PollForDecisionTaskRequest{Request: &apiv1.PollForDecisionTaskRequest{Identity: value}},
			func() proto.Message { return &matchingv1.PollForDecisionTaskRequest{} },
			func(m proto.Message) []string {
				return []string{hexString(m.(*matchingv1.PollForDecisionTaskRequest).GetRequest().GetIdentity())}
			},
			wireString(1, wireString(3, raw))),
	}
}

func main() {
	names := [][]byte{
		{}, {'x'}, {0}, {0xff}, {0xfe}, {0xef, 0xbf, 0xbd},
		{0xe1, 0x80, 0xff}, {0xc0, 0xaf}, {0xe2, 0x82}, []byte("é"), []byte("<>&"), []byte("\u2028\u2029"),
	}
	allBytes := make([]byte, 256)
	for i := range allBytes { allBytes[i] = byte(i) }
	names = append(names, allBytes)
	rows := make([]map[string]interface{}, 0, len(names))
	for _, raw := range names {
		encoded, err := json.Marshal(string(raw))
		if err != nil {
			panic(err)
		}
		input := append([]byte("e1b8e2d3-5c1f-4a77-9b0a-6a0a1f0e5d11_"), raw...)
		taskJSON, err := common.NewJSONTaskTokenSerializer().Serialize(&common.TaskToken{
			DomainID: string(raw), WorkflowID: string(raw), WorkflowType: string(raw), RunID: string(raw),
			ScheduleID: -7, ScheduleAttempt: 9, ActivityID: string(raw), ActivityType: string(raw),
		})
		if err != nil { panic(err) }
		queryJSON, err := common.NewJSONTaskTokenSerializer().SerializeQueryTaskToken(&common.QueryTaskToken{
			DomainID: string(raw), WorkflowID: string(raw), RunID: string(raw), TaskList: string(raw), TaskID: string(raw),
		})
		if err != nil { panic(err) }
		rows = append(rows, map[string]interface{}{
			"hex": hex.EncodeToString(raw), "length": len(raw),
			"validUtf8": utf8.Valid(raw), "hash32": farm.Hash32(input),
			"jsonText": string(encoded),
			"protobuf": protobufCases(raw),
			"taskTokenJSON": string(taskJSON), "queryTokenJSON": string(queryJSON),
		})
	}
	if err := json.NewEncoder(os.Stdout).Encode(rows); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
