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
	"github.com/google/uuid"
	"github.com/gocql/gocql"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	matchingv1 "github.com/uber/cadence/.gen/proto/matching/v1"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/service/matching/tasklist"
	"go.uber.org/yarpc/encoding/protobuf"
    "go.uber.org/yarpc/yarpcerrors"
    gogorpc "github.com/gogo/googleapis/google/rpc"
    gogostatus "github.com/gogo/status"
    grpcstatus "google.golang.org/grpc/status"
    googleproto "google.golang.org/protobuf/proto"
    cadencetypes "github.com/uber/cadence/common/types"
    protomapper "github.com/uber/cadence/common/types/mapper/proto"

)

// Follow YARPC's gogo error envelope -> grpc-go status conversion. The
// transport uses protobuf/v2 for the outer google.rpc.Status marshal and drops
// grpc-status-details-bin if it fails (for example invalid Status.message).
func grpcErrorCase(err error) map[string]interface{} {
    mapped := yarpcerrors.FromError(protomapper.FromError(err))
    envelope := &gogorpc.Status{}
    if err := proto.Unmarshal(mapped.Details(), envelope); err != nil { panic(err) }
    status, ok := grpcstatus.FromError(gogostatus.ErrorProto(envelope))
    if !ok { panic("expected gogo GRPCStatus conversion") }
    encoded, marshalErr := googleproto.Marshal(status.Proto())
    if marshalErr != nil { encoded = nil }
    return map[string]interface{}{
        "code": int32(status.Code()), "messageHex": hex.EncodeToString([]byte(status.Message())),
        "transportDetailsHex": hex.EncodeToString(encoded), "marshalError": errorText(marshalErr),
    }
}

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

// These inputs are assembled from wire primitives rather than generated
// messages: each captures the pinned gogo/YARPC decoder's actual behavior.
func nativeWireEdgeCases() []map[string]interface{} {
	varint := func(v uint64) []byte { return binary.AppendUvarint(nil, v) }
	tag := func(field, wire uint64) []byte { return varint(field<<3 | wire) }
	bytesField := func(field uint64, value []byte) []byte {
		out := tag(field, 2)
		out = varintAppend(out, uint64(len(value)))
		return append(out, value...)
	}
	join := func(parts ...[]byte) []byte { var out []byte; for _, part := range parts { out = append(out, part...) }; return out }
	inputs := []struct { name string; wire []byte }{
		{"duplicate_scalar_last_wins", join(bytesField(1, []byte("first")), bytesField(1, []byte("last")))},
		{"wrong_wire_scalar", join(bytesField(1, []byte("kept")), tag(1, 0), varint(7))},
		{"duplicate_raw_map_keys", join(
			bytesField(8, join(bytesField(1, []byte{0xff}), bytesField(2, []byte("raw")))),
			bytesField(8, join(bytesField(1, []byte{0xff}), bytesField(2, []byte("last-raw")))),
			bytesField(8, join(bytesField(1, []byte("\xef\xbf\xbd")), bytesField(2, []byte("replacement")))),
			bytesField(8, bytesField(2, []byte("default-key"))),
		)},
		{"nested_message_merge", join(bytesField(2, bytesField(1, []byte("workflow"))), bytesField(2, bytesField(2, []byte("run"))))},
		{"empty_and_zero_nested_presence", join(bytesField(5, nil), bytesField(5, []byte{0x08, 0x00}))},
		{"unknown_field", bytesField(99, []byte("ignored"))},
		{"truncated_varint", append(tag(1, 0), 0x80)},
		{"truncated_length", []byte{0x0a, 0x05, 'x'}},
		{"wrong_wire_type", tag(1, 5)},
	}
	out := make([]map[string]interface{}, 0, len(inputs))
	for _, input := range inputs {
		decoded := &matchingv1.AddDecisionTaskRequest{}
		codec, ok := protobuf.GetCodecForEncoding(protobuf.Encoding)
		if !ok { panic("missing pinned YARPC protobuf codec") }
		err := codec.Unmarshal(input.wire, decoded)
		row := map[string]interface{}{"name": input.name, "inputHex": hex.EncodeToString(input.wire), "error": errorText(err), "errorType": ""}
		if err != nil { row["errorType"] = fmt.Sprintf("%T", err) }
		row["domainIdHex"] = hex.EncodeToString([]byte(decoded.DomainId))
		row["workflowExecutionPresent"] = decoded.WorkflowExecution != nil
		if decoded.WorkflowExecution != nil {
			row["workflowIdHex"] = hex.EncodeToString([]byte(decoded.WorkflowExecution.WorkflowId))
			row["runIdHex"] = hex.EncodeToString([]byte(decoded.WorkflowExecution.RunId))
		}
		row["scheduleToStartTimeoutPresent"] = decoded.ScheduleToStartTimeout != nil
		maps := make([][]string, 0, len(decoded.PartitionConfig))
		for key, value := range decoded.PartitionConfig { maps = append(maps, []string{hex.EncodeToString([]byte(key)), hex.EncodeToString([]byte(value))}) }
		row["partitionConfigHex"] = maps
		out = append(out, row)
	}
	for _, input := range []struct { name string; wire []byte }{
		{"empty_wrapper_presence", bytesField(4, nil)},
		{"zero_wrapper_presence", bytesField(4, []byte{0x08, 0x00})},
		{"duplicate_wrapper_message_merge", join(bytesField(4, []byte{0x08, 0x07}), bytesField(4, nil))},
	} {
		decoded := &matchingv1.PollForDecisionTaskResponse{}
		codec, ok := protobuf.GetCodecForEncoding(protobuf.Encoding)
		if !ok { panic("missing pinned YARPC protobuf codec") }
		err := codec.Unmarshal(input.wire, decoded)
		row := map[string]interface{}{"name": input.name, "message": "pollDecisionResponse", "inputHex": hex.EncodeToString(input.wire), "error": errorText(err), "previousStartedPresent": decoded.PreviousStartedEventId != nil}
		if decoded.PreviousStartedEventId != nil { row["previousStartedValue"] = decoded.PreviousStartedEventId.Value }
		out = append(out, row)
	}
	return out
}

func varintAppend(out []byte, value uint64) []byte { return binary.AppendUvarint(out, value) }

func identifierCase(raw []byte) map[string]interface{} {
	id, err := tasklist.NewIdentifier("e1b8e2d3-5c1f-4a77-9b0a-6a0a1f0e5d11", string(raw), 0)
	row := map[string]interface{}{"inputHex": hex.EncodeToString(raw), "errorHex": ""}
	if err != nil {
		row["errorHex"] = hex.EncodeToString([]byte(err.Error()))
		return row
	}
	row["rootHex"] = hex.EncodeToString([]byte(id.GetRoot()))
	row["partition"] = id.Partition()
	parents := map[string]string{}
	for _, degree := range []int{0, 1, 2, 8, 9223372036854775807} {
		parents[fmt.Sprint(degree)] = hex.EncodeToString([]byte(id.Parent(degree)))
	}
	row["parentsHex"] = parents
	return row
}

func main() {
	names := [][]byte{
		{}, {'x'}, {0}, {0xff}, {0xfe}, {0xef, 0xbf, 0xbd},
		{0xe1, 0x80, 0xff}, {0xc0, 0xaf}, {0xe2, 0x82}, []byte("é"), []byte("<>&"), []byte("\u2028\u2029"),
	}
	allBytes := make([]byte, 256)
	for i := range allBytes { allBytes[i] = byte(i) }
	names = append(names, allBytes)
	canonical := []byte("e1b8e2d3-5c1f-4a77-9b0a-6a0a1f0e5d11")
	names = append(names, canonical, []byte("e1b8e2d35c1f4a779b0a6a0a1f0e5d11"), append([]byte("URN:UUID:"), canonical...))
	wrapped := append([]byte{0xff}, canonical...)
	names = append(names, append(wrapped, 0xfe))
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
		googleID, googleErr := uuid.Parse(string(raw))
		cassandraID, cassandraErr := gocql.ParseUUID(string(raw))
		partitionName := append([]byte("/__cadence_sys/"), raw...)
		partitionName = append(partitionName, []byte("/7")...)
		queryJSON, err := common.NewJSONTaskTokenSerializer().SerializeQueryTaskToken(&common.QueryTaskToken{
			DomainID: string(raw), WorkflowID: string(raw), RunID: string(raw), TaskList: string(raw), TaskID: string(raw),
		})
		if err != nil { panic(err) }
		rows = append(rows, map[string]interface{}{
			"hex": hex.EncodeToString(raw), "length": len(raw),
			"validUtf8": utf8.Valid(raw), "hash32": farm.Hash32(input),
			"jsonText": string(encoded),
			"protobuf": protobufCases(raw),
            "grpcQueryError": grpcErrorCase(&cadencetypes.QueryFailedError{Message: string(raw)}),
            "grpcVersionError": grpcErrorCase(&cadencetypes.ClientVersionNotSupportedError{
                FeatureVersion: string(raw), ClientImpl: string(raw), SupportedVersions: ">=1.5.0",
            }),
			"taskTokenJSON": string(taskJSON), "queryTokenJSON": string(queryJSON),
			"googleUUID": map[string]interface{}{"error": errorText(googleErr), "uuidHex": hex.EncodeToString(googleID[:])},
			"cassandraUUID": map[string]interface{}{"error": errorText(cassandraErr), "uuidHex": hex.EncodeToString(cassandraID[:])},
			"taskList": identifierCase(raw), "taskListPartition": identifierCase(partitionName),
		})
		if len(rows) == 1 { rows[0]["nativeWireEdgeCases"] = nativeWireEdgeCases() }
	}
	if err := json.NewEncoder(os.Stdout).Encode(rows); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
