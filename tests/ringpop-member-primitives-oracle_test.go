package swim

import (
	"bytes"
	"encoding/json"
	"github.com/uber/ringpop-go/logging"
	"os"
	"testing"
)

type oracleMember struct {
	Address     string            `json:"address"`
	Status      string            `json:"status"`
	Incarnation int64             `json:"incarnation"`
	Labels      map[string]string `json:"labels"`
}

type primitiveCase struct {
	Name           string        `json:"name"`
	Old            *oracleMember `json:"old,omitempty"`
	Gossip         oracleMember  `json:"gossip"`
	Process        bool          `json:"shouldProcessGossip"`
	LabelChecksum  int32         `json:"labelChecksum"`
	ChecksumString string        `json:"checksumString"`
}

type memberlistCase struct {
	Name              string         `json:"name"`
	Members           []oracleMember `json:"members"`
	GenChecksumString string         `json:"genChecksumString"`
	ComputeChecksum   uint32         `json:"computeChecksum"`
}

func TestGenerateRingpopMemberPrimitiveFixtures(t *testing.T) {
	out := os.Getenv("CADENCE_RINGPOP_MEMBER_OUTPUT")
	if out == "" {
		t.Fatal("CADENCE_RINGPOP_MEMBER_OUTPUT is required")
	}
	statuses := []string{Alive, Suspect, Faulty, Leave, Tombstone, "unknown"}
	cases := make([]primitiveCase, 0)
	appendCase := func(name string, old, gossip *Member) {
		var oldJSON *oracleMember
		if old != nil {
			oldJSON = toOracleMember(old)
		}
		var b bytes.Buffer
		gossip.checksumString(&b)
		cases = append(cases, primitiveCase{Name: name, Old: oldJSON, Gossip: *toOracleMember(gossip), Process: shouldProcessGossip(old, gossip), LabelChecksum: gossip.Labels.checksum(), ChecksumString: b.String()})
	}
	for _, status := range statuses {
		appendCase("status_"+status, &Member{Address: "n", Status: Alive, Incarnation: 10}, &Member{Address: "n", Status: status, Incarnation: 10})
	}
	appendCase("tombstone_old_nil", nil, &Member{Address: "n", Status: Tombstone, Incarnation: 10})
	appendCase("alive_old_nil", nil, &Member{Address: "n", Status: Alive, Incarnation: 10})
	appendCase("older_incarnation", &Member{Address: "n", Status: Alive, Incarnation: 11}, &Member{Address: "n", Status: Leave, Incarnation: 10})
	appendCase("newer_incarnation", &Member{Address: "n", Status: Leave, Incarnation: 10}, &Member{Address: "n", Status: Alive, Incarnation: 11})
	appendCase("equal_incarnation_equal_state", &Member{Address: "n", Status: Alive, Incarnation: 10}, &Member{Address: "n", Status: Alive, Incarnation: 10})
	var lowerPrecedenceOld, higherPrecedenceGossip LabelMap
	for i := 0; i < 10000; i++ {
		candidate := LabelMap{"precedence": string(rune('a'+i%26)) + string(rune('A'+i/26))}
		if lowerPrecedenceOld == nil {
			lowerPrecedenceOld = candidate
			continue
		}
		if candidate.checksum() > lowerPrecedenceOld.checksum() {
			higherPrecedenceGossip = candidate
			break
		}
	}
	if higherPrecedenceGossip == nil {
		t.Fatal("could not find higher checksum for precedence case")
	}
	appendCase("precedence_beats_higher_label_checksum", &Member{Address: "n", Status: Alive, Incarnation: 10, Labels: lowerPrecedenceOld}, &Member{Address: "n", Status: Suspect, Incarnation: 10, Labels: higherPrecedenceGossip})
	var lowerOld, higherGossip LabelMap
	for i := 0; i < 10000; i++ {
		candidate := LabelMap{"candidate": string(rune('a'+i%26)) + string(rune('A'+i/26))}
		if lowerOld == nil {
			lowerOld = candidate
			continue
		}
		if candidate.checksum() > lowerOld.checksum() {
			higherGossip = candidate
			break
		}
	}
	if higherGossip == nil {
		t.Fatal("could not find increasing label checksum pair")
	}
	appendCase("higher_label_checksum", &Member{Address: "n", Status: Alive, Incarnation: 10, Labels: lowerOld}, &Member{Address: "n", Status: Alive, Incarnation: 10, Labels: higherGossip})
	appendCase("unicode_empty_labels", &Member{Address: "n", Status: Alive, Incarnation: 10}, &Member{Address: "n-雪", Status: Alive, Incarnation: 10, Labels: LabelMap{"": "", "ключ": "значение🌱"}})
	appendCase("nil_labels", &Member{Address: "n", Status: Alive, Incarnation: 10}, &Member{Address: "n", Status: Alive, Incarnation: 10, Labels: nil})
	appendCase("empty_labels", &Member{Address: "n", Status: Alive, Incarnation: 10}, &Member{Address: "n", Status: Alive, Incarnation: 10, Labels: LabelMap{}})

	// Search simple labels using the original checksum implementation so both
	// signed checksum halves are represented without encoding an algorithm here.
	for _, wantPositive := range []bool{true, false} {
		found := false
		for i := 0; i < 10000 && !found; i++ {
			labels := LabelMap{"signed": string(rune('a'+i%26)) + string(rune('A'+i/26))}
			checksum := labels.checksum()
			if (wantPositive && checksum > 0) || (!wantPositive && checksum < 0) {
				name := "signed_positive_labels"
				if !wantPositive {
					name = "signed_negative_labels"
				}
				appendCase(name, &Member{Address: "n", Status: Alive, Incarnation: 10}, &Member{Address: "n", Status: Alive, Incarnation: 10, Labels: labels})
				found = true
			}
		}
		if !found {
			t.Fatalf("could not find signed label checksum, positive=%v", wantPositive)
		}
	}

	lists := make([]memberlistCase, 0)
	for _, tc := range []struct {
		name    string
		members []oracleMember
	}{
		{"sorted_with_tombstone", []oracleMember{{Address: "z", Status: Alive, Incarnation: 2}, {Address: "gone", Status: Tombstone, Incarnation: 8}, {Address: "a", Status: Suspect, Incarnation: 1}}},
		{"empty", []oracleMember{}},
	} {
		ml := &memberlist{node: &Node{}, logger: logging.Logger("fixture")}
		for _, m := range tc.members {
			member := &Member{Address: m.Address, Status: m.Status, Incarnation: m.Incarnation, Labels: LabelMap(m.Labels)}
			ml.members.list = append(ml.members.list, member)
		}
		gen := ml.genChecksumString()
		ml.ComputeChecksum()
		lists = append(lists, memberlistCase{Name: tc.name, Members: tc.members, GenChecksumString: gen, ComputeChecksum: ml.Checksum()})
	}
	result := struct {
		Source         string           `json:"source"`
		PrimitiveCases []primitiveCase  `json:"primitiveCases"`
		Memberlists    []memberlistCase `json:"memberlists"`
	}{
		Source: "github.com/uber/ringpop-go v0.10.0 swim/member.go and swim/memberlist.go", PrimitiveCases: cases, Memberlists: lists,
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(out, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func toOracleMember(m *Member) *oracleMember {
	if m == nil {
		return nil
	}
	labels := map[string]string{}
	for k, v := range m.Labels {
		labels[k] = v
	}
	return &oracleMember{Address: m.Address, Status: m.Status, Incarnation: m.Incarnation, Labels: labels}
}
