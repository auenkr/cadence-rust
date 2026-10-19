package membership

import (
	"encoding/json"
	"github.com/dgryski/go-farm"
	"github.com/uber/ringpop-go/hashring"
	rpm "github.com/uber/ringpop-go/membership"
	"os"
	"testing"
)

type rustRingHost struct {
	Address string `json:"address"`
	ID      string `json:"identity"`
}

func (h rustRingHost) GetAddress() string { return h.Address }
func (h rustRingHost) Identity() string {
	if h.ID == "" {
		return h.Address
	}
	return h.ID
}
func (h rustRingHost) Label(key string) (string, bool) { return "", false }
func TestRustMatchingMembership(t *testing.T) {
	type query struct {
		Key    string   `json:"key"`
		Count  int      `json:"count"`
		Owners []string `json:"owners"`
	}
	type fixture struct {
		Hosts   []rustRingHost `json:"hosts"`
		Queries []query        `json:"queries"`
	}
	var cases []fixture
	data, err := os.ReadFile(os.Getenv("CADENCE_MATCHING_RING_INPUT"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		ring := hashring.New(farm.Fingerprint32, 100)
		var members []rpm.Member
		for _, host := range cases[i].Hosts {
			members = append(members, host)
		}
		ring.AddMembers(members...)
		for j := range cases[i].Queries {
			q := &cases[i].Queries[j]
			q.Owners = ring.LookupN(q.Key, q.Count)
			if q.Owners == nil {
				q.Owners = []string{}
			}
		}
	}
	data, err = json.MarshalIndent(cases, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CADENCE_MATCHING_RING_OUTPUT"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestGoFarmhashFingerprint32ShortByteVectors(t *testing.T) {
	for _, tc := range []struct {
		input []byte
		want  uint32
	}{
		{[]byte{}, 3696677242},
		{[]byte{0x80}, 801663339},
		{[]byte{0xff}, 495582926},
		{[]byte{0xef, 0xbf, 0xbd}, 4143110158},
		{[]byte{0x80, 0xff, 0xef, 0xbf}, 740356336},
		{[]byte{0xef, 0xbf, 0xbd, 0x01}, 110792607},
		{[]byte{0x80, 0x00, 0xff, 0x7f}, 847840402},
	} {
		if got := farm.Fingerprint32(tc.input); got != tc.want {
			t.Errorf("Fingerprint32(%x) = %d, want %d", tc.input, got, tc.want)
		}
	}
}
