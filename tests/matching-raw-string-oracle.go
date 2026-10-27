// Original Go byte-string/hash/JSON behavior for Matching boundary tests.
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"unicode/utf8"

	farm "github.com/dgryski/go-farm"
)

func main() {
	names := [][]byte{
		{}, {'x'}, {0}, {0xff}, {0xfe}, {0xef, 0xbf, 0xbd},
		{0xe1, 0x80, 0xff}, {0xc0, 0xaf}, []byte("é"), []byte("<>&"),
	}
	rows := make([]map[string]interface{}, 0, len(names))
	for _, raw := range names {
		encoded, err := json.Marshal(string(raw))
		if err != nil {
			panic(err)
		}
		input := append([]byte("e1b8e2d3-5c1f-4a77-9b0a-6a0a1f0e5d11_"), raw...)
		rows = append(rows, map[string]interface{}{
			"hex": hex.EncodeToString(raw), "length": len(raw),
			"validUtf8": utf8.Valid(raw), "hash32": farm.Hash32(input),
			"jsonText": string(encoded),
		})
	}
	if err := json.NewEncoder(os.Stdout).Encode(rows); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
