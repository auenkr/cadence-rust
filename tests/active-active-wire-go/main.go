// Produce the zero-version active-active fixture with the actual Go mapper/codec.
package main

import (
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/types/mapper/thrift"
	"os"
)

func main() {
	value := &types.ActiveClusters{AttributeScopes: map[string]types.ClusterAttributeScope{
		"city": {ClusterAttributes: map[string]types.ActiveClusterInfo{"x": {ActiveClusterName: "a", FailoverVersion: 0}}},
	}}
	data, err := codec.NewThriftRWEncoder().Encode(thrift.FromActiveClusters(value))
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile(os.Args[1], data, 0644); err != nil {
		panic(err)
	}
}
