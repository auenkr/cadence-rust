package cache

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// This file is overlaid into common/cache for the original-Go oracle. It stays
// outside production Go so the test can exercise the private cache setup.
func (s *domainCacheSuite) TestDomainCacheUUIDSpellingOracle() {
	const canonical = "abcdefab-cdef-abcd-efab-cdefabcdefab"
	const name = "uuid-spelling-oracle-domain"
	alternates := []string{
		"ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB",
		"abcdefabcdefabcdefabcdefabcdefab",
		"-abcdefab-cdef-abcd-efab-cdefabcdefab-",
	}

	for _, alternate := range alternates {
		parsed, parseErr := gocql.ParseUUID(alternate)
		s.Require().NoError(parseErr)
		s.T().Logf("ORACLE alternate=%q gocql_parsed=%q canonical=%q", alternate, parsed.String(), canonical)

		// A successful point lookup establishes existence for the raw request
		// spelling, while the full metadata scan returns the canonical ID.
		s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{Name: "", ID: alternate}).Return(&persistence.GetDomainResponse{}, nil).Once()
		s.metadataMgr.On("GetMetadata", mock.Anything).Return(&persistence.GetMetadataResponse{NotificationVersion: 1}, nil).Once()
		s.metadataMgr.On("ListDomains", mock.Anything, mock.Anything).Return(&persistence.ListDomainsResponse{Domains: []*persistence.GetDomainResponse{{
			Info: &persistence.DomainInfo{ID: canonical, Name: name, Data: map[string]string{}},
			Config: &persistence.DomainConfig{Retention: 1, BadBinaries: types.BadBinaries{Binaries: map[string]*types.BadBinaryInfo{}}},
			ReplicationConfig: &persistence.DomainReplicationConfig{ActiveClusterName: cluster.TestCurrentClusterName, Clusters: []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}}},
			NotificationVersion: 0,
		}}}, nil).Once()

		got, err := s.domainCache.GetDomainName(alternate)
		s.T().Logf("ORACLE cold/refresh alternate=%q name=%q error_type=%T error=%v", alternate, got, err, err)
		s.Empty(got)
		s.ErrorContains(err, "exists but cannot be loaded")

		// The refresh populated only the canonical key. Repeating the raw key
		// hits the same miss path while the refresh throttle avoids another scan.
		s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{Name: "", ID: alternate}).Return(&persistence.GetDomainResponse{}, nil).Once()
		got, err = s.domainCache.GetDomainName(alternate)
		s.T().Logf("ORACLE warm alternate=%q name=%q error_type=%T error=%v", alternate, got, err, err)
		s.Empty(got)
		s.ErrorContains(err, "exists but cannot be loaded")

		got, err = s.domainCache.GetDomainName(canonical)
		s.T().Logf("ORACLE canonical alternate=%q name=%q error=%v", alternate, got, err)
		s.NoError(err)
		s.Equal(name, got)
	}
}
