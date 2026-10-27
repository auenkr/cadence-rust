package cache

import (
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
)

// This test is overlaid into common/cache only by the optional Actions oracle.
// It intentionally tests cache lookup behavior, not UUID parser acceptance.
func (s *domainCacheSuite) TestDomainCacheUUIDSpellingOracle() {
	const canonical = "abcdefab-cdef-abcd-efab-cdefabcdefab"
	const name = "uuid-spelling-oracle-domain"

	// Model a domain created after the cache's initial scan. A cold lookup of
	// its canonical ID must check persistence, refresh the canonical record into
	// the cache, and then serve the next lookup without another store call.
	s.Run("cold canonical UUID after initial scan", func() {
		s.metadataMgr.On("GetMetadata", mock.Anything).
			Return(&persistence.GetMetadataResponse{NotificationVersion: 1}, nil).Once()
		s.metadataMgr.On("ListDomains", mock.Anything, mock.Anything).
			Return(&persistence.ListDomainsResponse{}, nil).Once()
		s.NoError(s.domainCache.refreshDomains())

		s.domainCache.timeSource.(interface{ Advance(time.Duration) }).Advance(domainCacheMinRefreshInterval)
		s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{Name: "", ID: canonical}).
			Return(&persistence.GetDomainResponse{Info: &persistence.DomainInfo{ID: canonical, Name: name}}, nil).Once()
		s.metadataMgr.On("GetMetadata", mock.Anything).
			Return(&persistence.GetMetadataResponse{NotificationVersion: 2}, nil).Once()
		s.metadataMgr.On("ListDomains", mock.Anything, mock.Anything).
			Return(&persistence.ListDomainsResponse{Domains: []*persistence.GetDomainResponse{{
				Info: &persistence.DomainInfo{ID: canonical, Name: name, Data: map[string]string{}},
				Config: &persistence.DomainConfig{Retention: 1, BadBinaries: types.BadBinaries{Binaries: map[string]*types.BadBinaryInfo{}}},
				ReplicationConfig: &persistence.DomainReplicationConfig{
					ActiveClusterName: cluster.TestCurrentClusterName,
					Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
				},
				NotificationVersion: 2,
			}}}, nil).Once()

		got, err := s.domainCache.GetDomainName(canonical)
		s.T().Logf("cold canonical=%q name=%q error_type=%T error=%v", canonical, got, err, err)
		s.NoError(err)
		s.Equal(name, got)

		got, err = s.domainCache.GetDomainName(canonical)
		s.T().Logf("warm canonical=%q name=%q error_type=%T error=%v", canonical, got, err, err)
		s.NoError(err)
		s.Equal(name, got)
	})

	alternates := []string{
		"ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB",
		"abcdefabcdefabcdefabcdefabcdefab",
		"-abcdefab-cdef-abcd-efab-cdefabcdefab-",
	}

	for _, alternate := range alternates {
		s.Run(alternate, func() {
			// Force one independent refresh per spelling. The refresh interval is
			// otherwise shared by all cases in this suite test.
			s.domainCache.lastRefreshTime = time.Time{}
			s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{Name: "", ID: alternate}).
				Return(&persistence.GetDomainResponse{}, nil).Once()
			s.metadataMgr.On("GetMetadata", mock.Anything).
				Return(&persistence.GetMetadataResponse{NotificationVersion: 2}, nil).Once()
			s.metadataMgr.On("ListDomains", mock.Anything, mock.Anything).
				Return(&persistence.ListDomainsResponse{Domains: []*persistence.GetDomainResponse{{
					Info: &persistence.DomainInfo{ID: canonical, Name: name, Data: map[string]string{}},
					Config: &persistence.DomainConfig{Retention: 1, BadBinaries: types.BadBinaries{Binaries: map[string]*types.BadBinaryInfo{}}},
					ReplicationConfig: &persistence.DomainReplicationConfig{
						ActiveClusterName: cluster.TestCurrentClusterName,
						Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
					},
					NotificationVersion: 1,
				}}}, nil).Once()

			got, err := s.domainCache.GetDomainName(alternate)
			s.T().Logf("cold/refresh alternate=%q name=%q error_type=%T error=%v", alternate, got, err, err)
			s.Empty(got)
			s.ErrorContains(err, "exists but cannot be loaded")

			// A second raw-key lookup still misses after the canonical key was
			// inserted by refresh; its refresh is throttled by lastRefreshTime.
			s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{Name: "", ID: alternate}).
				Return(&persistence.GetDomainResponse{}, nil).Once()
			got, err = s.domainCache.GetDomainName(alternate)
			s.T().Logf("warm alternate=%q name=%q error_type=%T error=%v", alternate, got, err, err)
			s.Empty(got)
			s.ErrorContains(err, "exists but cannot be loaded")

			got, err = s.domainCache.GetDomainName(canonical)
			s.T().Logf("canonical alternate=%q name=%q error=%v", alternate, got, err)
			s.NoError(err)
			s.Equal(name, got)
		})
	}
}
