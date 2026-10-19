package cache

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"
)

func (s *domainCacheSuite) TestDomainCacheOracleTrace() {
	id1 := uuid.Parse("00000000-0000-0000-0000-000000000001").String()
	id2 := uuid.Parse("00000000-0000-0000-0000-000000000002").String()
	pageToken := []byte("next-page")
	makeDomain := func(id, name string, notification, config, failover, failoverNotification int64) *persistence.GetDomainResponse {
		return &persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: id, Name: name, Data: map[string]string{}},
			Config: &persistence.DomainConfig{Retention: int32(config)},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: cluster.TestCurrentClusterName}},
			},
			ConfigVersion:               config,
			FailoverVersion:             failover,
			FailoverNotificationVersion: failoverNotification,
			PreviousFailoverVersion:     3,
			NotificationVersion:         notification,
		}
	}
	r1 := makeDomain(id1, "alpha", 1, 5, 7, 2)
	r2 := makeDomain(id2, "beta", 3, 9, 11, 3)
	mockedTime := s.domainCache.timeSource.(interface{ Advance(time.Duration) })
	queueScan := func(metadata int64, first, second *persistence.GetDomainResponse, secondErr error) {
		s.metadataMgr.On("GetMetadata", mock.Anything).Return(&persistence.GetMetadataResponse{NotificationVersion: metadata}, nil).Once()
		s.metadataMgr.On("ListDomains", mock.Anything, &persistence.ListDomainsRequest{PageSize: domainCacheRefreshPageSize}).Return(&persistence.ListDomainsResponse{Domains: []*persistence.GetDomainResponse{first}, NextPageToken: pageToken}, nil).Once()
		if secondErr != nil {
			s.metadataMgr.On("ListDomains", mock.Anything, &persistence.ListDomainsRequest{PageSize: domainCacheRefreshPageSize, NextPageToken: pageToken}).Return(nil, secondErr).Once()
		} else {
			s.metadataMgr.On("ListDomains", mock.Anything, &persistence.ListDomainsRequest{PageSize: domainCacheRefreshPageSize, NextPageToken: pageToken}).Return(&persistence.ListDomainsResponse{Domains: []*persistence.GetDomainResponse{second}}, nil).Once()
		}
	}

	queueScan(3, r1, r2, nil)
	if err := s.domainCache.refreshDomains(); err != nil {
		s.T().Fatalf("initial refresh failed: %v", err)
	}
	var events []string
	var catchupMax int64
	s.domainCache.RegisterDomainChangeCallback("matching", func(c DomainCache, _ PrepareCallbackFn, _ CallbackFn) {
		for _, domain := range c.GetAllDomain() {
			if domain.GetFailoverNotificationVersion() > catchupMax {
				catchupMax = domain.GetFailoverNotificationVersion()
			}
		}
	}, func() { events = append(events, "prepare") }, func(domains []*DomainCacheEntry) {
		for _, domain := range domains {
			events = append(events, "changed:"+domain.GetInfo().ID+":"+fmtInt(domain.GetNotificationVersion()))
		}
	})
	updated1 := makeDomain(id1, "alpha", 2, 6, 8, 4)
	mockedTime.Advance(time.Second)
	queueScan(4, updated1, r2, nil)
	if err := s.domainCache.refreshDomains(); err != nil {
		s.T().Fatalf("update refresh failed: %v", err)
	}

	bad1 := makeDomain(id1, "alpha", 4, 8, 10, 5)
	mockedTime.Advance(time.Second)
	queueScan(5, bad1, r2, errors.New("injected page failure"))
	pageError := s.domainCache.refreshDomains()
	nameMiss := errors.New("missing name")
	idMiss := errors.New("missing id")
	missingID := uuid.Parse("00000000-0000-0000-0000-000000000003").String()
	s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{Name: "late"}).Return(nil, nameMiss).Once()
	s.metadataMgr.On("GetDomain", mock.Anything, &persistence.GetDomainRequest{ID: missingID}).Return(nil, idMiss).Once()
	_, nameMissErr := s.domainCache.GetDomain("late")
	_, idMissErr := s.domainCache.GetDomainByID(missingID)
	retained := s.domainCache.GetAllDomain()
	ids := make([]string, 0, len(retained))
	for id := range retained {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type entry struct {
		ID, Name                                             string
		Notification, Config, Failover, FailoverNotification int64
		Retention                                            int32
	}
	trace := struct {
		MetadataFirstPageSize int
		InitialIDs            []string
		CatchupMax            int64
		Events                []string
		FinalEntries          []entry
		PageError             string
		RetainedNotification  int64
		NameMiss, IDMiss      bool
	}{MetadataFirstPageSize: domainCacheRefreshPageSize, CatchupMax: catchupMax, Events: events, PageError: pageError.Error(), NameMiss: nameMissErr != nil, IDMiss: idMissErr != nil}
	trace.InitialIDs = []string{id1}
	for _, id := range ids {
		domain := retained[id]
		trace.FinalEntries = append(trace.FinalEntries, entry{ID: id, Name: domain.GetInfo().Name, Notification: domain.GetNotificationVersion(), Config: domain.GetConfigVersion(), Failover: domain.GetFailoverVersion(), FailoverNotification: domain.GetFailoverNotificationVersion(), Retention: domain.GetConfig().Retention})
	}
	trace.RetainedNotification = retained[id1].GetNotificationVersion()
	data, err := json.Marshal(trace)
	s.NoError(err)
	s.T().Logf("GO_DOMAIN_CACHE_TRACE=%s", data)
}

func fmtInt(value int64) string { return strconv.FormatInt(value, 10) }
