// Runs the real UpdateDomain controller against mocked persistence/transport.
package domain

import (
	"context"
	"encoding/json"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"go.uber.org/mock/gomock"
	"os"
	"testing"
	"time"
)

type rustUpdateConfig struct {
	Retention *int32             `json:"workflowExecutionRetentionPeriodInDays"`
	Emit      *bool              `json:"emitMetric"`
	Bad       *types.BadBinaries `json:"badBinaries"`
}
type rustUpdateReplication struct {
	Target   *string                                  `json:"activeClusterName"`
	Clusters []*types.ClusterReplicationConfiguration `json:"clusters"`
	Active   *rustActive                              `json:"activeClusters"`
}
type rustUpdateInfo struct {
	Description *string           `json:"description"`
	Owner       *string           `json:"ownerEmail"`
	Data        map[string]string `json:"data"`
}
type rustUpdateRequest struct {
	Info        *rustUpdateInfo        `json:"updatedInfo"`
	Config      *rustUpdateConfig      `json:"configuration"`
	Replication *rustUpdateReplication `json:"replicationConfiguration"`
	Delete      *string                `json:"deleteBadBinary"`
	Timeout     *int32                 `json:"failoverTimeoutInSeconds"`
}
type rustDisabledArchival struct{}

func (rustDisabledArchival) ClusterConfiguredForArchival() bool { return false }
func (rustDisabledArchival) GetClusterStatus() archiver.ArchivalStatus {
	return archiver.ArchivalDisabled
}
func (rustDisabledArchival) ReadEnabled() bool { return false }
func (rustDisabledArchival) GetDomainDefaultStatus() types.ArchivalStatus {
	return types.ArchivalStatusDisabled
}
func (rustDisabledArchival) GetDomainDefaultURI() string { return "" }
func rustUpdateNormalize(info *p.DomainInfo, cfg *p.DomainConfig, rep *p.DomainReplicationConfig, version int64) interface{} {
	return map[string]interface{}{"description": info.Description, "owner": info.OwnerEmail, "data": info.Data, "retention": cfg.Retention, "emit": cfg.EmitMetric, "bad": cfg.BadBinaries.Binaries, "target": rep.ActiveClusterName, "active": rustNormalizeActive(rep.ActiveClusters), "version": version}
}
func TestRustDomainUpdateFixtures(t *testing.T) {
	data, err := os.ReadFile(os.Getenv("CADENCE_DOMAIN_UPDATE_FIXTURES"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name           string            `json:"name"`
		Global         bool              `json:"global"`
		CurrentCluster string            `json:"currentCluster"`
		Existing       *rustActive       `json:"existing"`
		Target         string            `json:"initialTarget"`
		Request        rustUpdateRequest `json:"request"`
		InitialHistory *string           `json:"initialHistory"`
	}
	if err = json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	var original []map[string]json.RawMessage
	if err = json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	for index, c := range cases {
		ctrl := gomock.NewController(t)
		currentCluster := c.CurrentCluster
		if currentCluster == "" {
			currentCluster = "a"
		}
		meta := cluster.NewMetadata(config.ClusterGroupMetadata{FailoverVersionIncrement: 10, PrimaryClusterName: "a", CurrentClusterName: currentCluster, ClusterGroup: map[string]config.ClusterInformation{"a": {Enabled: true, InitialFailoverVersion: 0}, "b": {Enabled: true, InitialFailoverVersion: 2}}}, func(string) bool { return false }, metrics.NewNoopMetricsClient(), log.NewNoop())
		manager := p.NewMockDomainManager(ctrl)
		replicator := NewMockReplicator(ctrl)
		target := c.Target
		if target == "" {
			target = "a"
		}
		version := int64(-24)
		if c.Global {
			version = 0
		}
		replicas := []*p.ClusterReplicationConfig{{ClusterName: "a"}}
		if c.Global {
			replicas = append(replicas, &p.ClusterReplicationConfig{ClusterName: "b"})
		}
		initial := &p.GetDomainResponse{Info: &p.DomainInfo{ID: "00000000-0000-0000-0000-000000000001", Name: "domain", Description: "old", OwnerEmail: "owner", Data: map[string]string{"keep": "old"}}, Config: &p.DomainConfig{Retention: 7, EmitMetric: true, BadBinaries: types.BadBinaries{Binaries: map[string]*types.BadBinaryInfo{"old": {Reason: "old", CreatedTimeNano: func() *int64 { v := int64(1); return &v }()}}}}, ReplicationConfig: &p.DomainReplicationConfig{ActiveClusterName: target, Clusters: replicas, ActiveClusters: rustToActive(c.Existing)}, IsGlobalDomain: c.Global, FailoverVersion: version, ConfigVersion: 0, PreviousFailoverVersion: -1}
		if c.InitialHistory != nil {
			initial.Info.Data["FailoverHistory"] = *c.InitialHistory
		}
		stored := initial.DeepCopy()
		writes := 0
		manager.EXPECT().GetMetadata(gomock.Any()).Return(&p.GetMetadataResponse{NotificationVersion: 1}, nil)
		manager.EXPECT().GetDomain(gomock.Any(), gomock.Any()).Return(initial, nil)
		manager.EXPECT().UpdateDomain(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, r *p.UpdateDomainRequest) error {
			writes++
			stored.Info = r.Info
			stored.Config = r.Config
			stored.ReplicationConfig = r.ReplicationConfig
			stored.FailoverVersion = r.FailoverVersion
			stored.ConfigVersion = r.ConfigVersion
			stored.PreviousFailoverVersion = r.PreviousFailoverVersion
			stored.FailoverEndTime = r.FailoverEndTime
			return nil
		}).AnyTimes()
		replicator.EXPECT().HandleTransmissionTask(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		archival := &archiver.MockArchivalMetadata{}
		archival.On("GetHistoryConfig").Return(rustDisabledArchival{})
		archival.On("GetVisibilityConfig").Return(rustDisabledArchival{})
		handler := handlerImpl{domainManager: manager, domainReplicator: replicator, clusterMetadata: meta, domainAttrValidator: newAttrValidator(meta, 1), archivalMetadata: archival, timeSource: clock.NewMockedTimeSourceAt(time.Unix(1700000000, 0)), logger: log.NewNoop(), config: Config{MaxBadBinaryCount: func(string) int { return 10 }, FailoverCoolDown: func(string) time.Duration { return 0 }, FailoverHistoryMaxSize: func(string) int { return 5 }}}
		request := &types.UpdateDomainRequest{Name: "domain", DeleteBadBinary: c.Request.Delete, FailoverTimeoutInSeconds: c.Request.Timeout}
		if r := c.Request.Info; r != nil {
			request.Description = r.Description
			request.OwnerEmail = r.Owner
			request.Data = r.Data
		}
		if r := c.Request.Config; r != nil {
			request.WorkflowExecutionRetentionPeriodInDays = r.Retention
			request.EmitMetric = r.Emit
			request.BadBinaries = r.Bad
		}
		if r := c.Request.Replication; r != nil {
			request.ActiveClusterName = r.Target
			request.Clusters = r.Clusters
			request.ActiveClusters = rustToActive(r.Active)
		}
		response, err := handler.UpdateDomain(context.Background(), request)
		message := ""
		var normalized interface{}
		if err != nil {
			message = err.Error()
		} else {
			normalized = rustUpdateNormalize(&p.DomainInfo{Description: response.DomainInfo.Description, OwnerEmail: response.DomainInfo.OwnerEmail, Data: response.DomainInfo.Data}, &p.DomainConfig{Retention: response.Configuration.WorkflowExecutionRetentionPeriodInDays, EmitMetric: response.Configuration.EmitMetric, BadBinaries: *response.Configuration.BadBinaries}, &p.DomainReplicationConfig{ActiveClusterName: response.ReplicationConfiguration.ActiveClusterName, ActiveClusters: response.ReplicationConfiguration.ActiveClusters}, response.FailoverVersion)
		}
		expected := map[string]interface{}{"error": message, "writes": writes, "response": normalized, "stored": rustUpdateNormalize(stored.Info, stored.Config, stored.ReplicationConfig, stored.FailoverVersion), "configVersion": stored.ConfigVersion, "previousVersion": stored.PreviousFailoverVersion, "endTime": stored.FailoverEndTime}
		encoded, err := json.Marshal(expected)
		if err != nil {
			t.Fatal(err)
		}
		original[index]["expected"] = encoded
		ctrl.Finish()
	}
	encoded, err := json.MarshalIndent(original, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CADENCE_DOMAIN_UPDATE_OUTPUT"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
}
