// Command shard-distributor-real runs the REAL pinned Go shard-distributor
// (sharddistributorfx module: etcd executor store, etcd leader election,
// namespace manager and rebalancing leader loop) for the mixed-cluster gate.
// The pinned shard-manager module ships no server main, so this launcher only
// supplies what the service expects from its host: logger, metrics, dynamic
// config, a gRPC dispatcher and a static namespace config. A small HTTP control
// endpoint mirrors the fixture's "stats"/"assign" ops so the compat probe can
// run unchanged; it never writes assignments itself.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"
	"go.uber.org/yarpc"
	grpctransport "go.uber.org/yarpc/transport/grpc"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	sharddistributorv1 "github.com/cadence-workflow/shard-manager/.gen/proto/sharddistributor/v1"
	"github.com/cadence-workflow/shard-manager/common/clock"
	"github.com/cadence-workflow/shard-manager/common/dynamicconfig"
	"github.com/cadence-workflow/shard-manager/common/log"
	"github.com/cadence-workflow/shard-manager/common/metrics"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/config"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/sharddistributorfx"
	"github.com/cadence-workflow/shard-manager/service/sharddistributor/store"
	etcdstore "github.com/cadence-workflow/shard-manager/service/sharddistributor/store/etcd"
)

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func shardDistributionConfig(namespace, etcdEndpoint, prefix, ttl string) config.ShardDistribution {
	text := fmt.Sprintf(`
election:
  leaderPeriod: 60s
  maxRandomDelay: 500ms
  failedElectionCooldown: 1s
namespaces:
  - name: %[1]s
    type: ephemeral
leaderStore:
  storageParams:
    endpoints: [%[2]s]
    dialTimeout: 3s
    prefix: "%[3]s-leader"
    electionTTL: 5s
store:
  storageParams:
    endpoints: [%[2]s]
    dialTimeout: 3s
    prefix: "%[3]s-store"
process:
  period: 1s
  rebalanceCooldown: 250ms
  timeout: 2s
  heartbeatTTL: %[4]s
`, namespace, etcdEndpoint, prefix, ttl)
	var cfg config.ShardDistribution
	must(yaml.Unmarshal([]byte(text), &cfg))
	return cfg
}

func main() {
	namespace := envOr("SD_REAL_NAMESPACE", "matching")
	cfg := shardDistributionConfig(namespace, envOr("SD_REAL_ETCD", "127.0.0.1:2379"),
		envOr("SD_REAL_PREFIX", "cadence-rust-gate"), envOr("SD_REAL_HEARTBEAT_TTL", "5s"))
	listener, err := net.Listen("tcp", envOr("SD_REAL_ADDR", "127.0.0.1:0"))
	must(err)
	controlListener, err := net.Listen("tcp", envOr("SD_REAL_CONTROL_ADDR", "127.0.0.1:0"))
	must(err)
	zapLogger, err := zap.NewDevelopment()
	must(err)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "shard-manager",
		Inbounds: yarpc.Inbounds{grpctransport.NewTransport().NewInbound(listener)},
	})

	var kv store.Store
	app := fx.New(
		fx.NopLogger,
		fx.Provide(
			func() log.Logger { return log.NewLogger(zapLogger) },
			func() metrics.Client { return metrics.NewNoopMetricsClient() },
			func() *dynamicconfig.Collection { return dynamicconfig.NewNopCollection() },
			func() clock.TimeSource { return clock.NewRealTimeSource() },
			func() config.ShardDistribution { return cfg },
			func() *yarpc.Dispatcher { return dispatcher },
			fx.Annotated{Target: func() string { return "shard-distributor-real" }, Name: "hostname"},
		),
		etcdstore.Module,
		sharddistributorfx.Module,
		fx.Invoke(func(
			lc fx.Lifecycle,
			d *yarpc.Dispatcher,
			api sharddistributorv1.ShardDistributorAPIYARPCServer,
			executor sharddistributorv1.ShardDistributorExecutorAPIYARPCServer,
			s store.Store,
		) {
			kv = s
			d.Register(sharddistributorv1.BuildShardDistributorAPIYARPCProcedures(api))
			d.Register(sharddistributorv1.BuildShardDistributorExecutorAPIYARPCProcedures(executor))
			lc.Append(fx.Hook{OnStart: func(context.Context) error { return d.Start() }, OnStop: func(context.Context) error { return d.Stop() }})
		}),
	)
	startCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	must(app.Start(startCtx))
	cancel()

	// Self client: "assign" asks the real handler for the shard owner, which
	// makes the ephemeral namespace assign it through the leader loop's store.
	clientTransport := grpctransport.NewTransport()
	clientDispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "cadence-matching",
		Outbounds: yarpc.Outbounds{"shard-manager": {
			ServiceName: "shard-manager", Unary: clientTransport.NewSingleOutbound(listener.Addr().String()),
		}},
	})
	must(clientDispatcher.Start())
	api := sharddistributorv1.NewShardDistributorAPIYARPCClient(clientDispatcher.ClientConfig("shard-manager"))

	type command struct {
		Op        string   `json:"op"`
		Namespace string   `json:"namespace"`
		Shards    []string `json:"shards"`
	}
	control := http.NewServeMux()
	control.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		var cmd command
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ns := cmd.Namespace
		if ns == "" {
			ns = namespace
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		var result any
		switch cmd.Op {
		case "stats":
			state, err := kv.GetState(ctx, ns)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			result = state.Executors
		case "assign":
			owners := map[string]string{}
			for _, shard := range cmd.Shards {
				resp, err := api.GetShardOwner(ctx, &sharddistributorv1.GetShardOwnerRequest{Namespace: ns, ShardKey: shard})
				if err != nil {
					http.Error(w, "GetShardOwner "+shard+": "+err.Error(), http.StatusInternalServerError)
					return
				}
				owners[shard] = resp.GetOwner()
			}
			result = owners
		case "assignments":
			state, err := kv.GetState(ctx, ns)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			owners := map[string][]string{}
			for executor, assigned := range state.ShardAssignments {
				for shard := range assigned.AssignedShards {
					owners[executor] = append(owners[executor], shard)
				}
			}
			result = owners
		default:
			http.Error(w, fmt.Sprintf("unknown operation %q", cmd.Op), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
	})
	control.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready", "namespace": namespace})
	})
	go func() { _ = http.Serve(controlListener, control) }()
	must(json.NewEncoder(os.Stdout).Encode(map[string]string{
		"address": listener.Addr().String(), "control_address": controlListener.Addr().String(), "namespace": namespace,
	}))
	<-app.Done()
}
