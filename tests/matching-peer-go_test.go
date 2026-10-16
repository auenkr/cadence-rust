package cadence

import (
	"net"
	"os"
	"strconv"
	"testing"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	_ "github.com/uber/cadence/common/archiver/filestore"
	"github.com/uber/cadence/common/membership"
	_ "github.com/uber/cadence/common/persistence/sql/sqlplugin/sqlite"
	"github.com/uber/cadence/common/service"
	"go.uber.org/fx"
)

// Only discovery is substituted. Original Go Frontend, History and Worker
// handlers, clients, persistence and business logic run without modifications.
type rustMatchingPeers struct {
	membership.PeerProvider
	host membership.HostInfo
}

func (p rustMatchingPeers) GetMembers(name string) ([]membership.HostInfo, error) {
	if name == service.Matching {
		return []membership.HostInfo{p.host}, nil
	}
	return p.PeerProvider.GetMembers(name)
}
func TestRustMatchingGoServices(t *testing.T) {
	address := os.Getenv("CADENCE_RUST_MATCHING_ADDR")
	ip, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	host := membership.NewDetailedHostInfo(net.JoinHostPort(ip, portText), "rust-matching", membership.PortMap{membership.PortGRPC: uint16(port)})
	original := _commonModule
	defer func() { _commonModule = original }()
	_commonModule = fx.Options(original, fx.Decorate(func(provider membership.PeerProvider) membership.PeerProvider {
		return rustMatchingPeers{PeerProvider: provider, host: host}
	}))
	app := BuildCLI("compatibility", "original-go")
	err = app.Run([]string{"cadence-server", "--config", os.Getenv("CADENCE_GO_CONFIG"), "--zone", "sqlite", "start", "--auto-setup", "--services", "frontend,history,worker"})
	if err != nil {
		t.Fatal(err)
	}
}
