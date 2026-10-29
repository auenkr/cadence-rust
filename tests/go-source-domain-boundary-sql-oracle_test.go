package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
	"github.com/uber/cadence/common/persistence/wrappers/metered"
	"github.com/uber/cadence/common/persistence/wrappers/ratelimited"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/types"
)

// Use the SQL store and the same wrapper order as persistence/client/factory.go.
func TestSourceDomainBoundarySQLOracle(t *testing.T) {
	for _, source := range []string{"malformed-source", "\xffsource\x80"} {
		t.Run(source, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			db := sqlplugin.NewMockDB(ctrl) // no DB call is expected for malformed IDs
			limiter := quotas.NewMockLimiter(ctrl)
			store, err := newMetadataPersistenceV2(db, "active", log.NewNoop(), nil)
			require.NoError(t, err)
			manager := persistence.NewDomainManagerImpl(store, log.NewNoop(), nil, nil)
			manager = ratelimited.NewDomainManager(manager, limiter, quotas.NewCallerBypass(nil))
			manager = metered.NewDomainManager(manager, metrics.NewNoopMetricsClient(), log.NewNoop(), &config.Persistence{})
			request := &persistence.GetDomainRequest{ID: source}

			limiter.EXPECT().Allow().Return(false).Times(1)
			_, err = manager.GetDomain(context.Background(), request)
			require.Same(t, ratelimited.ErrPersistenceLimitExceeded, err)

			limiter.EXPECT().Allow().Return(true).Times(1)
			var captured interface{}
			func() {
				defer func() { captured = recover() }()
				_, _ = manager.GetDomain(context.Background(), request)
			}()
			require.NotNil(t, captured)
			var direct interface{}
			func() {
				defer func() { direct = recover() }()
				serialization.MustParseUUID(source)
			}()
			require.NotNil(t, direct)
			require.Equal(t, fmt.Sprint(direct), fmt.Sprint(captured))
			t.Logf("raw source bytes=%x throttled=%T parser panic=%T %v", []byte(source), err, captured, captured)
		})
	}

	// A valid alternate spelling reaches the actual SQL filter after parsing.
	t.Run("uppercase UUID reaches SQL", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		db := sqlplugin.NewMockDB(ctrl)
		id := serialization.MustParseUUID("abcdefab-cdef-abcd-efab-cdefabcdefab")
		db.EXPECT().SelectFromDomain(gomock.Any(), &sqlplugin.DomainFilter{ID: &id}).Return(nil, sql.ErrNoRows)
		store, err := newMetadataPersistenceV2(db, "active", log.NewNoop(), nil)
		require.NoError(t, err)
		_, err = store.GetDomain(context.Background(), &persistence.GetDomainRequest{ID: "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB"})
		require.IsType(t, &types.EntityNotExistsError{}, err)
	})
}
