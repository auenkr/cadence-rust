// Package sql exposes the private task store constructor to the matching
// task-writer subprocess oracle through a Go overlay only.
package sql

import (
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/serialization"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

// NewTaskStoreForWriterOracle uses the production private constructor and real
// serialization parser. This file is mapped into this package only by the
// dedicated Actions overlay workflow.
func NewTaskStoreForWriterOracle(db sqlplugin.DB) (persistence.TaskStore, error) {
	parser, err := serialization.NewParser(persistence.NewDefaultDynamicConfiguration())
	if err != nil {
		return nil, err
	}
	return newTaskPersistence(db, 1, log.NewNoop(), parser)
}
