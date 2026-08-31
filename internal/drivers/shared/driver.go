package driversshared

import (
	"context"
)

// The diff hides this table. Without that step, a diff prints a DROP statement for the
// table, and the user then erases the history of every migration.
const MigrationHistoryTableName = "dbdiff_migrations"

type DiffOptions struct {
	CompareData bool
}

// A driver value runs one diff at a time. Each Diff call binds the connection
// fields of the driver on entry and clears them on exit.
type Driver interface {
	Diff(ctx context.Context, source DataSource, target DataSource, options DiffOptions) (Instructions, error)
}

func FirstError(candidates ...error) error {
	for _, err := range candidates {
		if err != nil {
			return err
		}
	}

	return nil
}

// An early removal covers the object that blocks an addition of another section.
type SectionDiff struct {
	EarlyRemovals []Instruction
	Additions     []Instruction
	Removals      []Instruction
}
