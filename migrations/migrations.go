package migrations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

// DriverName names a database engine.
type DriverName = driversshared.DriverName

// ConnectionStringDataSource names a database. The field holds a URL, a DSN, a libpq
// keyword string, or a SQLite path.
type ConnectionStringDataSource = driversshared.ConnectionStringDataSource

// A Migrator applies the migrations of one directory to one database. Driver.Migrator
// of the drivers package creates it, so the migrator carries the engine name and the
// schema of the driver.
type Migrator struct {
	driver     DriverName
	schema     string
	database   *ConnectionStringDataSource
	migrations string
}

// NewMigrator creates a migrator for the named engine. Use Driver.Migrator of the
// drivers package instead of a direct call.
func NewMigrator(driver DriverName, schema string, options ...MigratorOption) *Migrator {
	migrator := &Migrator{
		driver: driver,
		schema: schema,
	}

	for _, option := range options {
		option(migrator)
	}

	return migrator
}

// MigratorOption configures NewMigrator and Driver.Migrator of the drivers package.
type MigratorOption func(*Migrator)

// WithDatabase names the database that the migrator reads or changes.
func WithDatabase(database *ConnectionStringDataSource) MigratorOption {
	return func(migrator *Migrator) {
		migrator.database = database
	}
}

// WithMigrations names the directory that holds the migration files.
func WithMigrations(directory string) MigratorOption {
	return func(migrator *Migrator) {
		migrator.migrations = directory
	}
}

type upOptions struct {
	toVersion string
	output    io.Writer
}

// UpOption configures Up.
type UpOption func(*upOptions)

// WithToVersion stops Up after the file with the given version.
func WithToVersion(version string) UpOption {
	return func(merged *upOptions) {
		merged.toVersion = version
	}
}

// WithOutput names the writer that receives one progress line per applied file. Without
// this option the lines go away.
func WithOutput(output io.Writer) UpOption {
	return func(merged *upOptions) {
		merged.output = output
	}
}

type State string

const (
	StateApplied    State = "applied"
	StatePending    State = "pending"
	StateChanged    State = "changed"
	StateMissing    State = "missing"
	StateOutOfOrder State = "out of order"
	StateDirty      State = "dirty"
)

type Entry struct {
	Version   string
	Name      string
	State     State
	AppliedAt time.Time
}

// Up applies every pending file, in one transaction per file.
func (m *Migrator) Up(ctx context.Context, options ...UpOption) error {
	merged := &upOptions{}

	for _, option := range options {
		option(merged)
	}

	migrator, set, err := m.open(ctx)
	if err != nil {
		return err
	}

	defer func() { _ = migrator.Close() }()

	output := merged.output
	if output == nil {
		output = io.Discard
	}

	runner := &internalmigrations.Runner{
		Migrator: migrator,
		Set:      set,
		Output:   output,
	}

	return runner.Up(ctx, merged.toVersion)
}

// Status reads the state of each migration file and of each history row.
func (m *Migrator) Status(ctx context.Context) ([]Entry, error) {
	migrator, set, err := m.open(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { _ = migrator.Close() }()

	var entries []Entry

	for _, entry := range set.Entries {
		entries = append(entries, Entry{
			Version:   entry.Migration.Version,
			Name:      entry.Migration.Name,
			State:     stateOf(entry.State),
			AppliedAt: entry.AppliedAt,
		})
	}

	return entries, nil
}

// A rename of an internal constant must not change a value of this package, so this
// switch maps each state by name.
func stateOf(state internalmigrations.State) State {
	switch state {
	case internalmigrations.StateApplied:
		return StateApplied
	case internalmigrations.StatePending:
		return StatePending
	case internalmigrations.StateChanged:
		return StateChanged
	case internalmigrations.StateMissing:
		return StateMissing
	case internalmigrations.StateOutOfOrder:
		return StateOutOfOrder
	case internalmigrations.StateDirty:
		return StateDirty
	default:
		return State(state)
	}
}

func (m *Migrator) open(ctx context.Context) (internalmigrations.Migrator, *internalmigrations.Set, error) {
	if m.database == nil || m.database.ConnectionString == "" {
		return nil, nil, fmt.Errorf("name the database with the WithDatabase option")
	}

	if m.migrations == "" {
		return nil, nil, fmt.Errorf("name the migration directory with the WithMigrations option")
	}

	migrator, set, err := internaldrivers.OpenMigrationSet(ctx, m.driver, m.database, m.schema, m.migrations)

	var openDatabaseError *internaldrivers.OpenDatabaseError
	if errors.As(err, &openDatabaseError) {
		return nil, nil, fmt.Errorf("failed to open the database: %w", err)
	}

	return migrator, set, err
}
