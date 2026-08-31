package migrations

import (
	"context"
	"fmt"
	"io"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

// DriverName names a database engine.
type DriverName = driversshared.DriverName

// ConnectionStringDataSource names a database. The field holds a URL, a DSN, a libpq
// keyword string, or a SQLite path.
type ConnectionStringDataSource = driversshared.ConnectionStringDataSource

// A Migrator applies the migrations of one engine. Driver.Migrator of the drivers
// package creates it, so the migrator carries the engine name and the schema of the
// driver.
type Migrator struct {
	driver DriverName
	schema string
}

// NewMigrator creates a migrator for the named engine. Use Driver.Migrator of the
// drivers package instead of a direct call.
func NewMigrator(driver DriverName, schema string) *Migrator {
	return &Migrator{
		driver: driver,
		schema: schema,
	}
}

type commonOptions struct {
	target    ConnectionStringDataSource
	directory string
	output    io.Writer
}

type upOptions struct {
	commonOptions

	toVersion string
}

type statusOptions struct {
	commonOptions
}

// Option configures every method of Migrator.
type Option interface {
	UpOption
	StatusOption
}

// UpOption configures Up.
type UpOption interface {
	applyUp(*upOptions)
}

// StatusOption configures Status.
type StatusOption interface {
	applyStatus(*statusOptions)
}

type commonOption func(*commonOptions)

func (apply commonOption) applyUp(target *upOptions) {
	apply(&target.commonOptions)
}

func (apply commonOption) applyStatus(target *statusOptions) {
	apply(&target.commonOptions)
}

type upOption func(*upOptions)

func (apply upOption) applyUp(target *upOptions) {
	apply(target)
}

// WithTargetDataSource names the database that receives the migrations.
func WithTargetDataSource(target ConnectionStringDataSource) Option {
	return commonOption(func(merged *commonOptions) {
		merged.target = target
	})
}

// WithMigrationDirectory names the directory that holds the migration files.
func WithMigrationDirectory(directory string) Option {
	return commonOption(func(merged *commonOptions) {
		merged.directory = directory
	})
}

// WithOutput names the writer that receives one progress line per applied file. Without
// this option the lines go away.
func WithOutput(output io.Writer) Option {
	return commonOption(func(merged *commonOptions) {
		merged.output = output
	})
}

// WithToVersion stops Up after the file with the given version.
func WithToVersion(version string) UpOption {
	return upOption(func(merged *upOptions) {
		merged.toVersion = version
	})
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
		option.applyUp(merged)
	}

	migrator, set, err := m.open(ctx, &merged.commonOptions)
	if err != nil {
		return err
	}

	defer func() { _ = migrator.Close() }()

	output := merged.output
	if output == nil {
		output = io.Discard
	}

	return coremigrations.ApplyMigrations(ctx, migrator, set, merged.toVersion, output)
}

// Status reads the state of each migration file and of each history row.
func (m *Migrator) Status(ctx context.Context, options ...StatusOption) ([]Entry, error) {
	merged := &statusOptions{}

	for _, option := range options {
		option.applyStatus(merged)
	}

	migrator, set, err := m.open(ctx, &merged.commonOptions)
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
func stateOf(state coremigrations.MigrationState) State {
	switch state {
	case coremigrations.MigrationApplied:
		return StateApplied
	case coremigrations.MigrationPending:
		return StatePending
	case coremigrations.MigrationChanged:
		return StateChanged
	case coremigrations.MigrationMissing:
		return StateMissing
	case coremigrations.MigrationOutOfOrder:
		return StateOutOfOrder
	case coremigrations.MigrationDirty:
		return StateDirty
	default:
		return State(state)
	}
}

func (m *Migrator) open(ctx context.Context, merged *commonOptions) (coremigrations.Migrator, *coremigrations.MigrationSet, error) {
	if merged.target.ConnectionString == "" {
		return nil, nil, fmt.Errorf("name the target with the WithTargetDataSource option")
	}

	if merged.directory == "" {
		return nil, nil, fmt.Errorf("name the migrations directory with the WithMigrationDirectory option")
	}

	migrator, err := coremigrations.NewMigrator(ctx, m.driver, merged.target, m.schema)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open the database: %w", err)
	}

	set, err := coremigrations.LoadMigrationSet(ctx, migrator, merged.directory)
	if err != nil {
		_ = migrator.Close()

		return nil, nil, err
	}

	return migrator, set, nil
}
