package migrations

import (
	"context"
	"fmt"
	"io"
	"time"

	dbdiffdrivers "github.com/quantumsheep/dbdiff/drivers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

type commonOptions struct {
	driver    dbdiffdrivers.DriverName
	database  string
	schema    string
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

// Option configures every function of this package.
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

// WithDatabase names the database that receives the migrations. An empty driver value
// starts the detection of the engine from the url value, which holds a SQLite path or a
// PostgreSQL connection string.
func WithDatabase(driver dbdiffdrivers.DriverName, url string) Option {
	return commonOption(func(target *commonOptions) {
		target.driver = driver
		target.database = url
	})
}

// WithSchema names the PostgreSQL schema. Without this option the search path of the
// connection string applies.
func WithSchema(schema string) Option {
	return commonOption(func(target *commonOptions) {
		target.schema = schema
	})
}

// WithDirectory names the directory that holds the migration files.
func WithDirectory(directory string) Option {
	return commonOption(func(target *commonOptions) {
		target.directory = directory
	})
}

// WithOutput names the writer that receives one progress line per applied file. Without
// this option the lines go away.
func WithOutput(output io.Writer) Option {
	return commonOption(func(target *commonOptions) {
		target.output = output
	})
}

// WithToVersion stops Up after the file with the given version.
func WithToVersion(version string) UpOption {
	return upOption(func(target *upOptions) {
		target.toVersion = version
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
func Up(ctx context.Context, options ...UpOption) error {
	merged := &upOptions{}

	for _, option := range options {
		option.applyUp(merged)
	}

	migrator, set, err := open(ctx, &merged.commonOptions)
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
func Status(ctx context.Context, options ...StatusOption) ([]Entry, error) {
	merged := &statusOptions{}

	for _, option := range options {
		option.applyStatus(merged)
	}

	migrator, set, err := open(ctx, &merged.commonOptions)
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

func open(ctx context.Context, merged *commonOptions) (coremigrations.Migrator, *coremigrations.MigrationSet, error) {
	if merged.database == "" {
		return nil, nil, fmt.Errorf("name the database with the WithDatabase option")
	}

	if merged.directory == "" {
		return nil, nil, fmt.Errorf("name the migrations directory with the WithDirectory option")
	}

	migrator, err := coremigrations.NewMigrator(ctx, merged.driver, merged.database, merged.schema)
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
