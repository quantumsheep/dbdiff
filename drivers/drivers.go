package drivers

import (
	"context"

	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

// DriverName names a database engine.
type DriverName = driversshared.DriverName

const (
	SQLiteDriverName   DriverName = driversshared.SQLiteDriverName
	PostgresDriverName DriverName = driversshared.PostgresDriverName
)

type diffOptions struct {
	driver               DriverName
	schema               string
	scratchServerVersion string
	compareData          bool
	comparePrivileges    bool
}

// Option configures Diff.
type Option interface {
	applyDiff(*diffOptions)
}

type diffOption func(*diffOptions)

func (apply diffOption) applyDiff(target *diffOptions) {
	apply(target)
}

// WithDriver names the engine. Without this option the detection reads the source and the
// target arguments.
func WithDriver(driver DriverName) Option {
	return diffOption(func(target *diffOptions) {
		target.driver = driver
	})
}

// WithSchema names the PostgreSQL schema on both sides. Without this option the search
// path of the connection string applies.
func WithSchema(schema string) Option {
	return diffOption(func(target *diffOptions) {
		target.schema = schema
	})
}

// WithScratchServerVersion names the PostgreSQL version of the temporary server that
// materializes a SQL source when both sides are SQL sources.
func WithScratchServerVersion(version string) Option {
	return diffOption(func(target *diffOptions) {
		target.scratchServerVersion = version
	})
}

// WithData adds the comparison of the rows.
func WithData() Option {
	return diffOption(func(target *diffOptions) {
		target.compareData = true
	})
}

// WithPrivileges adds the comparison of the PostgreSQL privileges.
func WithPrivileges() Option {
	return diffOption(func(target *diffOptions) {
		target.comparePrivileges = true
	})
}

// A Statement is one SQL statement of a diff.
type Statement struct {
	SQL string

	// Comment names the object that the statement changes.
	Comment string
}

// Diff reads the schema of the source and of the target, and it gives the statements that
// change the source. An argument names a database, a .sql file, or a directory of .sql
// files.
func Diff(ctx context.Context, source string, target string, options ...Option) ([]Statement, error) {
	merged := &diffOptions{}

	for _, option := range options {
		option.applyDiff(merged)
	}

	driver, err := internaldrivers.NewDriver(ctx, merged.driver, source, target, merged.schema,
		merged.scratchServerVersion, merged.compareData, merged.comparePrivileges)
	if err != nil {
		return nil, err
	}

	defer func() { _ = driver.Close() }()

	instructions, err := driver.Diff(ctx)
	if err != nil {
		return nil, err
	}

	var statements []Statement

	for _, instruction := range instructions {
		statements = append(statements, Statement{
			SQL:     instruction.String(),
			Comment: instruction.Comment(),
		})
	}

	return statements, nil
}
