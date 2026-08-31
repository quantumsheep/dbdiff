package drivers

import (
	"context"
	"fmt"
	"strings"

	internaldrivers "github.com/quantumsheep/dbdiff/internal/drivers"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/migrations"
)

// DriverName names a database engine.
type DriverName = driversshared.DriverName

const (
	SQLiteDriverName   DriverName = driversshared.SQLiteDriverName
	PostgresDriverName DriverName = driversshared.PostgresDriverName
	MySQLDriverName    DriverName = driversshared.MySQLDriverName
)

// A DataSource names one side of a diff.
type DataSource = driversshared.DataSource

// FileDataSource names one .sql file.
type FileDataSource = driversshared.FileDataSource

// FolderDataSource names a directory of .sql files.
type FolderDataSource = driversshared.FolderDataSource

// ConnectionStringDataSource names a database. The field holds a URL, a DSN, a libpq
// keyword string, or a SQLite path.
type ConnectionStringDataSource = driversshared.ConnectionStringDataSource

// NewFileDataSource names one .sql file.
func NewFileDataSource(path string) FileDataSource {
	return FileDataSource{
		Path: path,
	}
}

// NewFolderDataSource names a directory of .sql files.
func NewFolderDataSource(path string) FolderDataSource {
	return FolderDataSource{
		Path: path,
	}
}

// NewConnectionStringDataSource names a database. The argument holds a URL, a DSN, a
// libpq keyword string, or a SQLite path.
func NewConnectionStringDataSource(connectionString string) ConnectionStringDataSource {
	return ConnectionStringDataSource{
		ConnectionString: connectionString,
	}
}

// DetectDriver names the engine of two data sources. A SQL source gives no engine, so
// the other side decides.
func DetectDriver(source DataSource, target DataSource) (DriverName, error) {
	return driversshared.DetectDriver(source, target)
}

type driverOptions struct {
	version      string
	postgres     *PostgreSQLDriverConfig
	mysql        *MySQLDriverConfig
	ignoreTables []string
}

// Option configures NewDriver.
type Option func(*driverOptions)

// WithVersion names the version of the engine. Today only the PostgreSQL scratch server
// reads it.
func WithVersion(version string) Option {
	return func(target *driverOptions) {
		target.version = version
	}
}

// PostgreSQLDriverConfig holds the options of the postgres driver.
type PostgreSQLDriverConfig struct {
	// Schema names the schema on both sides. Without it the search path of the
	// connection string applies.
	Schema string

	ComparePrivileges bool
}

func WithPostgreSQLDriverConfig(config PostgreSQLDriverConfig) Option {
	return func(target *driverOptions) {
		target.postgres = &config
	}
}

// MySQLDriverConfig holds the options of the mysql driver.
type MySQLDriverConfig struct {
	ComparePrivileges bool
}

func WithMySQLDriverConfig(config MySQLDriverConfig) Option {
	return func(target *driverOptions) {
		target.mysql = &config
	}
}

// WithIgnoreTables names the tables that the diff ignores.
func WithIgnoreTables(tables ...string) Option {
	return func(target *driverOptions) {
		target.ignoreTables = append(target.ignoreTables, tables...)
	}
}

// A Driver diffs two data sources. A driver value runs one diff at a time.
type Driver struct {
	inner  driversshared.Driver
	name   DriverName
	schema string
}

func NewDriver(name DriverName, options ...Option) (*Driver, error) {
	merged := &driverOptions{}

	for _, option := range options {
		option(merged)
	}

	if merged.postgres != nil && name != PostgresDriverName {
		return nil, fmt.Errorf("the PostgreSQL config applies to the postgres driver only")
	}

	if merged.mysql != nil && name != MySQLDriverName {
		return nil, fmt.Errorf("the MySQL config applies to the mysql driver only")
	}

	internalOptions := internaldrivers.DriverOptions{
		Version:      merged.version,
		IgnoreTables: merged.ignoreTables,
	}

	if merged.postgres != nil {
		internalOptions.Schema = merged.postgres.Schema
		internalOptions.ComparePrivileges = merged.postgres.ComparePrivileges
	}

	if merged.mysql != nil {
		internalOptions.ComparePrivileges = merged.mysql.ComparePrivileges
	}

	inner, err := internaldrivers.NewDriver(name, internalOptions)
	if err != nil {
		return nil, err
	}

	return &Driver{
		inner:  inner,
		name:   name,
		schema: internalOptions.Schema,
	}, nil
}

// Migrator gives the migrator of the engine of the driver. The migrator reads the
// engine name and the schema from the driver.
func (d *Driver) Migrator(options ...migrations.MigratorOption) *migrations.Migrator {
	return migrations.NewMigrator(d.name, d.schema, options...)
}

type diffOptions struct {
	compareData bool
}

// DiffOption configures one Diff call.
type DiffOption func(*diffOptions)

// WithData adds the comparison of the rows.
func WithData() DiffOption {
	return func(target *diffOptions) {
		target.compareData = true
	}
}

// A Statement is one SQL statement of a diff.
type Statement struct {
	SQL string

	// Comment names the object that the statement changes.
	Comment string
}

// Statements holds the statements of a diff, in the order of the output of the diff
// command.
type Statements []Statement

// String gives the statements as one SQL text, one statement per line, in the same
// format as the output of the diff command.
func (statements Statements) String() string {
	texts := make([]string, 0, len(statements))

	for _, statement := range statements {
		texts = append(texts, statement.SQL)
	}

	return strings.Join(texts, "\n")
}

// Diff reads the schema of the source and of the target, and it gives the statements
// that change the source.
func (d *Driver) Diff(ctx context.Context, source DataSource, target DataSource,
	options ...DiffOption) (Statements, error) {
	merged := &diffOptions{}

	for _, option := range options {
		option(merged)
	}

	instructions, err := d.inner.Diff(ctx, source, target, driversshared.DiffOptions{
		CompareData: merged.compareData,
	})
	if err != nil {
		return nil, err
	}

	var statements Statements

	for _, instruction := range instructions {
		statements = append(statements, Statement{
			SQL:     instruction.String(),
			Comment: instruction.Comment(),
		})
	}

	return statements, nil
}
