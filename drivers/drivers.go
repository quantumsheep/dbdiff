package drivers

import (
	"context"
	"fmt"
	"strings"
	"sync"

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

// DetectDriver names the engine of the two sides of a diff. A SQL data source gives no
// engine, so the other side decides.
func DetectDriver(current DataSource, desired DataSource) (DriverName, error) {
	current, desired, err := normalizeSides(current, desired)
	if err != nil {
		return "", err
	}

	return driversshared.DetectDriver(current, desired)
}

func normalizeSides(current DataSource, desired DataSource) (DataSource, DataSource, error) {
	current, err := driversshared.NormalizeDataSource(current)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the current data source: %w", err)
	}

	desired, err = driversshared.NormalizeDataSource(desired)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read the desired data source: %w", err)
	}

	return current, desired, nil
}

type driverOptions struct {
	serverVersion   string
	scratchDatabase string
	postgres        *PostgreSQLDriverConfig
	mysql           *MySQLDriverConfig
	ignoreTables    []string
}

// Option configures NewDriver.
type Option func(*driverOptions)

// WithServerVersion names the version of the temporary server that runs a SQL data
// source. Only the postgres driver and the mysql driver accept this option.
func WithServerVersion(serverVersion string) Option {
	return func(merged *driverOptions) {
		merged.serverVersion = serverVersion
	}
}

// WithScratchDatabase names the connection URL of an existing server that runs a SQL data
// source. The driver creates its temporary databases on this server and drops them after
// the diff. The user of the URL must have the privilege to create and drop a database.
// Only the postgres driver and the mysql driver accept this option.
func WithScratchDatabase(connectionString string) Option {
	return func(merged *driverOptions) {
		merged.scratchDatabase = connectionString
	}
}

// PostgreSQLDriverConfig holds the options of the postgres driver.
type PostgreSQLDriverConfig struct {
	// Schema names the PostgreSQL schema on both sides. Without it, the search path of
	// the connection string applies.
	Schema string

	ComparePrivileges bool
}

func WithPostgreSQLDriverConfig(config PostgreSQLDriverConfig) Option {
	return func(merged *driverOptions) {
		merged.postgres = &config
	}
}

// MySQLDriverConfig holds the options of the mysql driver.
type MySQLDriverConfig struct {
	ComparePrivileges bool
}

func WithMySQLDriverConfig(config MySQLDriverConfig) Option {
	return func(merged *driverOptions) {
		merged.mysql = &config
	}
}

// WithIgnoreTables names the tables that the diff ignores.
func WithIgnoreTables(tables ...string) Option {
	return func(merged *driverOptions) {
		merged.ignoreTables = append(merged.ignoreTables, tables...)
	}
}

// A Driver diffs two data sources. Two Diff calls on one driver value run one after the
// other.
type Driver struct {
	// The inner driver keeps the connections of one diff in its fields.
	mutex sync.Mutex

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
		ServerVersion:   merged.serverVersion,
		ScratchDatabase: merged.scratchDatabase,
		IgnoreTables:    merged.ignoreTables,
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
	compareData  bool
	guessRenames bool
}

// DiffOption configures one Diff call.
type DiffOption func(*diffOptions)

// WithData adds the comparison of the rows.
func WithData() DiffOption {
	return func(merged *diffOptions) {
		merged.compareData = true
	}
}

// WithRenameGuess renames a column if exactly one removed column of the table has the same
// attributes as the added column. Without this option, the diff drops the removed column and
// adds the added column. Only the sqlite driver and the mysql driver guess a rename.
func WithRenameGuess() DiffOption {
	return func(merged *diffOptions) {
		merged.guessRenames = true
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

// Diff reads the current schema and the desired schema, and it gives the statements
// that make the current schema equal to the desired schema.
func (d *Driver) Diff(
	ctx context.Context,
	current DataSource,
	desired DataSource,
	options ...DiffOption,
) (Statements, error) {
	merged := &diffOptions{}

	for _, option := range options {
		option(merged)
	}

	current, desired, err := normalizeSides(current, desired)
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	instructions, err := d.inner.Diff(ctx, current, desired, driversshared.DiffOptions{
		CompareData:  merged.compareData,
		GuessRenames: merged.guessRenames,
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
