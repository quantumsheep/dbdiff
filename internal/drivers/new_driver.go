package drivers

import (
	"context"
	"fmt"

	driversmysql "github.com/quantumsheep/dbdiff/internal/drivers/mysql"
	driverspostgres "github.com/quantumsheep/dbdiff/internal/drivers/postgres"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	driverssqlite "github.com/quantumsheep/dbdiff/internal/drivers/sqlite"
	internalmigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

type DriverOptions struct {
	ServerVersion     string
	Schema            string
	ComparePrivileges bool
	IgnoreTables      []string
}

func NewDriver(driverName driversshared.DriverName, options DriverOptions) (driversshared.Driver, error) {
	switch driverName {
	case driversshared.SQLiteDriverName:
		if options.Schema != "" {
			return nil, fmt.Errorf("the schema option applies to the postgres driver only")
		}

		if options.ComparePrivileges {
			return nil, fmt.Errorf("the privileges option applies to the postgres driver and to the mysql driver")
		}

		if options.ServerVersion != "" {
			return nil, fmt.Errorf("the server version option applies to the postgres driver and to the mysql driver")
		}

		return driverssqlite.NewSQLiteDriver(&driverssqlite.SQLiteDriverConfig{
			IgnoreTables: options.IgnoreTables,
		}), nil
	case driversshared.MySQLDriverName:
		if options.Schema != "" {
			return nil, fmt.Errorf("the schema option applies to the postgres driver only")
		}

		return driversmysql.NewMySQLDriver(&driversmysql.MySQLDriverConfig{
			ComparePrivileges:    options.ComparePrivileges,
			ScratchServerVersion: options.ServerVersion,
			IgnoreTables:         options.IgnoreTables,
		}), nil
	case driversshared.PostgresDriverName:
		err := driverspostgres.ValidateScratchServerVersion(options.ServerVersion)
		if err != nil {
			return nil, err
		}

		return driverspostgres.NewPostgresDriver(&driverspostgres.PostgresDriverConfig{
			Schema:               options.Schema,
			ComparePrivileges:    options.ComparePrivileges,
			ScratchServerVersion: options.ServerVersion,
			IgnoreTables:         options.IgnoreTables,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverName)
	}
}

func NewMigrator(
	ctx context.Context,
	driverName driversshared.DriverName,
	database driversshared.ConnectionStringDataSource,
	schema string,
) (internalmigrations.Migrator, error) {
	if driverName == "" {
		detected, err := driversshared.DetectDriverOfDatabase(database)
		if err != nil {
			return nil, err
		}

		driverName = detected
	}

	switch driverName {
	case driversshared.SQLiteDriverName:
		return driverssqlite.NewSQLiteMigrator(database.ConnectionString)
	case driversshared.PostgresDriverName:
		return driverspostgres.NewPostgresMigrator(ctx, database.ConnectionString, schema)
	case driversshared.MySQLDriverName:
		return driversmysql.NewMySQLMigrator(ctx, database.ConnectionString)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverName)
	}
}

// OpenDatabaseError holds an error of NewMigrator in OpenMigrationSet. Each caller gives
// this error its own message.
type OpenDatabaseError struct {
	Err error
}

func (openDatabaseError *OpenDatabaseError) Error() string {
	return openDatabaseError.Err.Error()
}

func (openDatabaseError *OpenDatabaseError) Unwrap() error {
	return openDatabaseError.Err
}

func OpenMigrationSet(
	ctx context.Context,
	driverName driversshared.DriverName,
	database driversshared.ConnectionStringDataSource,
	schema string,
	directory string,
) (internalmigrations.Migrator, *internalmigrations.Set, error) {
	migrator, err := NewMigrator(ctx, driverName, database, schema)
	if err != nil {
		return nil, nil, &OpenDatabaseError{
			Err: err,
		}
	}

	set, err := internalmigrations.LoadSet(ctx, migrator, directory)
	if err != nil {
		_ = migrator.Close()

		return nil, nil, err
	}

	return migrator, set, nil
}
