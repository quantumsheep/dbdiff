package drivers

import (
	"fmt"

	driversmysql "github.com/quantumsheep/dbdiff/internal/drivers/mysql"
	driverspostgres "github.com/quantumsheep/dbdiff/internal/drivers/postgres"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	driverssqlite "github.com/quantumsheep/dbdiff/internal/drivers/sqlite"
)

type DriverOptions struct {
	Version           string
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

		if options.Version != "" {
			return nil, fmt.Errorf("the version option applies to the postgres driver only")
		}

		return driverssqlite.NewSQLiteDriver(&driverssqlite.SQLiteDriverConfig{
			IgnoreTables: options.IgnoreTables,
		}), nil
	case driversshared.MySQLDriverName:
		if options.Schema != "" {
			return nil, fmt.Errorf("the schema option applies to the postgres driver only")
		}

		if options.Version != "" {
			return nil, fmt.Errorf("the version option applies to the postgres driver only")
		}

		return driversmysql.NewMySQLDriver(&driversmysql.MySQLDriverConfig{
			ComparePrivileges: options.ComparePrivileges,
			IgnoreTables:      options.IgnoreTables,
		}), nil
	case driversshared.PostgresDriverName:
		err := driverspostgres.ValidateScratchServerVersion(options.Version)
		if err != nil {
			return nil, err
		}

		return driverspostgres.NewPostgresDriver(&driverspostgres.PostgresDriverConfig{
			Schema:               options.Schema,
			ComparePrivileges:    options.ComparePrivileges,
			ScratchServerVersion: options.Version,
			IgnoreTables:         options.IgnoreTables,
		}), nil
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverName)
	}
}
