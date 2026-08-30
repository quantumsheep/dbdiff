package drivers

import (
	"context"
	"fmt"

	driversmysql "github.com/quantumsheep/dbdiff/internal/drivers/mysql"
	driverspostgres "github.com/quantumsheep/dbdiff/internal/drivers/postgres"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	driverssqlite "github.com/quantumsheep/dbdiff/internal/drivers/sqlite"
)

func NewDriver(ctx context.Context, driverName driversshared.DriverName, currentSchema string, finalSchema string,
	schema string, scratchVersion string, compareData bool,
	comparePrivileges bool, ignoreTables []string) (driversshared.Driver, error) {
	if driverName == "" {
		detected, err := driversshared.DetectDriver(currentSchema, finalSchema)
		if err != nil {
			return nil, err
		}

		driverName = detected
	}

	switch driverName {
	case driversshared.SQLiteDriverName:
		if schema != "" {
			return nil, fmt.Errorf("the --schema flag applies to the postgres driver only")
		}

		if comparePrivileges {
			return nil, fmt.Errorf("the --privileges flag applies to the postgres driver and to the mysql driver")
		}

		if scratchVersion != "" {
			return nil, fmt.Errorf("the version key of dbdiff.yaml applies to the postgres driver only")
		}

		return driverssqlite.NewSQLiteDriver(ctx, &driverssqlite.SQLiteDriverConfig{
			SourceDatabasePath: currentSchema,
			TargetDatabasePath: finalSchema,
			CompareData:        compareData,
			IgnoreTables:       ignoreTables,
		})
	case driversshared.MySQLDriverName:
		if schema != "" {
			return nil, fmt.Errorf("the --schema flag applies to the postgres driver only")
		}

		if scratchVersion != "" {
			return nil, fmt.Errorf("the version key of dbdiff.yaml applies to the postgres driver only")
		}

		return driversmysql.NewMySQLDriver(ctx, &driversmysql.MySQLDriverConfig{
			SourceConnectionString: currentSchema,
			TargetConnectionString: finalSchema,
			CompareData:            compareData,
			ComparePrivileges:      comparePrivileges,
			IgnoreTables:           ignoreTables,
		})
	case driversshared.PostgresDriverName:
		return driverspostgres.NewPostgresDriver(ctx, &driverspostgres.PostgresDriverConfig{
			SourceConnectionString: currentSchema,
			TargetConnectionString: finalSchema,
			SourceSchema:           schema,
			TargetSchema:           schema,
			CompareData:            compareData,
			ComparePrivileges:      comparePrivileges,
			ScratchServerVersion:   scratchVersion,
			IgnoreTables:           ignoreTables,
		})
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverName)
	}
}
