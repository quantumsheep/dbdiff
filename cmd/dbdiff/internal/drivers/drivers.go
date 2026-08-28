package drivers

import (
	"context"
	"fmt"

	dbdiffdrivers "github.com/quantumsheep/dbdiff/drivers"
)

func NewDriver(ctx context.Context, driverName string, currentSchema string, finalSchema string,
	schema string, scratchVersion string, compareData bool,
	comparePrivileges bool) (dbdiffdrivers.Driver, error) {
	if driverName == "" {
		detected, err := dbdiffdrivers.DetectDriver(currentSchema, finalSchema)
		if err != nil {
			return nil, err
		}

		driverName = detected
	}

	switch driverName {
	case dbdiffdrivers.SQLiteDriverName:
		if schema != "" {
			return nil, fmt.Errorf("the --schema flag applies to the postgres driver only")
		}

		if comparePrivileges {
			return nil, fmt.Errorf("the --privileges flag applies to the postgres driver only")
		}

		if scratchVersion != "" {
			return nil, fmt.Errorf("the version key of dbdiff.yaml applies to the postgres driver only")
		}

		return dbdiffdrivers.NewSQLiteDriver(ctx, &dbdiffdrivers.SQLLiteDriverConfig{
			SourceDatabasePath: currentSchema,
			TargetDatabasePath: finalSchema,
			CompareData:        compareData,
		})
	case dbdiffdrivers.PostgresDriverName:
		return dbdiffdrivers.NewPostgresDriver(ctx, &dbdiffdrivers.PostgresDriverConfig{
			SourceConnectionString: currentSchema,
			TargetConnectionString: finalSchema,
			SourceSchema:           schema,
			TargetSchema:           schema,
			CompareData:            compareData,
			ComparePrivileges:      comparePrivileges,
			ScratchServerVersion:   scratchVersion,
		})
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driverName)
	}
}
