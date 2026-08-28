package drivers

import (
	"context"
	"fmt"
)

func NewDriver(ctx context.Context, driverName DriverName, currentSchema string, finalSchema string,
	schema string, scratchVersion string, compareData bool,
	comparePrivileges bool) (Driver, error) {
	if driverName == "" {
		detected, err := DetectDriver(currentSchema, finalSchema)
		if err != nil {
			return nil, err
		}

		driverName = detected
	}

	switch driverName {
	case SQLiteDriverName:
		if schema != "" {
			return nil, fmt.Errorf("the --schema flag applies to the postgres driver only")
		}

		if comparePrivileges {
			return nil, fmt.Errorf("the --privileges flag applies to the postgres driver only")
		}

		if scratchVersion != "" {
			return nil, fmt.Errorf("the version key of dbdiff.yaml applies to the postgres driver only")
		}

		return NewSQLiteDriver(ctx, &SQLiteDriverConfig{
			SourceDatabasePath: currentSchema,
			TargetDatabasePath: finalSchema,
			CompareData:        compareData,
		})
	case PostgresDriverName:
		return NewPostgresDriver(ctx, &PostgresDriverConfig{
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
