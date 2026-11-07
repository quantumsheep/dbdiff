package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresSequence(t *testing.T) {
	t.Run("CreateSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE SEQUENCE counter;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateSequenceInstruction{
				Name:      "counter",
				DataType:  "bigint",
				Increment: 1,
				Min:       1,
				Max:       9223372036854775807,
				Start:     1,
				Cache:     1,
			},
		})
	})

	t.Run("DropSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropSequenceInstruction{Name: "counter"},
		})
	})

	t.Run("AlterSequenceCache", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnDesired(`CREATE SEQUENCE counter CACHE 20;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Cache: sql.NullInt64{
					Int64: 20,
					Valid: true,
				},
			},
		})
	})

	t.Run("AlterSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnDesired(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Increment: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
				Max: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceRestartOnHigherMinimum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnCurrent(`SELECT nextval('counter');`)

		driver.ExecOnDesired(`CREATE SEQUENCE counter MINVALUE 100 START WITH 100;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Min: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Start: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Restart: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceRestartOnLowerMaximum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnCurrent(`SELECT nextval('counter') FROM generate_series(1, 10);`)

		driver.ExecOnDesired(`CREATE SEQUENCE counter MAXVALUE 5;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Max: sql.NullInt64{
					Int64: 5,
					Valid: true,
				},
				Restart: sql.NullInt64{
					Int64: 1,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceNoRestartWithinRange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnCurrent(`SELECT nextval('counter');`)

		driver.ExecOnDesired(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Increment: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
				Max: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceOfSerialColumnIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE users (id SERIAL);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:               "id",
						Type:               "integer",
						NotNull:            true,
						Serial:             "serial",
						SerialSequenceName: "users_id_seq",
					},
				},
			},
		})
	})
}
