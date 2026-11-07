package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresExtension(t *testing.T) {
	t.Run("CreateExtension", func(t *testing.T) {
		driver := NewTestPostgresDriverWithTwoDatabases(t)

		driver.ExecOnDesired(`CREATE EXTENSION pg_trgm;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateExtensionInstruction{Name: "pg_trgm"},
		})
	})

	t.Run("DropExtension", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE EXTENSION pg_trgm;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_trgm"},
		})
	})

	t.Run("DropExtensionThatOwnsAView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE EXTENSION pg_buffercache;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropExtensionInstruction{Name: "pg_buffercache"},
		})
	})
}
