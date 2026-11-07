package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresCompositeType(t *testing.T) {
	t.Run("CreateCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TYPE address AS (street TEXT, city VARCHAR(10));`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateCompositeTypeInstruction{
				Name: "address",
				Attributes: []*PostgresCompositeTypeAttribute{
					{
						Name: "street",
						Type: "text",
					},
					{
						Name: "city",
						Type: "character varying(10)",
					},
				},
			},
		})
	})

	t.Run("ModifyCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE address AS (street TEXT);`)
		driver.ExecOnDesired(`CREATE TYPE address AS (street TEXT, city TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
			&PostgresCreateCompositeTypeInstruction{
				Name: "address",
				Attributes: []*PostgresCompositeTypeAttribute{
					{
						Name: "street",
						Type: "text",
					},
					{
						Name: "city",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("DropCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE address AS (street TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
		})
	})
}
