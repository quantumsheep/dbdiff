package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresDomain(t *testing.T) {
	t.Run("CreateDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 1 CHECK (VALUE > 0);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateDomainInstruction{
				Name:     "positive_int",
				BaseType: "integer",
				Default: sql.NullString{
					String: "1",
					Valid:  true,
				},
				NotNull: true,
				Constraints: []*PostgresDomainConstraint{
					{
						Name: "positive_int_check",
						Def:  "CHECK ((VALUE > 0))",
					},
				},
			},
		})
	})

	t.Run("AlterDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE DOMAIN positive_int AS integer DEFAULT 1;`)
		driver.ExecOnDesired(`CREATE DOMAIN positive_int AS integer NOT NULL DEFAULT 2 CHECK (VALUE > 0);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterDomainInstruction{
				Name:   "positive_int",
				Action: &PostgresSetDomainDefaultAction{Expression: "2"},
			},
			&PostgresAlterDomainInstruction{
				Name:   "positive_int",
				Action: &PostgresSetDomainNotNullAction{},
			},
			&PostgresAlterDomainInstruction{
				Name: "positive_int",
				Action: &PostgresAddDomainConstraintAction{
					ConstraintName: "positive_int_check",
					Definition:     "CHECK ((VALUE > 0))",
				},
			},
		})
	})

	t.Run("RecreateDomainOnNewBaseType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE DOMAIN short_text AS integer;`)
		driver.ExecOnDesired(`CREATE DOMAIN short_text AS varchar(10);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropDomainInstruction{Name: "short_text"},
			&PostgresCreateDomainInstruction{
				Name:     "short_text",
				BaseType: "character varying(10)",
			},
		})
	})

	t.Run("DropDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropDomainInstruction{Name: "positive_int"},
		})
	})
}
