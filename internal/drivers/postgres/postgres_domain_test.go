package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
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

	t.Run("CreateDomainWithAComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE DOMAIN amount AS integer;
			COMMENT ON DOMAIN amount IS 'a value of money';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateDomainInstruction{
				Name:     "amount",
				BaseType: "integer",
			},
			&PostgresCommentOnDomainInstruction{
				Name: "amount",
				Text: "a value of money",
			},
		})
	})

	t.Run("ModifyDomainComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE DOMAIN amount AS integer;
			COMMENT ON DOMAIN amount IS 'a value';
		`)
		driver.ExecOnDesired(`
			CREATE DOMAIN amount AS integer;
			COMMENT ON DOMAIN amount IS 'a value of money';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnDomainInstruction{
				Name: "amount",
				Text: "a value of money",
			},
		})
	})

	t.Run("RecreateADomainWithAComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE DOMAIN amount AS integer;
			COMMENT ON DOMAIN amount IS 'a value of money';
		`)
		driver.ExecOnDesired(`
			CREATE DOMAIN amount AS bigint;
			COMMENT ON DOMAIN amount IS 'a value of money';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropDomainInstruction{
				Name: "amount",
			},
			&PostgresCreateDomainInstruction{
				Name:     "amount",
				BaseType: "bigint",
			},
			&PostgresCommentOnDomainInstruction{
				Name: "amount",
				Text: "a value of money",
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

	t.Run("ChangeTheBaseTypeOfADomainThatAColumnUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE DOMAIN amount AS integer CHECK (VALUE >= 0);
			CREATE TABLE payments (id INT, value amount DEFAULT 0);
			INSERT INTO payments VALUES (1, 5);
		`)
		driver.ExecOnDesired(`
			CREATE DOMAIN amount AS bigint CHECK (VALUE >= 0);
			CREATE TABLE payments (id INT, value amount DEFAULT 0);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresRenameTypeInstruction{
				ObjectType: "DOMAIN",
				Name:       "amount",
				NewName:    "dbdiff_old_amount",
			},
			&PostgresCreateDomainInstruction{
				Name:     "amount",
				BaseType: "bigint",
				Constraints: []*PostgresDomainConstraint{
					{
						Name: "amount_check",
						Def:  "CHECK ((VALUE >= 0))",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "value",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   `"amount"`,
						UsingCast:  true,
					},
				},
			},
			&PostgresDropDomainInstruction{
				Name: "dbdiff_old_amount",
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "value",
						Expression: "0",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("payments", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": int64(5)},
		}, rows)
	})

	t.Run("ChangeTheBaseTypeOfADomainThatAFunctionReturns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE FUNCTION double_amount(value amount) RETURNS amount LANGUAGE sql AS 'SELECT value * 2';
			CREATE TABLE payments (
				id INT,
				value amount DEFAULT double_amount(1) CHECK (double_amount(value) < 1000)
			);
		`
		driver.ExecOnCurrent(`CREATE DOMAIN amount AS integer;` + schema)
		driver.ExecOnCurrent(`INSERT INTO payments VALUES (1, 5);`)
		driver.ExecOnDesired(`CREATE DOMAIN amount AS bigint;` + schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "payments_value_check",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "value",
					},
				},
			},
			&PostgresDropFunctionInstruction{
				Name:      "double_amount",
				Arguments: "value amount",
			},
			&PostgresRenameTypeInstruction{
				ObjectType: "DOMAIN",
				Name:       "amount",
				NewName:    "dbdiff_old_amount",
			},
			&PostgresCreateDomainInstruction{
				Name:     "amount",
				BaseType: "bigint",
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   `"amount"`,
						UsingCast:  true,
					},
				},
			},
			&PostgresDropDomainInstruction{
				Name: "dbdiff_old_amount",
			},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION double_amount(value amount)\n RETURNS amount\n LANGUAGE sql\nAS $function$SELECT value * 2$function$",
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "value",
						Expression: "double_amount((1)::amount)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "payments",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "payments_value_check",
							Type: "c",
							Def:  "CHECK (((double_amount(value))::bigint < 1000))",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("payments", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": int64(5)},
		}, rows)
	})

	t.Run("DropDomain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropDomainInstruction{Name: "positive_int"},
		})
	})
}
