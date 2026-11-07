package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
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

	t.Run("ChangeTheAttributesOfACompositeTypeThatAColumnUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE pair AS (x INT, y TEXT);
			CREATE TABLE points (id INT, value pair);
			INSERT INTO points VALUES (1, (1, 'a'));
		`)
		driver.ExecOnDesired(`
			CREATE TYPE pair AS (x INT, z BIGINT);
			CREATE TABLE points (id INT, value pair);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTypeDropAttributeInstruction{
				Name:          "pair",
				AttributeName: "y",
			},
			&PostgresAlterTypeAddAttributeInstruction{
				Name: "pair",
				Attribute: &PostgresCompositeTypeAttribute{
					Name: "z",
					Type: "bigint",
				},
			},
		})

		rows := driver.FetchAllFromCurrent("points", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "value": "(1,)"},
		}, rows)
	})

	t.Run("ChangeACompositeTypeThatAViewReturns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE pair AS (x INT, y TEXT);
			CREATE VIEW pairs AS SELECT ROW(1, 'a')::pair AS value;
		`)
		driver.ExecOnDesired(`
			CREATE TYPE pair AS (x BIGINT, y TEXT);
			CREATE VIEW pairs AS SELECT ROW(1, 'a')::pair AS value;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{
				Name: "pairs",
			},
			&PostgresDropTypeInstruction{
				Name: "pair",
			},
			&PostgresCreateCompositeTypeInstruction{
				Name: "pair",
				Attributes: []*PostgresCompositeTypeAttribute{
					{
						Name: "x",
						Type: "bigint",
					},
					{
						Name: "y",
						Type: "text",
					},
				},
			},
			&PostgresCreateViewInstruction{
				Name:  "pairs",
				Query: " SELECT ROW((1)::bigint, 'a'::text)::pair AS value;",
			},
		})

		rows := driver.FetchAllFromCurrent("pairs", "")

		require.Equal(t, []map[string]any{
			{"value": "(1,a)"},
		}, rows)
	})

	t.Run("ChangeTheAttributeTypeOfACompositeTypeThatAColumnUses", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE pair AS (x INT, y TEXT);
			CREATE TABLE points (id INT, value pair);
		`)
		driver.ExecOnDesired(`
			CREATE TYPE pair AS (x BIGINT, y TEXT);
			CREATE TABLE points (id INT, value pair);
		`)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the column value of table points uses the type "pair". `+
			`dbdiff must drop and create this type again, and it cannot drop and create this object again`)
	})

	t.Run("DropCompositeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TYPE address AS (street TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropTypeInstruction{Name: "address"},
		})
	})
}
