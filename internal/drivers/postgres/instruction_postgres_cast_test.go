package driverspostgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresCastInstructions(t *testing.T) {
	t.Run("PostgresCreateCastWithFunction", func(t *testing.T) {
		instruction := &PostgresCreateCastInstruction{
			SourceType: "point2d",
			TargetType: "text",
			Method:     "f",
			Context:    "a",
			Function:   "point2d_to_text(point2d)",
		}

		require.Equal(t,
			`CREATE CAST (point2d AS text) WITH FUNCTION point2d_to_text(point2d) AS ASSIGNMENT;`,
			instruction.String())
	})

	t.Run("PostgresCreateCastWithInout", func(t *testing.T) {
		instruction := &PostgresCreateCastInstruction{
			SourceType: "point2d",
			TargetType: "point3d",
			Method:     "i",
		}

		require.Equal(t, `CREATE CAST (point2d AS point3d) WITH INOUT;`, instruction.String())
	})

	t.Run("PostgresCreateCastWithoutFunction", func(t *testing.T) {
		instruction := &PostgresCreateCastInstruction{
			SourceType: "money_cents",
			TargetType: "integer",
			Method:     "b",
			Context:    "i",
		}

		require.Equal(t,
			`CREATE CAST (money_cents AS integer) WITHOUT FUNCTION AS IMPLICIT;`,
			instruction.String())
	})

	t.Run("PostgresDropCast", func(t *testing.T) {
		instruction := &PostgresDropCastInstruction{
			SourceType: "point2d",
			TargetType: "text",
		}

		require.Equal(t, `DROP CAST (point2d AS text);`, instruction.String())
	})
}

func TestPostgresCastInstructionComments(t *testing.T) {
	t.Run("PostgresCreateCast", func(t *testing.T) {
		instruction := &PostgresCreateCastInstruction{
			SourceType: "integer",
			TargetType: "text",
		}

		require.Equal(t, `Create the cast "integer" to "text"`, instruction.Comment())
	})

	t.Run("PostgresDropCast", func(t *testing.T) {
		instruction := &PostgresDropCastInstruction{
			SourceType: "integer",
			TargetType: "text",
		}

		require.Equal(t, `Drop the cast "integer" to "text"`, instruction.Comment())
	})
}
