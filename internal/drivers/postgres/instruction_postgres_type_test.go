package driverspostgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresTypeInstructions(t *testing.T) {
	t.Run("PostgresCommentOnType", func(t *testing.T) {
		instruction := &PostgresCommentOnTypeInstruction{
			Name: "mood",
			Text: "the state of a person",
		}

		require.Equal(t,
			`COMMENT ON TYPE "mood" IS 'the state of a person';`,
			instruction.String())
	})

	t.Run("PostgresCreateEnumType", func(t *testing.T) {
		instruction := &PostgresCreateEnumTypeInstruction{
			Name:   "mood",
			Labels: []string{"sad", "happy"},
		}

		require.Equal(t, `CREATE TYPE "mood" AS ENUM ('sad', 'happy');`, instruction.String())
	})

	t.Run("PostgresAlterTypeAddValue", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "calm",
		}

		require.Equal(t, `ALTER TYPE "mood" ADD VALUE 'calm';`, instruction.String())
	})

	t.Run("PostgresAlterTypeAddValueBefore", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:   "mood",
			Value:  "calm",
			Before: "happy",
		}

		require.Equal(t, `ALTER TYPE "mood" ADD VALUE 'calm' BEFORE 'happy';`, instruction.String())
	})

	t.Run("PostgresAlterTypeAddValueAfter", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "calm",
			After: "sad",
		}

		require.Equal(t, `ALTER TYPE "mood" ADD VALUE 'calm' AFTER 'sad';`, instruction.String())
	})

	t.Run("PostgresDropType", func(t *testing.T) {
		instruction := &PostgresDropTypeInstruction{Name: "mood"}

		require.Equal(t, `DROP TYPE "mood";`, instruction.String())
	})

	t.Run("PostgresCreateCompositeType", func(t *testing.T) {
		instruction := &PostgresCreateCompositeTypeInstruction{
			Name: "address",
			Attributes: []*PostgresCompositeTypeAttribute{
				{
					Name: "street",
					Type: "text",
				},
				{
					Name: "number",
					Type: "integer",
				},
			},
		}

		require.Equal(t, `CREATE TYPE "address" AS (
	"street" text,
	"number" integer
);`, instruction.String())
	})

	t.Run("PostgresCreateDomain", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive",
			BaseType: "integer",
		}

		require.Equal(t, `CREATE DOMAIN "positive" AS integer;`, instruction.String())
	})

	t.Run("PostgresCreateDomainWithEveryPart", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive",
			BaseType: "integer",
			Default: sql.NullString{
				String: "1",
				Valid:  true,
			},
			NotNull: true,
			Constraints: []*PostgresDomainConstraint{
				{
					Name: "positive_check",
					Def:  "CHECK ((VALUE > 0))",
				},
			},
		}

		require.Equal(t,
			`CREATE DOMAIN "positive" AS integer DEFAULT 1 NOT NULL CONSTRAINT "positive_check" CHECK ((VALUE > 0));`,
			instruction.String())
	})

	t.Run("PostgresDropDomain", func(t *testing.T) {
		instruction := &PostgresDropDomainInstruction{Name: "positive"}

		require.Equal(t, `DROP DOMAIN "positive";`, instruction.String())
	})

	t.Run("PostgresAlterDomainSetDefault", func(t *testing.T) {
		instruction := &PostgresAlterDomainInstruction{
			Name:   "positive",
			Action: &PostgresSetDomainDefaultAction{Expression: "5"},
		}

		require.Equal(t, `ALTER DOMAIN "positive" SET DEFAULT 5;`, instruction.String())
	})

	t.Run("PostgresDropDomainDefaultAction", func(t *testing.T) {
		action := &PostgresDropDomainDefaultAction{}

		require.Equal(t, "DROP DEFAULT", action.DomainActionClause())
	})

	t.Run("PostgresSetDomainNotNullAction", func(t *testing.T) {
		action := &PostgresSetDomainNotNullAction{}

		require.Equal(t, "SET NOT NULL", action.DomainActionClause())
	})

	t.Run("PostgresDropDomainNotNullAction", func(t *testing.T) {
		action := &PostgresDropDomainNotNullAction{}

		require.Equal(t, "DROP NOT NULL", action.DomainActionClause())
	})

	t.Run("PostgresAddDomainConstraintAction", func(t *testing.T) {
		action := &PostgresAddDomainConstraintAction{
			ConstraintName: "positive_check",
			Definition:     "CHECK ((VALUE > 0))",
		}

		require.Equal(t,
			`ADD CONSTRAINT "positive_check" CHECK ((VALUE > 0))`,
			action.DomainActionClause())
	})

	t.Run("PostgresDropDomainConstraintAction", func(t *testing.T) {
		action := &PostgresDropDomainConstraintAction{ConstraintName: "positive_check"}

		require.Equal(t, `DROP CONSTRAINT "positive_check"`, action.DomainActionClause())
	})
}

func TestPostgresTypeInstructionComments(t *testing.T) {
	t.Run("PostgresCommentOnType", func(t *testing.T) {
		instruction := &PostgresCommentOnTypeInstruction{
			Name: "mood",
			Text: "the state of a person",
		}

		require.Equal(t, `Modify the type "mood"`, instruction.Comment())
	})

	t.Run("PostgresCreateEnumType", func(t *testing.T) {
		instruction := &PostgresCreateEnumTypeInstruction{
			Name:   "mood",
			Labels: []string{"sad", "happy"},
		}

		require.Equal(t, `Create the type "mood"`, instruction.Comment())
	})

	t.Run("PostgresAlterTypeAddValue", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "calm",
		}

		require.Equal(t, `Modify the type "mood"`, instruction.Comment())
	})

	t.Run("PostgresDropType", func(t *testing.T) {
		instruction := &PostgresDropTypeInstruction{
			Name: "colour",
		}

		require.Equal(t, `Drop the type "colour"`, instruction.Comment())
	})

	t.Run("PostgresCreateCompositeType", func(t *testing.T) {
		instruction := &PostgresCreateCompositeTypeInstruction{
			Name: "address",
		}

		require.Equal(t, `Create the type "address"`, instruction.Comment())
	})

	t.Run("PostgresCreateDomain", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive_integer",
			BaseType: "integer",
		}

		require.Equal(t, `Create the domain "positive_integer"`, instruction.Comment())
	})

	t.Run("PostgresDropDomain", func(t *testing.T) {
		instruction := &PostgresDropDomainInstruction{
			Name: "old_domain",
		}

		require.Equal(t, `Drop the domain "old_domain"`, instruction.Comment())
	})

	t.Run("PostgresAlterDomain", func(t *testing.T) {
		instruction := &PostgresAlterDomainInstruction{
			Name:   "email_address",
			Action: &PostgresSetDomainNotNullAction{},
		}

		require.Equal(t, `Modify the domain "email_address"`, instruction.Comment())
	})
}
