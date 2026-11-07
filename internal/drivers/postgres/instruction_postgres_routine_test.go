package driverspostgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresRoutineInstructions(t *testing.T) {
	t.Run("PostgresCommentOnRoutine", func(t *testing.T) {
		instruction := &PostgresCommentOnRoutineInstruction{
			Name:      "add",
			Arguments: "integer, integer",
			Text:      "the sum of the two values",
		}

		require.Equal(t,
			`COMMENT ON ROUTINE "add"(integer, integer) IS 'the sum of the two values';`,
			instruction.String())
	})

	t.Run("PostgresCreateFunction", func(t *testing.T) {
		instruction := &PostgresCreateFunctionInstruction{
			Definition: "CREATE FUNCTION add(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql",
		}

		require.Equal(t,
			"CREATE FUNCTION add(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql;",
			instruction.String())
	})

	t.Run("PostgresDropFunction", func(t *testing.T) {
		instruction := &PostgresDropFunctionInstruction{
			Name:      "add",
			Arguments: "integer",
		}

		require.Equal(t, `DROP FUNCTION "add"(integer);`, instruction.String())
	})

	t.Run("PostgresCreateProcedure", func(t *testing.T) {
		instruction := &PostgresCreateProcedureInstruction{
			Definition: "CREATE PROCEDURE audit(entry text) LANGUAGE sql AS $$ SELECT 1 $$",
		}

		require.Equal(t,
			"CREATE PROCEDURE audit(entry text) LANGUAGE sql AS $$ SELECT 1 $$;",
			instruction.String())
	})

	t.Run("PostgresDropProcedure", func(t *testing.T) {
		instruction := &PostgresDropProcedureInstruction{
			Name:      "audit",
			Arguments: "entry text",
		}

		require.Equal(t, `DROP PROCEDURE "audit"(entry text);`, instruction.String())
	})

	t.Run("PostgresCreateAggregate", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:               "total",
			Arguments:          "integer",
			TransitionFunction: "int4pl",
			StateType:          "integer",
		}

		require.Equal(t,
			`CREATE AGGREGATE "total"(integer) (SFUNC = int4pl, STYPE = integer);`,
			instruction.String())
	})

	t.Run("PostgresCreateAggregateWithEveryOption", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:                "total",
			Arguments:           "integer",
			TransitionFunction:  "int4pl",
			TransitionSpace:     128,
			StateType:           "integer",
			FinalFunctionExtra:  true,
			FinalFunctionModify: "s",
			FinalFunction: sql.NullString{
				String: "int4out",
				Valid:  true,
			},
			CombineFunction: sql.NullString{
				String: "int4pl",
				Valid:  true,
			},
			SerializeFunction: sql.NullString{
				String: "numeric_avg_serialize",
				Valid:  true,
			},
			DeserializeFunction: sql.NullString{
				String: "numeric_avg_deserialize",
				Valid:  true,
			},
			InitialCondition: sql.NullString{
				String: "0",
				Valid:  true,
			},
			MovingTransitionFunction: sql.NullString{
				String: "int4pl",
				Valid:  true,
			},
			MovingInverseTransitionFunction: sql.NullString{
				String: "int4mi",
				Valid:  true,
			},
			MovingStateType: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			MovingTransitionSpace: 256,
			MovingFinalFunction: sql.NullString{
				String: "int4out",
				Valid:  true,
			},
			MovingFinalFunctionExtra:  true,
			MovingFinalFunctionModify: "w",
			MovingInitialCondition: sql.NullString{
				String: "0",
				Valid:  true,
			},
			SortOperator: sql.NullString{
				String: ">",
				Valid:  true,
			},
			Parallel: "s",
		}

		require.Equal(t,
			`CREATE AGGREGATE "total"(integer) (SFUNC = int4pl, STYPE = integer, SSPACE = 128, `+
				`FINALFUNC = int4out, FINALFUNC_EXTRA, FINALFUNC_MODIFY = SHAREABLE, COMBINEFUNC = int4pl, `+
				`SERIALFUNC = numeric_avg_serialize, DESERIALFUNC = numeric_avg_deserialize, INITCOND = '0', `+
				`MSFUNC = int4pl, MINVFUNC = int4mi, MSTYPE = integer, MSSPACE = 256, MFINALFUNC = int4out, `+
				`MFINALFUNC_EXTRA, MFINALFUNC_MODIFY = READ_WRITE, MINITCOND = '0', SORTOP = OPERATOR(>), `+
				`PARALLEL = SAFE);`,
			instruction.String())
	})

	t.Run("PostgresDropAggregate", func(t *testing.T) {
		instruction := &PostgresDropAggregateInstruction{
			Name:      "total",
			Arguments: "integer",
		}

		require.Equal(t, `DROP AGGREGATE "total"(integer);`, instruction.String())
	})

	t.Run("PostgresCreateOperator", func(t *testing.T) {
		instruction := &PostgresCreateOperatorInstruction{
			Name:     "===",
			Function: "int4eq",
			LeftArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
		}

		require.Equal(t,
			`CREATE OPERATOR === (FUNCTION = "int4eq", LEFTARG = integer, RIGHTARG = integer);`,
			instruction.String())
	})

	t.Run("PostgresCreateOperatorWithEveryOption", func(t *testing.T) {
		instruction := &PostgresCreateOperatorInstruction{
			Name:     "===",
			Function: "int4eq",
			LeftArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			Commutator: sql.NullString{
				String: "===",
				Valid:  true,
			},
			Negator: sql.NullString{
				String: "!==",
				Valid:  true,
			},
			RestrictFunction: sql.NullString{
				String: "eqsel",
				Valid:  true,
			},
			JoinFunction: sql.NullString{
				String: "eqjoinsel",
				Valid:  true,
			},
			CanHash:  true,
			CanMerge: true,
		}

		require.Equal(t,
			`CREATE OPERATOR === (FUNCTION = "int4eq", LEFTARG = integer, RIGHTARG = integer, `+
				`COMMUTATOR = ===, NEGATOR = !==, RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES);`,
			instruction.String())
	})

	t.Run("PostgresDropOperatorWithoutALeftArgument", func(t *testing.T) {
		instruction := &PostgresDropOperatorInstruction{
			Name: "===",
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
		}

		require.Equal(t, `DROP OPERATOR === (NONE, integer);`, instruction.String())
	})
}

func TestPostgresRoutineInstructionComments(t *testing.T) {
	t.Run("PostgresCommentOnRoutine", func(t *testing.T) {
		instruction := &PostgresCommentOnRoutineInstruction{
			Name:      "add",
			Arguments: "integer, integer",
			Text:      "the sum of the two values",
		}

		require.Equal(t, `Modify the function "add"`, instruction.Comment())
	})

	t.Run("PostgresCreateFunction", func(t *testing.T) {
		instruction := &PostgresCreateFunctionInstruction{
			Definition: "CREATE OR REPLACE FUNCTION add(a integer, b integer)\n RETURNS integer\nAS $function$ SELECT a + b $function$",
		}

		require.Equal(t, `Create the function "add"`, instruction.Comment())
	})

	t.Run("PostgresDropFunction", func(t *testing.T) {
		instruction := &PostgresDropFunctionInstruction{
			Name:      "subtract",
			Arguments: "integer, integer",
		}

		require.Equal(t, `Drop the function "subtract"`, instruction.Comment())
	})

	t.Run("PostgresCreateProcedure", func(t *testing.T) {
		instruction := &PostgresCreateProcedureInstruction{
			Definition: "CREATE OR REPLACE PROCEDURE audit(entry text)\n LANGUAGE sql\nAS $procedure$ SELECT 1 $procedure$",
		}

		require.Equal(t, `Create the procedure "audit"`, instruction.Comment())
	})

	t.Run("PostgresDropProcedure", func(t *testing.T) {
		instruction := &PostgresDropProcedureInstruction{
			Name:      "audit",
			Arguments: "entry text",
		}

		require.Equal(t, `Drop the procedure "audit"`, instruction.Comment())
	})

	t.Run("PostgresCreateAggregate", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:      "total",
			Arguments: "integer",
		}

		require.Equal(t, `Create the aggregate "total"`, instruction.Comment())
	})

	t.Run("PostgresDropAggregate", func(t *testing.T) {
		instruction := &PostgresDropAggregateInstruction{
			Name:      "average",
			Arguments: "integer",
		}

		require.Equal(t, `Drop the aggregate "average"`, instruction.Comment())
	})

	t.Run("PostgresCreateOperator", func(t *testing.T) {
		instruction := &PostgresCreateOperatorInstruction{
			Name:     "===",
			Function: "equality",
		}

		require.Equal(t, "Create the operator ===", instruction.Comment())
	})

	t.Run("PostgresDropOperator", func(t *testing.T) {
		instruction := &PostgresDropOperatorInstruction{
			Name: "!==",
		}

		require.Equal(t, "Drop the operator !==", instruction.Comment())
	})

	t.Run("DefinitionWithNoName", func(t *testing.T) {
		instruction := &PostgresCreateFunctionInstruction{
			Definition: "DO $$ BEGIN END $$",
		}

		require.Equal(t, "Create the function", instruction.Comment())
	})
}
