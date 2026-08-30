package driversmysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstructionComments(t *testing.T) {
	t.Run("MySQLCreateTable", func(t *testing.T) {
		instruction := &MySQLCreateTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Create the table "users"`, instruction.Comment())
	})

	t.Run("MySQLDropTable", func(t *testing.T) {
		instruction := &MySQLDropTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Drop the table "users"`, instruction.Comment())
	})

	t.Run("MySQLAlterTable", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("MySQLCreateIndex", func(t *testing.T) {
		instruction := &MySQLCreateIndexInstruction{
			Name:      "idx_users_email",
			TableName: "users",
		}

		require.Equal(t, `Create the index "idx_users_email" of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLDropIndex", func(t *testing.T) {
		instruction := &MySQLDropIndexInstruction{
			Name:      "idx_users_email",
			TableName: "users",
		}

		require.Equal(t, `Drop the index "idx_users_email" of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLCreateView", func(t *testing.T) {
		instruction := &MySQLCreateViewInstruction{
			Name: "adult_users",
		}

		require.Equal(t, `Create the view "adult_users"`, instruction.Comment())
	})

	t.Run("MySQLDropView", func(t *testing.T) {
		instruction := &MySQLDropViewInstruction{
			Name: "adult_users",
		}

		require.Equal(t, `Drop the view "adult_users"`, instruction.Comment())
	})

	t.Run("MySQLCreateTrigger", func(t *testing.T) {
		instruction := &MySQLCreateTriggerInstruction{
			Name:      "trg_users_insert",
			TableName: "users",
		}

		require.Equal(t, `Create the trigger "trg_users_insert" of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLDropTrigger", func(t *testing.T) {
		instruction := &MySQLDropTriggerInstruction{
			Name: "trg_users_insert",
		}

		require.Equal(t, `Drop the trigger "trg_users_insert"`, instruction.Comment())
	})

	t.Run("MySQLCreateRoutine", func(t *testing.T) {
		instruction := &MySQLCreateRoutineInstruction{
			Type: "FUNCTION",
			Name: "double_it",
		}

		require.Equal(t, `Create the function "double_it"`, instruction.Comment())
	})

	t.Run("MySQLDropRoutine", func(t *testing.T) {
		instruction := &MySQLDropRoutineInstruction{
			Type: "PROCEDURE",
			Name: "prune_users",
		}

		require.Equal(t, `Drop the procedure "prune_users"`, instruction.Comment())
	})

	t.Run("MySQLCreateEvent", func(t *testing.T) {
		instruction := &MySQLCreateEventInstruction{
			Name: "nightly",
		}

		require.Equal(t, `Create the event "nightly"`, instruction.Comment())
	})

	t.Run("MySQLDropEvent", func(t *testing.T) {
		instruction := &MySQLDropEventInstruction{
			Name: "nightly",
		}

		require.Equal(t, `Drop the event "nightly"`, instruction.Comment())
	})

	t.Run("MySQLCreateSequence", func(t *testing.T) {
		instruction := &MySQLCreateSequenceInstruction{
			Name: "order_numbers",
		}

		require.Equal(t, `Create the sequence "order_numbers"`, instruction.Comment())
	})

	t.Run("MySQLDropSequence", func(t *testing.T) {
		instruction := &MySQLDropSequenceInstruction{
			Name: "order_numbers",
		}

		require.Equal(t, `Drop the sequence "order_numbers"`, instruction.Comment())
	})

	t.Run("MySQLGrant", func(t *testing.T) {
		instruction := &MySQLGrantInstruction{
			TableName: "users",
		}

		require.Equal(t, `Grant the privileges of the table "users"`, instruction.Comment())
		require.Equal(t, "Grant the privileges of the database", (&MySQLGrantInstruction{}).Comment())
	})

	t.Run("MySQLRevoke", func(t *testing.T) {
		instruction := &MySQLRevokeInstruction{
			TableName: "users",
		}

		require.Equal(t, `Revoke the privileges of the table "users"`, instruction.Comment())
		require.Equal(t, "Revoke the privileges of the database", (&MySQLRevokeInstruction{}).Comment())
	})

	t.Run("MySQLInsert", func(t *testing.T) {
		instruction := &MySQLInsertInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLUpdate", func(t *testing.T) {
		instruction := &MySQLUpdateInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLDelete", func(t *testing.T) {
		instruction := &MySQLDeleteInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLSetForeignKeyChecks", func(t *testing.T) {
		require.Equal(t, "Turn the enforcement of the foreign keys off for the creation order of the tables",
			(&MySQLSetForeignKeyChecksInstruction{}).Comment())
		require.Equal(t, "Turn the enforcement of the foreign keys on again",
			(&MySQLSetForeignKeyChecksInstruction{Enabled: true}).Comment())
	})
}
