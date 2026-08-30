package driversshared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInstructionComments(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		instruction := &SQLInsertInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("InsertSelect", func(t *testing.T) {
		instruction := &SQLInsertSelectInstruction{
			TableName:       "_users_temp",
			SourceTableName: "users",
		}

		require.Equal(t, `Change the rows of the table "_users_temp"`, instruction.Comment())
	})

	t.Run("Update", func(t *testing.T) {
		instruction := &SQLUpdateInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("Delete", func(t *testing.T) {
		instruction := &SQLDeleteInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("DropTable", func(t *testing.T) {
		instruction := &SQLDropTableInstruction{
			Name: "audit",
		}

		require.Equal(t, `Drop the table "audit"`, instruction.Comment())
	})

	t.Run("DropView", func(t *testing.T) {
		instruction := &SQLDropViewInstruction{
			Name: "old_users",
		}

		require.Equal(t, `Drop the view "old_users"`, instruction.Comment())
	})

	t.Run("DropIndex", func(t *testing.T) {
		instruction := &SQLDropIndexInstruction{
			Name: "audit_date",
		}

		require.Equal(t, `Drop the index "audit_date"`, instruction.Comment())
	})

	t.Run("Comment", func(t *testing.T) {
		instruction := &SQLCommentInstruction{
			Text: "the table holds no primary key",
		}

		require.Empty(t, instruction.Comment())
	})
}
