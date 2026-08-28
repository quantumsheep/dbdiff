package drivers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func commentInstruction(text string) Instruction {
	return &SQLCommentInstruction{
		Text: text,
	}
}

func TestInstructionComments(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		instruction := &SQLInsertInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresInsertOverriding", func(t *testing.T) {
		instruction := &PostgresInsertOverridingInstruction{
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

	t.Run("SQLiteAlterTable", func(t *testing.T) {
		instruction := &SQLiteAlterTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateTable", func(t *testing.T) {
		instruction := &SQLiteCreateTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Create the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateVirtualTable", func(t *testing.T) {
		instruction := &SQLiteCreateVirtualTableInstruction{
			Definition: `CREATE VIRTUAL TABLE "documents" USING fts5(title, body)`,
		}

		require.Equal(t, `Create the virtual table "documents"`, instruction.Comment())
	})

	t.Run("SQLitePragmaForeignKeys", func(t *testing.T) {
		require.Equal(t, "Turn the enforcement of the foreign keys off for the recreation of a table",
			(&SQLitePragmaForeignKeysInstruction{}).Comment())
		require.Equal(t, "Turn the enforcement of the foreign keys on again",
			(&SQLitePragmaForeignKeysInstruction{Enabled: true}).Comment())
	})

	t.Run("SQLiteCreateIndex", func(t *testing.T) {
		instruction := &SQLiteCreateIndexInstruction{
			Name:      "users_email",
			TableName: "users",
		}

		require.Equal(t, `Create the index "users_email" of the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateTrigger", func(t *testing.T) {
		instruction := &SQLiteCreateTriggerInstruction{
			Definition: `CREATE TRIGGER "log_insert" AFTER INSERT ON "users" BEGIN SELECT 1; END`,
		}

		require.Equal(t, `Create the trigger "log_insert" of the table "users"`, instruction.Comment())
	})

	t.Run("SQLiteCreateView", func(t *testing.T) {
		instruction := &SQLiteCreateViewInstruction{
			Definition: `CREATE VIEW "active_users" AS SELECT id FROM users`,
		}

		require.Equal(t, `Create the view "active_users"`, instruction.Comment())
	})

	t.Run("SQLiteDropTrigger", func(t *testing.T) {
		instruction := &SQLiteDropTriggerInstruction{
			Name: "log_delete",
		}

		require.Equal(t, `Drop the trigger "log_delete"`, instruction.Comment())
	})

	t.Run("PostgresAlterTable", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "posts",
		}

		require.Equal(t, `Modify the table "posts"`, instruction.Comment())
	})

	t.Run("PostgresCreateTable", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Create the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCreateTablePartition", func(t *testing.T) {
		instruction := &PostgresCreateTablePartitionInstruction{
			Name:       "measurements_2026",
			ParentName: "measurements",
		}

		require.Equal(t, `Create the table "measurements_2026"`, instruction.Comment())
	})

	t.Run("PostgresCreateIndex", func(t *testing.T) {
		instruction := &PostgresCreateIndexInstruction{
			Definition: "CREATE UNIQUE INDEX posts_slug ON posts USING btree (slug)",
		}

		require.Equal(t, `Create the index "posts_slug" of the table "posts"`, instruction.Comment())
	})

	t.Run("PostgresCreateTrigger", func(t *testing.T) {
		instruction := &PostgresCreateTriggerInstruction{
			Definition: "CREATE TRIGGER sync_updated_at BEFORE UPDATE ON scim FOR EACH ROW EXECUTE FUNCTION sync_updated_at()",
		}

		require.Equal(t, `Create the trigger "sync_updated_at" of the table "scim"`, instruction.Comment())
	})

	t.Run("PostgresDropTrigger", func(t *testing.T) {
		instruction := &PostgresDropTriggerInstruction{
			Name:      "sync_created_at",
			TableName: "scim",
		}

		require.Equal(t, `Drop the trigger "sync_created_at" of the table "scim"`, instruction.Comment())
	})

	t.Run("PostgresCreatePolicy", func(t *testing.T) {
		instruction := &PostgresCreatePolicyInstruction{
			Name:      "read_own_row",
			TableName: "users",
		}

		require.Equal(t, `Create the policy "read_own_row" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresDropPolicy", func(t *testing.T) {
		instruction := &PostgresDropPolicyInstruction{
			Name:      "read_every_row",
			TableName: "users",
		}

		require.Equal(t, `Drop the policy "read_every_row" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCreateRule", func(t *testing.T) {
		instruction := &PostgresCreateRuleInstruction{
			Definition: "CREATE RULE notify_me AS\n    ON UPDATE TO users DO\n NOTIFY users;",
		}

		require.Equal(t, `Create the rule "notify_me" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresDropRule", func(t *testing.T) {
		instruction := &PostgresDropRuleInstruction{
			Name:      "notify_them",
			TableName: "users",
		}

		require.Equal(t, `Drop the rule "notify_them" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnTable", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{
			Name: "users",
			Text: "the people of the site",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnColumn", func(t *testing.T) {
		instruction := &PostgresCommentOnColumnInstruction{
			TableName:  "users",
			ColumnName: "id",
			Text:       "the key",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCreateMaterializedView", func(t *testing.T) {
		instruction := &PostgresCreateMaterializedViewInstruction{
			Name:  "sales_of_the_month",
			Query: "SELECT 1",
		}

		require.Equal(t, `Create the materialized view "sales_of_the_month"`, instruction.Comment())
	})

	t.Run("PostgresDropMaterializedView", func(t *testing.T) {
		instruction := &PostgresDropMaterializedViewInstruction{
			Name: "sales_of_the_year",
		}

		require.Equal(t, `Drop the materialized view "sales_of_the_year"`, instruction.Comment())
	})

	t.Run("PostgresCreateView", func(t *testing.T) {
		instruction := &PostgresCreateViewInstruction{
			Name:  "recent_posts",
			Query: "SELECT id FROM posts",
		}

		require.Equal(t, `Create the view "recent_posts"`, instruction.Comment())
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

	t.Run("PostgresCreateExtension", func(t *testing.T) {
		instruction := &PostgresCreateExtensionInstruction{
			Name: "pgcrypto",
		}

		require.Equal(t, `Create the extension "pgcrypto"`, instruction.Comment())
	})

	t.Run("PostgresAlterExtension", func(t *testing.T) {
		instruction := &PostgresAlterExtensionInstruction{
			Name:       "postgis",
			NewVersion: "3.4.0",
		}

		require.Equal(t, `Modify the extension "postgis"`, instruction.Comment())
	})

	t.Run("PostgresDropExtension", func(t *testing.T) {
		instruction := &PostgresDropExtensionInstruction{
			Name: "hstore",
		}

		require.Equal(t, `Drop the extension "hstore"`, instruction.Comment())
	})

	t.Run("PostgresCreateOwnedSequence", func(t *testing.T) {
		instruction := &PostgresCreateOwnedSequenceInstruction{
			Name: "users_id_seq",
		}

		require.Equal(t, `Create the sequence "users_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresCreateSequence", func(t *testing.T) {
		instruction := &PostgresCreateSequenceInstruction{
			Name:     "users_id_seq",
			DataType: "bigint",
		}

		require.Equal(t, `Create the sequence "users_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresAlterSequence", func(t *testing.T) {
		instruction := &PostgresAlterSequenceInstruction{
			Name: "posts_id_seq",
		}

		require.Equal(t, `Modify the sequence "posts_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresDropSequence", func(t *testing.T) {
		instruction := &PostgresDropSequenceInstruction{
			Name: "audit_id_seq",
		}

		require.Equal(t, `Drop the sequence "audit_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresCreateStatistics", func(t *testing.T) {
		instruction := &PostgresCreateStatisticsInstruction{
			Definition: "CREATE STATISTICS users_city_state (ndistinct) ON city, state FROM users",
		}

		require.Equal(t, `Create the statistics object "users_city_state" of the table "users"`,
			instruction.Comment())
	})

	t.Run("PostgresDropStatistics", func(t *testing.T) {
		instruction := &PostgresDropStatisticsInstruction{
			Name: "users_city_zip",
		}

		require.Equal(t, `Drop the statistics object "users_city_zip"`, instruction.Comment())
	})

	t.Run("PostgresGrant", func(t *testing.T) {
		instruction := &PostgresGrantInstruction{
			Privileges: []string{"SELECT"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "reader",
		}

		require.Equal(t, `Change the privileges of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresRevoke", func(t *testing.T) {
		instruction := &PostgresRevokeInstruction{
			Privileges: []string{"UPDATE"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "reader",
		}

		require.Equal(t, `Change the privileges of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresSetOwner", func(t *testing.T) {
		instruction := &PostgresSetOwnerInstruction{
			ObjectType: "MATERIALIZED VIEW",
			ObjectName: "sales_of_the_month",
			Owner:      "application",
		}

		require.Equal(t, `Change the owner of the materialized view "sales_of_the_month"`,
			instruction.Comment())
	})

	t.Run("DefinitionWithNoName", func(t *testing.T) {
		instruction := &PostgresCreateFunctionInstruction{
			Definition: "DO $$ BEGIN END $$",
		}

		require.Equal(t, "Create the function", instruction.Comment())
	})

	t.Run("DefinitionWithAQuotedName", func(t *testing.T) {
		instruction := &SQLiteCreateViewInstruction{
			Definition: `CREATE VIEW "we""ird" AS SELECT 1`,
		}

		require.Equal(t, `Create the view "we""ird"`, instruction.Comment())
	})
}

func TestAnnotateInstructions(t *testing.T) {
	t.Run("NoInstruction", func(t *testing.T) {
		require.Empty(t, AnnotateInstructions(nil))
	})

	t.Run("OneCommentForEachObject", func(t *testing.T) {
		createTable := &SQLiteCreateTableInstruction{
			Name: "users",
		}
		dropTable := &SQLDropTableInstruction{
			Name: "audit",
		}

		require.Equal(t, []Instruction{
			commentInstruction(`Create the table "users"`),
			createTable,
			commentInstruction(`Drop the table "audit"`),
			dropTable,
		}, AnnotateInstructions([]Instruction{createTable, dropTable}))
	})

	t.Run("OneCommentForAGroupOfOneObject", func(t *testing.T) {
		dropColumn := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []AlterTableAction{
				&SQLDropColumnAction{
					ColumnName: "age",
				},
			},
		}
		dropConstraint := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []AlterTableAction{
				&PostgresDropConstraintAction{
					ConstraintName: "users_age_check",
				},
			},
		}

		require.Equal(t, []Instruction{
			commentInstruction(`Modify the table "users"`),
			dropColumn,
			dropConstraint,
		}, AnnotateInstructions([]Instruction{dropColumn, dropConstraint}))
	})

	t.Run("TableRecreation", func(t *testing.T) {
		createTemporaryTable := &SQLiteCreateTableInstruction{
			Name: "_users_temp",
		}
		copyRows := &SQLInsertSelectInstruction{
			TableName:         "_users_temp",
			ColumnNames:       []string{"id"},
			SelectExpressions: []string{`"id"`},
			SourceTableName:   "users",
		}
		dropTable := &SQLDropTableInstruction{
			Name: "users",
		}
		renameTable := &SQLiteAlterTableInstruction{
			Name: "_users_temp",
			Action: &SQLRenameTableAction{
				NewName: "users",
			},
		}
		createIndex := &SQLiteCreateIndexInstruction{
			Name:      "users_email",
			TableName: "users",
		}
		createTrigger := &SQLiteCreateTriggerInstruction{
			Definition: `CREATE TRIGGER "log_insert" AFTER INSERT ON "users" BEGIN SELECT 1; END`,
		}
		createOtherTable := &SQLiteCreateTableInstruction{
			Name: "posts",
		}

		require.Equal(t, []Instruction{
			commentInstruction(`Recreate the table "users"`),
			createTemporaryTable,
			copyRows,
			dropTable,
			renameTable,
			createIndex,
			createTrigger,
			commentInstruction(`Create the table "posts"`),
			createOtherTable,
		}, AnnotateInstructions([]Instruction{
			createTemporaryTable,
			copyRows,
			dropTable,
			renameTable,
			createIndex,
			createTrigger,
			createOtherTable,
		}))
	})

	t.Run("TemporaryTableWithoutARename", func(t *testing.T) {
		createTemporaryTable := &SQLiteCreateTableInstruction{
			Name: "_users_temp",
		}

		require.Equal(t, []Instruction{
			commentInstruction(`Create the table "_users_temp"`),
			createTemporaryTable,
		}, AnnotateInstructions([]Instruction{createTemporaryTable}))
	})

	t.Run("CommentOfTheDriver", func(t *testing.T) {
		note := commentInstruction(`The table "logs" holds no primary key`)
		insert := &SQLInsertInstruction{
			TableName: "users",
		}

		require.Equal(t, []Instruction{
			note,
			commentInstruction(`Change the rows of the table "users"`),
			insert,
		}, AnnotateInstructions([]Instruction{note, insert}))
	})
}
