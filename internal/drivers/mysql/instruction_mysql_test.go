package driversmysql

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLInstructions(t *testing.T) {
	t.Run("QuoteIdentifier", func(t *testing.T) {
		require.Equal(t, "`weird `` name`", QuoteIdentifier("weird ` name"))
	})

	t.Run("QuoteLiteral", func(t *testing.T) {
		require.Equal(t, `'it''s a\\b'`, QuoteLiteral(`it's a\b`))
	})

	t.Run("MySQLCreateTable", func(t *testing.T) {
		instruction := &MySQLCreateTableInstruction{
			Name: "users",
			Columns: []*MySQLColumn{
				{
					Name:          "id",
					Type:          "int",
					NotNull:       true,
					AutoIncrement: true,
				},
				{
					Name: "name",
					Type: "varchar(100)",
					Default: sql.NullString{
						String: "anonymous",
						Valid:  true,
					},
				},
			},
			PrimaryKey: []string{"id"},
			CheckConstraints: []*MySQLCheckConstraint{
				{
					Name:       "chk_users_id",
					Expression: "(`id` > 0)",
				},
			},
			ForeignKeys: []*MySQLForeignKey{
				{
					Name:              "fk_users_group",
					Columns:           []string{"group_id"},
					ReferencedTable:   "groups",
					ReferencedColumns: []string{"id"},
					OnUpdate:          "NO ACTION",
					OnDelete:          "CASCADE",
				},
			},
		}

		require.Equal(t, "CREATE TABLE `users` (\n"+
			"\t`id` int NOT NULL AUTO_INCREMENT,\n"+
			"\t`name` varchar(100) DEFAULT 'anonymous',\n"+
			"\tPRIMARY KEY (`id`),\n"+
			"\tCONSTRAINT `chk_users_id` CHECK (`id` > 0),\n"+
			"\tCONSTRAINT `fk_users_group` FOREIGN KEY (`group_id`) REFERENCES `groups` (`id`) ON DELETE CASCADE\n"+
			");", instruction.String())
	})

	t.Run("MySQLDropTable", func(t *testing.T) {
		instruction := &MySQLDropTableInstruction{
			Name: "users",
		}

		require.Equal(t, "DROP TABLE `users`;", instruction.String())
	})

	t.Run("MySQLAlterTableAddColumn", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLAddColumnAction{
				Column: &MySQLColumn{
					Name: "email",
					Type: "varchar(255)",
				},
			},
		}

		require.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255);", instruction.String())
	})

	t.Run("MySQLAlterTableModifyColumn", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLModifyColumnAction{
				Column: &MySQLColumn{
					Name:    "age",
					Type:    "bigint",
					NotNull: true,
				},
			},
		}

		require.Equal(t, "ALTER TABLE `users` MODIFY COLUMN `age` bigint NOT NULL;", instruction.String())
	})

	t.Run("MySQLAlterTableDropColumn", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLDropColumnAction{
				ColumnName: "email",
			},
		}

		require.Equal(t, "ALTER TABLE `users` DROP COLUMN `email`;", instruction.String())
	})

	t.Run("MySQLAlterTableRenameColumn", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLRenameColumnAction{
				ColumnName:    "name",
				NewColumnName: "full_name",
			},
		}

		require.Equal(t, "ALTER TABLE `users` RENAME COLUMN `name` TO `full_name`;", instruction.String())
	})

	t.Run("MySQLAlterTableAddPrimaryKey", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLAddPrimaryKeyAction{
				Columns: []string{"id", "tenant_id"},
			},
		}

		require.Equal(t, "ALTER TABLE `users` ADD PRIMARY KEY (`id`, `tenant_id`);", instruction.String())
	})

	t.Run("MySQLAlterTableDropPrimaryKey", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name:   "users",
			Action: &MySQLDropPrimaryKeyAction{},
		}

		require.Equal(t, "ALTER TABLE `users` DROP PRIMARY KEY;", instruction.String())
	})

	t.Run("MySQLAlterTableAddForeignKey", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "books",
			Action: &MySQLAddForeignKeyAction{
				ForeignKey: &MySQLForeignKey{
					Name:              "fk_books_author",
					Columns:           []string{"author_id"},
					ReferencedTable:   "authors",
					ReferencedColumns: []string{"id"},
					OnUpdate:          "CASCADE",
					OnDelete:          "SET NULL",
				},
			},
		}

		require.Equal(t, "ALTER TABLE `books` ADD CONSTRAINT `fk_books_author` "+
			"FOREIGN KEY (`author_id`) REFERENCES `authors` (`id`) "+
			"ON DELETE SET NULL ON UPDATE CASCADE;", instruction.String())
	})

	t.Run("MySQLAlterTableDropForeignKey", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "books",
			Action: &MySQLDropForeignKeyAction{
				Name: "fk_books_author",
			},
		}

		require.Equal(t, "ALTER TABLE `books` DROP FOREIGN KEY `fk_books_author`;", instruction.String())
	})

	t.Run("MySQLAlterTableAddCheckConstraint", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLAddCheckConstraintAction{
				CheckConstraint: &MySQLCheckConstraint{
					Name:       "chk_users_age",
					Expression: "`age` > 0",
				},
			},
		}

		require.Equal(t, "ALTER TABLE `users` ADD CONSTRAINT `chk_users_age` CHECK (`age` > 0);",
			instruction.String())
	})

	t.Run("MySQLAlterTableDropCheckConstraint", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "users",
			Action: &MySQLDropCheckConstraintAction{
				Name: "chk_users_age",
			},
		}

		require.Equal(t, "ALTER TABLE `users` DROP CONSTRAINT `chk_users_age`;", instruction.String())
	})

	t.Run("MySQLCreateIndex", func(t *testing.T) {
		instruction := &MySQLCreateIndexInstruction{
			Kind:      "UNIQUE",
			Name:      "idx_users_email",
			TableName: "users",
			Keys:      []string{"`email`", "`name`(10) DESC"},
		}

		require.Equal(t, "CREATE UNIQUE INDEX `idx_users_email` ON `users` (`email`, `name`(10) DESC);",
			instruction.String())
	})

	t.Run("MySQLDropIndex", func(t *testing.T) {
		instruction := &MySQLDropIndexInstruction{
			Name:      "idx_users_email",
			TableName: "users",
		}

		require.Equal(t, "DROP INDEX `idx_users_email` ON `users`;", instruction.String())
	})

	t.Run("MySQLColumnPosition", func(t *testing.T) {
		column := &MySQLColumn{
			Name: "name",
			Type: "text",
		}

		require.Equal(t, "ADD COLUMN `name` text AFTER `id`",
			(&MySQLAddColumnAction{
				Column: column,
				After:  "id",
			}).TableActionClause())
		require.Equal(t, "ADD COLUMN `name` text FIRST",
			(&MySQLAddColumnAction{
				Column: column,
				First:  true,
			}).TableActionClause())
		require.Equal(t, "ADD COLUMN `id` int NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST",
			(&MySQLAddColumnAction{
				Column: &MySQLColumn{
					Name:          "id",
					Type:          "int",
					NotNull:       true,
					AutoIncrement: true,
				},
				First:      true,
				PrimaryKey: true,
			}).TableActionClause())
		require.Equal(t, "MODIFY COLUMN `name` text AFTER `id`",
			(&MySQLModifyColumnAction{
				Column: column,
				After:  "id",
			}).TableActionClause())
		require.Equal(t, "MODIFY COLUMN `name` text FIRST",
			(&MySQLModifyColumnAction{
				Column: column,
				First:  true,
			}).TableActionClause())
	})

	t.Run("MySQLActionList", func(t *testing.T) {
		action := &MySQLActionList{
			Actions: []driversshared.AlterTableAction{
				&MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:          "uid",
						Type:          "bigint",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
				&MySQLAddPrimaryKeyAction{
					Columns: []string{"a", "uid"},
				},
				&MySQLAddIndexAction{
					Name: "k",
					Keys: []string{"`uid`"},
				},
				&MySQLAddIndexAction{
					Kind: "UNIQUE",
					Name: "u",
					Keys: []string{"`uid`", "`a`"},
				},
			},
		}

		require.Equal(t, "ADD COLUMN `uid` bigint NOT NULL AUTO_INCREMENT, ADD PRIMARY KEY (`a`, `uid`), "+
			"ADD INDEX `k` (`uid`), ADD UNIQUE INDEX `u` (`uid`, `a`)", action.TableActionClause())
	})

	t.Run("MySQLCreateView", func(t *testing.T) {
		instruction := &MySQLCreateViewInstruction{
			Name:       "adult_users",
			Definition: "select `id` from `users`",
		}

		require.Equal(t, "CREATE VIEW `adult_users` AS select `id` from `users`;", instruction.String())
	})

	t.Run("MySQLCreateOrReplaceView", func(t *testing.T) {
		instruction := &MySQLCreateViewInstruction{
			Name:       "adult_users",
			Definition: "select `id` from `users`",
			OrReplace:  true,
		}

		require.Equal(t, "CREATE OR REPLACE VIEW `adult_users` AS select `id` from `users`;",
			instruction.String())
	})

	t.Run("MySQLCreateViewWithAttributes", func(t *testing.T) {
		instruction := &MySQLCreateViewInstruction{
			Name:         "adult_users",
			Definition:   "select `id` from `users`",
			OrReplace:    true,
			SecurityType: "INVOKER",
			CheckOption:  "CASCADED",
		}

		require.Equal(t, "CREATE OR REPLACE SQL SECURITY INVOKER VIEW `adult_users` AS select `id` from `users` "+
			"WITH CASCADED CHECK OPTION;", instruction.String())
	})

	t.Run("MySQLDropView", func(t *testing.T) {
		instruction := &MySQLDropViewInstruction{
			Name: "adult_users",
		}

		require.Equal(t, "DROP VIEW `adult_users`;", instruction.String())
	})

	t.Run("MySQLCreateTableWithOptionsAndPartition", func(t *testing.T) {
		instruction := &MySQLCreateTableInstruction{
			Name: "logs",
			Columns: []*MySQLColumn{
				{
					Name:    "id",
					Type:    "int",
					NotNull: true,
				},
			},
			PrimaryKey: []string{"id"},
			Engine:     "MyISAM",
			Collation:  "utf8mb4_bin",
			Partition:  "PARTITION BY HASH (`id`)\nPARTITIONS 4",
		}

		require.Equal(t, "CREATE TABLE `logs` (\n"+
			"\t`id` int NOT NULL,\n"+
			"\tPRIMARY KEY (`id`)\n"+
			") ENGINE = MyISAM DEFAULT COLLATE = utf8mb4_bin\n"+
			"PARTITION BY HASH (`id`)\nPARTITIONS 4;", instruction.String())
	})

	t.Run("MySQLAlterTableEngine", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "logs",
			Action: &MySQLEngineAction{
				Engine: "InnoDB",
			},
		}

		require.Equal(t, "ALTER TABLE `logs` ENGINE = InnoDB;", instruction.String())
	})

	t.Run("MySQLAlterTableConvertToCharacterSet", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "logs",
			Action: &MySQLConvertToCharacterSetAction{
				CharacterSet: "utf8mb4",
				Collation:    "utf8mb4_bin",
			},
		}

		require.Equal(t, "ALTER TABLE `logs` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;",
			instruction.String())
	})

	t.Run("MySQLAlterTablePartition", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name: "logs",
			Action: &MySQLPartitionAction{
				Clause: "PARTITION BY HASH (`id`)\nPARTITIONS 4",
			},
		}

		require.Equal(t, "ALTER TABLE `logs` PARTITION BY HASH (`id`)\nPARTITIONS 4;", instruction.String())
	})

	t.Run("MySQLAlterTableRemovePartitioning", func(t *testing.T) {
		instruction := &MySQLAlterTableInstruction{
			Name:   "logs",
			Action: &MySQLRemovePartitioningAction{},
		}

		require.Equal(t, "ALTER TABLE `logs` REMOVE PARTITIONING;", instruction.String())
	})

	t.Run("MySQLCreateTrigger", func(t *testing.T) {
		instruction := &MySQLCreateTriggerInstruction{
			Name:      "trg_users_insert",
			Timing:    "BEFORE",
			Event:     "INSERT",
			TableName: "users",
			Statement: "SET NEW.note = 'new'",
		}

		require.Equal(t,
			"CREATE TRIGGER `trg_users_insert` BEFORE INSERT ON `users` FOR EACH ROW SET NEW.note = 'new';",
			instruction.String())
	})

	t.Run("MySQLDropTrigger", func(t *testing.T) {
		instruction := &MySQLDropTriggerInstruction{
			Name: "trg_users_insert",
		}

		require.Equal(t, "DROP TRIGGER `trg_users_insert`;", instruction.String())
	})

	t.Run("MySQLCreateRoutine", func(t *testing.T) {
		instruction := &MySQLCreateRoutineInstruction{
			Type:       "FUNCTION",
			Name:       "double_it",
			Definition: "CREATE FUNCTION `double_it`(x int) RETURNS int\nRETURN x * 2",
		}

		require.Equal(t, "CREATE FUNCTION `double_it`(x int) RETURNS int\nRETURN x * 2;",
			instruction.String())
	})

	t.Run("MySQLDropRoutine", func(t *testing.T) {
		instruction := &MySQLDropRoutineInstruction{
			Type: "PROCEDURE",
			Name: "prune_users",
		}

		require.Equal(t, "DROP PROCEDURE `prune_users`;", instruction.String())
	})

	t.Run("MySQLCreateEvent", func(t *testing.T) {
		instruction := &MySQLCreateEventInstruction{
			Name:       "nightly",
			Definition: "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY DO SET @cleaned = 1",
		}

		require.Equal(t, "CREATE EVENT `nightly` ON SCHEDULE EVERY 1 DAY DO SET @cleaned = 1;",
			instruction.String())
	})

	t.Run("MySQLDropEvent", func(t *testing.T) {
		instruction := &MySQLDropEventInstruction{
			Name: "nightly",
		}

		require.Equal(t, "DROP EVENT `nightly`;", instruction.String())
	})

	t.Run("MySQLCreateSequence", func(t *testing.T) {
		instruction := &MySQLCreateSequenceInstruction{
			Name:      "order_numbers",
			Start:     1000,
			Minimum:   1,
			Maximum:   9223372036854775806,
			Increment: 10,
			Cache:     1000,
			Cycle:     true,
		}

		require.Equal(t, "CREATE SEQUENCE `order_numbers` START WITH 1000 MINVALUE 1 "+
			"MAXVALUE 9223372036854775806 INCREMENT BY 10 CACHE 1000 CYCLE;", instruction.String())
	})

	t.Run("MySQLAlterSequenceOneClause", func(t *testing.T) {
		instruction := &MySQLAlterSequenceInstruction{
			Name: "order_numbers",
			Increment: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
		}

		require.Equal(t, "ALTER SEQUENCE `order_numbers` INCREMENT BY 5;", instruction.String())
	})

	t.Run("MySQLAlterSequenceEveryClauseInOrder", func(t *testing.T) {
		instruction := &MySQLAlterSequenceInstruction{
			Name: "order_numbers",
			Start: sql.NullInt64{
				Int64: 7,
				Valid: true,
			},
			Minimum: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
			Maximum: sql.NullInt64{
				Int64: 500,
				Valid: true,
			},
			Increment: sql.NullInt64{
				Int64: 2,
				Valid: true,
			},
			Cache: sql.NullInt64{
				Int64: 20,
				Valid: true,
			},
			Cycle: sql.NullBool{
				Bool:  false,
				Valid: true,
			},
		}

		require.Equal(t, "ALTER SEQUENCE `order_numbers` START WITH 7 MINVALUE 5 MAXVALUE 500 "+
			"INCREMENT BY 2 CACHE 20 NOCYCLE;", instruction.String())
	})

	t.Run("MySQLDropSequence", func(t *testing.T) {
		instruction := &MySQLDropSequenceInstruction{
			Name: "order_numbers",
		}

		require.Equal(t, "DROP SEQUENCE `order_numbers`;", instruction.String())
	})

	t.Run("MySQLGrant", func(t *testing.T) {
		instruction := &MySQLGrantInstruction{
			Privileges:      []string{"SELECT", "UPDATE"},
			TableName:       "users",
			Grantee:         "'reader'@'%'",
			WithGrantOption: true,
		}

		require.Equal(t, "GRANT SELECT, UPDATE ON `users` TO 'reader'@'%' WITH GRANT OPTION;",
			instruction.String())
	})

	t.Run("MySQLGrantOnColumns", func(t *testing.T) {
		instruction := &MySQLGrantInstruction{
			Privileges: []string{"SELECT"},
			Columns:    []string{"id", "name"},
			TableName:  "users",
			Grantee:    "'reader'@'%'",
		}

		require.Equal(t, "GRANT SELECT (`id`, `name`) ON `users` TO 'reader'@'%';", instruction.String())
	})

	t.Run("MySQLGrantOnTheDatabase", func(t *testing.T) {
		instruction := &MySQLGrantInstruction{
			Privileges: []string{"SELECT"},
			Grantee:    "'reader'@'%'",
		}

		require.Equal(t, "GRANT SELECT ON * TO 'reader'@'%';", instruction.String())
	})

	t.Run("MySQLRevoke", func(t *testing.T) {
		instruction := &MySQLRevokeInstruction{
			Privileges: []string{"DELETE", "GRANT OPTION"},
			TableName:  "users",
			Grantee:    "'reader'@'%'",
		}

		require.Equal(t, "REVOKE DELETE, GRANT OPTION ON `users` FROM 'reader'@'%';",
			instruction.String())
	})

	t.Run("MySQLRevokeOnColumns", func(t *testing.T) {
		instruction := &MySQLRevokeInstruction{
			Privileges: []string{"SELECT"},
			Columns:    []string{"id", "name"},
			TableName:  "users",
			Grantee:    "'reader'@'%'",
		}

		require.Equal(t, "REVOKE SELECT (`id`, `name`) ON `users` FROM 'reader'@'%';", instruction.String())
	})

	t.Run("MySQLInsert", func(t *testing.T) {
		instruction := &MySQLInsertInstruction{
			TableName:   "users",
			ColumnNames: []string{"id", "name"},
			Expressions: []string{"1", "'Alice'"},
		}

		require.Equal(t, "INSERT INTO `users` (`id`, `name`) VALUES (1, 'Alice');",
			instruction.String())
	})

	t.Run("MySQLUpdate", func(t *testing.T) {
		instruction := &MySQLUpdateInstruction{
			TableName: "users",
			SetClauses: []*MySQLSetClause{
				{
					ColumnName: "name",
					Expression: "'Alice'",
				},
			},
			Condition: driversshared.RowKeyCondition[Backtick]([]string{"id", "tenant"},
				map[string]string{"id": "1", "tenant": "NULL"}),
		}

		require.Equal(t, "UPDATE `users` SET `name` = 'Alice' WHERE `id` = 1 AND `tenant` IS NULL;",
			instruction.String())
	})

	t.Run("MySQLDelete", func(t *testing.T) {
		instruction := &MySQLDeleteInstruction{
			TableName: "users",
			Condition: driversshared.RowKeyCondition[Backtick]([]string{"id"}, map[string]string{"id": "2"}),
		}

		require.Equal(t, "DELETE FROM `users` WHERE `id` = 2;", instruction.String())
	})

	t.Run("MySQLSetForeignKeyChecks", func(t *testing.T) {
		require.Equal(t, "SET FOREIGN_KEY_CHECKS = 0;",
			(&MySQLSetForeignKeyChecksInstruction{}).String())
		require.Equal(t, "SET FOREIGN_KEY_CHECKS = 1;",
			(&MySQLSetForeignKeyChecksInstruction{Enabled: true}).String())
	})

	t.Run("ColumnDefinitionWithEveryClause", func(t *testing.T) {
		column := &MySQLColumn{
			Name:    "title",
			Type:    "varchar(100)",
			NotNull: true,
			Default: sql.NullString{
				String: "upper('x')",
				Valid:  true,
			},
			DefaultIsExpression: true,
			Collation:           "utf8mb4_bin",
			Comment:             "the title",
		}

		require.Equal(t,
			"`title` varchar(100) COLLATE utf8mb4_bin NOT NULL DEFAULT (upper('x')) COMMENT 'the title'",
			column.Definition())
	})

	t.Run("ColumnDefinitionOfAGeneratedColumn", func(t *testing.T) {
		column := &MySQLColumn{
			Name:                "gross",
			Type:                "int",
			GeneratedExpression: "`net` * 2",
			GeneratedStored:     true,
		}

		require.Equal(t, "`gross` int GENERATED ALWAYS AS (`net` * 2) STORED", column.Definition())
	})

	t.Run("ColumnDefinitionWithACurrentTimestampDefault", func(t *testing.T) {
		column := &MySQLColumn{
			Name: "updated_at",
			Type: "timestamp",
			Default: sql.NullString{
				String: "CURRENT_TIMESTAMP",
				Valid:  true,
			},
			DefaultIsExpression: true,
			OnUpdate:            "CURRENT_TIMESTAMP",
		}

		require.Equal(t, "`updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
			column.Definition())
	})
}

func TestMySQLInstructionComments(t *testing.T) {
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

	t.Run("MySQLAlterSequence", func(t *testing.T) {
		instruction := &MySQLAlterSequenceInstruction{
			Name: "order_numbers",
		}

		require.Equal(t, `Modify the sequence "order_numbers"`, instruction.Comment())
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

	t.Run("MySQLGrantOnColumns", func(t *testing.T) {
		instruction := &MySQLGrantInstruction{
			TableName: "users",
			Columns:   []string{"id", "name"},
		}

		require.Equal(t, `Grant the privileges of the table "users"`, instruction.Comment())
	})

	t.Run("MySQLRevoke", func(t *testing.T) {
		instruction := &MySQLRevokeInstruction{
			TableName: "users",
		}

		require.Equal(t, `Revoke the privileges of the table "users"`, instruction.Comment())
		require.Equal(t, "Revoke the privileges of the database", (&MySQLRevokeInstruction{}).Comment())
	})

	t.Run("MySQLRevokeOnColumns", func(t *testing.T) {
		instruction := &MySQLRevokeInstruction{
			TableName: "users",
			Columns:   []string{"id", "name"},
		}

		require.Equal(t, `Revoke the privileges of the table "users"`, instruction.Comment())
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
		require.Equal(t, "Turn the enforcement of the foreign keys off for the order of the instructions",
			(&MySQLSetForeignKeyChecksInstruction{}).Comment())
		require.Equal(t, "Turn the enforcement of the foreign keys on again",
			(&MySQLSetForeignKeyChecksInstruction{Enabled: true}).Comment())
	})
}
