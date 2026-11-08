package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLForeignKey(t *testing.T) {
	t.Run("CreateTablesWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLCreateTableInstruction{
				Name: "authors",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLCreateTableInstruction{
				Name: "books",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "author_id",
						Type: "int",
					},
				},
				PrimaryKey: []string{"id"},
				ForeignKeys: []*MySQLForeignKey{
					{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "CASCADE",
					},
				},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("AddForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
		})
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE publishers (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));
			ALTER TABLE books ADD CONSTRAINT fk_books_publisher
				FOREIGN KEY (publisher_id) REFERENCES publishers (id);
		`)
		driver.ExecOnCurrent("CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));")

		// The new table holds no row before the data diff, so the enforcement of the
		// foreign keys goes off.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLCreateTableInstruction{
				Name: "publishers",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_publisher",
						Columns:           []string{"publisher_id"},
						ReferencedTable:   "publishers",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("RemoveForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_books_author",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "fk_books_author",
				TableName: "books",
			},
		})
	})

	t.Run("RestrictEqualsNoAction", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
					ON DELETE RESTRICT ON UPDATE RESTRICT
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("ChangeTypeOfAForeignKeyColumnPair", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id bigint NOT NULL, PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id bigint,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
			INSERT INTO parents VALUES (1);
			INSERT INTO children VALUES (10, 1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_children_parent",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "parent_id",
						Type: "bigint",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "parents",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "bigint",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_children_parent",
						Columns:           []string{"parent_id"},
						ReferencedTable:   "parents",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(10), "parent_id": int64(1)},
		}, driver.FetchAllFromCurrent("children", "ORDER BY id"))
	})

	t.Run("DropAReferencedIndexAndItsForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE a_codes (id int NOT NULL, code int, PRIMARY KEY (id));
			CREATE TABLE b_users (id int NOT NULL, code int, PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE a_codes (id int NOT NULL, code int, PRIMARY KEY (id), UNIQUE KEY uc (code));
			CREATE TABLE b_users (
				id int NOT NULL,
				code int,
				PRIMARY KEY (id),
				CONSTRAINT fk_users_code FOREIGN KEY (code) REFERENCES a_codes (code)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "b_users",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_users_code",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "uc",
				TableName: "a_codes",
			},
			&MySQLDropIndexInstruction{
				Name:      "fk_users_code",
				TableName: "b_users",
			},
		})
	})

	t.Run("RecreateAReferencedGeneratedColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (
				id int NOT NULL,
				code int GENERATED ALWAYS AS (id * 10) STORED,
				PRIMARY KEY (id),
				UNIQUE KEY uc (code)
			);
			CREATE TABLE children (
				id int NOT NULL,
				parent_code int,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_code) REFERENCES parents (code)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE parents (id int NOT NULL, code int, PRIMARY KEY (id), UNIQUE KEY uc (code));
			CREATE TABLE children (
				id int NOT NULL,
				parent_code int,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_code) REFERENCES parents (code)
			);
			INSERT INTO parents VALUES (1, 10);
			INSERT INTO children VALUES (5, 10);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_children_parent",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "uc",
				TableName: "parents",
			},
			&MySQLAlterTableInstruction{
				Name: "parents",
				Action: &MySQLDropColumnAction{
					ColumnName: "code",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "parents",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "code",
						Type:                "int",
						GeneratedExpression: "(`id` * 10)",
						GeneratedStored:     true,
					},
				},
			},
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "uc",
				TableName: "parents",
				Keys:      []string{"`code`"},
			},
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_children_parent",
						Columns:           []string{"parent_code"},
						ReferencedTable:   "parents",
						ReferencedColumns: []string{"code"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "code": int64(10)},
		}, driver.FetchAllFromCurrent("parents", "ORDER BY id"))
		require.Equal(t, []map[string]any{
			{"id": int64(5), "parent_code": int64(10)},
		}, driver.FetchAllFromCurrent("children", "ORDER BY id"))
	})

	t.Run("CommentOnAReferencedColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id int NOT NULL COMMENT 'the parent', PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "parents",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
						Comment: "the parent",
					},
				},
			},
		})
	})

	t.Run("RenameBothColumnsOfAForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.GuessRenames = true

		driver.ExecOnDesired(`
			CREATE TABLE parents (parent_key int NOT NULL, PRIMARY KEY (parent_key));
			CREATE TABLE children (
				id int NOT NULL,
				parent_key int,
				PRIMARY KEY (id),
				KEY fk_children_parent (parent_key),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_key) REFERENCES parents (parent_key)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id int,
				PRIMARY KEY (id),
				KEY fk_children_parent (parent_id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
			INSERT INTO parents VALUES (1);
			INSERT INTO children VALUES (10, 1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "parent_id",
					NewColumnName: "parent_key",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "parents",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "id",
					NewColumnName: "parent_key",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(10), "parent_key": int64(1)},
		}, driver.FetchAllFromCurrent("children", "ORDER BY id"))
	})

	t.Run("MoveConstraintNamesFromADroppedTableToANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE new_books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id),
				CONSTRAINT books_id_positive CHECK (id > 0)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE old_books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id),
				CONSTRAINT books_id_positive CHECK (id > 0)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLAlterTableInstruction{
				Name: "old_books",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_books_author",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "old_books",
				Action: &MySQLDropCheckConstraintAction{
					Name: "books_id_positive",
				},
			},
			&MySQLCreateTableInstruction{
				Name: "new_books",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "author_id",
						Type: "int",
					},
				},
				PrimaryKey: []string{"id"},
				CheckConstraints: []*MySQLCheckConstraint{
					{
						Name:       "books_id_positive",
						Expression: "(`id` > 0)",
					},
				},
				ForeignKeys: []*MySQLForeignKey{
					{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
			&MySQLDropTableInstruction{
				Name: "old_books",
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("DropTableWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLDropTableInstruction{
				Name: "books",
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})
}

func TestMariaDBForeignKey(t *testing.T) {
	t.Run("ChangeTypeOfAForeignKeyColumnPair", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id bigint NOT NULL, PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id bigint,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE parents (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE children (
				id int NOT NULL,
				parent_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES parents (id)
			);
			INSERT INTO parents VALUES (1);
			INSERT INTO children VALUES (10, 1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_children_parent",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "parent_id",
						Type: "bigint(20)",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "parents",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "bigint(20)",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "children",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_children_parent",
						Columns:           []string{"parent_id"},
						ReferencedTable:   "parents",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "RESTRICT",
						OnDelete:          "RESTRICT",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(10), "parent_id": int64(1)},
		}, driver.FetchAllFromCurrent("children", "ORDER BY id"))
	})

	t.Run("NoActionEqualsRestrict", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
					ON DELETE NO ACTION ON UPDATE NO ACTION
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("ForeignKey", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "RESTRICT",
						OnDelete:          "RESTRICT",
					},
				},
			},
		})
	})
}
