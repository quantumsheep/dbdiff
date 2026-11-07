package driverssqlite

import (
	"database/sql"
	"path/filepath"
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteForeignKey(t *testing.T) {
	t.Run("CreateTableWithADeferredForeignKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id) ON DELETE CASCADE
					DEFERRABLE INITIALLY DEFERRED
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "parents",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
				},
			},
			&SQLiteCreateTableInstruction{
				Name: "children",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name: "parent",
						Type: "INTEGER",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{
					{
						Table:      "parents",
						From:       []string{"parent"},
						To:         []string{"id"},
						OnUpdate:   "NO ACTION",
						OnDelete:   "CASCADE",
						Deferrable: "DEFERRABLE INITIALLY DEFERRED",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithAForeignKeyWithoutParentColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "parents",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
				},
			},
			&SQLiteCreateTableInstruction{
				Name: "children",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name: "parent",
						Type: "INTEGER",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{
					{
						Table:    "parents",
						From:     []string{"parent"},
						To:       []string{},
						OnUpdate: "NO ACTION",
						OnDelete: "NO ACTION",
					},
				},
			},
		})
	})

	t.Run("ModifyForeignKeyDeferrable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
			);
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO children (id, parent) VALUES (1, 1);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("children",
				&SQLiteCreateTableInstruction{
					Name: "_children_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name: "parent",
							Type: "INTEGER",
						},
					},
					ForeignKeys: []*SQLiteForeignKey{
						{
							Table:      "parents",
							From:       []string{"parent"},
							To:         []string{"id"},
							OnUpdate:   "NO ACTION",
							OnDelete:   "NO ACTION",
							Deferrable: "DEFERRABLE INITIALLY DEFERRED",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_children_temp",
					ColumnNames:       []string{"id", "parent"},
					SelectExpressions: []string{`"id"`, `"parent"`},
					FromTableName:     "children",
				},
				&driversshared.SQLDropTableInstruction{Name: "children"},
				&SQLiteAlterTableInstruction{
					Name:   "_children_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "children"},
				},
			),
			recreationEpilogue("children"),
		))

		rows := driver.FetchAllFromCurrent("children", "")
		require.Equal(t, []map[string]any{
			{"id": int64(1), "parent": int64(1)},
		}, rows)
	})

	t.Run("RecreateTableWithForeignKeysOnTheSameColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (owner INTEGER);
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO items (owner) VALUES (1);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (
				owner INTEGER,
				CONSTRAINT items_first FOREIGN KEY (owner) REFERENCES parents(id),
				CONSTRAINT items_second FOREIGN KEY (owner) REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED,
				CONSTRAINT items_third FOREIGN KEY (owner) REFERENCES parents(id) ON DELETE CASCADE
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("items",
				&SQLiteCreateTableInstruction{
					Name: "_items_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "owner",
							Type: "INTEGER",
						},
					},
					ForeignKeys: []*SQLiteForeignKey{
						{
							Name:     "items_third",
							Table:    "parents",
							From:     []string{"owner"},
							To:       []string{"id"},
							OnUpdate: "NO ACTION",
							OnDelete: "CASCADE",
						},
						{
							Name:     "items_first",
							Table:    "parents",
							From:     []string{"owner"},
							To:       []string{"id"},
							OnUpdate: "NO ACTION",
							OnDelete: "NO ACTION",
						},
						{
							Name:       "items_second",
							Table:      "parents",
							From:       []string{"owner"},
							To:         []string{"id"},
							OnUpdate:   "NO ACTION",
							OnDelete:   "NO ACTION",
							Deferrable: "DEFERRABLE INITIALLY DEFERRED",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_items_temp",
					ColumnNames:       []string{"owner"},
					SelectExpressions: []string{`"owner"`},
					FromTableName:     "items",
				},
				&driversshared.SQLDropTableInstruction{Name: "items"},
				&SQLiteAlterTableInstruction{
					Name:   "_items_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "items"},
				},
			),
			recreationEpilogue("items"),
		))

		rows := driver.FetchAllFromCurrent("items", "")
		require.Equal(t, []map[string]any{
			{"owner": int64(1)},
		}, rows)

		driver.RequireInstructions(nil)
	})

	t.Run("DeferrableOfATableForeignKeyOfOneColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER,
				FOREIGN KEY ("parent") REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("ForeignKeys", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice');
			INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'First Post'), (2, 1, 'Second Post');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT,
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("posts",
				&SQLiteCreateTableInstruction{
					Name: "_posts_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name: "user_id",
							Type: "INTEGER",
						},
						{
							Name: "title",
							Type: "TEXT",
						},
					},
					ForeignKeys: []*SQLiteForeignKey{
						{
							Table:    "users",
							From:     []string{"user_id"},
							To:       []string{"id"},
							OnUpdate: "NO ACTION",
							OnDelete: "CASCADE",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_posts_temp",
					ColumnNames:       []string{"id", "user_id", "title"},
					SelectExpressions: []string{`"id"`, `"user_id"`, `"title"`},
					FromTableName:     "posts",
				},
				&driversshared.SQLDropTableInstruction{Name: "posts"},
				&SQLiteAlterTableInstruction{
					Name:   "_posts_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "posts"},
				},
			),
			recreationEpilogue("posts"),
		))

		rows := driver.FetchAllFromCurrent("posts", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "user_id": int64(1), "title": "First Post"},
			{"id": int64(2), "user_id": int64(1), "title": "Second Post"},
		}, rows)
	})

	t.Run("RecreationWithABrokenForeignKeyChangesNothing", func(t *testing.T) {
		currentPath := filepath.Join(t.TempDir(), "current.sqlite")
		driver := NewTestSQLiteDriverWithPaths(t, filepath.Join(t.TempDir(), "desired.sqlite"), currentPath)

		driver.ExecOnCurrent(`
			CREATE TABLE p (id INTEGER PRIMARY KEY);
			CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER);

			INSERT INTO c (id, pid) VALUES (1, 99);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE p (id INTEGER PRIMARY KEY);
			CREATE TABLE c (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p (id));
		`)

		expected := slices.Concat(
			recreationPrologue(),
			recreated("c",
				&SQLiteCreateTableInstruction{
					Name: "_c_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name: "pid",
							Type: "INTEGER",
						},
					},
					ForeignKeys: []*SQLiteForeignKey{
						{
							Table:    "p",
							From:     []string{"pid"},
							To:       []string{"id"},
							OnUpdate: "NO ACTION",
							OnDelete: "NO ACTION",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_c_temp",
					ColumnNames:       []string{"id", "pid"},
					SelectExpressions: []string{`"id"`, `"pid"`},
					FromTableName:     "c",
				},
				&driversshared.SQLDropTableInstruction{Name: "c"},
				&SQLiteAlterTableInstruction{
					Name:   "_c_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "c"},
				},
			),
			recreationEpilogue("c"),
		)
		script := driver.RequireInstructions(expected)

		// The sqlite3 shell with -bail stops at the error and closes the connection, and the
		// close rolls the open savepoint back.
		scriptConnection, err := sql.Open("sqlite", currentPath)
		require.NoError(t, err)

		_, err = scriptConnection.ExecContext(t.Context(), script)
		require.EqualError(t, err, "constraint failed: CHECK constraint failed: a_foreign_key_is_broken (275)")
		require.NoError(t, scriptConnection.Close())

		driver.RequireInstructions(expected)

		rows := driver.FetchAllFromCurrent("c", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "pid": int64(99)},
		}, rows)
	})

	t.Run("OldBrokenForeignKeyOfAnotherTableDoesNotStopARecreation", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE p (id INTEGER PRIMARY KEY);
			CREATE TABLE orphans (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p (id));
			CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT);

			INSERT INTO orphans (id, pid) VALUES (1, 99);
			INSERT INTO t (id, c) VALUES (1, 'x');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE p (id INTEGER PRIMARY KEY);
			CREATE TABLE orphans (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES p (id));
			CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT NOT NULL DEFAULT '');
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("t",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_t_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name:    "c",
							Type:    "TEXT",
							NotNull: true,
							Default: sql.NullString{
								String: "''",
								Valid:  true,
							},
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_t_temp",
					ColumnNames:       []string{"id", "c"},
					SelectExpressions: []string{`"id"`, `"c"`},
					FromTableName:     "t",
				},
				&driversshared.SQLDropTableInstruction{Name: "t"},
				&SQLiteAlterTableInstruction{
					Name:   "_t_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "t"},
				},
			),
			recreationEpilogue("t"),
		))

		require.Equal(t, []map[string]any{
			{"id": int64(1), "c": "x"},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
		require.Equal(t, []map[string]any{
			{"id": int64(1), "pid": int64(99)},
		}, driver.FetchAllFromCurrent("orphans", "ORDER BY id"))
	})
}
