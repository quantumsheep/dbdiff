package driverssqlite

import (
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("posts", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "user_id": int64(1), "title": "First Post"},
			{"id": int64(2), "user_id": int64(1), "title": "Second Post"},
		}, rows)
	})
}
