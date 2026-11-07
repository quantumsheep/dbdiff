package driverssqlite

import (
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteIndex(t *testing.T) {
	t.Run("CreateIndexWithDirectionAndCollation", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE items (a INTEGER, b TEXT);`)

		driver.ExecOnDesired(`
			CREATE TABLE items (a INTEGER, b TEXT);
			CREATE INDEX items_sorted ON items (a DESC, b COLLATE NOCASE ASC);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "items_sorted",
				TableName: "items",
				Keys:      []string{`"a" DESC`, `"b" COLLATE NOCASE`},
			},
		})
	})

	t.Run("ModifyIndexDirection", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a DESC);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "items_sorted"},
			&SQLiteCreateIndexInstruction{
				Name:      "items_sorted",
				TableName: "items",
				Keys:      []string{`"a" DESC`},
			},
		})
	})

	t.Run("IndexWithExplicitAscMatchesTheDefault", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a ASC);
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`},
			},
		})
	})

	t.Run("DropIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_users_name"},
		})
	})

	t.Run("ModifyIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name, email);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_users_name"},
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`, `"email"`},
			},
		})
	})

	t.Run("CreatePartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_active",
				TableName: "users",
				Keys:      []string{`"name"`},
				Where:     "active = 1",
			},
		})
	})

	t.Run("ModifyPartialIndexCondition", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 0;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_users_active"},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_active",
				TableName: "users",
				Keys:      []string{`"name"`},
				Where:     "active = 1",
			},
		})
	})

	t.Run("CreateExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (lower(name), id);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"lower(name)", `"id"`},
			},
		})
	})

	t.Run("ModifyExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (upper(name));
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (lower(name));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "idx_users_name"},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"lower(name)"},
			},
		})
	})

	t.Run("RecreateTableWithPartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;

			INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1), (2, 'Bob', 0);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("users",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_users_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name:    "name",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "active",
							Type:    "INTEGER",
							NotNull: true,
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "name", "active"},
					SelectExpressions: []string{`"id"`, `"name"`, `"active"`},
					FromTableName:     "users",
				},
				&driversshared.SQLDropTableInstruction{Name: "users"},
				&SQLiteAlterTableInstruction{
					Name:   "_users_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "users"},
				},
				&SQLiteCreateIndexInstruction{
					Name:      "idx_users_active",
					TableName: "users",
					Keys:      []string{`"name"`},
					Where:     "active = 1",
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "active": int64(1)},
			{"id": int64(2), "name": "Bob", "active": int64(0)},
		}, rows)
	})

	t.Run("RecreateTableWithNewIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("users",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_users_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name: "age",
							Type: "INTEGER",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "age"},
					SelectExpressions: []string{`"id"`, `"age"`},
					FromTableName:     "users",
				},
				&driversshared.SQLDropTableInstruction{Name: "users"},
				&SQLiteAlterTableInstruction{
					Name:   "_users_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "users"},
				},
				&SQLiteCreateIndexInstruction{
					Name:      "idx_users_age",
					TableName: "users",
					Keys:      []string{`"age"`},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30)},
		}, rows)
	})

	t.Run("RecreateTableWithRemovedIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("users",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_users_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name: "age",
							Type: "INTEGER",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "age"},
					SelectExpressions: []string{`"id"`, `"age"`},
					FromTableName:     "users",
				},
				&driversshared.SQLDropTableInstruction{Name: "users"},
				&SQLiteAlterTableInstruction{
					Name:   "_users_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "users"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30)},
		}, rows)
	})

	t.Run("IndexNameInOtherCaseIsTheSameIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE INDEX Users_Name ON users (name);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE INDEX users_name ON users (name);
		`)
		driver.RequireInstructions(nil)
	})
}
