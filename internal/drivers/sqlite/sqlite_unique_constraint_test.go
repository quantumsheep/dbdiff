package driverssqlite

import (
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteUniqueConstraint(t *testing.T) {
	t.Run("CreateTableWithAUniqueConstraintConflict", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`CREATE TABLE items (code INTEGER, UNIQUE (code) ON CONFLICT REPLACE);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "items",
				Columns: []*SQLiteColumn{
					{
						Name:           "code",
						Type:           "INTEGER",
						Unique:         true,
						UniqueConflict: "REPLACE",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithATableConflictClause", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE u (a INTEGER, b INTEGER, UNIQUE (a, b) ON CONFLICT FAIL);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "u",
				Columns: []*SQLiteColumn{
					{
						Name: "a",
						Type: "INTEGER",
					},
					{
						Name: "b",
						Type: "INTEGER",
					},
				},
				UniqueConstraints: []*SQLiteUniqueConstraint{
					{
						Columns:  []string{"a", "b"},
						Conflict: "FAIL",
					},
				},
			},
		})
	})

	t.Run("ImplicitUniqueIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
			CREATE INDEX idx_users_name ON users (name);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`},
			},
		})
	})

	t.Run("RecreateTableWithUniqueColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age TEXT
			);

			INSERT INTO users (id, email, age) VALUES (1, 'alice@example.com', '30'), (2, 'bob@example.com', '25');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age INTEGER
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
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
							Name:   "email",
							Type:   "TEXT",
							Unique: true,
						},
						{
							Name: "age",
							Type: "INTEGER",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "email", "age"},
					SelectExpressions: []string{`"id"`, `"email"`, `"age"`},
					FromTableName:     "users",
				},
				&driversshared.SQLDropTableInstruction{Name: "users"},
				&SQLiteAlterTableInstruction{
					Name:   "_users_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "users"},
				},
			),
			recreationEpilogue("users"),
		))

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "alice@example.com", "age": int64(30)},
			{"id": int64(2), "email": "bob@example.com", "age": int64(25)},
		}, rows)

		_, err := driver.currentConnection.ExecContext(t.Context(), `INSERT INTO users (id, email, age) VALUES (3, 'alice@example.com', 40);`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: users.email")
	})

	t.Run("CreateTableWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "members",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:    "team",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "name",
						Type:    "TEXT",
						NotNull: true,
					},
				},
				UniqueConstraints: []*SQLiteUniqueConstraint{
					{Columns: []string{"team", "name"}},
				},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice');`)

		_, err := driver.currentConnection.ExecContext(t.Context(), `INSERT INTO members (id, team, name) VALUES (2, 'red', 'Alice');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: members.team, members.name")
	})

	t.Run("CreateTableWithAUniqueConstraintOnAQuotedNameWithAComma", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE pairs (
				"a,b" TEXT,
				c TEXT,
				CONSTRAINT pairs_unique UNIQUE ("a,b" COLLATE NOCASE, c)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "pairs",
				Columns: []*SQLiteColumn{
					{
						Name: "a,b",
						Type: "TEXT",
					},
					{
						Name: "c",
						Type: "TEXT",
					},
				},
				UniqueConstraints: []*SQLiteUniqueConstraint{
					{
						Name:    "pairs_unique",
						Columns: []string{"a,b", "c"},
						Keys:    []string{`"a,b" COLLATE NOCASE`, `"c"`},
					},
				},
			},
		})
	})

	t.Run("CreateTableWithUniqueConstraintsWhoseColumnNamesJoinToTheSameText", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE pairs (
				a TEXT,
				b TEXT,
				"a,b" TEXT,
				CONSTRAINT pairs_two UNIQUE (a, b DESC) ON CONFLICT REPLACE,
				CONSTRAINT pairs_one UNIQUE ("a,b")
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "pairs",
				Columns: []*SQLiteColumn{
					{
						Name: "a",
						Type: "TEXT",
					},
					{
						Name: "b",
						Type: "TEXT",
					},
					{
						Name: "a,b",
						Type: "TEXT",
					},
				},
				UniqueConstraints: []*SQLiteUniqueConstraint{
					{
						Name:     "pairs_two",
						Columns:  []string{"a", "b"},
						Keys:     []string{`"a"`, `"b" DESC`},
						Conflict: "REPLACE",
					},
					{
						Name:    "pairs_one",
						Columns: []string{"a,b"},
					},
				},
			},
		})
	})

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL
			);

			INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice'), (2, 'blue', 'Bob');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("members",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_members_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name:    "team",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "name",
							Type:    "TEXT",
							NotNull: true,
						},
					},
					UniqueConstraints: []*SQLiteUniqueConstraint{
						{Columns: []string{"team", "name"}},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_members_temp",
					ColumnNames:       []string{"id", "team", "name"},
					SelectExpressions: []string{`"id"`, `"team"`, `"name"`},
					FromTableName:     "members",
				},
				&driversshared.SQLDropTableInstruction{Name: "members"},
				&SQLiteAlterTableInstruction{
					Name:   "_members_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "members"},
				},
			),
			recreationEpilogue("members"),
		))

		rows := driver.FetchAllFromCurrent("members", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "team": "red", "name": "Alice"},
			{"id": int64(2), "team": "blue", "name": "Bob"},
		}, rows)
	})

	t.Run("ModifyUniqueConstraintKeyModifiers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);

			INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice'), (2, 'blue', 'Bob');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team COLLATE NOCASE, name DESC)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("members",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_members_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name:    "team",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name:    "name",
							Type:    "TEXT",
							NotNull: true,
						},
					},
					UniqueConstraints: []*SQLiteUniqueConstraint{
						{
							Columns: []string{"team", "name"},
							Keys:    []string{`"team" COLLATE NOCASE`, `"name" DESC`},
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_members_temp",
					ColumnNames:       []string{"id", "team", "name"},
					SelectExpressions: []string{`"id"`, `"team"`, `"name"`},
					FromTableName:     "members",
				},
				&driversshared.SQLDropTableInstruction{Name: "members"},
				&SQLiteAlterTableInstruction{
					Name:   "_members_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "members"},
				},
			),
			recreationEpilogue("members"),
		))

		rows := driver.FetchAllFromCurrent("members", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "team": "red", "name": "Alice"},
			{"id": int64(2), "team": "blue", "name": "Bob"},
		}, rows)

		driver.RequireInstructions(nil)
	})

	t.Run("EqualUniqueConstraintKeyModifiers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		schema := `
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team COLLATE NOCASE, name DESC)
			);
		`

		driver.ExecOnCurrent(schema)
		driver.ExecOnDesired(schema)

		driver.RequireInstructions(nil)
	})
}
