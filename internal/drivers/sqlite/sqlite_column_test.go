package driverssqlite

import (
	"database/sql"
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteColumn(t *testing.T) {
	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "email",
						Type: "TEXT",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": nil},
			{"id": int64(2), "name": "Bob", "email": nil},
		}, rows)
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &driversshared.SQLDropColumnAction{
					ColumnName: "email",
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("RenameColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				full_name TEXT NOT NULL
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "name",
					NewColumnName: "full_name",
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "full_name": "Alice"},
			{"id": int64(2), "full_name": "Bob"},
		}, rows)
	})

	t.Run("RenameColumnWithSeveralCandidates", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT,
				name_b TEXT
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name:   "users",
				Action: &driversshared.SQLDropColumnAction{ColumnName: "name_a"},
			},
			&SQLiteAlterTableInstruction{
				Name:   "users",
				Action: &driversshared.SQLDropColumnAction{ColumnName: "name_b"},
			},
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "first_name",
						Type: "TEXT",
					},
				},
			},
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "last_name",
						Type: "TEXT",
					},
				},
			},
		})
	})

	t.Run("RenameColumnTakesOneCandidateOnly", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT
			);

			INSERT INTO users (id, name_a) VALUES (1, 'Alice');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "name_a",
					NewColumnName: "first_name",
				},
			},
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "last_name",
						Type: "TEXT",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "first_name": "Alice", "last_name": nil},
		}, rows)
	})

	t.Run("RenameTwoColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				a TEXT,
				b INTEGER
			);

			INSERT INTO users (id, a, b) VALUES (1, 'Alice', 30);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				x TEXT,
				y INTEGER
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "a",
					NewColumnName: "x",
				},
			},
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "b",
					NewColumnName: "y",
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "x": "Alice", "y": int64(30)},
		}, rows)
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age TEXT
			);

			INSERT INTO users (id, name, age) VALUES (1, 'Alice', '30'), (2, 'Bob', '25');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
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
							Name:    "name",
							Type:    "TEXT",
							NotNull: true,
						},
						{
							Name: "age",
							Type: "INTEGER",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "name", "age"},
					SelectExpressions: []string{`"id"`, `"name"`, `"age"`},
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
			{"id": int64(1), "name": "Alice", "age": int64(30)},
			{"id": int64(2), "name": "Bob", "age": int64(25)},
		}, rows)
	})

	t.Run("ModifyColumnSetNotNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
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
							Name:    "name",
							Type:    "TEXT",
							NotNull: true,
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "name"},
					SelectExpressions: []string{`"id"`, `"name"`},
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
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("ModifyColumnDropNotNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
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
							Name: "name",
							Type: "TEXT",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "name"},
					SelectExpressions: []string{`"id"`, `"name"`},
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
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("CreateTableWithGeneratedColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE measures (
				value INTEGER,
				stored_double INTEGER GENERATED ALWAYS AS (value * 2) STORED,
				virtual_triple INTEGER GENERATED ALWAYS AS (value * 3) VIRTUAL
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "measures",
				Columns: []*SQLiteColumn{
					{
						Name: "value",
						Type: "INTEGER",
					},
					{
						Name:                "stored_double",
						Type:                "INTEGER",
						GeneratedExpression: "(value * 2)",
						GeneratedStored:     true,
					},
					{
						Name:                "virtual_triple",
						Type:                "INTEGER",
						GeneratedExpression: "(value * 3)",
					},
				},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO measures (value) VALUES (4);`)

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":          int64(4),
				"stored_double":  int64(8),
				"virtual_triple": int64(12),
			},
		}, rows)
	})

	t.Run("AddVirtualGeneratedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INTEGER);
			INSERT INTO measures (value) VALUES (2);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE measures (
				value INTEGER,
				triple INTEGER GENERATED ALWAYS AS (value * 3) VIRTUAL
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "measures",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name:                "triple",
						Type:                "INTEGER",
						GeneratedExpression: "(value * 3)",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":  int64(2),
				"triple": int64(6),
			},
		}, rows)
	})

	t.Run("AddStoredGeneratedColumnRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INTEGER);
			INSERT INTO measures (value) VALUES (3);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE measures (
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("measures",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_measures_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "value",
							Type: "INTEGER",
						},
						{
							Name:                "double",
							Type:                "INTEGER",
							GeneratedExpression: "(value * 2)",
							GeneratedStored:     true,
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_measures_temp",
					ColumnNames:       []string{"value"},
					SelectExpressions: []string{`"value"`},
					FromTableName:     "measures",
				},
				&driversshared.SQLDropTableInstruction{Name: "measures"},
				&SQLiteAlterTableInstruction{
					Name:   "_measures_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "measures"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":  int64(3),
				"double": int64(6),
			},
		}, rows)
	})

	t.Run("RecreateTableWithGeneratedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (
				label INTEGER,
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);

			INSERT INTO measures (label, value) VALUES (7, 5);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE measures (
				label TEXT,
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("measures",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_measures_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "label",
							Type: "TEXT",
						},
						{
							Name: "value",
							Type: "INTEGER",
						},
						{
							Name:                "double",
							Type:                "INTEGER",
							GeneratedExpression: "(value * 2)",
							GeneratedStored:     true,
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_measures_temp",
					ColumnNames:       []string{"label", "value"},
					SelectExpressions: []string{`"label"`, `"value"`},
					FromTableName:     "measures",
				},
				&driversshared.SQLDropTableInstruction{Name: "measures"},
				&SQLiteAlterTableInstruction{
					Name:   "_measures_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "measures"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"label":  "7",
				"value":  int64(5),
				"double": int64(10),
			},
		}, rows)
	})

	t.Run("ModifyGeneratedExpression", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (
				value INTEGER,
				multiple INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);

			INSERT INTO measures (value) VALUES (3);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE measures (
				value INTEGER,
				multiple INTEGER GENERATED ALWAYS AS (value * 5) STORED
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("measures",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_measures_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "value",
							Type: "INTEGER",
						},
						{
							Name:                "multiple",
							Type:                "INTEGER",
							GeneratedExpression: "(value * 5)",
							GeneratedStored:     true,
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_measures_temp",
					ColumnNames:       []string{"value"},
					SelectExpressions: []string{`"value"`},
					FromTableName:     "measures",
				},
				&driversshared.SQLDropTableInstruction{Name: "measures"},
				&SQLiteAlterTableInstruction{
					Name:   "_measures_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "measures"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":    int64(3),
				"multiple": int64(15),
			},
		}, rows)
	})

	t.Run("CreateTableWithAutoIncrement", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "logs",
				Columns: []*SQLiteColumn{
					{
						Name:          "id",
						Type:          "INTEGER",
						PrimaryKey:    true,
						AutoIncrement: true,
					},
					{
						Name: "body",
						Type: "TEXT",
					},
				},
			},
		})
	})

	t.Run("ModifyColumnCollationRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)
		driver.ExecOnDesired(`CREATE TABLE people (name TEXT COLLATE NOCASE);`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("people",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_people_temp",
					Columns: []*SQLiteColumn{
						{
							Name:      "name",
							Type:      "TEXT",
							Collation: "NOCASE",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_people_temp",
					ColumnNames:       []string{"name"},
					SelectExpressions: []string{`"name"`},
					FromTableName:     "people",
				},
				&driversshared.SQLDropTableInstruction{Name: "people"},
				&SQLiteAlterTableInstruction{
					Name:   "_people_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "people"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("people", "")
		require.Equal(t, []map[string]any{
			{"name": "Ada"},
		}, rows)
	})

	t.Run("CreateTableWithConflictClauses", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE t (
				id INTEGER PRIMARY KEY ON CONFLICT REPLACE,
				v TEXT UNIQUE ON CONFLICT IGNORE,
				w TEXT NOT NULL ON CONFLICT ABORT
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "t",
				Columns: []*SQLiteColumn{
					{
						Name:               "id",
						Type:               "INTEGER",
						PrimaryKey:         true,
						PrimaryKeyConflict: "REPLACE",
					},
					{
						Name:           "v",
						Type:           "TEXT",
						Unique:         true,
						UniqueConflict: "IGNORE",
					},
					{
						Name:            "w",
						Type:            "TEXT",
						NotNull:         true,
						NotNullConflict: "ABORT",
					},
				},
			},
		})
	})

	t.Run("ModifyConflictClauseRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE t (id INTEGER PRIMARY KEY);
			INSERT INTO t (id) VALUES (1);
		`)
		driver.ExecOnDesired(`CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT REPLACE);`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("t",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_t_temp",
					Columns: []*SQLiteColumn{
						{
							Name:               "id",
							Type:               "INTEGER",
							PrimaryKey:         true,
							PrimaryKeyConflict: "REPLACE",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_t_temp",
					ColumnNames:       []string{"id"},
					SelectExpressions: []string{`"id"`},
					FromTableName:     "t",
				},
				&driversshared.SQLDropTableInstruction{Name: "t"},
				&SQLiteAlterTableInstruction{
					Name:   "_t_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "t"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("t", "")
		require.Equal(t, []map[string]any{
			{"id": int64(1)},
		}, rows)
	})

	t.Run("RowIterationError", func(t *testing.T) {
		db, err := sql.Open("sqlite3_failing_rows", "")
		require.NoError(t, err)

		defer func() { require.NoError(t, db.Close()) }()

		driver := &SQLiteDriver{}

		_, _, err = driver.GetTableColumns(t.Context(), db, "users", parseTableDefinition(""))
		require.ErrorIs(t, err, errRowIteration)
	})
}
