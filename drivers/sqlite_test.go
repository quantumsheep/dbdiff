package drivers

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type TestingSQLiteDriver struct {
	*SQLiteDriver

	tb testing.TB
}

func (d *TestingSQLiteDriver) Close() error {
	d.tb.Helper()
	return d.SQLiteDriver.Close()
}

func (d *TestingSQLiteDriver) ExecOnTarget(sqlStatements string) {
	d.tb.Helper()

	_, err := d.TargetDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()

	_, err := d.SourceDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) RequireInstructions(expected []Instruction) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context())
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return RenderInstructions(instructions)
}

func (d *TestingSQLiteDriver) FetchAllFromSource(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	columns, err := d.GetTableColumns(d.tb.Context(), d.SourceDatabaseConnection, table)
	require.NoError(d.tb, err)

	rows, err := d.SourceDatabaseConnection.Query(fmt.Sprintf("SELECT * FROM %q %s;", table, additionalRules))
	require.NoError(d.tb, err)

	var results []map[string]any

	for rows.Next() {
		columnValues := make([]any, len(columns))

		columnPointers := make([]any, len(columns))
		for i := range columnPointers {
			columnPointers[i] = &columnValues[i]
		}

		err := rows.Scan(columnPointers...)
		require.NoError(d.tb, err)

		row := make(map[string]any)

		for i, column := range columns {
			row[column.Name] = columnValues[i]
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

func NewTestSQLiteDriver(tb testing.TB) *TestingSQLiteDriver {
	tb.Helper()

	targetDatabasePath := filepath.Join(tb.TempDir(), "target.sqlite")
	sourceDatabasePath := filepath.Join(tb.TempDir(), "source.sqlite")

	return NewTestSQLiteDriverWithPaths(tb, targetDatabasePath, sourceDatabasePath)
}

// The driver refuses a database file that is absent, so the constructor creates each
// empty file first. A SQL source builds its own database.
func NewTestSQLiteDriverWithPaths(tb testing.TB, targetPath string, sourcePath string) *TestingSQLiteDriver {
	tb.Helper()

	for _, path := range []string{targetPath, sourcePath} {
		if IsSQLSource(path) {
			continue
		}

		file, err := os.OpenFile(TrimSQLitePrefix(path), os.O_CREATE, 0o600)
		require.NoError(tb, err)
		require.NoError(tb, file.Close())
	}

	driver, err := NewSQLiteDriver(tb.Context(), &SQLiteDriverConfig{
		TargetDatabasePath: targetPath,
		SourceDatabasePath: sourcePath,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, driver.Close())
	})

	return &TestingSQLiteDriver{
		SQLiteDriver: driver,
		tb:           tb,
	}
}

func WriteSQLFile(tb testing.TB, directory string, name string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, name)

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}

var errRowIteration = errors.New("row iteration failed")

type failingRows struct {
	columns  []string
	rows     [][]driver.Value
	position int
}

func (r *failingRows) Columns() []string {
	return r.columns
}

func (r *failingRows) Close() error {
	return nil
}

func (r *failingRows) Next(dest []driver.Value) error {
	if r.position >= len(r.rows) {
		return errRowIteration
	}

	copy(dest, r.rows[r.position])
	r.position++

	return nil
}

type failingStmt struct {
	columns []string
	rows    [][]driver.Value
}

func (s *failingStmt) Close() error {
	return nil
}

func (s *failingStmt) NumInput() int {
	return -1
}

func (s *failingStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("failingStmt does not support Exec")
}

func (s *failingStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &failingRows{columns: s.columns, rows: s.rows}, nil
}

type failingConn struct {
	columns []string
	rows    [][]driver.Value
}

// The definition query answers with one row, so the failure of Next belongs to the PRAGMA.
func (c *failingConn) Prepare(query string) (driver.Stmt, error) {
	if strings.Contains(query, "sqlite_master") {
		return &failingStmt{
			columns: []string{"sql"},
			rows: [][]driver.Value{
				{"CREATE TABLE users (id INTEGER PRIMARY KEY)"},
			},
		}, nil
	}

	return &failingStmt{columns: c.columns, rows: c.rows}, nil
}

func (c *failingConn) Close() error {
	return nil
}

func (c *failingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("failingConn does not support Begin")
}

type failingDriver struct {
	columns []string
	rows    [][]driver.Value
}

func (d *failingDriver) Open(name string) (driver.Conn, error) {
	return &failingConn{columns: d.columns, rows: d.rows}, nil
}

// A second call to sql.Register with the same name panics.
func init() {
	sql.Register("sqlite3_failing_rows", &failingDriver{
		columns: []string{"cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"},
		rows: [][]driver.Value{
			{int64(0), "id", "INTEGER", int64(0), nil, int64(1), int64(0)},
		},
	})
}

func TestSQLiteDriver(t *testing.T) {
	t.Run("NoChanges", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.RequireInstructions(nil)
	})

	t.Run("CreateTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "users",
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
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": nil},
			{"id": int64(2), "name": "Bob", "email": nil},
		}, rows)
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLDropColumnAction{
					ColumnName: "email",
				},
			},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("RenameColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				full_name TEXT NOT NULL
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLRenameColumnAction{
					ColumnName:    "name",
					NewColumnName: "full_name",
				},
			},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "full_name": "Alice"},
			{"id": int64(2), "full_name": "Bob"},
		}, rows)
	})

	t.Run("RenameColumnWithSeveralCandidates", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT,
				name_b TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name:   "users",
				Action: &SQLDropColumnAction{ColumnName: "name_a"},
			},
			&SQLiteAlterTableInstruction{
				Name:   "users",
				Action: &SQLDropColumnAction{ColumnName: "name_b"},
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

		driver.ExecOnSource(diff)
	})

	t.Run("RenameColumnTakesOneCandidateOnly", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT
			);

			INSERT INTO users (id, name_a) VALUES (1, 'Alice');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLRenameColumnAction{
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

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "first_name": "Alice", "last_name": nil},
		}, rows)
	})

	t.Run("RenameTwoColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				a TEXT,
				b INTEGER
			);

			INSERT INTO users (id, a, b) VALUES (1, 'Alice', 30);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				x TEXT,
				y INTEGER
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLRenameColumnAction{
					ColumnName:    "a",
					NewColumnName: "x",
				},
			},
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLRenameColumnAction{
					ColumnName:    "b",
					NewColumnName: "y",
				},
			},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "x": "Alice", "y": int64(30)},
		}, rows)
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age TEXT
			);

			INSERT INTO users (id, name, age) VALUES (1, 'Alice', '30'), (2, 'Bob', '25');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age INTEGER
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "name", "age"},
				SelectExpressions: []string{`"id"`, `"name"`, `"age"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "age": int64(30)},
			{"id": int64(2), "name": "Bob", "age": int64(25)},
		}, rows)
	})

	t.Run("ModifyColumnSetNotNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "name"},
				SelectExpressions: []string{`"id"`, `"name"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("ModifyColumnDropNotNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "name"},
				SelectExpressions: []string{`"id"`, `"name"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("CreateTableWithGeneratedColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				value INTEGER,
				stored_double INTEGER GENERATED ALWAYS AS (value * 2) STORED,
				virtual_triple INTEGER GENERATED ALWAYS AS (value * 3) VIRTUAL
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		driver.ExecOnSource(`INSERT INTO measures (value) VALUES (4);`)

		rows := driver.FetchAllFromSource("measures", "")
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

		driver.ExecOnSource(`
			CREATE TABLE measures (value INTEGER);
			INSERT INTO measures (value) VALUES (2);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				value INTEGER,
				triple INTEGER GENERATED ALWAYS AS (value * 3) VIRTUAL
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":  int64(2),
				"triple": int64(6),
			},
		}, rows)
	})

	t.Run("AddStoredGeneratedColumnRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE measures (value INTEGER);
			INSERT INTO measures (value) VALUES (3);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_measures_temp",
				ColumnNames:       []string{"value"},
				SelectExpressions: []string{`"value"`},
				SourceTableName:   "measures",
			},
			&SQLDropTableInstruction{Name: "measures"},
			&SQLiteAlterTableInstruction{
				Name:   "_measures_temp",
				Action: &SQLRenameTableAction{NewName: "measures"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":  int64(3),
				"double": int64(6),
			},
		}, rows)
	})

	t.Run("RecreateTableWithGeneratedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE measures (
				label INTEGER,
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);

			INSERT INTO measures (label, value) VALUES (7, 5);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				label TEXT,
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_measures_temp",
				ColumnNames:       []string{"label", "value"},
				SelectExpressions: []string{`"label"`, `"value"`},
				SourceTableName:   "measures",
			},
			&SQLDropTableInstruction{Name: "measures"},
			&SQLiteAlterTableInstruction{
				Name:   "_measures_temp",
				Action: &SQLRenameTableAction{NewName: "measures"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("measures", "")
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

		driver.ExecOnSource(`
			CREATE TABLE measures (
				value INTEGER,
				multiple INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);

			INSERT INTO measures (value) VALUES (3);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				value INTEGER,
				multiple INTEGER GENERATED ALWAYS AS (value * 5) STORED
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_measures_temp",
				ColumnNames:       []string{"value"},
				SelectExpressions: []string{`"value"`},
				SourceTableName:   "measures",
			},
			&SQLDropTableInstruction{Name: "measures"},
			&SQLiteAlterTableInstruction{
				Name:   "_measures_temp",
				Action: &SQLRenameTableAction{NewName: "measures"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":    int64(3),
				"multiple": int64(15),
			},
		}, rows)
	})

	t.Run("CreateTableWithoutRowIDAndStrict", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE sessions (id TEXT PRIMARY KEY, token TEXT) WITHOUT ROWID, STRICT;
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "sessions",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "TEXT",
						NotNull:    true,
						PrimaryKey: true,
					},
					{
						Name: "token",
						Type: "TEXT",
					},
				},
				WithoutRowID: true,
				Strict:       true,
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("AddStrictRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE events (id INTEGER, label TEXT);
			INSERT INTO events (id, label) VALUES (1, 'start');
		`)
		driver.ExecOnTarget(`CREATE TABLE events (id INTEGER, label TEXT) STRICT;`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_events_temp",
				Columns: []*SQLiteColumn{
					{
						Name: "id",
						Type: "INTEGER",
					},
					{
						Name: "label",
						Type: "TEXT",
					},
				},
				Strict: true,
			},
			&SQLInsertSelectInstruction{
				TableName:         "_events_temp",
				ColumnNames:       []string{"id", "label"},
				SelectExpressions: []string{`"id"`, `"label"`},
				SourceTableName:   "events",
			},
			&SQLDropTableInstruction{Name: "events"},
			&SQLiteAlterTableInstruction{
				Name:   "_events_temp",
				Action: &SQLRenameTableAction{NewName: "events"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("events", "")
		require.Equal(t, []map[string]any{
			{
				"id":    int64(1),
				"label": "start",
			},
		}, rows)
	})

	t.Run("CreateTableWithAutoIncrement", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithCollationAndChecks", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE people (
				name TEXT COLLATE NOCASE,
				age INTEGER CHECK (age > 0),
				CHECK (length(name) < 100)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "people",
				Columns: []*SQLiteColumn{
					{
						Name:      "name",
						Type:      "TEXT",
						Collation: "NOCASE",
					},
					{
						Name:  "age",
						Type:  "INTEGER",
						Check: "(age > 0)",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{Expression: "(length(name) < 100)"},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithANamedPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT,
				member TEXT,
				CONSTRAINT pk_memberships PRIMARY KEY (team, member)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "memberships",
				Columns: []*SQLiteColumn{
					{
						Name: "team",
						Type: "TEXT",
					},
					{
						Name: "member",
						Type: "TEXT",
					},
				},
				PrimaryKey:     []string{"team", "member"},
				PrimaryKeyName: "pk_memberships",
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithAUniqueConstraintConflict", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`CREATE TABLE items (code INTEGER, UNIQUE (code) ON CONFLICT REPLACE);`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithoutSpacesBeforeTheParentheses", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				price INTEGER CHECK(price > 0),
				total INTEGER GENERATED ALWAYS AS(price * 2) VIRTUAL,
				parent INTEGER,
				CONSTRAINT fk_parent FOREIGN KEY(parent) REFERENCES parents(id),
				CHECK(id > 0)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Name: "items",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:  "price",
						Type:  "INTEGER",
						Check: "(price > 0)",
					},
					{
						Name:                "total",
						Type:                "INTEGER",
						GeneratedExpression: "(price * 2)",
					},
					{
						Name: "parent",
						Type: "INTEGER",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{
					{
						Name:     "fk_parent",
						Table:    "parents",
						From:     []string{"parent"},
						To:       []string{"id"},
						OnUpdate: "NO ACTION",
						OnDelete: "NO ACTION",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Expression: "(id > 0)",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyColumnCollationRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)
		driver.ExecOnTarget(`CREATE TABLE people (name TEXT COLLATE NOCASE);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_people_temp",
				ColumnNames:       []string{"name"},
				SelectExpressions: []string{`"name"`},
				SourceTableName:   "people",
			},
			&SQLDropTableInstruction{Name: "people"},
			&SQLiteAlterTableInstruction{
				Name:   "_people_temp",
				Action: &SQLRenameTableAction{NewName: "people"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("people", "")
		require.Equal(t, []map[string]any{
			{"name": "Ada"},
		}, rows)
	})

	t.Run("ReorderedChecksGiveNoDiff", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE TABLE people (age INTEGER, CHECK (age > 0), CHECK (age < 200));`)
		driver.ExecOnTarget(`CREATE TABLE people (age INTEGER, CHECK (age < 200), CHECK (age > 0));`)
		driver.RequireInstructions(nil)
	})

	t.Run("ReorderColumnsRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE people (name TEXT, age INTEGER);
			INSERT INTO people (name, age) VALUES ('Ada', 36);
		`)
		driver.ExecOnTarget(`CREATE TABLE people (age INTEGER, name TEXT);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_people_temp",
				Columns: []*SQLiteColumn{
					{
						Name: "age",
						Type: "INTEGER",
					},
					{
						Name: "name",
						Type: "TEXT",
					},
				},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_people_temp",
				ColumnNames:       []string{"age", "name"},
				SelectExpressions: []string{`"age"`, `"name"`},
				SourceTableName:   "people",
			},
			&SQLDropTableInstruction{Name: "people"},
			&SQLiteAlterTableInstruction{
				Name:   "_people_temp",
				Action: &SQLRenameTableAction{NewName: "people"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("people", "")
		require.Equal(t, []map[string]any{
			{"age": int64(36), "name": "Ada"},
		}, rows)
	})

	t.Run("AddTableCheckRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE people (name TEXT, CHECK (length(name) < 100));
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_people_temp",
				Columns: []*SQLiteColumn{
					{
						Name: "name",
						Type: "TEXT",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{Expression: "(length(name) < 100)"},
				},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_people_temp",
				ColumnNames:       []string{"name"},
				SelectExpressions: []string{`"name"`},
				SourceTableName:   "people",
			},
			&SQLDropTableInstruction{Name: "people"},
			&SQLiteAlterTableInstruction{
				Name:   "_people_temp",
				Action: &SQLRenameTableAction{NewName: "people"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("people", "")
		require.Equal(t, []map[string]any{
			{"name": "Ada"},
		}, rows)
	})

	t.Run("CreateTableWithANamedCheck", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE people (age INTEGER, CONSTRAINT age_is_positive CHECK (age > 0));
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "people",
				Columns: []*SQLiteColumn{
					{
						Name: "age",
						Type: "INTEGER",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Name:       "age_is_positive",
						Expression: "(age > 0)",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateIndexWithDirectionAndCollation", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE TABLE items (a INTEGER, b TEXT);`)

		driver.ExecOnTarget(`
			CREATE TABLE items (a INTEGER, b TEXT);
			CREATE INDEX items_sorted ON items (a DESC, b COLLATE NOCASE ASC);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "items_sorted",
				TableName: "items",
				Keys:      []string{`"a" DESC`, `"b" COLLATE NOCASE`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyIndexDirection", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a DESC);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "items_sorted"},
			&SQLiteCreateIndexInstruction{
				Name:      "items_sorted",
				TableName: "items",
				Keys:      []string{`"a" DESC`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("IndexWithExplicitAscMatchesTheDefault", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE items (a INTEGER);
			CREATE INDEX items_sorted ON items (a ASC);
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithConflictClauses", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE t (
				id INTEGER PRIMARY KEY ON CONFLICT REPLACE,
				v TEXT UNIQUE ON CONFLICT IGNORE,
				w TEXT NOT NULL ON CONFLICT ABORT
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithATableConflictClause", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE u (a INTEGER, b INTEGER, UNIQUE (a, b) ON CONFLICT FAIL);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyConflictClauseRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE t (id INTEGER PRIMARY KEY);
			INSERT INTO t (id) VALUES (1);
		`)
		driver.ExecOnTarget(`CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT REPLACE);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_t_temp",
				ColumnNames:       []string{"id"},
				SelectExpressions: []string{`"id"`},
				SourceTableName:   "t",
			},
			&SQLDropTableInstruction{Name: "t"},
			&SQLiteAlterTableInstruction{
				Name:   "_t_temp",
				Action: &SQLRenameTableAction{NewName: "t"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("t", "")
		require.Equal(t, []map[string]any{
			{"id": int64(1)},
		}, rows)
	})

	t.Run("CreateTableWithADeferredForeignKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id) ON DELETE CASCADE
					DEFERRABLE INITIALLY DEFERRED
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithAForeignKeyWithoutParentColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyForeignKeyDeferrable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
			);
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO children (id, parent) VALUES (1, 1);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_children_temp",
				ColumnNames:       []string{"id", "parent"},
				SelectExpressions: []string{`"id"`, `"parent"`},
				SourceTableName:   "children",
			},
			&SQLDropTableInstruction{Name: "children"},
			&SQLiteAlterTableInstruction{
				Name:   "_children_temp",
				Action: &SQLRenameTableAction{NewName: "children"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("children", "")
		require.Equal(t, []map[string]any{
			{"id": int64(1), "parent": int64(1)},
		}, rows)
	})

	t.Run("DeferrableOfATableForeignKeyOfOneColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER,
				FOREIGN KEY ("parent") REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
			);
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithNamedConstraints", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (
				id INTEGER,
				owner INTEGER,
				CONSTRAINT items_positive CHECK (id > 0),
				CONSTRAINT items_unique UNIQUE (id),
				CONSTRAINT items_owner FOREIGN KEY (owner) REFERENCES parents(id)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
				Name: "items",
				Columns: []*SQLiteColumn{
					{
						Name: "id",
						Type: "INTEGER",
					},
					{
						Name: "owner",
						Type: "INTEGER",
					},
				},
				UniqueConstraints: []*SQLiteUniqueConstraint{
					{
						Name:    "items_unique",
						Columns: []string{"id"},
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Name:       "items_positive",
						Expression: "(id > 0)",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{
					{
						Name:     "items_owner",
						Table:    "parents",
						From:     []string{"owner"},
						To:       []string{"id"},
						OnUpdate: "NO ACTION",
						OnDelete: "NO ACTION",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyConstraintNameRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_old CHECK (id > 0));
			INSERT INTO items (id) VALUES (5);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_new CHECK (id > 0));
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_items_temp",
				Columns: []*SQLiteColumn{
					{
						Name: "id",
						Type: "INTEGER",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Name:       "items_new",
						Expression: "(id > 0)",
					},
				},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_items_temp",
				ColumnNames:       []string{"id"},
				SelectExpressions: []string{`"id"`},
				SourceTableName:   "items",
			},
			&SQLDropTableInstruction{Name: "items"},
			&SQLiteAlterTableInstruction{
				Name:   "_items_temp",
				Action: &SQLRenameTableAction{NewName: "items"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("items", "")
		require.Equal(t, []map[string]any{
			{"id": int64(5)},
		}, rows)
	})

	t.Run("EqualCheckConstraints", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_positive CHECK (id > 0));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_positive CHECK (id > 0));
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE VIRTUAL TABLE docs USING fts4(title, body);
			CREATE TABLE plain (id INTEGER);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "plain",
				Columns: []*SQLiteColumn{
					{
						Name: "id",
						Type: "INTEGER",
					},
				},
			},
			&SQLiteCreateVirtualTableInstruction{
				Definition: "CREATE VIRTUAL TABLE docs USING fts4(title, body)",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "docs"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)
		driver.ExecOnTarget(`CREATE VIRTUAL TABLE docs USING fts4(title, body, tags);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "docs"},
			&SQLiteCreateVirtualTableInstruction{
				Definition: "CREATE VIRTUAL TABLE docs USING fts4(title, body, tags)",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("EqualVirtualTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)
		driver.ExecOnTarget(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)
		driver.RequireInstructions(nil)
	})

	t.Run("DropTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("DropTablesInTheReverseOrder", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "children"},
			&SQLDropTableInstruction{Name: "parents"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE "order ""list""" (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
			CREATE INDEX "idx name" ON "order ""list""" (name);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        `order "list"`,
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
			&SQLiteCreateIndexInstruction{
				Name:      "idx name",
				TableName: `order "list"`,
				Keys:      []string{`"name"`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("DropIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_users_name"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name, email);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_users_name"},
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`, `"email"`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreatePartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_active",
				TableName: "users",
				Keys:      []string{`"name"`},
				Condition: &SQLiteIndexPredicateCondition{Expression: "active = 1"},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyPartialIndexCondition", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 0;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_users_active"},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_active",
				TableName: "users",
				Keys:      []string{`"name"`},
				Condition: &SQLiteIndexPredicateCondition{Expression: "active = 1"},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (lower(name), id);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"lower(name)", `"id"`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ModifyExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (upper(name));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (lower(name));
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_users_name"},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"lower(name)"},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("RecreateTableWithPartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;

			INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1), (2, 'Bob', 0);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "name", "active"},
				SelectExpressions: []string{`"id"`, `"name"`, `"active"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_active",
				TableName: "users",
				Keys:      []string{`"name"`},
				Condition: &SQLiteIndexPredicateCondition{Expression: "active = 1"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "active": int64(1)},
			{"id": int64(2), "name": "Bob", "active": int64(0)},
		}, rows)
	})

	t.Run("ImplicitUniqueIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
			CREATE INDEX idx_users_name ON users (name);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("RecreateTableWithUniqueColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age TEXT
			);

			INSERT INTO users (id, email, age) VALUES (1, 'alice@example.com', '30'), (2, 'bob@example.com', '25');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age INTEGER
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "email", "age"},
				SelectExpressions: []string{`"id"`, `"email"`, `"age"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "alice@example.com", "age": int64(30)},
			{"id": int64(2), "email": "bob@example.com", "age": int64(25)},
		}, rows)

		_, err := driver.SourceDatabaseConnection.Exec(`INSERT INTO users (id, email, age) VALUES (3, 'alice@example.com', 40);`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: users.email")
	})

	t.Run("CreateTableWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		driver.ExecOnSource(`INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice');`)

		_, err := driver.SourceDatabaseConnection.Exec(`INSERT INTO members (id, team, name) VALUES (2, 'red', 'Alice');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: members.team, members.name")
	})

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL
			);

			INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice'), (2, 'blue', 'Bob');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_members_temp",
				ColumnNames:       []string{"id", "team", "name"},
				SelectExpressions: []string{`"id"`, `"team"`, `"name"`},
				SourceTableName:   "members",
			},
			&SQLDropTableInstruction{Name: "members"},
			&SQLiteAlterTableInstruction{
				Name:   "_members_temp",
				Action: &SQLRenameTableAction{NewName: "members"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("members", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "team": "red", "name": "Alice"},
			{"id": int64(2), "team": "blue", "name": "Bob"},
		}, rows)
	})

	t.Run("ModifyUniqueConstraintKeyModifiers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);

			INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice'), (2, 'blue', 'Bob');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team COLLATE NOCASE, name DESC)
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_members_temp",
				ColumnNames:       []string{"id", "team", "name"},
				SelectExpressions: []string{`"id"`, `"team"`, `"name"`},
				SourceTableName:   "members",
			},
			&SQLDropTableInstruction{Name: "members"},
			&SQLiteAlterTableInstruction{
				Name:   "_members_temp",
				Action: &SQLRenameTableAction{NewName: "members"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("members", "ORDER BY id")

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

		driver.ExecOnSource(schema)
		driver.ExecOnTarget(schema)

		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT,
				PRIMARY KEY (member, team)
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "memberships",
				Columns: []*SQLiteColumn{
					{
						Name:    "team",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "member",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name: "role",
						Type: "TEXT",
					},
				},
				PrimaryKey: []string{"member", "team"},
			},
		})

		driver.ExecOnSource(diff)
		driver.ExecOnSource(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');`)

		_, err := driver.SourceDatabaseConnection.Exec(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'guest');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: memberships.member, memberships.team")
	})

	t.Run("CreateTableWithIntegerPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE counters (
				id INTEGER PRIMARY KEY,
				total INTEGER
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "counters",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name: "total",
						Type: "INTEGER",
					},
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.ExecOnSource(`INSERT INTO counters (total) VALUES (5);`)

		rows := driver.FetchAllFromSource("counters", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "total": int64(5)},
		}, rows)
	})

	t.Run("RecreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level TEXT,
				PRIMARY KEY (team, member)
			);

			INSERT INTO memberships (team, member, level) VALUES ('red', 'Alice', '3'), ('blue', 'Bob', '1');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level INTEGER,
				PRIMARY KEY (team, member)
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_memberships_temp",
				Columns: []*SQLiteColumn{
					{
						Name:    "team",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "member",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name: "level",
						Type: "INTEGER",
					},
				},
				PrimaryKey: []string{"team", "member"},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_memberships_temp",
				ColumnNames:       []string{"team", "member", "level"},
				SelectExpressions: []string{`"team"`, `"member"`, `"level"`},
				SourceTableName:   "memberships",
			},
			&SQLDropTableInstruction{Name: "memberships"},
			&SQLiteAlterTableInstruction{
				Name:   "_memberships_temp",
				Action: &SQLRenameTableAction{NewName: "memberships"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "blue", "member": "Bob", "level": int64(1)},
			{"team": "red", "member": "Alice", "level": int64(3)},
		}, rows)
	})

	t.Run("ModifyCompositePrimaryKeyKeyModifiers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				PRIMARY KEY (member, team)
			);

			INSERT INTO memberships (team, member) VALUES ('red', 'Alice');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				PRIMARY KEY (member COLLATE NOCASE, team DESC)
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_memberships_temp",
				Columns: []*SQLiteColumn{
					{
						Name:    "team",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "member",
						Type:    "TEXT",
						NotNull: true,
					},
				},
				PrimaryKey:     []string{"member", "team"},
				PrimaryKeyKeys: []string{`"member" COLLATE NOCASE`, `"team" DESC`},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_memberships_temp",
				ColumnNames:       []string{"team", "member"},
				SelectExpressions: []string{`"team"`, `"member"`},
				SourceTableName:   "memberships",
			},
			&SQLDropTableInstruction{Name: "memberships"},
			&SQLiteAlterTableInstruction{
				Name:   "_memberships_temp",
				Action: &SQLRenameTableAction{NewName: "memberships"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("memberships", "ORDER BY member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice"},
		}, rows)

		driver.RequireInstructions(nil)
	})

	t.Run("ModifyCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, role)
			);

			INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, member)
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_memberships_temp",
				Columns: []*SQLiteColumn{
					{
						Name:    "team",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "member",
						Type:    "TEXT",
						NotNull: true,
					},
					{
						Name:    "role",
						Type:    "TEXT",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"team", "member"},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_memberships_temp",
				ColumnNames:       []string{"team", "member", "role"},
				SelectExpressions: []string{`"team"`, `"member"`, `"role"`},
				SourceTableName:   "memberships",
			},
			&SQLDropTableInstruction{Name: "memberships"},
			&SQLiteAlterTableInstruction{
				Name:   "_memberships_temp",
				Action: &SQLRenameTableAction{NewName: "memberships"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice", "role": "lead"},
		}, rows)
	})

	t.Run("RecreateTableWithPrimaryKeyAndIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (email, age) VALUES ('alice@example.com', '30'), ('bob@example.com', '25');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_users_temp",
				Columns: []*SQLiteColumn{
					{
						Name:       "email",
						Type:       "TEXT",
						PrimaryKey: true,
					},
					{
						Name: "age",
						Type: "INTEGER",
					},
				},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"email", "age"},
				SelectExpressions: []string{`"email"`, `"age"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_age",
				TableName: "users",
				Keys:      []string{`"age"`},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY email")

		require.Equal(t, []map[string]any{
			{"email": "alice@example.com", "age": int64(30)},
			{"email": "bob@example.com", "age": int64(25)},
		}, rows)
	})

	t.Run("RecreateTableWithNewIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "age"},
				SelectExpressions: []string{`"id"`, `"age"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_age",
				TableName: "users",
				Keys:      []string{`"age"`},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30)},
		}, rows)
	})

	t.Run("RecreateTableWithRemovedIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "age"},
				SelectExpressions: []string{`"id"`, `"age"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30)},
		}, rows)
	})

	t.Run("RecreateTableWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_users_temp",
				ColumnNames:       []string{"id", "age"},
				SelectExpressions: []string{`"id"`, `"age"`},
				SourceTableName:   "users",
			},
			&SQLDropTableInstruction{Name: "users"},
			&SQLiteAlterTableInstruction{
				Name:   "_users_temp",
				Action: &SQLRenameTableAction{NewName: "users"},
			},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)

		triggers, err := driver.GetTriggers(t.Context(), driver.SourceDatabaseConnection, "users")
		require.NoError(t, err)
		require.Len(t, triggers, 1)
		require.Equal(t, "users_insert", triggers[0].Name)
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 999; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
			CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 4; END;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
			},
			&SQLiteDropTriggerInstruction{Name: "users_update"},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END",
			},
			&SQLiteDropTriggerInstruction{Name: "users_audit"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateTableWithTriggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "users",
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
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE VIEW old_view AS SELECT id FROM users;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name FROM users;
			CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin';
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin'",
			},
			&SQLDropViewInstruction{Name: "users_view"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT name FROM users",
			},
			&SQLDropViewInstruction{Name: "old_view"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CreateViewTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnSource(diff)

		driver.ExecOnSource(`INSERT INTO users_view (id, name) VALUES (1, 'alice');`)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "alice"},
		}, rows)
	})

	t.Run("DropViewTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteDropTriggerInstruction{Name: "users_view_insert"},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("RecreateViewWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name, id FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDropViewInstruction{Name: "users_view"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT name, id FROM users",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnSource(diff)

		driver.ExecOnSource(`INSERT INTO users_view (id, name) VALUES (2, 'bob');`)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "bob"},
		}, rows)
	})

	t.Run("CreateViewWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT id, name FROM users",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("ForeignKeys", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT
			);

			INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'First Post'), (2, 1, 'Second Post');
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT,
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);
		`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
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
			&SQLInsertSelectInstruction{
				TableName:         "_posts_temp",
				ColumnNames:       []string{"id", "user_id", "title"},
				SelectExpressions: []string{`"id"`, `"user_id"`, `"title"`},
				SourceTableName:   "posts",
			},
			&SQLDropTableInstruction{Name: "posts"},
			&SQLiteAlterTableInstruction{
				Name:   "_posts_temp",
				Action: &SQLRenameTableAction{NewName: "posts"},
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("posts", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "user_id": int64(1), "title": "First Post"},
			{"id": int64(2), "user_id": int64(1), "title": "Second Post"},
		}, rows)
	})

	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"3", "'Carol'"},
			},
			&SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Robert'",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
			&SQLDeleteInstruction{
				TableName: "users",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "4",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
		}, rows)
	})

	t.Run("CompareRowsOfANewTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
			INSERT INTO users (id, name) VALUES (2, 'Bob'), (1, 'Alice');
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "users",
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
			&SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"1", "'Alice'"},
			},
			&SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("CompareRowsWithAGeneratedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				price INTEGER,
				total INTEGER GENERATED ALWAYS AS (price * 2) VIRTUAL
			);
		`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO items (id, price) VALUES (1, 10), (2, 20);`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO items (id, price) VALUES (1, 15), (3, 30);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "items",
				ColumnNames: []string{"id", "price"},
				Expressions: []string{"3", "30"},
			},
			&SQLUpdateInstruction{
				TableName: "items",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "price",
						Expression: "15",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&SQLDeleteInstruction{
				TableName: "items",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("items", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "price": int64(15), "total": int64(30)},
			{"id": int64(3), "price": int64(30), "total": int64(60)},
		}, rows)
	})

	t.Run("CompareRowsWithADateColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE events (id INTEGER PRIMARY KEY, at DATE);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO events (id, at) VALUES (1, 'alpha');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO events (id, at) VALUES (1, 'omega'), (2, '2024-01-01');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "events",
				ColumnNames: []string{"id", "at"},
				Expressions: []string{"2", "'2024-01-01'"},
			},
			&SQLUpdateInstruction{
				TableName: "events",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "at",
						Expression: "'omega'",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CompareRowsDeletesAChildRowFirst", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
			);
		`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO children (id, parent) VALUES (10, 1);
		`)

		driver.ExecOnTarget(schema)
		diff := driver.RequireInstructions([]Instruction{
			&SQLDeleteInstruction{
				TableName: "children",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "10",
						},
					},
				},
			},
			&SQLDeleteInstruction{
				TableName: "parents",
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO logs (message) VALUES ('stop');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO logs (message) VALUES ('start');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("logs", "")

		require.Equal(t, []map[string]any{
			{"message": "stop"},
		}, rows)
	})

	t.Run("CompareRowsWithAQuoteAndWithNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'not empty');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO notes (id, body) VALUES (1, 'it''s a note'), (2, NULL), (3, NULL);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "notes",
				ColumnNames: []string{"id", "body"},
				Expressions: []string{"3", "NULL"},
			},
			&SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "body",
						Expression: "'it''s a note'",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "body",
						Expression: "NULL",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{
						&SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("notes", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "it's a note"},
			{"id": int64(2), "body": nil},
			{"id": int64(3), "body": nil},
		}, rows)
	})

	t.Run("CompareRowsWithAnInfiniteValue", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL);`

		driver.ExecOnSource(schema)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO readings (id, value) VALUES (1, 9e999), (2, -9e999), (3, 1.5);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"1", "9e999"},
			},
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "-9e999"},
			},
			&SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"3", "1.5"},
			},
		})

		driver.ExecOnSource(diff)
		driver.RequireInstructions(nil)
	})

	t.Run("CompareRowsOfATableWithTheKeyOnAnotherColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (a INTEGER PRIMARY KEY, b INTEGER);`)
		driver.ExecOnSource(`INSERT INTO items (a, b) VALUES (1, 2);`)

		driver.ExecOnTarget(`CREATE TABLE items (a INTEGER, b INTEGER PRIMARY KEY);`)
		driver.ExecOnTarget(`INSERT INTO items (a, b) VALUES (1, 2);`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLitePragmaForeignKeysInstruction{},
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_items_temp",
				Columns: []*SQLiteColumn{
					{
						Name: "a",
						Type: "INTEGER",
					},
					{
						Name:       "b",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
				},
			},
			&SQLInsertSelectInstruction{
				TableName:         "_items_temp",
				ColumnNames:       []string{"a", "b"},
				SelectExpressions: []string{`"a"`, `"b"`},
				SourceTableName:   "items",
			},
			&SQLDropTableInstruction{Name: "items"},
			&SQLiteAlterTableInstruction{
				Name:   "_items_temp",
				Action: &SQLRenameTableAction{NewName: "items"},
			},
			&SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the source, so dbdiff compares no row of it.`,
			},
			&SQLitePragmaForeignKeysInstruction{Enabled: true},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (identifier INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnSource(`INSERT INTO items (identifier, label) VALUES (1, 'old');`)

		driver.ExecOnTarget(`CREATE TABLE items (code INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnTarget(`INSERT INTO items (code, label) VALUES (1, 'first');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "items",
				Action: &SQLRenameColumnAction{
					ColumnName:    "identifier",
					NewColumnName: "code",
				},
			},
			&SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the source, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("items", "ORDER BY code")

		require.Equal(t, []map[string]any{
			{"code": int64(1), "label": "old"},
		}, rows)
	})

	t.Run("NullPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (email TEXT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (email, name) VALUES (NULL, 'Bob');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (email, name) VALUES (NULL, 'Alice');`)
		diff := driver.RequireInstructions([]Instruction{
			&SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &SQLConjunctionCondition{
					Conditions: []Condition{&SQLIsNullCondition{ColumnName: "email"}},
				},
			},
		})

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "")

		require.Equal(t, []map[string]any{{"email": nil, "name": "Alice"}}, rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)
		diff := driver.RequireInstructions(nil)

		driver.ExecOnSource(diff)

		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("RowIterationError", func(t *testing.T) {
		db, err := sql.Open("sqlite3_failing_rows", "")
		require.NoError(t, err)

		defer func() { require.NoError(t, db.Close()) }()

		driver := &SQLiteDriver{}

		_, err = driver.GetTableColumns(t.Context(), db, "users")
		require.ErrorIs(t, err, errRowIteration)
	})

	t.Run("SQLFileTarget", func(t *testing.T) {
		targetPath := WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		sourcePath := filepath.Join(t.TempDir(), "source.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, targetPath, sourcePath)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice');
		`)

		diff := driver.RequireInstructions([]Instruction{
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

		driver.ExecOnSource(diff)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": nil},
		}, rows)
	})

	t.Run("MigrationsDirectorySource", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		WriteSQLFile(t, migrationsDirectory, "002_add_email.up.sql", `
			ALTER TABLE users ADD COLUMN email TEXT;
		`)
		WriteSQLFile(t, migrationsDirectory, "002_add_email.down.sql", `
			ALTER TABLE users DROP COLUMN email;
		`)
		WriteSQLFile(t, migrationsDirectory, "010_add_index.up.sql", `
			CREATE INDEX users_email ON users (email);
		`)
		WriteSQLFile(t, migrationsDirectory, "notes.txt", `This file holds no SQL.`)

		sourcePath := filepath.Join(t.TempDir(), "source.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, migrationsDirectory, sourcePath)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				Name: "users",
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
						Name: "email",
						Type: "TEXT",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "users_email",
				TableName: "users",
				Keys:      []string{`"email"`},
			},
		})

		driver.ExecOnSource(diff)

		driver.ExecOnSource(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');`)
		rows := driver.FetchAllFromSource("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
		}, rows)
	})

	t.Run("SQLFileTargetOnBothSides", func(t *testing.T) {
		targetPath := WriteSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			CREATE TABLE audit (
				id INTEGER PRIMARY KEY
			);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, targetPath, sourcePath)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "email",
						Type: "TEXT",
					},
				},
			},
			&SQLDropTableInstruction{
				Name: "audit",
			},
		})

		driver.ExecOnSource(diff)
	})

	t.Run("EmptyDirectoryTarget", func(t *testing.T) {
		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, t.TempDir(), sourcePath)

		driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{
				Name: "users",
			},
		})
	})

	t.Run("InvalidSQLFileTarget", func(t *testing.T) {
		targetPath := WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (;`)
		sourcePath := filepath.Join(t.TempDir(), "source.sqlite")

		_, err := NewSQLiteDriver(t.Context(), &SQLiteDriverConfig{
			TargetDatabasePath: targetPath,
			SourceDatabasePath: sourcePath,
		})
		require.ErrorContains(t, err, "schema.sql")
	})

	t.Run("HistoryTableStaysOutOfTheDiff", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE dbdiff_migrations (
				version TEXT NOT NULL PRIMARY KEY,
				name TEXT NOT NULL,
				checksum TEXT NOT NULL,
				applied_at TEXT NOT NULL
			);
		`)

		driver.RequireInstructions(nil)
	})
}
