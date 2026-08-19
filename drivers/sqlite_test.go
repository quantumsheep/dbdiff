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

func (d *TestingSQLiteDriver) ExecOnSource(sqlStatements string) {
	d.tb.Helper()

	_, err := d.SourceDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) ExecOnTarget(sqlStatements string) {
	d.tb.Helper()

	_, err := d.TargetDatabaseConnection.Exec(sqlStatements)
	require.NoError(d.tb, err)
}

// RequireInstructions compares the instructions of the diff. The SQL text of each kind
// belongs to instruction_test.go, so this method compares no text. It returns the rendered
// diff, so the caller applies it to the target.
func (d *TestingSQLiteDriver) RequireInstructions(expected []Instruction) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context())
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return RenderInstructions(instructions)
}

func (d *TestingSQLiteDriver) FetchAllFromTarget(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	columns, err := d.GetTableColumns(d.tb.Context(), d.TargetDatabaseConnection, table)
	require.NoError(d.tb, err)

	rows, err := d.TargetDatabaseConnection.Query(fmt.Sprintf("SELECT * FROM %q %s;", table, additionalRules))
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

	sourceDatabasePath := filepath.Join(tb.TempDir(), "source.sqlite")
	targetDatabasePath := filepath.Join(tb.TempDir(), "target.sqlite")

	return NewTestSQLiteDriverWithPaths(tb, sourceDatabasePath, targetDatabasePath)
}

func NewTestSQLiteDriverWithPaths(tb testing.TB, sourcePath string, targetPath string) *TestingSQLiteDriver {
	tb.Helper()

	driver, err := NewSQLiteDriver(tb.Context(), &SQLLiteDriverConfig{
		SourceDatabasePath: sourcePath,
		TargetDatabasePath: targetPath,
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

// A read method must return this error through rows.Err.
var errRowIteration = errors.New("row iteration failed")

// failingRows yields a fixed set of rows, then fails. It simulates a connection that
// breaks in the middle of a read.
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

// GetTableColumns reads the definition of the table first, and it reads PRAGMA table_xinfo
// second. The definition query answers with one row, so the failure of Next belongs to the
// PRAGMA alone.
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

		driver.ExecOnSource(`
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

		driver.ExecOnTarget(diff)
	})

	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
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

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLDropColumnAction{
					ColumnName: "email",
				},
			},
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
				full_name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
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

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
				first_name TEXT,
				last_name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT,
				name_b TEXT
			);
		`)

		// Two target columns hold the same attributes, so the rename guess is unsafe.
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

		driver.ExecOnTarget(diff)
	})

	t.Run("RenameColumnTakesOneCandidateOnly", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				first_name TEXT,
				last_name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name_a TEXT
			);

			INSERT INTO users (id, name_a) VALUES (1, 'Alice');
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

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "first_name": "Alice", "last_name": nil},
		}, rows)
	})

	t.Run("RenameTwoColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				x TEXT,
				y INTEGER
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				a TEXT,
				b INTEGER
			);

			INSERT INTO users (id, a, b) VALUES (1, 'Alice', 30);
		`)

		// The distinct types of x and y give each rename exactly one candidate, so the
		// detection is unambiguous. The order must follow the source columns: x before y.
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

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
				age INTEGER
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				age TEXT
			);

			INSERT INTO users (id, name, age) VALUES (1, 'Alice', '30'), (2, 'Bob', '25');
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
				name TEXT
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("CreateTableWithGeneratedColumns", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
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

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO measures (value) VALUES (4);`)

		rows := driver.FetchAllFromTarget("measures", "")
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
			CREATE TABLE measures (
				value INTEGER,
				triple INTEGER GENERATED ALWAYS AS (value * 3) VIRTUAL
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (value INTEGER);
			INSERT INTO measures (value) VALUES (2);
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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":  int64(2),
				"triple": int64(6),
			},
		}, rows)
	})

	// SQLite refuses an ADD COLUMN action that holds a STORED generated column, so this
	// change needs a new table.
	t.Run("AddStoredGeneratedColumnRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE measures (
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (value INTEGER);
			INSERT INTO measures (value) VALUES (3);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":  int64(3),
				"double": int64(6),
			},
		}, rows)
	})

	// The INSERT statement of a recreation names no generated column, because SQLite
	// computes that column.
	t.Run("RecreateTableWithGeneratedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE measures (
				label TEXT,
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				label INTEGER,
				value INTEGER,
				double INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);

			INSERT INTO measures (label, value) VALUES (7, 5);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("measures", "")
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
				multiple INTEGER GENERATED ALWAYS AS (value * 5) STORED
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE measures (
				value INTEGER,
				multiple INTEGER GENERATED ALWAYS AS (value * 2) STORED
			);

			INSERT INTO measures (value) VALUES (3);
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":    int64(3),
				"multiple": int64(15),
			},
		}, rows)
	})

	t.Run("CreateTableWithoutRowIDAndStrict", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
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

		driver.ExecOnTarget(diff)
	})

	// A table option belongs to the CREATE TABLE statement, so a change of that option
	// needs a new table.
	t.Run("AddStrictRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE TABLE events (id INTEGER, label TEXT) STRICT;`)
		driver.ExecOnTarget(`
			CREATE TABLE events (id INTEGER, label TEXT);
			INSERT INTO events (id, label) VALUES (1, 'start');
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("events", "")
		require.Equal(t, []map[string]any{
			{
				"id":    int64(1),
				"label": "start",
			},
		}, rows)
	})

	t.Run("CreateTableWithAutoIncrement", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);`)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateTableWithCollationAndChecks", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
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
				CheckConstraints: []string{"(length(name) < 100)"},
			},
		})

		driver.ExecOnTarget(diff)
	})

	// SQLite holds no ALTER COLUMN action, so a new collation needs a new table.
	t.Run("ModifyColumnCollationRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`CREATE TABLE people (name TEXT COLLATE NOCASE);`)
		driver.ExecOnTarget(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("people", "")
		require.Equal(t, []map[string]any{
			{"name": "Ada"},
		}, rows)
	})

	// A table constraint belongs to the CREATE TABLE statement, so a new check needs a new
	// table.
	t.Run("AddTableCheckRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE people (name TEXT, CHECK (length(name) < 100));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "_people_temp",
				Columns: []*SQLiteColumn{
					{
						Name: "name",
						Type: "TEXT",
					},
				},
				CheckConstraints: []string{"(length(name) < 100)"},
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
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("people", "")
		require.Equal(t, []map[string]any{
			{"name": "Ada"},
		}, rows)
	})

	// A table constraint can hold a name, so the parser reads the keyword CHECK after the
	// keyword CONSTRAINT too.
	t.Run("CreateTableWithANamedCheck", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
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
				CheckConstraints: []string{"(age > 0)"},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireInstructions([]Instruction{
			&SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateIndexes", func(t *testing.T) {
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
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("DropIndexes", func(t *testing.T) {
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
			&SQLDropIndexInstruction{Name: "idx_users_name"},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyIndexes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name, email);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (name);

			INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreatePartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_active",
				TableName: "users",
				Keys:      []string{`"name"`},
				Condition: &SQLiteIndexPredicateCondition{Expression: "active = 1"},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyPartialIndexCondition", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 0;
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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE UNIQUE INDEX idx_users_name ON users (lower(name), id);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Unique:    true,
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"lower(name)", `"id"`},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("ModifyExpressionIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (lower(name));
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
			CREATE INDEX idx_users_name ON users (upper(name));
		`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLDropIndexInstruction{Name: "idx_users_name"},
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{"lower(name)"},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateTableWithPartialIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT,
				active INTEGER NOT NULL
			);
			CREATE INDEX idx_users_active ON users (name) WHERE active = 1;

			INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1), (2, 'Bob', 0);
		`)

		// The recreation of the table drops the index. The last statement builds it again.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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
			CREATE INDEX idx_users_name ON users (name);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				name TEXT
			);
		`)

		// The UNIQUE constraint builds an index. That index stays out of the diff.
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "idx_users_name",
				TableName: "users",
				Keys:      []string{`"name"`},
			},
		})

		driver.ExecOnTarget(diff)
	})

	t.Run("RecreateTableWithUniqueColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age INTEGER
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				email TEXT UNIQUE,
				age TEXT
			);

			INSERT INTO users (id, email, age) VALUES (1, 'alice@example.com', '30'), (2, 'bob@example.com', '25');
		`)

		// The recreation keeps the constraint in the definition of the column. SQLite
		// refuses a CREATE INDEX statement with the name of the index of a constraint.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "alice@example.com", "age": int64(30)},
			{"id": int64(2), "email": "bob@example.com", "age": int64(25)},
		}, rows)

		// The new table refuses a second row with the email of the first row.
		_, err := driver.TargetDatabaseConnection.Exec(`INSERT INTO users (id, email, age) VALUES (3, 'alice@example.com', 40);`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: users.email")
	})

	t.Run("CreateTableWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)

		// A UNIQUE constraint of two or more columns keeps the order of its columns.
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
				UniqueConstraints: [][]string{{"team", "name"}},
			},
		})

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice');`)

		_, err := driver.TargetDatabaseConnection.Exec(`INSERT INTO members (id, team, name) VALUES (2, 'red', 'Alice');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: members.team, members.name")
	})

	t.Run("AddUniqueConstraint", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL,
				UNIQUE (team, name)
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE members (
				id INTEGER PRIMARY KEY,
				team TEXT NOT NULL,
				name TEXT NOT NULL
			);

			INSERT INTO members (id, team, name) VALUES (1, 'red', 'Alice'), (2, 'blue', 'Bob');
		`)

		// SQLite adds no table constraint, so the new constraint needs a recreation.
		diff := driver.RequireInstructions([]Instruction{
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
				UniqueConstraints: [][]string{{"team", "name"}},
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("members", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "team": "red", "name": "Alice"},
			{"id": int64(2), "team": "blue", "name": "Bob"},
		}, rows)
	})

	t.Run("CreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT,
				PRIMARY KEY (member, team)
			);
		`)

		// The key order differs from the column order of the table.
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

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');`)

		_, err := driver.TargetDatabaseConnection.Exec(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'guest');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: memberships.member, memberships.team")
	})

	t.Run("CreateTableWithIntegerPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE counters (
				id INTEGER PRIMARY KEY,
				total INTEGER
			);
		`)

		// An INTEGER PRIMARY KEY is the alias of the rowid. The form of a table constraint
		// changes the type of the key, so the key keeps the column constraint form.
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

		driver.ExecOnTarget(diff)
		driver.ExecOnTarget(`INSERT INTO counters (total) VALUES (5);`)

		rows := driver.FetchAllFromTarget("counters", "ORDER BY id")

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
				level INTEGER,
				PRIMARY KEY (team, member)
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level TEXT,
				PRIMARY KEY (team, member)
			);

			INSERT INTO memberships (team, member, level) VALUES ('red', 'Alice', '3'), ('blue', 'Bob', '1');
		`)

		// The type of the column "level" changes, so the new table keeps the whole key.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "blue", "member": "Bob", "level": int64(1)},
			{"team": "red", "member": "Alice", "level": int64(3)},
		}, rows)
	})

	t.Run("ModifyCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, member)
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, role)
			);

			INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');
		`)

		// The primary key of the target holds another column.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice", "role": "lead"},
		}, rows)
	})

	t.Run("RecreateTableWithPrimaryKeyAndIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (email, age) VALUES ('alice@example.com', '30'), ('bob@example.com', '25');
		`)

		// The recreation prints the explicit index only, and no index of the PRIMARY KEY.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY email")

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
				age INTEGER
			);
			CREATE INDEX idx_users_age ON users (age);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		// The recreation builds each index of the source. The index diff prints no second
		// statement for the same index.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30)},
		}, rows)
	})

	t.Run("RecreateTableWithRemovedIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		// The DROP TABLE statement of the recreation removes the index of the target. The
		// index diff prints no DROP INDEX statement for that index.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30)},
		}, rows)
	})

	t.Run("RecreateTableWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		// The DROP TABLE statement of the recreation removes the trigger of the target. The
		// recreation builds each trigger of the source again.
		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)

		triggers, err := driver.GetTableTriggers(t.Context(), driver.TargetDatabaseConnection, "users")
		require.NoError(t, err)
		require.Len(t, triggers, 1)
		require.Equal(t, "users_insert", triggers[0].Name)
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 999; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
			CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 4; END;
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

		driver.ExecOnTarget(diff)
	})

	t.Run("CreateTableWithTriggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
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

		driver.ExecOnTarget(diff)
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name FROM users;
			CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin';
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE VIEW old_view AS SELECT id FROM users;
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

		driver.ExecOnTarget(diff)
	})

	t.Run("ForeignKeys", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnSource(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT,
				FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
			);
		`)

		driver.ExecOnTarget(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE posts (
				id INTEGER PRIMARY KEY,
				user_id INTEGER,
				title TEXT
			);

			INSERT INTO posts (id, user_id, title) VALUES (1, 1, 'First Post'), (2, 1, 'Second Post');
		`)

		diff := driver.RequireInstructions([]Instruction{
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
		})

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("posts", "ORDER BY id")

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
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
		}, rows)
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO logs (message) VALUES ('start');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO logs (message) VALUES ('stop');`)

		diff := driver.RequireInstructions([]Instruction{
			&SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("logs", "")

		require.Equal(t, []map[string]any{
			{"message": "stop"},
		}, rows)
	})

	t.Run("CompareRowsWithAQuoteAndWithNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO notes (id, body) VALUES (1, 'it''s a note'), (2, NULL), (3, NULL);`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'not empty');`)

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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("notes", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "it's a note"},
			{"id": int64(2), "body": nil},
			{"id": int64(3), "body": nil},
		}, rows)
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnSource(`CREATE TABLE items (code INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnSource(`INSERT INTO items (code, label) VALUES (1, 'first');`)

		driver.ExecOnTarget(`CREATE TABLE items (identifier INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnTarget(`INSERT INTO items (identifier, label) VALUES (1, 'old');`)

		// The target holds no column with the name of the key.
		diff := driver.RequireInstructions([]Instruction{
			&SQLiteAlterTableInstruction{
				Name: "items",
				Action: &SQLRenameColumnAction{
					ColumnName:    "identifier",
					NewColumnName: "code",
				},
			},
			&SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the target, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("items", "ORDER BY code")

		require.Equal(t, []map[string]any{
			{"code": int64(1), "label": "old"},
		}, rows)
	})

	t.Run("NullPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (email TEXT PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (email, name) VALUES (NULL, 'Alice');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (email, name) VALUES (NULL, 'Bob');`)

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

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "")

		require.Equal(t, []map[string]any{{"email": nil, "name": "Alice"}}, rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		driver.ExecOnSource(schema)
		driver.ExecOnSource(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)

		driver.ExecOnTarget(schema)
		driver.ExecOnTarget(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		diff := driver.RequireInstructions(nil)

		driver.ExecOnTarget(diff)

		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("RowIterationError", func(t *testing.T) {
		db, err := sql.Open("sqlite3_failing_rows", "")
		require.NoError(t, err)

		defer db.Close()

		driver := &SQLiteDriver{}

		// The read method must return the failure of Next through rows.Err.
		_, err = driver.GetTableColumns(t.Context(), db, "users")
		require.ErrorIs(t, err, errRowIteration)
	})

	t.Run("SQLFileSource", func(t *testing.T) {
		sourcePath := WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		targetPath := filepath.Join(t.TempDir(), "target.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, sourcePath, targetPath)

		driver.ExecOnTarget(`
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

		driver.ExecOnTarget(diff)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

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

		targetPath := filepath.Join(t.TempDir(), "target.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, migrationsDirectory, targetPath)

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

		driver.ExecOnTarget(diff)

		driver.ExecOnTarget(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');`)
		rows := driver.FetchAllFromTarget("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
		}, rows)
	})

	t.Run("SQLFileSourceOnBothSides", func(t *testing.T) {
		sourcePath := WriteSQLFile(t, t.TempDir(), "source.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		targetPath := WriteSQLFile(t, t.TempDir(), "target.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			CREATE TABLE audit (
				id INTEGER PRIMARY KEY
			);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, sourcePath, targetPath)

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

		driver.ExecOnTarget(diff)
	})

	t.Run("EmptyDirectorySource", func(t *testing.T) {
		emptyDirectory := t.TempDir()
		targetPath := filepath.Join(t.TempDir(), "target.sqlite")

		_, err := NewSQLiteDriver(t.Context(), &SQLLiteDriverConfig{
			SourceDatabasePath: emptyDirectory,
			TargetDatabasePath: targetPath,
		})
		require.ErrorContains(t, err, "holds no .sql file")
	})

	t.Run("InvalidSQLFileSource", func(t *testing.T) {
		sourcePath := WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (;`)
		targetPath := filepath.Join(t.TempDir(), "target.sqlite")

		_, err := NewSQLiteDriver(t.Context(), &SQLLiteDriverConfig{
			SourceDatabasePath: sourcePath,
			TargetDatabasePath: targetPath,
		})
		require.ErrorContains(t, err, "schema.sql")
	})
}
