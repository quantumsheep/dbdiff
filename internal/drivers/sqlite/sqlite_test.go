package driverssqlite

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

type TestingSQLiteDriver struct {
	*SQLiteDriver

	tb          testing.TB
	current     driversshared.DataSource
	desired     driversshared.DataSource
	CompareData bool

	currentConnection *sql.DB
	desiredConnection *sql.DB
}

func (d *TestingSQLiteDriver) ExecOnDesired(sqlStatements string) {
	d.tb.Helper()

	_, err := d.desiredDatabase().Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) ExecOnCurrent(sqlStatements string) {
	d.tb.Helper()

	_, err := d.currentDatabase().Exec(sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) currentDatabase() *sql.DB {
	d.tb.Helper()

	return d.database(d.current, &d.currentConnection, "current")
}

func (d *TestingSQLiteDriver) desiredDatabase() *sql.DB {
	d.tb.Helper()

	return d.database(d.desired, &d.desiredConnection, "desired")
}

// A SQL source holds no connection of its own, because the harness opens one only for
// a side that a database path names. A test that reads or writes such a side materializes
// it here, on its first use. The driver materializes its own copy of a SQL source on
// every Diff call, so a write through this accessor never reaches the diff.
func (d *TestingSQLiteDriver) database(current driversshared.DataSource, connection **sql.DB, role string) *sql.DB {
	d.tb.Helper()

	if *connection != nil {
		return *connection
	}

	path, isSQLSource := driversshared.SQLSourcePath(current)
	require.True(d.tb, isSQLSource, "the %s side holds no open connection", role)

	sqlSource, err := driversshared.NewSQLSource(path)
	require.NoError(d.tb, err)

	opened, err := sql.Open("sqlite3", filepath.Join(d.tb.TempDir(), role+".sqlite"))
	require.NoError(d.tb, err)

	err = sqlSource.ApplyTo(d.tb.Context(), opened)
	require.NoError(d.tb, err)

	d.tb.Cleanup(func() {
		require.NoError(d.tb, opened.Close())
	})

	*connection = opened

	return *connection
}

func (d *TestingSQLiteDriver) RequireInstructions(expected driversshared.Instructions) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context(), d.current, d.desired, driversshared.DiffOptions{CompareData: d.CompareData})
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return instructions.String()
}

func (d *TestingSQLiteDriver) RequireDiffApplies(expected driversshared.Instructions) {
	d.tb.Helper()

	d.ExecOnCurrent(d.RequireInstructions(expected))
	d.RequireInstructions(nil)
}

func (d *TestingSQLiteDriver) FetchAllFromCurrent(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	currentConnection := d.currentDatabase()

	rows, err := currentConnection.Query(fmt.Sprintf("SELECT * FROM %q %s;", table, additionalRules))
	require.NoError(d.tb, err)

	columns, err := rows.Columns()
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
			row[column] = columnValues[i]
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

func NewTestSQLiteDriver(tb testing.TB) *TestingSQLiteDriver {
	tb.Helper()

	desiredDatabasePath := filepath.Join(tb.TempDir(), "desired.sqlite")
	currentDatabasePath := filepath.Join(tb.TempDir(), "current.sqlite")

	return NewTestSQLiteDriverWithPaths(tb, desiredDatabasePath, currentDatabasePath)
}

// The driver refuses a database file that is absent, so the harness creates each
// empty file first. A SQL source builds its own database.
func NewTestSQLiteDriverWithPaths(tb testing.TB, desiredPath string, currentPath string) *TestingSQLiteDriver {
	tb.Helper()

	for _, path := range []string{desiredPath, currentPath} {
		if driversshared.IsSQLSource(driversshared.ParseDataSource(path)) {
			continue
		}

		file, err := os.OpenFile(TrimSQLitePrefix(path), os.O_CREATE, 0o600)
		require.NoError(tb, err)
		require.NoError(tb, file.Close())
	}

	driver := NewSQLiteDriver(&SQLiteDriverConfig{})

	current := driversshared.ParseDataSource(currentPath)
	desired := driversshared.ParseDataSource(desiredPath)

	testingDriver := &TestingSQLiteDriver{
		SQLiteDriver: driver,
		tb:           tb,
		current:      current,
		desired:      desired,
	}

	if !driversshared.IsSQLSource(current) {
		connection, err := sql.Open("sqlite3", TrimSQLitePrefix(currentPath))
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, connection.Close())
		})

		testingDriver.currentConnection = connection
	}

	if !driversshared.IsSQLSource(desired) {
		connection, err := sql.Open("sqlite3", TrimSQLitePrefix(desiredPath))
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, connection.Close())
		})

		testingDriver.desiredConnection = connection
	}

	return testingDriver
}

func recreated(tableName string, instructions ...driversshared.Instruction) []driversshared.Instruction {
	wrapped := make([]driversshared.Instruction, len(instructions))

	for index, instruction := range instructions {
		wrapped[index] = &SQLiteTableRecreationInstruction{
			Instruction: instruction,
			TableName:   tableName,
		}
	}

	return wrapped
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

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

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

	t.Run("CreateTableWithoutRowIDAndStrict", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE sessions (id TEXT PRIMARY KEY, token TEXT) WITHOUT ROWID, STRICT;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

	t.Run("AddStrictRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE events (id INTEGER, label TEXT);
			INSERT INTO events (id, label) VALUES (1, 'start');
		`)
		driver.ExecOnDesired(`CREATE TABLE events (id INTEGER, label TEXT) STRICT;`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("events",
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_events_temp",
					ColumnNames:       []string{"id", "label"},
					SelectExpressions: []string{`"id"`, `"label"`},
					FromTableName:     "events",
				},
				&driversshared.SQLDropTableInstruction{Name: "events"},
				&SQLiteAlterTableInstruction{
					Name:   "_events_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "events"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("events", "")
		require.Equal(t, []map[string]any{
			{
				"id":    int64(1),
				"label": "start",
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

	t.Run("CreateTableWithCollationAndChecks", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE people (
				name TEXT COLLATE NOCASE,
				age INTEGER CHECK (age > 0),
				CHECK (length(name) < 100)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

	t.Run("CreateTableWithANamedPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT,
				member TEXT,
				CONSTRAINT pk_memberships PRIMARY KEY (team, member)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

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

	t.Run("CreateTableWithoutSpacesBeforeTheParentheses", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
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

	t.Run("ReorderedChecksGiveNoDiff", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE people (age INTEGER, CHECK (age > 0), CHECK (age < 200));`)
		driver.ExecOnDesired(`CREATE TABLE people (age INTEGER, CHECK (age < 200), CHECK (age > 0));`)
		driver.RequireInstructions(nil)
	})

	t.Run("ReorderColumnsRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE people (name TEXT, age INTEGER);
			INSERT INTO people (name, age) VALUES ('Ada', 36);
		`)
		driver.ExecOnDesired(`CREATE TABLE people (age INTEGER, name TEXT);`)
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
							Name: "age",
							Type: "INTEGER",
						},
						{
							Name: "name",
							Type: "TEXT",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_people_temp",
					ColumnNames:       []string{"age", "name"},
					SelectExpressions: []string{`"age"`, `"name"`},
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
			{"age": int64(36), "name": "Ada"},
		}, rows)
	})

	t.Run("AddTableCheckRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE people (name TEXT, CHECK (length(name) < 100));
		`)
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
							Name: "name",
							Type: "TEXT",
						},
					},
					CheckConstraints: []*SQLiteCheckConstraint{
						{Expression: "(length(name) < 100)"},
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

	t.Run("CreateTableWithANamedCheck", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE people (age INTEGER, CONSTRAINT age_is_positive CHECK (age > 0));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

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

	t.Run("RecreationAvoidsTheNameOfAnExistingTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE t (id INTEGER PRIMARY KEY);
			CREATE TABLE _t_temp (id INTEGER PRIMARY KEY);
			CREATE INDEX _T_TEMP2 ON _t_temp (id);
			INSERT INTO t (id) VALUES (1);
			INSERT INTO _t_temp (id) VALUES (2);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE t (id INTEGER PRIMARY KEY ON CONFLICT REPLACE);
			CREATE TABLE _t_temp (id INTEGER PRIMARY KEY);
			CREATE INDEX _T_TEMP2 ON _t_temp (id);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("t",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_t_temp3",
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
					TableName:         "_t_temp3",
					ColumnNames:       []string{"id"},
					SelectExpressions: []string{`"id"`},
					FromTableName:     "t",
				},
				&driversshared.SQLDropTableInstruction{Name: "t"},
				&SQLiteAlterTableInstruction{
					Name:   "_t_temp3",
					Action: &driversshared.SQLRenameTableAction{NewName: "t"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		require.Equal(t, []map[string]any{
			{"id": int64(1)},
		}, driver.FetchAllFromCurrent("t", ""))
		require.Equal(t, []map[string]any{
			{"id": int64(2)},
		}, driver.FetchAllFromCurrent("_t_temp", ""))
	})

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

	t.Run("CreateTableWithNamedConstraints", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (
				id INTEGER,
				owner INTEGER,
				CONSTRAINT items_positive CHECK (id > 0),
				CONSTRAINT items_unique UNIQUE (id),
				CONSTRAINT items_owner FOREIGN KEY (owner) REFERENCES parents(id)
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
	})

	t.Run("ModifyConstraintNameRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_old CHECK (id > 0));
			INSERT INTO items (id) VALUES (5);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_new CHECK (id > 0));
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("items",
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_items_temp",
					ColumnNames:       []string{"id"},
					SelectExpressions: []string{`"id"`},
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
			{"id": int64(5)},
		}, rows)
	})

	t.Run("EqualCheckConstraints", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_positive CHECK (id > 0));
		`)

		driver.ExecOnDesired(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_positive CHECK (id > 0));
		`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE VIRTUAL TABLE docs USING fts4(title, body);
			CREATE TABLE plain (id INTEGER);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

	t.Run("DropVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "docs"},
		})
	})

	t.Run("ModifyVirtualTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE docs USING fts4(title, body, tags);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "docs"},
			&SQLiteCreateVirtualTableInstruction{
				Definition: "CREATE VIRTUAL TABLE docs USING fts4(title, body, tags)",
			},
		})
	})

	t.Run("EqualVirtualTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)
		driver.ExecOnDesired(`CREATE VIRTUAL TABLE docs USING fts4(title, body);`)
		driver.RequireInstructions(nil)
	})

	t.Run("DropTables", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("DropTablesInTheReverseOrder", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "children"},
			&driversshared.SQLDropTableInstruction{Name: "parents"},
		})
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE "order ""list""" (
				id INTEGER PRIMARY KEY,
				name TEXT
			);
			CREATE INDEX "idx name" ON "order ""list""" (name);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "alice@example.com", "age": int64(30)},
			{"id": int64(2), "email": "bob@example.com", "age": int64(25)},
		}, rows)

		_, err := driver.currentConnection.Exec(`INSERT INTO users (id, email, age) VALUES (3, 'alice@example.com', 40);`)
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

		_, err := driver.currentConnection.Exec(`INSERT INTO members (id, team, name) VALUES (2, 'red', 'Alice');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: members.team, members.name")
	})

	t.Run("CreateTableAndIndexWithComments", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE notes (
				id INTEGER PRIMARY KEY, -- the key (one, two
				body TEXT CHECK (body <> ''), /* a comment, with ( */
				score INTEGER -- a last comment )
			);
			CREATE INDEX notes_score ON notes (score DESC, -- a key comment, (
				lower(body)) WHERE score > 0 -- a condition comment
			;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "notes",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:  "body",
						Type:  "TEXT",
						Check: "(body <> '')",
					},
					{
						Name: "score",
						Type: "INTEGER",
					},
				},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "notes_score",
				TableName: "notes",
				Keys:      []string{`"score" DESC`, "lower(body)"},
				Where:     "score > 0",
			},
		})
	})

	t.Run("CreateTableAndIndexWithABackslashStringAfterAWord", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE codes (
				name TEXT CHECK (name NOT LIKE '%x\%' ESCAPE'\'),
				code INTEGER
			);
			CREATE INDEX codes_name ON codes (name) WHERE name LIKE 'a\%' ESCAPE'\';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "codes",
				Columns: []*SQLiteColumn{
					{
						Name:  "name",
						Type:  "TEXT",
						Check: `(name NOT LIKE '%x\%' ESCAPE'\')`,
					},
					{
						Name: "code",
						Type: "INTEGER",
					},
				},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "codes_name",
				TableName: "codes",
				Keys:      []string{`"name"`},
				Where:     `name LIKE 'a\%' ESCAPE'\'`,
			},
		})
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
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
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
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

	t.Run("CreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT,
				PRIMARY KEY (member, team)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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

		driver.ExecOnCurrent(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');`)

		_, err := driver.currentConnection.Exec(`INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'guest');`)
		require.ErrorContains(t, err, "UNIQUE constraint failed: memberships.member, memberships.team")
	})

	t.Run("CreateTableWithIntegerPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE counters (
				id INTEGER PRIMARY KEY,
				total INTEGER
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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

		driver.ExecOnCurrent(`INSERT INTO counters (total) VALUES (5);`)

		rows := driver.FetchAllFromCurrent("counters", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "total": int64(5)},
		}, rows)
	})

	t.Run("RecreateTableWithCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level TEXT,
				PRIMARY KEY (team, member)
			);

			INSERT INTO memberships (team, member, level) VALUES ('red', 'Alice', '3'), ('blue', 'Bob', '1');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				level INTEGER,
				PRIMARY KEY (team, member)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("memberships",
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_memberships_temp",
					ColumnNames:       []string{"team", "member", "level"},
					SelectExpressions: []string{`"team"`, `"member"`, `"level"`},
					FromTableName:     "memberships",
				},
				&driversshared.SQLDropTableInstruction{Name: "memberships"},
				&SQLiteAlterTableInstruction{
					Name:   "_memberships_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "memberships"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "blue", "member": "Bob", "level": int64(1)},
			{"team": "red", "member": "Alice", "level": int64(3)},
		}, rows)
	})

	t.Run("ModifyCompositePrimaryKeyKeyModifiers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				PRIMARY KEY (member, team)
			);

			INSERT INTO memberships (team, member) VALUES ('red', 'Alice');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				PRIMARY KEY (member COLLATE NOCASE, team DESC)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("memberships",
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_memberships_temp",
					ColumnNames:       []string{"team", "member"},
					SelectExpressions: []string{`"team"`, `"member"`},
					FromTableName:     "memberships",
				},
				&driversshared.SQLDropTableInstruction{Name: "memberships"},
				&SQLiteAlterTableInstruction{
					Name:   "_memberships_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "memberships"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("memberships", "ORDER BY member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice"},
		}, rows)

		driver.RequireInstructions(nil)
	})

	t.Run("ModifyCompositePrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, role)
			);

			INSERT INTO memberships (team, member, role) VALUES ('red', 'Alice', 'lead');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE memberships (
				team TEXT NOT NULL,
				member TEXT NOT NULL,
				role TEXT NOT NULL,
				PRIMARY KEY (team, member)
			);
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("memberships",
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_memberships_temp",
					ColumnNames:       []string{"team", "member", "role"},
					SelectExpressions: []string{`"team"`, `"member"`, `"role"`},
					FromTableName:     "memberships",
				},
				&driversshared.SQLDropTableInstruction{Name: "memberships"},
				&SQLiteAlterTableInstruction{
					Name:   "_memberships_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "memberships"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("memberships", "ORDER BY team, member")

		require.Equal(t, []map[string]any{
			{"team": "red", "member": "Alice", "role": "lead"},
		}, rows)
	})

	t.Run("RecreateTableWithPrimaryKeyAndIndex", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
				age TEXT
			);
			CREATE INDEX idx_users_age ON users (age);

			INSERT INTO users (email, age) VALUES ('alice@example.com', '30'), ('bob@example.com', '25');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				email TEXT PRIMARY KEY,
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"email", "age"},
					SelectExpressions: []string{`"email"`, `"age"`},
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

		rows := driver.FetchAllFromCurrent("users", "ORDER BY email")

		require.Equal(t, []map[string]any{
			{"email": "alice@example.com", "age": int64(30)},
			{"email": "bob@example.com", "age": int64(25)},
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

	t.Run("RecreateTableWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
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
				&SQLiteCreateTriggerInstruction{
					Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		triggers, err := driver.GetTriggers(t.Context(), driver.currentConnection, "users")
		require.NoError(t, err)
		require.Len(t, triggers, 1)
		require.Equal(t, "users_insert", triggers[0].Name)
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 999; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
			CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 4; END;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
			},
			&SQLiteDropTriggerInstruction{Name: "users_update"},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END",
			},
			&SQLiteDropTriggerInstruction{Name: "users_audit"},
		})
	})

	t.Run("TriggersAndIndexesPrintInNameOrder", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE items (a INTEGER, b TEXT);`)

		driver.ExecOnDesired(`
			CREATE TABLE items (a INTEGER, b TEXT);
			CREATE INDEX items_b ON items (b);
			CREATE INDEX items_a ON items (a);
			CREATE TRIGGER items_tb AFTER UPDATE ON items BEGIN SELECT 2; END;
			CREATE TRIGGER items_ta AFTER INSERT ON items BEGIN SELECT 1; END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateIndexInstruction{
				Name:      "items_a",
				TableName: "items",
				Keys:      []string{`"a"`},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "items_b",
				TableName: "items",
				Keys:      []string{`"b"`},
			},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER items_ta AFTER INSERT ON items BEGIN SELECT 1; END",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER items_tb AFTER UPDATE ON items BEGIN SELECT 2; END",
			},
		})
	})

	t.Run("CreateTableWithTriggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE VIEW old_view AS SELECT id FROM users;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name FROM users;
			CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW admins_view AS SELECT name FROM users WHERE name = 'admin'",
			},
			&driversshared.SQLDropViewInstruction{Name: "users_view"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT name FROM users",
			},
			&driversshared.SQLDropViewInstruction{Name: "old_view"},
		})
	})

	t.Run("CreateViewTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users_view (id, name) VALUES (1, 'alice');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "alice"},
		}, rows)
	})

	t.Run("DropViewTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteDropTriggerInstruction{Name: "users_view_insert"},
		})
	})

	t.Run("RecreateViewWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name, id FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "users_view"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT name, id FROM users",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users_view (id, name) VALUES (2, 'bob');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "bob"},
		}, rows)
	})

	t.Run("CreateViewWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT id, name FROM users",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})
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

	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"3", "'Carol'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Robert'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "4",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
		}, rows)
	})

	t.Run("CompareRowsOfANewTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
			INSERT INTO users (id, name) VALUES (2, 'Bob'), (1, 'Alice');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"1", "'Alice'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

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

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO items (id, price) VALUES (1, 10), (2, 20);`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO items (id, price) VALUES (1, 15), (3, 30);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "items",
				ColumnNames: []string{"id", "price"},
				Expressions: []string{"3", "30"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "items",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "price",
						Expression: "15",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "items",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("items", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "price": int64(15), "total": int64(30)},
			{"id": int64(3), "price": int64(30), "total": int64(60)},
		}, rows)
	})

	t.Run("CompareRowsWithADateColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE events (id INTEGER PRIMARY KEY, at DATE);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO events (id, at) VALUES (1, 'alpha');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO events (id, at) VALUES (1, 'omega'), (2, '2024-01-01');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "events",
				ColumnNames: []string{"id", "at"},
				Expressions: []string{"2", "'2024-01-01'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "events",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "at",
						Expression: "'omega'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})
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

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO children (id, parent) VALUES (10, 1);
		`)

		driver.ExecOnDesired(schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDeleteInstruction{
				TableName: "children",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "10",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "parents",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO logs (message) VALUES ('stop');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO logs (message) VALUES ('start');`)
		diff := driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnCurrent(diff)

		rows := driver.FetchAllFromCurrent("logs", "")

		require.Equal(t, []map[string]any{
			{"message": "stop"},
		}, rows)
	})

	t.Run("CompareRowsWithAQuoteAndWithNull", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'not empty');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO notes (id, body) VALUES (1, 'it''s a note'), (2, NULL), (3, NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "notes",
				ColumnNames: []string{"id", "body"},
				Expressions: []string{"3", "NULL"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: "'it''s a note'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: "NULL",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("notes", "ORDER BY id")

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

		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO readings (id, value) VALUES (1, 9e999), (2, -9e999), (3, 1.5);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"1", "9e999"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "-9e999"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"3", "1.5"},
			},
		})
	})

	t.Run("CompareRowsOfATableWithTheKeyOnAnotherColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE items (a INTEGER PRIMARY KEY, b INTEGER);`)
		driver.ExecOnCurrent(`INSERT INTO items (a, b) VALUES (1, 2);`)

		driver.ExecOnDesired(`CREATE TABLE items (a INTEGER, b INTEGER PRIMARY KEY);`)
		driver.ExecOnDesired(`INSERT INTO items (a, b) VALUES (1, 2);`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("items",
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
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_items_temp",
					ColumnNames:       []string{"a", "b"},
					SelectExpressions: []string{`"a"`, `"b"`},
					FromTableName:     "items",
				},
				&driversshared.SQLDropTableInstruction{Name: "items"},
				&SQLiteAlterTableInstruction{
					Name:   "_items_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "items"},
				},
			),
			[]driversshared.Instruction{
				&driversshared.SQLCommentInstruction{
					Text: `The table "items" holds another primary key in the current schema, so dbdiff compares no row of it.`,
				},
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE items (identifier INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO items (identifier, label) VALUES (1, 'old');`)

		driver.ExecOnDesired(`CREATE TABLE items (code INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnDesired(`INSERT INTO items (code, label) VALUES (1, 'first');`)
		diff := driver.RequireInstructions([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "items",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "identifier",
					NewColumnName: "code",
				},
			},
			&driversshared.SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the current schema, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnCurrent(diff)

		rows := driver.FetchAllFromCurrent("items", "ORDER BY code")

		require.Equal(t, []map[string]any{
			{"code": int64(1), "label": "old"},
		}, rows)
	})

	t.Run("NullPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (email TEXT PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (email, name) VALUES (NULL, 'Bob');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (email, name) VALUES (NULL, 'Alice');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{&driversshared.SQLIsNullCondition{ColumnName: "email"}},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "")

		require.Equal(t, []map[string]any{{"email": nil, "name": "Alice"}}, rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)
		diff := driver.RequireInstructions(nil)

		driver.ExecOnCurrent(diff)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
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

	t.Run("SQLFileDesired", func(t *testing.T) {
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, desiredPath, currentPath)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			INSERT INTO users (id, name) VALUES (1, 'Alice');
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
		}, rows)
	})

	t.Run("MigrationsDirectoryAsDesired", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.up.sql", `
			ALTER TABLE users ADD COLUMN email TEXT;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.down.sql", `
			ALTER TABLE users DROP COLUMN email;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "010_add_index.up.sql", `
			CREATE INDEX users_email ON users (email);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "notes.txt", `This file holds no SQL.`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, migrationsDirectory, currentPath)

		driver.RequireDiffApplies([]driversshared.Instruction{
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

		driver.ExecOnCurrent(`INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');`)
		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "email": "alice@example.com"},
		}, rows)
	})

	t.Run("GooseMigrationsDirectorySource", func(t *testing.T) {
		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "00001_create_users.sql", `
			-- +goose Up
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			-- +goose Down
			DROP TABLE users;
		`)

		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewTestSQLiteDriverWithPaths(t, migrationsDirectory, currentPath)

		driver.RequireDiffApplies([]driversshared.Instruction{
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
				},
				ForeignKeys: []*SQLiteForeignKey{},
			},
		})
	})

	t.Run("SQLFileDesiredOnBothSides", func(t *testing.T) {
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "desired.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);
		`)

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			CREATE TABLE audit (
				id INTEGER PRIMARY KEY
			);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, desiredPath, currentPath)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "email",
						Type: "TEXT",
					},
				},
			},
			&driversshared.SQLDropTableInstruction{
				Name: "audit",
			},
		})

		driver.ExecOnCurrent(diff)
	})

	t.Run("EmptyDirectoryDesired", func(t *testing.T) {
		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INTEGER PRIMARY KEY);
		`)

		driver := NewTestSQLiteDriverWithPaths(t, t.TempDir(), currentPath)

		driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{
				Name: "users",
			},
		})
	})

	t.Run("InvalidSQLFileDesired", func(t *testing.T) {
		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (;`)
		currentPath := filepath.Join(t.TempDir(), "current.sqlite")

		driver := NewSQLiteDriver(&SQLiteDriverConfig{})

		_, err := driver.Diff(t.Context(), driversshared.ParseDataSource(currentPath),
			driversshared.ParseDataSource(desiredPath), driversshared.DiffOptions{})
		require.ErrorContains(t, err, "schema.sql")
	})

	t.Run("HistoryTableStaysOutOfTheDiff", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE dbdiff_migrations (
				version TEXT NOT NULL PRIMARY KEY,
				name TEXT NOT NULL,
				checksum TEXT NOT NULL,
				applied_at TEXT NOT NULL
			);
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);

			CREATE TABLE ignored_dropped (
				id INTEGER PRIMARY KEY
			);

			CREATE TABLE ignored_changed (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				email TEXT
			);

			CREATE TABLE ignored_created (
				id INTEGER PRIMARY KEY
			);

			CREATE TABLE ignored_changed (
				id INTEGER PRIMARY KEY,
				email TEXT NOT NULL
			);
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

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
	})
}
