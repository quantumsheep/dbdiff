package driverssqlite

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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

	_, err := d.desiredDatabase().ExecContext(d.tb.Context(), sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingSQLiteDriver) ExecOnCurrent(sqlStatements string) {
	d.tb.Helper()

	_, err := d.currentDatabase().ExecContext(d.tb.Context(), sqlStatements)
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
func (d *TestingSQLiteDriver) database(dataSource driversshared.DataSource, connection **sql.DB, role string) *sql.DB {
	d.tb.Helper()

	if *connection != nil {
		return *connection
	}

	path, isSQLSource := driversshared.SQLSourcePath(dataSource)
	require.True(d.tb, isSQLSource, "the %s side holds no open connection", role)

	sqlSource, err := driversshared.NewSQLSource(path)
	require.NoError(d.tb, err)

	opened, err := sql.Open("sqlite", filepath.Join(d.tb.TempDir(), role+".sqlite"))
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

	rows, err := currentConnection.QueryContext(d.tb.Context(), fmt.Sprintf("SELECT * FROM %q %s;", table, additionalRules))
	require.NoError(d.tb, err)

	defer func() { require.NoError(d.tb, rows.Close()) }()

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
		connection, err := sql.Open("sqlite", TrimSQLitePrefix(currentPath))
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, connection.Close())
		})

		testingDriver.currentConnection = connection
	}

	if !driversshared.IsSQLSource(desired) {
		connection, err := sql.Open("sqlite", TrimSQLitePrefix(desiredPath))
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, connection.Close())
		})

		testingDriver.desiredConnection = connection
	}

	return testingDriver
}

func recreationPrologue() []driversshared.Instruction {
	return []driversshared.Instruction{
		&SQLitePragmaForeignKeysInstruction{},
		&SQLiteSavepointInstruction{},
	}
}

func recreationEpilogue(recreatedTableNames ...string) []driversshared.Instruction {
	return []driversshared.Instruction{
		&SQLiteForeignKeyCheckInstruction{
			Step: createForeignKeyCheckTable,
		},
		&SQLiteForeignKeyCheckInstruction{
			Step:                countForeignKeyViolations,
			RecreatedTableNames: recreatedTableNames,
		},
		&SQLiteForeignKeyCheckInstruction{
			Step: dropForeignKeyCheckTable,
		},
		&SQLiteSavepointInstruction{
			Release: true,
		},
		&SQLitePragmaForeignKeysInstruction{
			Enabled: true,
		},
	}
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
	return &failingRows{
		columns: s.columns,
		rows:    s.rows,
	}, nil
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

	return &failingStmt{
		columns: c.columns,
		rows:    c.rows,
	}, nil
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
	return &failingConn{
		columns: d.columns,
		rows:    d.rows,
	}, nil
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

	t.Run("IgnoredTableNameInOtherCase", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE ignored (id INTEGER PRIMARY KEY);`)

		driver.IgnoreTables = []string{"Ignored"}

		driver.RequireInstructions(nil)
	})
}
