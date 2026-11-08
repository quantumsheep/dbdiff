package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

const mysqlTestConnectionString = "root:password@tcp(localhost:3306)/dbdiff"
const mariadbTestConnectionString = "root:password@tcp(localhost:3307)/dbdiff"

// The tests of the MySQL driver need the two servers of docker-compose.yml, and a runner
// of macOS or of Windows starts no service container. The variable stays empty on a
// runner of Linux, because a silent skip hides a server that fails there.
const skipMySQLServerVariable = "DBDIFF_TEST_SKIP_MYSQL"

func skipWithoutMySQLServer(tb testing.TB) {
	tb.Helper()

	if os.Getenv(skipMySQLServerVariable) != "" {
		tb.Skipf("%s stops the tests that need a MySQL server", skipMySQLServerVariable)
	}
}

type TestingMySQLDriver struct {
	*MySQLDriver

	tb   testing.TB
	conn *sql.DB

	current driversshared.DataSource
	desired driversshared.DataSource

	desiredDatabase string
	currentDatabase string

	CompareData  bool
	GuessRenames bool

	currentConnection *sql.DB
	desiredConnection *sql.DB
}

func NewTestMySQLDriver(tb testing.TB) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithServer(tb, mysqlTestConnectionString)
}

// MariaDB reads the same driver code, and its catalog reports the types, the defaults,
// the check clauses, and the view definitions with another text.
func NewTestMariaDBDriver(tb testing.TB) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithServer(tb, mariadbTestConnectionString)
}

func newTestMySQLDriverWithServer(tb testing.TB, serverConnectionString string) *TestingMySQLDriver {
	tb.Helper()

	skipWithoutMySQLServer(tb)

	conn, err := OpenMySQLConnection(serverConnectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	id := time.Now().UnixNano()
	desiredDatabase := fmt.Sprintf("desired_%d", id)
	currentDatabase := fmt.Sprintf("current_%d", id)

	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(desiredDatabase))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(currentDatabase))
	require.NoError(tb, err)

	// The connection stays open for this cleanup. A closed connection drops no database.
	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(desiredDatabase))
		require.NoError(tb, err)

		_, err = conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(currentDatabase))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	desiredConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, desiredDatabase)
	currentConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, currentDatabase)

	// The harness diffs a call at a time, and the driver fields go nil between two calls, so
	// the harness keeps its own connection of each side for ExecOnDesired and ExecOnCurrent.
	desiredConnection, err := OpenMySQLConnection(desiredConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, desiredConnection.Close())
	})

	currentConnection, err := OpenMySQLConnection(currentConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, currentConnection.Close())
	})

	driver := NewMySQLDriver(&MySQLDriverConfig{})

	return &TestingMySQLDriver{
		MySQLDriver:       driver,
		tb:                tb,
		conn:              conn,
		current:           driversshared.ConnectionStringDataSource{ConnectionString: currentConnectionString},
		desired:           driversshared.ConnectionStringDataSource{ConnectionString: desiredConnectionString},
		desiredDatabase:   desiredDatabase,
		currentDatabase:   currentDatabase,
		currentConnection: currentConnection,
		desiredConnection: desiredConnection,
	}
}

func NewTestMySQLDriverWithDesiredDirectory(tb testing.TB, directory string) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithDesiredDirectory(tb, mysqlTestConnectionString, directory)
}

func NewTestMariaDBDriverWithDesiredDirectory(tb testing.TB, directory string) *TestingMySQLDriver {
	tb.Helper()

	return newTestMySQLDriverWithDesiredDirectory(tb, mariadbTestConnectionString, directory)
}

// The driver materializes the desired directory as a scratch database on a temporary server.
func newTestMySQLDriverWithDesiredDirectory(
	tb testing.TB,
	serverConnectionString string,
	directory string,
) *TestingMySQLDriver {
	tb.Helper()

	skipWithoutMySQLServer(tb)

	conn, err := OpenMySQLConnection(serverConnectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	currentDatabase := fmt.Sprintf("current_%d", time.Now().UnixNano())

	_, err = conn.ExecContext(tb.Context(), "CREATE DATABASE "+QuoteIdentifier(currentDatabase))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE "+QuoteIdentifier(currentDatabase))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	currentConnectionString := testConnectionStringWithDatabase(tb, serverConnectionString, currentDatabase)

	currentConnection, err := OpenMySQLConnection(currentConnectionString)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, currentConnection.Close())
	})

	driver := NewMySQLDriver(&MySQLDriverConfig{})

	return &TestingMySQLDriver{
		MySQLDriver:       driver,
		tb:                tb,
		conn:              conn,
		current:           driversshared.ConnectionStringDataSource{ConnectionString: currentConnectionString},
		desired:           driversshared.ParseDataSource(directory),
		currentDatabase:   currentDatabase,
		currentConnection: currentConnection,
	}
}

func testConnectionStringWithDatabase(tb testing.TB, serverConnectionString string, database string) string {
	tb.Helper()

	config, err := ParseMySQLConnectionString(serverConnectionString)
	require.NoError(tb, err)

	config.DBName = database

	return config.FormatDSN()
}

// CREATE USER fails when the user exists already, so IF NOT EXISTS keeps the tests
// repeatable.
func (d *TestingMySQLDriver) CreateTestUser(name string) {
	d.tb.Helper()

	_, err := d.conn.ExecContext(d.tb.Context(),
		"CREATE USER IF NOT EXISTS "+QuoteIdentifier(name)+"@'%';")
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) ExecOnDesired(sqlStatements string) {
	d.tb.Helper()

	_, err := d.desiredConnection.ExecContext(d.tb.Context(), sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) ExecOnCurrent(sqlStatements string) {
	d.tb.Helper()

	_, err := d.currentConnection.ExecContext(d.tb.Context(), sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingMySQLDriver) RequireInstructions(expected driversshared.Instructions) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context(), d.current, d.desired, driversshared.DiffOptions{
		CompareData:  d.CompareData,
		GuessRenames: d.GuessRenames,
	})
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return instructions.String()
}

func (d *TestingMySQLDriver) RequireDiffApplies(expected driversshared.Instructions) {
	d.tb.Helper()

	d.ExecOnCurrent(d.RequireInstructions(expected))
	d.RequireInstructions(nil)
}

// The driver of MySQL gives an int64 value for an integer column and a byte slice for
// every other column of a plain query. The rows keep the int64 values and turn the byte
// slices into strings.
func (d *TestingMySQLDriver) FetchAllFromCurrent(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.currentConnection.QueryContext(
		d.tb.Context(),
		fmt.Sprintf("SELECT * FROM %s %s;", QuoteIdentifier(table), additionalRules),
	)
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
			value := columnValues[i]

			text, isText := value.([]byte)
			if isText {
				value = string(text)
			}

			row[column] = value
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

// The diff runs two times. The first diff proves the instructions. The second diff keeps
// the scratch database open, so the test can read its name and prove the drop.
func requireDiffOnScratchDatabase(
	t *testing.T,
	serverConnectionString string,
	scratchDatabase string,
	expected driversshared.Instructions,
) {
	t.Helper()

	directory := t.TempDir()
	sqltest.WriteSQLFile(t, directory, "001_users.sql", `
		CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));
		CREATE VIEW adult_users AS SELECT id FROM users WHERE age >= 18;
	`)

	driver := newTestMySQLDriverWithDesiredDirectory(t, serverConnectionString, directory)
	driver.ScratchDatabase = scratchDatabase

	driver.RequireDiffApplies(expected)

	release, err := driver.openSides(t.Context(), driver.current, driver.desired)
	require.NoError(t, err)

	require.Len(t, driver.scratchDatabases, 1)

	scratchDatabaseName := driver.scratchDatabases[0].name
	require.True(t, strings.HasPrefix(scratchDatabaseName, "dbdiff_desired_"))
	require.Nil(t, driver.scratchServer)

	require.NoError(t, release())

	var remainingDatabases int

	err = driver.conn.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", scratchDatabaseName).Scan(&remainingDatabases)
	require.NoError(t, err)
	require.Equal(t, 0, remainingDatabases)
}

func TestMySQLDriver(t *testing.T) {
	t.Run("NoDifference", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireInstructions(nil)
	})

	t.Run("MigrationHistoryTableStaysHidden", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnCurrent("CREATE TABLE dbdiff_migrations (version varchar(255) NOT NULL, PRIMARY KEY (version));")

		driver.RequireInstructions(nil)
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));
			CREATE TABLE ignored_created (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, name varchar(255), PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_dropped (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, description varchar(255), PRIMARY KEY (id));
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(255)",
					},
				},
			},
		})
	})
}

func TestMariaDBDriver(t *testing.T) {
	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));
			CREATE TABLE ignored_created (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, name varchar(255), PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_dropped (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE ignored_changed (id int NOT NULL, description varchar(255), PRIMARY KEY (id));
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(255)",
					},
				},
			},
		})
	})
}
