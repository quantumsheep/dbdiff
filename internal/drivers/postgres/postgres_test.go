package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

const postgresTestConnectionString = "postgres://user:password@localhost:5432/dbdiff?sslmode=disable"

// The tests of the PostgreSQL driver need a server on the port 5432, and a runner of macOS
// or of Windows starts no service container. The variable stays empty on a runner of Linux,
// because a silent skip hides a server that fails there.
const skipPostgresServerVariable = "DBDIFF_TEST_SKIP_POSTGRES"

func skipWithoutPostgresServer(tb testing.TB) {
	tb.Helper()

	if os.Getenv(skipPostgresServerVariable) != "" {
		tb.Skipf("%s stops the tests that need a PostgreSQL server", skipPostgresServerVariable)
	}
}

type TestingPostgresDriver struct {
	*PostgresDriver

	tb   testing.TB
	conn *sql.DB

	current driversshared.DataSource
	desired driversshared.DataSource

	desiredSchema string
	currentSchema string

	CompareData bool

	currentConnection *sql.DB
	desiredConnection *sql.DB
}

func NewTestPostgresDriver(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	skipWithoutPostgresServer(tb)

	connectionString := postgresTestConnectionString
	conn, err := sql.Open("pgx", connectionString)
	require.NoError(tb, err)

	err = conn.PingContext(tb.Context())
	require.NoError(tb, err)

	id := time.Now().UnixNano()
	desiredSchema := fmt.Sprintf("desired_%d", id)
	currentSchema := fmt.Sprintf("current_%d", id)

	// CREATE ROLE fails when the role exists already, so the DO block tests it first.
	_, err = conn.ExecContext(tb.Context(), `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbdiff_reader') THEN
				CREATE ROLE dbdiff_reader;
			END IF;
		END
		$$;
	`)
	require.NoError(tb, err)

	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", desiredSchema))
	require.NoError(tb, err)
	_, err = conn.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", currentSchema))
	require.NoError(tb, err)

	// The connection stays open for this cleanup. A closed connection drops no schema.
	tb.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", desiredSchema))
		require.NoError(tb, err)

		_, err = conn.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", currentSchema))
		require.NoError(tb, err)

		require.NoError(tb, conn.Close())
	})

	desiredConnectionString := fmt.Sprintf("%s&search_path=%s", connectionString, desiredSchema)
	currentConnectionString := fmt.Sprintf("%s&search_path=%s", connectionString, currentSchema)

	// The harness diffs a call at a time, and the driver fields go nil between two calls, so
	// the harness keeps its own connection of each side for ExecOnDesired and ExecOnCurrent.
	desiredConnection, err := OpenPostgresConnection(desiredConnectionString, "")
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, desiredConnection.Close())
	})

	currentConnection, err := OpenPostgresConnection(currentConnectionString, "")
	require.NoError(tb, err)
	tb.Cleanup(func() {
		require.NoError(tb, currentConnection.Close())
	})

	driver := NewPostgresDriver(&PostgresDriverConfig{})

	return &TestingPostgresDriver{
		PostgresDriver:    driver,
		tb:                tb,
		conn:              conn,
		current:           driversshared.ConnectionStringDataSource{ConnectionString: currentConnectionString},
		desired:           driversshared.ConnectionStringDataSource{ConnectionString: desiredConnectionString},
		desiredSchema:     desiredSchema,
		currentSchema:     currentSchema,
		currentConnection: currentConnection,
		desiredConnection: desiredConnection,
	}
}

// PostgreSQL holds one extension one time per database, so a test of an extension needs
// this harness instead of NewTestPostgresDriver.
func NewTestPostgresDriverWithTwoDatabases(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	skipWithoutPostgresServer(tb)

	adminConn, err := sql.Open("pgx", postgresTestConnectionString)
	require.NoError(tb, err)

	err = adminConn.PingContext(tb.Context())
	require.NoError(tb, err)

	// A CREATE DATABASE statement needs a connection outside a transaction.
	id := time.Now().UnixNano()
	desiredDatabase := fmt.Sprintf("dbdiff_desired_%d", id)
	currentDatabase := fmt.Sprintf("dbdiff_current_%d", id)

	_, err = adminConn.ExecContext(tb.Context(), fmt.Sprintf("CREATE DATABASE %s", desiredDatabase))
	require.NoError(tb, err)
	_, err = adminConn.ExecContext(tb.Context(), fmt.Sprintf("CREATE DATABASE %s", currentDatabase))
	require.NoError(tb, err)

	// The admin connection stays open for this cleanup.
	tb.Cleanup(func() {
		_, err := adminConn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE %s", desiredDatabase))
		require.NoError(tb, err)

		_, err = adminConn.ExecContext(context.Background(), fmt.Sprintf("DROP DATABASE %s", currentDatabase))
		require.NoError(tb, err)

		require.NoError(tb, adminConn.Close())
	})

	desiredConnectionString := fmt.Sprintf("postgres://user:password@localhost:5432/%s?sslmode=disable", desiredDatabase)
	currentConnectionString := fmt.Sprintf("postgres://user:password@localhost:5432/%s?sslmode=disable", currentDatabase)

	desiredConnection, err := OpenPostgresConnection(desiredConnectionString, "")
	require.NoError(tb, err)

	currentConnection, err := OpenPostgresConnection(currentConnectionString, "")
	require.NoError(tb, err)

	// Go calls a cleanup in the reverse order of its registration, so this one closes every
	// connection before the database drop above. A drop fails while a connection uses it.
	tb.Cleanup(func() {
		require.NoError(tb, desiredConnection.Close())
		require.NoError(tb, currentConnection.Close())
	})

	driver := NewPostgresDriver(&PostgresDriverConfig{})

	return &TestingPostgresDriver{
		PostgresDriver:    driver,
		tb:                tb,
		conn:              adminConn,
		current:           driversshared.ConnectionStringDataSource{ConnectionString: currentConnectionString},
		desired:           driversshared.ConnectionStringDataSource{ConnectionString: desiredConnectionString},
		desiredSchema:     "public",
		currentSchema:     "public",
		currentConnection: currentConnection,
		desiredConnection: desiredConnection,
	}
}

func NewTestPostgresDriverWithPrivileges(tb testing.TB) *TestingPostgresDriver {
	tb.Helper()

	driver := NewTestPostgresDriver(tb)
	driver.ComparePrivileges = true

	return driver
}

func (d *TestingPostgresDriver) ExecOnDesired(sqlStatements string) {
	d.tb.Helper()
	_, err := d.desiredConnection.ExecContext(d.tb.Context(), sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) ExecOnCurrent(sqlStatements string) {
	d.tb.Helper()
	_, err := d.currentConnection.ExecContext(d.tb.Context(), sqlStatements)
	require.NoError(d.tb, err)
}

func (d *TestingPostgresDriver) RequireInstructions(expected driversshared.Instructions) string {
	d.tb.Helper()

	instructions, err := d.Diff(d.tb.Context(), d.current, d.desired, driversshared.DiffOptions{CompareData: d.CompareData})
	require.NoError(d.tb, err)
	require.Equal(d.tb, expected, instructions)

	return instructions.String()
}

func (d *TestingPostgresDriver) RequireDiffApplies(expected driversshared.Instructions) {
	d.tb.Helper()

	d.ExecOnCurrent(d.RequireInstructions(expected))
	d.RequireInstructions(nil)
}

func (d *TestingPostgresDriver) serialSequenceOfCurrent(tableName string, columnName string) sql.NullString {
	d.tb.Helper()

	var sequenceName sql.NullString

	err := d.currentConnection.QueryRowContext(d.tb.Context(), `SELECT pg_get_serial_sequence($1, $2)`,
		driversshared.QuoteIdentifier(tableName), columnName).Scan(&sequenceName)
	require.NoError(d.tb, err)

	return sequenceName
}

func (d *TestingPostgresDriver) FetchAllFromCurrent(table string, additionalRules string) []map[string]any {
	d.tb.Helper()

	rows, err := d.currentConnection.QueryContext(
		d.tb.Context(),
		fmt.Sprintf("SELECT * FROM %s %s;", driversshared.QuoteIdentifier(table), additionalRules),
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
			row[column] = columnValues[i]
		}

		results = append(results, row)
	}

	require.NoError(d.tb, rows.Err())

	return results
}

// A test that diffs two SQL sources builds no live connection of its own, because the
// scratch server that materializes them stops as soon as Diff returns. This helper gives
// such a test a schema on the already-running test server, applies the SQL source to it,
// and returns a connection that stays open so the test can prove that the generated SQL of
// the diff runs.
func openVerificationConnection(tb testing.TB, sqlSourcePath string) *sql.DB {
	tb.Helper()

	adminConnection, err := sql.Open("pgx", postgresTestConnectionString)
	require.NoError(tb, err)

	schemaName := fmt.Sprintf("verify_%d", time.Now().UnixNano())

	_, err = adminConnection.ExecContext(tb.Context(), fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		_, err := adminConnection.ExecContext(context.Background(), fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		require.NoError(tb, err)

		require.NoError(tb, adminConnection.Close())
	})

	connectionString := fmt.Sprintf("%s&search_path=%s", postgresTestConnectionString, schemaName)

	connection, err := OpenPostgresConnection(connectionString, "")
	require.NoError(tb, err)

	tb.Cleanup(func() {
		require.NoError(tb, connection.Close())
	})

	sqlSource, err := driversshared.NewSQLSource(sqlSourcePath)
	require.NoError(tb, err)

	require.NoError(tb, sqlSource.ApplyTo(tb.Context(), connection))

	return connection
}

func TestPostgresDriver(t *testing.T) {
	skipWithoutPostgresServer(t)

	t.Run("ExplicitSchema", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)

		harness.ExecOnDesired(`CREATE TABLE users (id INT NOT NULL);`)

		// The two connection strings hold no search path, so the schema fields of the
		// driver select it.
		driver := NewPostgresDriver(&PostgresDriverConfig{})
		driver.DesiredSchema = harness.desiredSchema
		driver.CurrentSchema = harness.currentSchema

		current := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}
		desired := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		diff := instructions.String()
		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL
);`, diff)

		harness.ExecOnCurrent(diff)
	})

	t.Run("UnknownSchema", func(t *testing.T) {
		driver := NewPostgresDriver(&PostgresDriverConfig{
			Schema: "schema_that_does_not_exist",
		})

		current := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}
		desired := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}

		_, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the desired database has no schema with the name "schema_that_does_not_exist"`)
	})

	t.Run("HistoryTableStaysOutOfTheDiff", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE dbdiff_migrations (
				version text NOT NULL PRIMARY KEY,
				name text NOT NULL,
				checksum text NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now()
			);
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("HistoryTableStaysOutOfThePrivilegesDiff", func(t *testing.T) {
		driver := NewTestPostgresDriverWithPrivileges(t)

		driver.ExecOnCurrent(`
			CREATE TABLE dbdiff_migrations (
				version text NOT NULL PRIMARY KEY,
				name text NOT NULL,
				checksum text NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now()
			);
			GRANT SELECT ON dbdiff_migrations TO dbdiff_reader;
		`)

		driver.RequireInstructions(nil)
	})

	t.Run("IgnoredTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT, name TEXT);
			CREATE TABLE ignored_created (id INT);
			CREATE TABLE ignored_changed (id INT, name TEXT);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE TABLE ignored_dropped (id INT);
			CREATE TABLE ignored_changed (id INT, email TEXT);
		`)

		driver.IgnoreTables = []string{"ignored_created", "ignored_dropped", "ignored_changed"}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "name",
							Type: "text",
						},
					},
				},
			},
		})
	})

	t.Run("SchemaNameWithRegularExpressionCharacters", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)

		id := time.Now().UnixNano()
		desiredSchema := fmt.Sprintf("dbdiff.desired+$%d", id)
		currentSchema := fmt.Sprintf("dbdiff.current+$%d", id)
		schema := `
			CREATE TYPE mood AS ENUM ('sad', 'happy');
			CREATE TABLE people (id INT, current_mood mood);
			CREATE INDEX people_id ON people (id);
			CREATE STATISTICS people_statistics ON id, current_mood FROM people;
			CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END';
			CREATE TRIGGER people_touch BEFORE INSERT ON people FOR EACH ROW EXECUTE FUNCTION touch();
			CREATE RULE people_guard AS ON DELETE TO people DO INSTEAD NOTHING;
			CREATE MATERIALIZED VIEW people_ids AS SELECT id FROM people;
			CREATE INDEX people_ids_id ON people_ids (id);
		`

		for _, name := range []string{desiredSchema, currentSchema} {
			_, err := harness.conn.ExecContext(t.Context(), "CREATE SCHEMA "+driversshared.QuoteIdentifier(name))
			require.NoError(t, err)

			t.Cleanup(func() {
				_, err := harness.conn.ExecContext(context.Background(),
					"DROP SCHEMA "+driversshared.QuoteIdentifier(name)+" CASCADE")
				require.NoError(t, err)
			})

			connection, err := OpenPostgresConnection(postgresTestConnectionString, name)
			require.NoError(t, err)

			_, err = connection.ExecContext(t.Context(), schema)
			require.NoError(t, err)
			require.NoError(t, connection.Close())
		}

		driver := NewPostgresDriver(&PostgresDriverConfig{})
		driver.DesiredSchema = desiredSchema
		driver.CurrentSchema = currentSchema

		source := driversshared.ConnectionStringDataSource{ConnectionString: postgresTestConnectionString}

		instructions, err := driver.Diff(t.Context(), source, source, driversshared.DiffOptions{})
		require.NoError(t, err)
		require.Empty(t, instructions)
	})
}
