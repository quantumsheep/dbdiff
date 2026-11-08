package driverspostgres

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/quantumsheep/dbdiff/internal/sqltest"
	"github.com/stretchr/testify/require"
)

func TestPostgresSQLSource(t *testing.T) {
	skipWithoutPostgresServer(t)

	t.Run("SQLFileDesired", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.up.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.up.sql", `
			ALTER TABLE users ADD COLUMN email TEXT;
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_add_email.down.sql", `
			ALTER TABLE users DROP COLUMN email;
		`)

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		current := driversshared.ParseDataSource(currentPath)
		desired := driversshared.ParseDataSource(migrationsDirectory)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "email",
							Type: "text",
						},
					},
				},
			},
		}, instructions)

		requireDiffAppliesToSQLSource(t, currentPath, desired, instructions)
	})

	t.Run("EmptyDirectoryDesired", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		current := driversshared.ParseDataSource(currentPath)
		desired := driversshared.ParseDataSource(t.TempDir())

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&driversshared.SQLDropTableInstruction{
				Name: "users",
			},
		}, instructions)

		requireDiffAppliesToSQLSource(t, currentPath, desired, instructions)
	})

	t.Run("SQLSourceWithConcurrentIndex", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrationsDirectory := t.TempDir()

		sqltest.WriteSQLFile(t, migrationsDirectory, "001_create_users.sql", `-- dbdiff:no-transaction
			CREATE TABLE users (id INT NOT NULL, email TEXT, username TEXT);
			CREATE UNIQUE INDEX CONCURRENTLY "uq_users_email" ON users (email);
		`)
		sqltest.WriteSQLFile(t, migrationsDirectory, "002_set_username_not_null.sql", `-- dbdiff:no-transaction
			DROP INDEX CONCURRENTLY "uq_users_email";
			ALTER TABLE users ALTER COLUMN username SET NOT NULL;
			CREATE INDEX CONCURRENTLY "ix_users_email" ON users (email);
		`)

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE users (id INT NOT NULL, email TEXT, username TEXT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		current := driversshared.ParseDataSource(currentPath)
		desired := driversshared.ParseDataSource(migrationsDirectory)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresCreateIndexInstruction{
				Definition: `CREATE INDEX ix_users_email ON users USING btree (email)`,
			},
		}, instructions)

		requireDiffAppliesToSQLSource(t, currentPath, desired, instructions)
	})

	t.Run("SQLSourceWithRenamedNotNullColumn", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "desired.sql", `
			CREATE TABLE mfa (id INT, login_mfa_id INT NOT NULL);
		`)

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE mfa (id INT, login_id INT NOT NULL);
			ALTER TABLE mfa RENAME COLUMN login_id TO login_mfa_id;
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		current := driversshared.ParseDataSource(currentPath)
		desired := driversshared.ParseDataSource(desiredPath)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)
		require.Empty(t, instructions)
	})

	t.Run("SQLSourceWithNotNullChange", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "desired.sql", `
			CREATE TABLE mfa (id INT NOT NULL, label TEXT);
		`)

		currentPath := sqltest.WriteSQLFile(t, t.TempDir(), "current.sql", `
			CREATE TABLE mfa (id INT, label TEXT NOT NULL);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		current := driversshared.ParseDataSource(currentPath)
		desired := driversshared.ParseDataSource(desiredPath)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
			&PostgresAlterTableInstruction{
				Name: "mfa",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{
						ColumnName: "id",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "mfa",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{
						ColumnName: "label",
					},
				},
			},
		}, instructions)

		requireDiffAppliesToSQLSource(t, currentPath, desired, instructions)
	})

	t.Run("DetectScratchVersion", func(t *testing.T) {
		connection, err := sql.Open("pgx", postgresTestConnectionString)
		require.NoError(t, err)

		defer func() {
			require.NoError(t, connection.Close())
		}()

		var versionNumber int

		row := connection.QueryRowContext(t.Context(), "SELECT current_setting('server_version_num')::int")
		require.NoError(t, row.Scan(&versionNumber))

		major := versionNumber / 10000

		version := DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString)
		require.Equal(t, postgresScratchVersions[major], version)
		require.True(t, strings.HasPrefix(string(version), fmt.Sprintf("%d.", major)), version)
	})

	t.Run("DetectScratchVersionOfUnreachableServer", func(t *testing.T) {
		version := DetectPostgresScratchVersion(t.Context(), "postgres://user:password@127.0.0.1:1/absent?sslmode=disable")
		require.Equal(t, embeddedpostgres.PostgresVersion(""), version)
	})

	t.Run("ScratchVersionOfSources", func(t *testing.T) {
		sqlPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (id INT);`)

		liveVersion := DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString)
		require.NotEmpty(t, liveVersion)

		sqlSource := driversshared.ParseDataSource(sqlPath)
		liveSource := driversshared.ParseDataSource(postgresTestConnectionString)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		version := driver.scratchVersionOfSources(t.Context(), liveSource, sqlSource)
		require.Equal(t, liveVersion, version)

		version = driver.scratchVersionOfSources(t.Context(), sqlSource, liveSource)
		require.Equal(t, liveVersion, version)

		version = driver.scratchVersionOfSources(t.Context(), sqlSource, sqlSource)
		require.Equal(t, embeddedpostgres.PostgresVersion(""), version)

		versionedDriver := NewPostgresDriver(&PostgresDriverConfig{
			ScratchServerVersion: "17.11.0",
		})

		version = versionedDriver.scratchVersionOfSources(t.Context(), sqlSource, sqlSource)
		require.Equal(t, embeddedpostgres.PostgresVersion("17.11.0"), version)

		version = versionedDriver.scratchVersionOfSources(t.Context(), sqlSource, liveSource)
		require.Equal(t, liveVersion, version)
	})

	t.Run("ValidateScratchServerVersion", func(t *testing.T) {
		require.NoError(t, ValidateScratchServerVersion(""))
		require.NoError(t, ValidateScratchServerVersion("17"))
		require.NoError(t, ValidateScratchServerVersion("17.11.0"))

		require.EqualError(t, ValidateScratchServerVersion("99"), `unknown postgres version "99"`)
		require.EqualError(t, ValidateScratchServerVersion("5.2"), `unknown postgres version "5.2"`)
	})

	t.Run("SQLFileDesiredAgainstDatabase", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		harness := NewTestPostgresDriver(t)
		harness.ExecOnCurrent(`CREATE TABLE users (id INT NOT NULL);`)

		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{})

		current := harness.current
		desired := driversshared.ParseDataSource(desiredPath)

		version := driver.scratchVersionOfSources(t.Context(), current, desired)
		require.Equal(t, DetectPostgresScratchVersion(t.Context(), postgresTestConnectionString), version)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
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
		}, instructions)

		harness.ExecOnCurrent(instructions.String())

		instructions, err = driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)
		require.Empty(t, instructions)
	})

	t.Run("SQLFileDesiredOnAScratchDatabase", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)
		harness.ExecOnCurrent(`CREATE TABLE users (id INT NOT NULL);`)

		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `
			CREATE TABLE users (id INT NOT NULL, name TEXT);
			CREATE VIEW user_names AS SELECT name FROM users;
		`)

		driver := NewPostgresDriver(&PostgresDriverConfig{
			ScratchDatabase: postgresTestConnectionString,
		})

		current := harness.current
		desired := driversshared.ParseDataSource(desiredPath)

		instructions, err := driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)

		require.Equal(t, driversshared.Instructions{
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
			&PostgresCreateViewInstruction{
				Name:  "user_names",
				Query: " SELECT name\n   FROM users;",
			},
		}, instructions)

		harness.ExecOnCurrent(instructions.String())

		instructions, err = driver.Diff(t.Context(), current, desired, driversshared.DiffOptions{})
		require.NoError(t, err)
		require.Empty(t, instructions)

		release, err := driver.openSides(t.Context(), current, desired)
		require.NoError(t, err)

		scratchDatabaseNames := slices.Clone(driver.scratchDatabaseNames)
		require.Len(t, scratchDatabaseNames, 1)
		require.True(t, strings.HasPrefix(scratchDatabaseNames[0], "dbdiff_desired_"))

		require.NoError(t, release())

		var remainingDatabases int

		err = harness.conn.QueryRowContext(t.Context(),
			"SELECT count(*) FROM pg_database WHERE datname = $1", scratchDatabaseNames[0]).Scan(&remainingDatabases)
		require.NoError(t, err)
		require.Equal(t, 0, remainingDatabases)
	})
	t.Run("ScratchDatabaseThatTheCleanupCannotDrop", func(t *testing.T) {
		harness := NewTestPostgresDriver(t)

		desiredPath := sqltest.WriteSQLFile(t, t.TempDir(), "schema.sql", `CREATE TABLE users (id INT NOT NULL);`)

		driver := NewPostgresDriver(&PostgresDriverConfig{
			ScratchDatabase: postgresTestConnectionString,
		})

		release, err := driver.openSides(t.Context(), harness.current, driversshared.ParseDataSource(desiredPath))
		require.NoError(t, err)

		scratchDatabaseName := driver.scratchDatabaseNames[0]

		blockingConnection, err := sql.Open("pgx",
			strings.Replace(postgresTestConnectionString, "/dbdiff?", "/"+scratchDatabaseName+"?", 1))
		require.NoError(t, err)
		require.NoError(t, blockingConnection.PingContext(t.Context()))

		err = release()
		require.EqualError(t, err, fmt.Sprintf(
			"failed to drop the temporary database %q: ERROR: database %q is being accessed by other users (SQLSTATE 55006)",
			scratchDatabaseName, scratchDatabaseName))

		require.NoError(t, blockingConnection.Close())

		_, err = harness.conn.ExecContext(t.Context(), "DROP DATABASE "+driversshared.QuoteIdentifier(scratchDatabaseName))
		require.NoError(t, err)
	})
}
