package driversshared

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectDriver(t *testing.T) {
	t.Run("SQLiteConnectionURL", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("sqlite://current.db"), ParseDataSource("sqlite://desired.db"))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("PostgresConnectionURL", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("postgres://user@localhost/current"), ParseDataSource("postgresql://user@localhost/desired"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordString", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("host=localhost user=app dbname=current"), ParseDataSource("host=localhost user=app dbname=desired"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordStringWithAQuotedValue", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("host=localhost user=app password='a b'"), ParseDataSource("host=localhost user=app password='c d'"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordStringWithAnUnknownKeyword", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("host=localhost future_option=1"), ParseDataSource("host=localhost future_option=2"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("FilePathWithAnEqualSign", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("stats=v2.db"), ParseDataSource("stats=v1.db"))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("DatabaseFilePath", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("current.db"), ParseDataSource("./desired.sqlite"))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("SQLFileAgainstDatabase", func(t *testing.T) {
		currentPath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver(ParseDataSource(currentPath), ParseDataSource("postgres://user@localhost/desired"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("DatabaseAgainstSQLFile", func(t *testing.T) {
		desiredPath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver(ParseDataSource("desired.db"), ParseDataSource(desiredPath))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("TwoSQLFiles", func(t *testing.T) {
		currentPath := writeDetectionSQLFile(t, "current.sql")
		desiredPath := writeDetectionSQLFile(t, "desired.sql")

		_, err := DetectDriver(ParseDataSource(currentPath), ParseDataSource(desiredPath))

		require.EqualError(t, err, fmt.Sprintf("cannot detect the driver of the current schema %q and the desired schema %q. Name the driver", currentPath, desiredPath))
	})

	t.Run("TwoDirectories", func(t *testing.T) {
		currentDirectory := t.TempDir()
		desiredDirectory := t.TempDir()

		_, err := DetectDriver(ParseDataSource(currentDirectory), ParseDataSource(desiredDirectory))

		require.EqualError(t, err, fmt.Sprintf("cannot detect the driver of the current schema %q and the desired schema %q. Name the driver", currentDirectory, desiredDirectory))
	})

	t.Run("UnknownScheme", func(t *testing.T) {
		_, err := DetectDriver(ParseDataSource("oracle://user@localhost/current"), ParseDataSource("oracle://user@localhost/desired"))

		require.EqualError(t, err, `cannot detect the driver of the current schema "oracle://user@localhost/current" and the desired schema "oracle://user@localhost/desired". Name the driver`)
	})

	t.Run("MySQLScheme", func(t *testing.T) {
		driverName, err := DetectDriver(ParseDataSource("mysql://user@localhost/current"), ParseDataSource("mysql://user@localhost/desired"))

		require.NoError(t, err)
		require.Equal(t, MySQLDriverName, driverName)
	})

	t.Run("MariaDBScheme", func(t *testing.T) {
		driverName, err := DetectDriver(ParseDataSource("mariadb://user@localhost/current"), ParseDataSource(""))

		require.NoError(t, err)
		require.Equal(t, MySQLDriverName, driverName)
	})

	t.Run("TwoDifferentEngines", func(t *testing.T) {
		_, err := DetectDriver(ParseDataSource("sqlite://current.db"), ParseDataSource("postgres://user@localhost/desired"))

		require.EqualError(t, err, `the current schema "sqlite://current.db" names the sqlite3 driver and the desired schema "postgres://user@localhost/desired" names the postgres driver. Name the driver`)
	})
}

func TestDetectDriverOfDatabase(t *testing.T) {
	t.Run("ConnectionURL", func(t *testing.T) {
		name, err := DetectDriverOfDatabase(ParseDataSource("postgres://user@localhost/app"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("UnknownSchemeNamesTheDatabaseOneTime", func(t *testing.T) {
		_, err := DetectDriverOfDatabase(ParseDataSource("oracle://user@localhost/app"))

		require.EqualError(t, err, `cannot detect the driver of the database "oracle://user@localhost/app". Name the driver`)
	})
}

func writeDetectionSQLFile(tb testing.TB, name string) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), name)

	err := os.WriteFile(path, []byte("CREATE TABLE users (id INTEGER);"), 0o600)
	require.NoError(tb, err)

	return path
}
