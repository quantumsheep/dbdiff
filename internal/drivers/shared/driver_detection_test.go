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
		name, err := DetectDriver(ParseDataSource("sqlite://target.db"), ParseDataSource("sqlite://source.db"))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("PostgresConnectionURL", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("postgres://user@localhost/target"), ParseDataSource("postgresql://user@localhost/source"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordString", func(t *testing.T) {
		name, err := DetectDriver(ParseDataSource("host=localhost user=app dbname=target"), ParseDataSource("host=localhost user=app dbname=source"))

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
		name, err := DetectDriver(ParseDataSource("target.db"), ParseDataSource("./source.sqlite"))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("SQLFileAgainstDatabase", func(t *testing.T) {
		targetPath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver(ParseDataSource(targetPath), ParseDataSource("postgres://user@localhost/source"))

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("DatabaseAgainstSQLFile", func(t *testing.T) {
		sourcePath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver(ParseDataSource("source.db"), ParseDataSource(sourcePath))

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("TwoSQLFiles", func(t *testing.T) {
		targetPath := writeDetectionSQLFile(t, "target.sql")
		sourcePath := writeDetectionSQLFile(t, "source.sql")

		_, err := DetectDriver(ParseDataSource(targetPath), ParseDataSource(sourcePath))

		require.EqualError(t, err, fmt.Sprintf("cannot detect the driver of the source %q and the target %q. Name the driver", targetPath, sourcePath))
	})

	t.Run("TwoDirectories", func(t *testing.T) {
		targetDirectory := t.TempDir()
		sourceDirectory := t.TempDir()

		_, err := DetectDriver(ParseDataSource(targetDirectory), ParseDataSource(sourceDirectory))

		require.EqualError(t, err, fmt.Sprintf("cannot detect the driver of the source %q and the target %q. Name the driver", targetDirectory, sourceDirectory))
	})

	t.Run("UnknownScheme", func(t *testing.T) {
		_, err := DetectDriver(ParseDataSource("oracle://user@localhost/target"), ParseDataSource("oracle://user@localhost/source"))

		require.EqualError(t, err, `cannot detect the driver of the source "oracle://user@localhost/target" and the target "oracle://user@localhost/source". Name the driver`)
	})

	t.Run("MySQLScheme", func(t *testing.T) {
		driverName, err := DetectDriver(ParseDataSource("mysql://user@localhost/target"), ParseDataSource("mysql://user@localhost/source"))

		require.NoError(t, err)
		require.Equal(t, MySQLDriverName, driverName)
	})

	t.Run("MariaDBScheme", func(t *testing.T) {
		driverName, err := DetectDriver(ParseDataSource("mariadb://user@localhost/target"), ParseDataSource(""))

		require.NoError(t, err)
		require.Equal(t, MySQLDriverName, driverName)
	})

	t.Run("TwoDifferentEngines", func(t *testing.T) {
		_, err := DetectDriver(ParseDataSource("sqlite://target.db"), ParseDataSource("postgres://user@localhost/source"))

		require.EqualError(t, err, `the source "sqlite://target.db" names the sqlite3 driver and the target "postgres://user@localhost/source" names the postgres driver. Name the driver`)
	})
}

func writeDetectionSQLFile(tb testing.TB, name string) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), name)

	err := os.WriteFile(path, []byte("CREATE TABLE users (id INTEGER);"), 0o600)
	require.NoError(tb, err)

	return path
}
