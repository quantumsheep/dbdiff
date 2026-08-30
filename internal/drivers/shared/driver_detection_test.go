package driversshared

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectDriver(t *testing.T) {
	t.Run("SQLiteConnectionURL", func(t *testing.T) {
		name, err := DetectDriver("sqlite://target.db", "sqlite://source.db")

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("PostgresConnectionURL", func(t *testing.T) {
		name, err := DetectDriver("postgres://user@localhost/target", "postgresql://user@localhost/source")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordString", func(t *testing.T) {
		name, err := DetectDriver("host=localhost user=app dbname=target", "host=localhost user=app dbname=source")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordStringWithAQuotedValue", func(t *testing.T) {
		name, err := DetectDriver("host=localhost user=app password='a b'", "host=localhost user=app password='c d'")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordStringWithAnUnknownKeyword", func(t *testing.T) {
		name, err := DetectDriver("host=localhost future_option=1", "host=localhost future_option=2")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("FilePathWithAnEqualSign", func(t *testing.T) {
		name, err := DetectDriver("stats=v2.db", "stats=v1.db")

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("DatabaseFilePath", func(t *testing.T) {
		name, err := DetectDriver("target.db", "./source.sqlite")

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("SQLFileAgainstDatabase", func(t *testing.T) {
		targetPath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver(targetPath, "postgres://user@localhost/source")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("DatabaseAgainstSQLFile", func(t *testing.T) {
		sourcePath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver("source.db", sourcePath)

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("TwoSQLFiles", func(t *testing.T) {
		targetPath := writeDetectionSQLFile(t, "target.sql")
		sourcePath := writeDetectionSQLFile(t, "source.sql")

		_, err := DetectDriver(targetPath, sourcePath)

		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot detect the driver")
		require.Contains(t, err.Error(), "--driver")
	})

	t.Run("TwoDirectories", func(t *testing.T) {
		_, err := DetectDriver(t.TempDir(), t.TempDir())

		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot detect the driver")
	})

	t.Run("UnknownScheme", func(t *testing.T) {
		_, err := DetectDriver("oracle://user@localhost/target", "oracle://user@localhost/source")

		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot detect the driver")
	})

	t.Run("MySQLScheme", func(t *testing.T) {
		driverName, err := DetectDriver("mysql://user@localhost/target", "mysql://user@localhost/source")

		require.NoError(t, err)
		require.Equal(t, MySQLDriverName, driverName)
	})

	t.Run("MariaDBScheme", func(t *testing.T) {
		driverName, err := DetectDriver("mariadb://user@localhost/target", "")

		require.NoError(t, err)
		require.Equal(t, MySQLDriverName, driverName)
	})

	t.Run("TwoDifferentEngines", func(t *testing.T) {
		_, err := DetectDriver("sqlite://target.db", "postgres://user@localhost/source")

		require.Error(t, err)
		require.Contains(t, err.Error(), "names the sqlite3 driver")
		require.Contains(t, err.Error(), "names the postgres driver")
	})
}

func writeDetectionSQLFile(tb testing.TB, name string) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), name)

	err := os.WriteFile(path, []byte("CREATE TABLE users (id INTEGER);"), 0o600)
	require.NoError(tb, err)

	return path
}
