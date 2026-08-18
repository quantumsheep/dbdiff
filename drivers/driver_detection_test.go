package drivers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectDriver(t *testing.T) {
	t.Run("SQLiteConnectionURL", func(t *testing.T) {
		name, err := DetectDriver("sqlite://source.db", "sqlite://target.db")

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("PostgresConnectionURL", func(t *testing.T) {
		name, err := DetectDriver("postgres://user@localhost/source", "postgresql://user@localhost/target")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("PostgresKeywordString", func(t *testing.T) {
		name, err := DetectDriver("host=localhost user=app dbname=source", "host=localhost user=app dbname=target")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("DatabaseFilePath", func(t *testing.T) {
		name, err := DetectDriver("source.db", "./target.sqlite")

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("SQLFileAgainstDatabase", func(t *testing.T) {
		sourcePath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver(sourcePath, "postgres://user@localhost/target")

		require.NoError(t, err)
		require.Equal(t, PostgresDriverName, name)
	})

	t.Run("DatabaseAgainstSQLFile", func(t *testing.T) {
		targetPath := writeDetectionSQLFile(t, "schema.sql")

		name, err := DetectDriver("target.db", targetPath)

		require.NoError(t, err)
		require.Equal(t, SQLiteDriverName, name)
	})

	t.Run("TwoSQLFiles", func(t *testing.T) {
		sourcePath := writeDetectionSQLFile(t, "source.sql")
		targetPath := writeDetectionSQLFile(t, "target.sql")

		_, err := DetectDriver(sourcePath, targetPath)

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
		_, err := DetectDriver("mysql://user@localhost/source", "mysql://user@localhost/target")

		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot detect the driver")
	})

	t.Run("TwoDifferentEngines", func(t *testing.T) {
		_, err := DetectDriver("sqlite://source.db", "postgres://user@localhost/target")

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
