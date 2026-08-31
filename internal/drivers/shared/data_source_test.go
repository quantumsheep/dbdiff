package driversshared

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDataSource(t *testing.T) {
	t.Run("ConnectionURL", func(t *testing.T) {
		require.Equal(t, ConnectionStringDataSource{
			ConnectionString: "postgres://localhost/db",
		}, ParseDataSource("postgres://localhost/db"))
	})

	t.Run("SQLFile", func(t *testing.T) {
		require.Equal(t, FileDataSource{
			Path: "schema.sql",
		}, ParseDataSource("schema.sql"))
	})

	t.Run("Directory", func(t *testing.T) {
		directory := t.TempDir()

		require.Equal(t, FolderDataSource{
			Path: directory,
		}, ParseDataSource(directory))
	})

	t.Run("BarePath", func(t *testing.T) {
		require.Equal(t, ConnectionStringDataSource{
			ConnectionString: "stats.db",
		}, ParseDataSource("stats.db"))
	})

	t.Run("EmptyString", func(t *testing.T) {
		require.Equal(t, ConnectionStringDataSource{
			ConnectionString: "",
		}, ParseDataSource(""))
	})

	t.Run("SQLFileInAURL", func(t *testing.T) {
		require.Equal(t, ConnectionStringDataSource{
			ConnectionString: "postgres://localhost/db.sql",
		}, ParseDataSource("postgres://localhost/db.sql"))
	})

	t.Run("ExistingFileWithoutSQLExtension", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "database")

		err := os.WriteFile(path, nil, 0o600)
		require.NoError(t, err)

		require.Equal(t, ConnectionStringDataSource{
			ConnectionString: path,
		}, ParseDataSource(path))
	})
}

func TestIsSQLSource(t *testing.T) {
	require.True(t, IsSQLSource(FileDataSource{Path: "schema.sql"}))
	require.True(t, IsSQLSource(FolderDataSource{Path: "migrations"}))
	require.False(t, IsSQLSource(ConnectionStringDataSource{ConnectionString: "app.db"}))
}

func TestSQLSourcePath(t *testing.T) {
	path, ok := SQLSourcePath(FileDataSource{Path: "schema.sql"})
	require.True(t, ok)
	require.Equal(t, "schema.sql", path)

	path, ok = SQLSourcePath(FolderDataSource{Path: "migrations"})
	require.True(t, ok)
	require.Equal(t, "migrations", path)

	_, ok = SQLSourcePath(ConnectionStringDataSource{ConnectionString: "app.db"})
	require.False(t, ok)
}
