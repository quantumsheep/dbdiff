package drivers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDriverAndDiff(t *testing.T) {
	writeSchema := func(t *testing.T, name string, content string) FileDataSource {
		t.Helper()

		path := filepath.Join(t.TempDir(), name)

		err := os.WriteFile(path, []byte(content), 0o600)
		require.NoError(t, err)

		return FileDataSource{
			Path: path,
		}
	}

	t.Run("StatementsOfASQLiteDiff", func(t *testing.T) {
		current := writeSchema(t, "current.sql", "")
		desired := writeSchema(t, "desired.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), current, desired)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Contains(t, statements[0].SQL, "CREATE TABLE")
		require.Contains(t, statements[0].Comment, "users")
	})

	t.Run("StatementsRenderAsOneText", func(t *testing.T) {
		statements := Statements{
			{
				SQL: "CREATE TABLE \"users\" (\"id\" INTEGER PRIMARY KEY);",
			},
			{
				SQL: "DROP TABLE \"projects\";",
			},
		}

		require.Equal(t,
			"CREATE TABLE \"users\" (\"id\" INTEGER PRIMARY KEY);\nDROP TABLE \"projects\";",
			statements.String())
	})

	t.Run("StatementsOfAFolderDataSourceDiff", func(t *testing.T) {
		current := writeSchema(t, "current.sql", "")

		desiredDirectory := t.TempDir()
		err := os.WriteFile(filepath.Join(desiredDirectory, "schema.sql"), []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600)
		require.NoError(t, err)

		desired := FolderDataSource{
			Path: desiredDirectory,
		}

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), current, desired)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Contains(t, statements[0].SQL, "CREATE TABLE")
		require.Contains(t, statements[0].Comment, "users")
	})

	t.Run("EqualSchemas", func(t *testing.T) {
		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"
		current := writeSchema(t, "current.sql", schema)
		desired := writeSchema(t, "desired.sql", schema)

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), current, desired)
		require.NoError(t, err)
		require.Empty(t, statements)
	})

	t.Run("DataOption", func(t *testing.T) {
		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);\nINSERT INTO users (id) VALUES (1);\n"
		current := writeSchema(t, "current.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		desired := writeSchema(t, "desired.sql", schema)

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), current, desired, WithData())
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Contains(t, statements[0].SQL, "INSERT INTO")
	})

	t.Run("PostgreSQLConfigOnTheSQLiteDriver", func(t *testing.T) {
		_, err := NewDriver(SQLiteDriverName, WithPostgreSQLDriverConfig(PostgreSQLDriverConfig{
			Schema: "public",
		}))
		require.ErrorContains(t, err, "postgres")
	})

	t.Run("MySQLConfigOnTheSQLiteDriver", func(t *testing.T) {
		_, err := NewDriver(SQLiteDriverName, WithMySQLDriverConfig(MySQLDriverConfig{
			ComparePrivileges: true,
		}))
		require.ErrorContains(t, err, "mysql")
	})

	t.Run("ServerVersionOnTheSQLiteDriver", func(t *testing.T) {
		_, err := NewDriver(SQLiteDriverName, WithServerVersion("16"))
		require.EqualError(t, err, "the server version option applies to the postgres driver and to the mysql driver")
	})

	t.Run("ScratchDatabaseOnTheSQLiteDriver", func(t *testing.T) {
		_, err := NewDriver(SQLiteDriverName, WithScratchDatabase("postgres://localhost/scratch"))
		require.EqualError(t, err, "the scratch database option applies to the postgres driver and to the mysql driver")
	})

	t.Run("ScratchDatabaseWithServerVersion", func(t *testing.T) {
		_, err := NewDriver(PostgresDriverName, WithScratchDatabase("postgres://localhost/scratch"), WithServerVersion("17"))
		require.EqualError(t, err, "the scratch database option and the server version option cannot go together, because dbdiff starts no temporary server for a scratch database")
	})

	t.Run("ScratchDatabaseOfAnotherEngine", func(t *testing.T) {
		_, err := NewDriver(MySQLDriverName, WithScratchDatabase("postgres://localhost/scratch"))
		require.EqualError(t, err, "the scratch database must be a connection URL of the mysql driver")
	})
}

func TestDetectDriver(t *testing.T) {
	name, err := DetectDriver(ConnectionStringDataSource{
		ConnectionString: "postgres://localhost/db",
	}, FileDataSource{
		Path: "schema.sql",
	})
	require.NoError(t, err)
	require.Equal(t, PostgresDriverName, name)
}
