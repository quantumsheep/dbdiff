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
		source := writeSchema(t, "source.sql", "")
		target := writeSchema(t, "target.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), source, target)
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
		source := writeSchema(t, "source.sql", "")

		targetDirectory := t.TempDir()
		err := os.WriteFile(filepath.Join(targetDirectory, "schema.sql"), []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600)
		require.NoError(t, err)

		target := FolderDataSource{
			Path: targetDirectory,
		}

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), source, target)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Contains(t, statements[0].SQL, "CREATE TABLE")
		require.Contains(t, statements[0].Comment, "users")
	})

	t.Run("EqualSchemas", func(t *testing.T) {
		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"
		source := writeSchema(t, "source.sql", schema)
		target := writeSchema(t, "target.sql", schema)

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), source, target)
		require.NoError(t, err)
		require.Empty(t, statements)
	})

	t.Run("DataOption", func(t *testing.T) {
		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);\nINSERT INTO users (id) VALUES (1);\n"
		source := writeSchema(t, "source.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		target := writeSchema(t, "target.sql", schema)

		driver, err := NewDriver(SQLiteDriverName)
		require.NoError(t, err)

		statements, err := driver.Diff(t.Context(), source, target, WithData())
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

	t.Run("VersionOnTheSQLiteDriver", func(t *testing.T) {
		_, err := NewDriver(SQLiteDriverName, WithVersion("16"))
		require.ErrorContains(t, err, "version")
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
