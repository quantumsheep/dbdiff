package drivers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiff(t *testing.T) {
	writeSchema := func(t *testing.T, name string, content string) string {
		t.Helper()

		path := filepath.Join(t.TempDir(), name)

		err := os.WriteFile(path, []byte(content), 0o600)
		require.NoError(t, err)

		return path
	}

	t.Run("StatementsOfASQLiteDiff", func(t *testing.T) {
		source := writeSchema(t, "source.sql", "")
		target := writeSchema(t, "target.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		statements, err := Diff(t.Context(), source, target, WithDriver(SQLiteDriverName))
		require.NoError(t, err)
		require.Len(t, statements, 1)
		require.Contains(t, statements[0].SQL, "CREATE TABLE")
		require.Contains(t, statements[0].Comment, "users")
	})

	t.Run("EqualSchemas", func(t *testing.T) {
		schema := "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"
		source := writeSchema(t, "source.sql", schema)
		target := writeSchema(t, "target.sql", schema)

		statements, err := Diff(t.Context(), source, target, WithDriver(SQLiteDriverName))
		require.NoError(t, err)
		require.Empty(t, statements)
	})

	t.Run("SchemaOptionWithTheSQLiteDriver", func(t *testing.T) {
		source := writeSchema(t, "source.sql", "")
		target := writeSchema(t, "target.sql", "")

		_, err := Diff(t.Context(), source, target, WithDriver(SQLiteDriverName), WithSchema("public"))
		require.ErrorContains(t, err, "postgres")
	})
}
