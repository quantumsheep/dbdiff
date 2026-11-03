package driversshared

import (
	"database/sql"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestQueryAll(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	scanName := func(rows *sql.Rows) (string, error) {
		var name string

		err := rows.Scan(&name)

		return name, err
	}

	t.Run("ReadsEveryRow", func(t *testing.T) {
		names, err := QueryAll(t.Context(), db, "SELECT 'a' UNION ALL SELECT ? UNION ALL SELECT 'c'", scanName, "b")
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "c"}, names)
	})

	t.Run("NoRow", func(t *testing.T) {
		names, err := QueryAll(t.Context(), db, "SELECT 'a' WHERE 0", scanName)
		require.NoError(t, err)
		require.Nil(t, names)
	})

	t.Run("ScanError", func(t *testing.T) {
		_, err := QueryAll(t.Context(), db, "SELECT 'a'", func(rows *sql.Rows) (string, error) {
			return "", errors.New("the scan fails")
		})
		require.EqualError(t, err, "the scan fails")
	})

	t.Run("QueryError", func(t *testing.T) {
		_, err := QueryAll(t.Context(), db, "SELECT FROM", scanName)
		require.EqualError(t, err, `near "FROM": syntax error`)
	})
}
