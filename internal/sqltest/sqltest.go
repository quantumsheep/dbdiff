package sqltest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func WriteSQLFile(tb testing.TB, directory string, name string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, name)

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}
