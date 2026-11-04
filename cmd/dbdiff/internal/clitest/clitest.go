package clitest

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cmdroot "github.com/quantumsheep/dbdiff/cmd/dbdiff/cmd/root"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func Run(tb testing.TB, args ...string) Result {
	tb.Helper()

	var stdout, stderr bytes.Buffer

	command := cmdroot.Command()
	command.Reader = strings.NewReader("")
	command.Writer = &stdout
	command.ErrWriter = &stderr

	err := command.Run(context.Background(), append([]string{"dbdiff"}, args...))
	exitCode := cmdroot.ExitCode(err, &stderr)

	return Result{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}
}

type MigrationProject struct {
	MigrationsDirectory string
	DatabasePath        string
	ConfigPath          string
}

func NewMigrationProject(tb testing.TB, databaseStatements string) MigrationProject {
	tb.Helper()

	directory := tb.TempDir()

	migrationsDirectory := MakeMigrationsDirectory(tb, directory)

	databasePath := filepath.Join(directory, "app.sqlite")
	WriteSQLiteDatabase(tb, databasePath, databaseStatements)

	configPath := WriteMigrationConfig(tb, directory, "driver: sqlite3\nmigrations: "+migrationsDirectory+"\n")

	return MigrationProject{
		MigrationsDirectory: migrationsDirectory,
		DatabasePath:        databasePath,
		ConfigPath:          configPath,
	}
}

func WriteSQLiteDatabase(tb testing.TB, path string, sqlStatements string) {
	tb.Helper()

	database, err := sql.Open("sqlite", path)
	require.NoError(tb, err)

	defer func() {
		require.NoError(tb, database.Close())
	}()

	_, err = database.Exec(sqlStatements)
	require.NoError(tb, err)
}

func WriteMigrationConfig(tb testing.TB, directory string, content string) string {
	tb.Helper()

	path := filepath.Join(directory, "dbdiff.yaml")

	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(tb, err)

	return path
}

func MakeMigrationsDirectory(tb testing.TB, directory string) string {
	tb.Helper()

	migrations := filepath.Join(directory, "migrations")
	require.NoError(tb, os.Mkdir(migrations, 0o750))

	return migrations
}
