package migrations

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quantumsheep/dbdiff/internal/drivers"
	"github.com/stretchr/testify/require"
)

func TestMigrationStatusAndPreview(t *testing.T) {
	t.Run("StatusOfADatabaseWithNoHistoryTable", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Len(t, set.Pending(), 1)

		require.Empty(t, migrator.TableNames())

		status := RenderMigrationStatus(set)
		require.Contains(t, status, "20260814101500_init")
		require.Contains(t, status, "pending")
	})

	t.Run("PreviewPrintsEachPendingFile", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE INDEX users_id ON users (id);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		preview, err := RenderMigrationPreview(set)
		require.NoError(t, err)
		require.Contains(t, preview, "20260814101500_init")
		require.Contains(t, preview, "CREATE INDEX users_id")
		require.NotContains(t, preview, "[1/")
	})

	t.Run("PreviewOfNoPendingMigration", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		set, err := LoadMigrationSet(t.Context(), migrator, t.TempDir())
		require.NoError(t, err)

		preview, err := RenderMigrationPreview(set)
		require.NoError(t, err)
		require.Contains(t, preview, "up to date")
	})

	t.Run("PreviewNamesAnOutOfOrderFile", func(t *testing.T) {
		outOfOrderFile := &Migration{
			Version: "20260814101500",
			Name:    "init",
			Path:    "20260814101500_init.sql",
		}

		appliedFile := &Migration{
			Version:  "20260822143000",
			Name:     "add_created_at",
			Path:     "20260822143000_add_created_at.sql",
			Checksum: "bbb",
		}

		set := NewMigrationSet([]*Migration{outOfOrderFile, appliedFile}, []AppliedMigration{
			{
				Version:  "20260822143000",
				Name:     "add_created_at",
				Checksum: "bbb",
			},
		})
		require.Equal(t, MigrationOutOfOrder, set.Entries[0].State)

		preview, err := RenderMigrationPreview(set)
		require.NoError(t, err)
		require.Contains(t, preview, "20260814101500_init")
		require.Contains(t, preview, "out of order")
		require.NotContains(t, preview, "up to date")
	})

	t.Run("RunPreviewChangesNothing", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, RunMigrationPreview(t.Context(), migrator, set, io.Discard))
		require.Empty(t, migrator.TableNames())
	})

	t.Run("RunPreviewReportsABadStatement", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"NOT SQL AT ALL;\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.Error(t, RunMigrationPreview(t.Context(), migrator, set, io.Discard))
	})

	t.Run("RunPreviewSeesTheFilesBeforeIt", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		WriteSQLFile(t, directory, "20260822143000_email.sql",
			"ALTER TABLE users ADD COLUMN email TEXT;\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, RunMigrationPreview(t.Context(), migrator, set, io.Discard))
		require.Empty(t, migrator.TableNames())
	})

	t.Run("RunPreviewSkipsAFileThatOptsOutOfTheTransaction", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"-- dbdiff:no-transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, RunMigrationPreview(t.Context(), migrator, set, output))

		require.Contains(t, output.String(), "20260814101500_init")
		require.Empty(t, migrator.TableNames())
	})
}

func TestApplyMigrations(t *testing.T) {
	t.Run("ApplyOneFile", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
				"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", output))
		require.Contains(t, output.String(), "20260814101500_init")

		require.Equal(t, []string{"dbdiff_migrations", "notes", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
	})

	t.Run("ApplyRunsOnlyThePendingFiles", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		WriteSQLFile(t, directory, "20260822143000_notes.sql",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		require.Equal(t, []string{"dbdiff_migrations", "notes", "users"}, migrator.TableNames())
	})

	t.Run("ApplyStopsAfterTheNamedVersion", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		WriteSQLFile(t, directory, "20260822143000_notes.sql",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "20260814101500", output))
		require.Contains(t, output.String(), "20260814101500_init")
		require.NotContains(t, output.String(), "20260822143000_notes")

		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
	})

	t.Run("ApplyRefusesAVersionThatNoFileHolds", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = ApplyMigrations(t.Context(), migrator, set, "20990101000000", io.Discard)
		require.ErrorContains(t, err, "no migration with the version 20990101000000")

		require.Empty(t, migrator.TableNames())
	})

	t.Run("ApplyWithNoPendingFile", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		set, err := LoadMigrationSet(t.Context(), migrator, t.TempDir())
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", output))
		require.Contains(t, output.String(), "up to date")
	})

	t.Run("ABadStatementRollsTheWholeFileBack", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\nNOT SQL AT ALL;\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = ApplyMigrations(t.Context(), migrator, set, "", io.Discard)
		require.ErrorContains(t, err, "20260814101500_init")

		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Empty(t, applied)
	})

	t.Run("AChangedFileStopsTheRun", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n")

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = ApplyMigrations(t.Context(), migrator, set, "", io.Discard)
		require.ErrorContains(t, err, "changed")
	})

	t.Run("ApplyToleratesAnotherProcessThatAppliedTheFileFirst", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Len(t, set.Pending(), 1)

		other, err := NewSQLiteMigrator(migrator.Path)
		require.NoError(t, err)

		defer func() { require.NoError(t, other.Close()) }()

		otherSet, err := LoadMigrationSet(t.Context(), other, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), other, otherSet, "", io.Discard))

		output := &bytes.Buffer{}
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", output))
		require.Contains(t, output.String(), "up to date")

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
	})

	t.Run("GenerateThenApplyLeavesNoDiff", func(t *testing.T) {
		directory := t.TempDir()

		schema := WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);\n"+
				"CREATE INDEX users_email ON users (email);\n")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		paths := GenerateSQLiteMigration(t, schema, migrations)
		require.NotEmpty(t, paths)

		sourcePath := filepath.Join(directory, "source.sqlite")

		migrator, err := NewSQLiteMigrator(sourcePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, migrator.Close()) }()

		set, err := LoadMigrationSet(t.Context(), migrator, migrations)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		driver, err := drivers.NewSQLiteDriver(t.Context(), &drivers.SQLiteDriverConfig{
			TargetDatabasePath: schema,
			SourceDatabasePath: sourcePath,
		})
		require.NoError(t, err)

		defer func() { require.NoError(t, driver.Close()) }()

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.Empty(t, instructions)
	})

	t.Run("AFileThatOptsOutOfTheTransaction", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"-- dbdiff:no-transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))
		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
	})

	t.Run("AFileWithoutATransactionThatFailsInTheMiddle", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"-- dbdiff:no-transaction\n"+
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = ApplyMigrations(t.Context(), migrator, set, "", io.Discard)
		require.Error(t, err)
		require.Contains(t, err.Error(), "stay applied")

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
		require.True(t, applied[0].Dirty)

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationDirty, set.Entries[0].State)

		err = ApplyMigrations(t.Context(), migrator, set, "", io.Discard)
		require.Error(t, err)
		require.Contains(t, err.Error(), "dirty")
	})
}

func TestRunMigrationRepair(t *testing.T) {
	t.Run("AChangedFileTakesTheChecksumOfTheFile", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationChanged, set.Entries[0].State)

		output := &bytes.Buffer{}
		require.NoError(t, RunMigrationRepair(t.Context(), migrator, set, output))
		require.Contains(t, output.String(), "Updated the checksum")

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationApplied, set.Entries[0].State)
	})

	t.Run("AMissingFileLosesItsRow", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		require.NoError(t, os.Remove(filepath.Join(directory, "20260814101500_init.sql")))

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationMissing, set.Entries[0].State)

		output := &bytes.Buffer{}
		require.NoError(t, RunMigrationRepair(t.Context(), migrator, set, output))
		require.Contains(t, output.String(), "Deleted the row")

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Empty(t, set.Entries)
	})

	t.Run("ADirtyFileLosesItsRow", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"-- dbdiff:no-transaction\n"+
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
				"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Error(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		_, err = migrator.Connection.ExecContext(t.Context(), "DROP TABLE users;")
		require.NoError(t, err)

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationDirty, set.Entries[0].State)

		output := &bytes.Buffer{}
		require.NoError(t, RunMigrationRepair(t.Context(), migrator, set, output))
		require.Contains(t, output.String(), "Deleted the dirty row")

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.Equal(t, MigrationPending, set.Entries[0].State)
	})

	t.Run("ACleanRecordNeedsNoRepair", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		set, err = LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, RunMigrationRepair(t.Context(), migrator, set, output))
		require.Contains(t, output.String(), "The record needs no repair.")
	})
}

func TestStepMigration(t *testing.T) {
	writeTwoFiles := func(tb testing.TB, directory string) {
		tb.Helper()

		WriteSQLFile(tb, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
		WriteSQLFile(tb, directory, "20260822143000_notes.sql",
			"CREATE TABLE notes (id INTEGER PRIMARY KEY);\n")
	}

	t.Run("ApplyEachFile", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()
		writeTwoFiles(t, directory)

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader("a\na\n"), output))

		require.Equal(t, []string{"dbdiff_migrations", "notes", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 2)
	})

	t.Run("ApplyTheRest", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()
		writeTwoFiles(t, directory)

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader("r\n"), io.Discard))
		require.Equal(t, []string{"dbdiff_migrations", "notes", "users"}, migrator.TableNames())
	})

	t.Run("QuitKeepsTheFilesThatCommitted", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()
		writeTwoFiles(t, directory)

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader("a\nq\n"), output))

		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())

		applied, err := migrator.AppliedMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, applied, 1)
	})

	t.Run("AFileOfSeveralStatementsAppliesWhole", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY, updated_at TEXT);\n"+
				"CREATE TRIGGER touch AFTER UPDATE ON users\n"+
				"BEGIN\n"+
				"  UPDATE users SET updated_at = 'now' WHERE id = NEW.id;\n"+
				"END;\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader("a\n"), io.Discard))
		require.Equal(t, []string{"dbdiff_migrations", "users"}, migrator.TableNames())
	})

	t.Run("AFileOfAnOlderDbdiffAppliesWhole", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()

		WriteSQLFile(t, directory, "20260814101500_init.sql",
			"-- dbdiff 1.0\n\n-- dbdiff:statement\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n"+
				"-- dbdiff:statement\nCREATE TABLE notes (id INTEGER PRIMARY KEY);\n")

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader("a\n"), io.Discard))
		require.Equal(t, []string{"dbdiff_migrations", "notes", "users"}, migrator.TableNames())
	})

	t.Run("AnUnknownAnswerAsksAgain", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()
		writeTwoFiles(t, directory)

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader("x\nr\n"), output))

		require.Contains(t, output.String(), "Answer a, r, or q")
		require.Equal(t, []string{"dbdiff_migrations", "notes", "users"}, migrator.TableNames())
	})

	t.Run("AClosedInputNamesTheUpCommand", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)
		directory := t.TempDir()
		writeTwoFiles(t, directory)

		set, err := LoadMigrationSet(t.Context(), migrator, directory)
		require.NoError(t, err)

		err = StepMigration(t.Context(), migrator, set, strings.NewReader(""), io.Discard)
		require.ErrorContains(t, err, "up")
		require.Equal(t, []string{"dbdiff_migrations"}, migrator.TableNames())
	})

	t.Run("StepWithNoPendingFile", func(t *testing.T) {
		migrator := NewTestSQLiteMigrator(t)

		set, err := LoadMigrationSet(t.Context(), migrator, t.TempDir())
		require.NoError(t, err)

		output := &bytes.Buffer{}
		require.NoError(t, StepMigration(t.Context(), migrator, set, strings.NewReader(""), output))
		require.Contains(t, output.String(), "up to date")
	})
}

func TestVerifyMigrations(t *testing.T) {
	newAppliedSource := func(tb testing.TB) (string, string) {
		tb.Helper()

		directory := tb.TempDir()

		migrations := filepath.Join(directory, "migrations")
		require.NoError(tb, os.Mkdir(migrations, 0o750))

		WriteSQLFile(tb, migrations, "20260814101500_init.sql",
			"CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

		sourcePath := filepath.Join(directory, "source.sqlite")

		migrator, err := NewSQLiteMigrator(sourcePath)
		require.NoError(tb, err)
		tb.Cleanup(func() {
			require.NoError(tb, migrator.Close())
		})

		set, err := LoadMigrationSet(tb.Context(), migrator, migrations)
		require.NoError(tb, err)
		require.NoError(tb, ApplyMigrations(tb.Context(), migrator, set, "", io.Discard))

		return sourcePath, migrations
	}

	t.Run("ACleanDatabaseGivesNoInstruction", func(t *testing.T) {
		sourcePath, migrations := newAppliedSource(t)

		migrator, err := NewSQLiteMigrator(sourcePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, migrator.Close()) }()

		set, err := LoadMigrationSet(t.Context(), migrator, migrations)
		require.NoError(t, err)

		directory, cleanup, err := MigrationVerifyDirectory(set)
		require.NoError(t, err)

		defer cleanup()

		driver, err := drivers.NewSQLiteDriver(t.Context(), &drivers.SQLiteDriverConfig{
			TargetDatabasePath: directory,
			SourceDatabasePath: sourcePath,
		})
		require.NoError(t, err)

		defer func() { require.NoError(t, driver.Close()) }()

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.Empty(t, instructions)
	})

	t.Run("AHandEditedDatabaseGivesTheDrift", func(t *testing.T) {
		sourcePath, migrations := newAppliedSource(t)

		migrator, err := NewSQLiteMigrator(sourcePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, migrator.Close()) }()

		_, err = migrator.Connection.ExecContext(t.Context(),
			`CREATE TABLE audit (id INTEGER PRIMARY KEY);`)
		require.NoError(t, err)

		set, err := LoadMigrationSet(t.Context(), migrator, migrations)
		require.NoError(t, err)

		directory, cleanup, err := MigrationVerifyDirectory(set)
		require.NoError(t, err)

		defer cleanup()

		driver, err := drivers.NewSQLiteDriver(t.Context(), &drivers.SQLiteDriverConfig{
			TargetDatabasePath: directory,
			SourceDatabasePath: sourcePath,
		})
		require.NoError(t, err)

		defer func() { require.NoError(t, driver.Close()) }()

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, instructions)
		require.Contains(t, drivers.RenderInstructions(instructions), `DROP TABLE "audit"`)
	})

	t.Run("TheHistoryTableIsNoDrift", func(t *testing.T) {
		sourcePath, migrations := newAppliedSource(t)

		migrator, err := NewSQLiteMigrator(sourcePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, migrator.Close()) }()

		set, err := LoadMigrationSet(t.Context(), migrator, migrations)
		require.NoError(t, err)

		directory, cleanup, err := MigrationVerifyDirectory(set)
		require.NoError(t, err)

		defer cleanup()

		driver, err := drivers.NewSQLiteDriver(t.Context(), &drivers.SQLiteDriverConfig{
			TargetDatabasePath: directory,
			SourceDatabasePath: sourcePath,
		})
		require.NoError(t, err)

		defer func() { require.NoError(t, driver.Close()) }()

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.NotContains(t, drivers.RenderInstructions(instructions), "dbdiff_migrations")
	})

	t.Run("ADatabaseWithNoAppliedMigration", func(t *testing.T) {
		directory := t.TempDir()

		sourcePath := filepath.Join(directory, "source.sqlite")

		migrator, err := NewSQLiteMigrator(sourcePath)
		require.NoError(t, err)

		defer func() { require.NoError(t, migrator.Close()) }()

		set, err := LoadMigrationSet(t.Context(), migrator, t.TempDir())
		require.NoError(t, err)

		verifyDirectory, cleanup, err := MigrationVerifyDirectory(set)
		require.NoError(t, err)

		defer cleanup()

		driver, err := drivers.NewSQLiteDriver(t.Context(), &drivers.SQLiteDriverConfig{
			TargetDatabasePath: verifyDirectory,
			SourceDatabasePath: sourcePath,
		})
		require.NoError(t, err)

		defer func() { require.NoError(t, driver.Close()) }()

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.Empty(t, instructions)
	})
}

func TestPostgresNoTransactionMigration(t *testing.T) {
	migrator := NewTestPostgresMigrator(t)

	_, err := migrator.Connection.ExecContext(t.Context(),
		`CREATE TABLE users (id INT, email TEXT, username TEXT);`)
	require.NoError(t, err)

	directory := t.TempDir()
	WriteSQLFile(t, directory, "20260101000000_concurrent_index.sql",
		`-- dbdiff:no-transaction
DROP INDEX CONCURRENTLY IF EXISTS ix_users_email;
ALTER TABLE users ALTER COLUMN username SET NOT NULL;
CREATE INDEX CONCURRENTLY ix_users_email ON users (email);
`)

	set, err := LoadMigrationSet(t.Context(), migrator, directory)
	require.NoError(t, err)

	output := &bytes.Buffer{}

	err = ApplyMigrations(t.Context(), migrator, set, "", output)
	require.NoError(t, err)
	require.Contains(t, output.String(), "Applied 20260101000000_concurrent_index.")
	require.Contains(t, migrator.TableNames(), "users")

	set, err = LoadMigrationSet(t.Context(), migrator, directory)
	require.NoError(t, err)
	require.Equal(t, MigrationApplied, set.Entries[0].State)
}

func TestPostgresMigrationRunner(t *testing.T) {
	t.Run("GenerateThenApplyLeavesNoDiff", func(t *testing.T) {
		if testing.Short() {
			t.Skip("the temporary postgres server needs a download on the first run")
		}

		migrator := NewTestPostgresMigrator(t)

		scratchVersion := drivers.DetectPostgresScratchVersion(t.Context(), PostgresTestConnectionString)
		require.NotEmpty(t, scratchVersion)

		directory := t.TempDir()

		schemaFile := WriteSQLFile(t, directory, "schema.sql",
			"CREATE TABLE users (id INT PRIMARY KEY, email TEXT NOT NULL);\n"+
				"CREATE INDEX users_email ON users (email);\n")

		migrations := filepath.Join(directory, "migrations")
		require.NoError(t, os.Mkdir(migrations, 0o750))

		generateDriver, err := drivers.NewPostgresDriver(t.Context(), &drivers.PostgresDriverConfig{
			TargetConnectionString: schemaFile,
			SourceConnectionString: migrations,
			ScratchServerVersion:   string(scratchVersion),
		})
		require.NoError(t, err)

		defer func() { require.NoError(t, generateDriver.Close()) }()

		generatedInstructions, err := generateDriver.Diff(t.Context())
		require.NoError(t, err)

		paths, err := WriteMigrationFiles(migrations, "add_users", time.Now(), "test", generatedInstructions)
		require.NoError(t, err)
		require.NotEmpty(t, paths)

		set, err := LoadMigrationSet(t.Context(), migrator, migrations)
		require.NoError(t, err)
		require.NoError(t, ApplyMigrations(t.Context(), migrator, set, "", io.Discard))

		sourceConnectionString := fmt.Sprintf("%s&search_path=%s", PostgresTestConnectionString, migrator.Schema())

		driver, err := drivers.NewPostgresDriver(t.Context(), &drivers.PostgresDriverConfig{
			TargetConnectionString: schemaFile,
			SourceConnectionString: sourceConnectionString,
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, driver.Close())
		})

		instructions, err := driver.Diff(t.Context())
		require.NoError(t, err)
		require.Empty(t, instructions)
	})
}
