package migrations

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func LoadMigrationSet(ctx context.Context, migrator Migrator, directory string) (*MigrationSet, error) {
	files, err := ReadMigrationDirectory(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read the migrations directory: %w", err)
	}

	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the history table: %w", err)
	}

	return NewMigrationSet(files, applied), nil
}

func RenderMigrationStatus(set *MigrationSet) string {
	if len(set.Entries) == 0 {
		return "The directory holds no migration.\n"
	}

	var builder strings.Builder

	for _, entry := range set.Entries {
		fmt.Fprintf(&builder, "%-40s %s", entry.FileName(), entry.State)

		if entry.State == MigrationApplied {
			builder.WriteString("   ")
			builder.WriteString(entry.AppliedAt.UTC().Format(time.RFC3339))
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

func RenderMigrationPreview(set *MigrationSet) (string, error) {
	err := set.RecordError()
	if err != nil {
		return "", err
	}

	var builder strings.Builder

	for _, entry := range set.Entries {
		if entry.State != MigrationOutOfOrder {
			continue
		}

		fmt.Fprintf(&builder, "%s is out of order. The command up will refuse it.\n", entry.FileName())
	}

	pending := set.Pending()
	if len(pending) == 0 {
		if builder.Len() == 0 {
			return "The database is up to date.\n", nil
		}

		return builder.String(), nil
	}

	for _, entry := range pending {
		content, err := entry.Content()
		if err != nil {
			return "", err
		}

		builder.WriteString(entry.FileName())
		builder.WriteString("\n")
		builder.WriteString("  ")
		builder.WriteString(indentMigrationStatement(strings.TrimSpace(content)))
		builder.WriteString("\n")
	}

	return builder.String(), nil
}

func indentMigrationStatement(statement string) string {
	return strings.ReplaceAll(statement, "\n", "\n  ")
}

// One transaction holds every pending file, because a file can read an object of an
// earlier file.
func RunMigrationPreview(ctx context.Context, migrator Migrator, set *MigrationSet, output io.Writer) error {
	err := set.RecordError()
	if err != nil {
		return err
	}

	pending := set.Pending()
	if len(pending) == 0 {
		return nil
	}

	transaction, err := migrator.Begin(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to open the preview transaction: %w", err)
	}

	defer func() { _ = transaction.Rollback() }()

	for _, entry := range pending {
		content, err := entry.Content()
		if err != nil {
			return err
		}

		if !driversshared.FileUsesTransaction(content) {
			_, _ = fmt.Fprintf(output, "%s runs outside a transaction, so preview does not run it.\n",
				entry.FileName())

			continue
		}

		err = transaction.Apply(ctx, content)
		if err != nil {
			return fmt.Errorf("%s failed: %w", entry.FileName(), err)
		}
	}

	return nil
}

func ApplyMigrations(ctx context.Context, migrator Migrator, set *MigrationSet,
	lastVersion string, output io.Writer) error {
	return applyPendingMigrations(ctx, migrator, set, nil, lastVersion, output)
}

func StepMigration(ctx context.Context, migrator Migrator, set *MigrationSet,
	input io.Reader, output io.Writer) error {
	return applyPendingMigrations(ctx, migrator, set, bufio.NewReader(input), "", output)
}

// An empty version keeps every pending file. A named version keeps the files up to that
// version, and it must name a file of the directory.
func pendingMigrationsUpTo(set *MigrationSet, lastVersion string) ([]*MigrationEntry, error) {
	pending := set.Pending()

	if lastVersion == "" {
		return pending, nil
	}

	holdsVersion := false

	for _, entry := range set.Entries {
		if entry.Migration.Version == lastVersion {
			holdsVersion = true
		}
	}

	if !holdsVersion {
		return nil, fmt.Errorf("the directory holds no migration with the version %s", lastVersion)
	}

	var kept []*MigrationEntry

	for _, entry := range pending {
		if entry.Migration.Version <= lastVersion {
			kept = append(kept, entry)
		}
	}

	return kept, nil
}

func applyPendingMigrations(ctx context.Context, migrator Migrator, set *MigrationSet,
	reader *bufio.Reader, lastVersion string, output io.Writer) error {
	err := set.ProblemError()
	if err != nil {
		return err
	}

	pendingBeforeLock, err := pendingMigrationsUpTo(set, lastVersion)
	if err != nil {
		return err
	}

	if len(pendingBeforeLock) == 0 {
		_, _ = fmt.Fprintln(output, "The database is up to date.")

		return nil
	}

	locked, err := migrator.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("failed to take the migration lock: %w", err)
	}

	if !locked {
		_, _ = fmt.Fprintln(output, "Another process holds the migration lock. dbdiff waits for it.")

		err = migrator.Lock(ctx)
		if err != nil {
			return fmt.Errorf("failed to take the migration lock: %w", err)
		}
	}

	defer func() { _ = migrator.Unlock(ctx) }()

	err = migrator.EnsureHistoryTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to create the history table: %w", err)
	}

	set, err = reloadMigrationSet(ctx, migrator, set)
	if err != nil {
		return err
	}

	err = set.ProblemError()
	if err != nil {
		return err
	}

	pending, err := pendingMigrationsUpTo(set, lastVersion)
	if err != nil {
		return err
	}

	if len(pending) == 0 {
		_, _ = fmt.Fprintln(output, "The database is up to date.")

		return nil
	}

	applyTheRest := reader == nil

	for _, entry := range pending {
		if !applyTheRest {
			content, err := entry.Content()
			if err != nil {
				return err
			}

			_, _ = fmt.Fprintf(output, "%s\n", entry.FileName())
			_, _ = fmt.Fprintf(output, "  %s\n", indentMigrationStatement(strings.TrimSpace(content)))

			answer, err := readStepAnswer(reader, output)
			if err != nil {
				return err
			}

			if answer == "q" {
				_, _ = fmt.Fprintln(output, "The run stopped. Every file that dbdiff applied stays applied.")

				return nil
			}

			applyTheRest = answer == "r"
		}

		err := applyOneMigration(ctx, migrator, entry, output)
		if err != nil {
			return err
		}
	}

	return nil
}

// RunMigrationRepair makes the record agree with the files. A changed row takes the
// checksum of its file. A missing row and a dirty row go away. An out of order file
// needs a new generate, so repair does not touch it.
func RunMigrationRepair(ctx context.Context, migrator Migrator, set *MigrationSet,
	output io.Writer) error {
	err := migrator.Lock(ctx)
	if err != nil {
		return fmt.Errorf("failed to take the migration lock: %w", err)
	}

	defer func() { _ = migrator.Unlock(ctx) }()

	set, err = reloadMigrationSet(ctx, migrator, set)
	if err != nil {
		return err
	}

	repaired := 0

	for _, entry := range set.Entries {
		switch entry.State {
		case MigrationChanged:
			err = migrator.UpdateChecksum(ctx, entry.Migration)
			if err != nil {
				return fmt.Errorf("failed to update the checksum of %s: %w", entry.FileName(), err)
			}

			_, _ = fmt.Fprintf(output, "Updated the checksum of %s.\n", entry.FileName())
			repaired++
		case MigrationMissing:
			err = migrator.DeleteRecord(ctx, entry.Migration.Version)
			if err != nil {
				return fmt.Errorf("failed to delete the row of %s: %w", entry.FileName(), err)
			}

			_, _ = fmt.Fprintf(output, "Deleted the row of %s.\n", entry.FileName())
			repaired++
		case MigrationDirty:
			err = migrator.DeleteRecord(ctx, entry.Migration.Version)
			if err != nil {
				return fmt.Errorf("failed to delete the row of %s: %w", entry.FileName(), err)
			}

			_, _ = fmt.Fprintf(output, "Deleted the dirty row of %s. The next up runs the whole file again.\n",
				entry.FileName())
			repaired++
		}
	}

	if repaired == 0 {
		_, _ = fmt.Fprintln(output, "The record needs no repair.")
	}

	return nil
}

// Another process can apply a file between the first read of the state and the lock, so
// this second read builds the set again under the lock.
func reloadMigrationSet(ctx context.Context, migrator Migrator, set *MigrationSet) (*MigrationSet, error) {
	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	return NewMigrationSet(migrationFilesOf(set), applied), nil
}

func migrationFilesOf(set *MigrationSet) []*Migration {
	var files []*Migration

	for _, entry := range set.Entries {
		if entry.State == MigrationMissing {
			continue
		}

		files = append(files, entry.Migration)
	}

	return files
}

func applyOneMigration(ctx context.Context, migrator Migrator, entry *MigrationEntry,
	output io.Writer) error {
	content, err := entry.Content()
	if err != nil {
		return err
	}

	if !driversshared.FileUsesTransaction(content) {
		return applyOneMigrationWithoutTransaction(ctx, migrator, entry, content, output)
	}

	transaction, err := migrator.Begin(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to open the transaction of %s: %w", entry.FileName(), err)
	}

	err = transaction.Apply(ctx, content)
	if err != nil {
		_ = transaction.Rollback()

		return fmt.Errorf("%s failed: %w", entry.FileName(), err)
	}

	err = transaction.Record(ctx, entry.Migration)
	if err != nil {
		_ = transaction.Rollback()

		return fmt.Errorf("failed to record %s: %w", entry.FileName(), err)
	}

	err = transaction.Commit()
	if err != nil {
		return fmt.Errorf("failed to record %s: %w", entry.FileName(), err)
	}

	_, _ = fmt.Fprintf(output, "Applied %s.\n", entry.FileName())

	return nil
}

// The dirty row keeps a half apply visible, and its primary key stops a second process
// that runs the same file.
func applyOneMigrationWithoutTransaction(ctx context.Context, migrator Migrator,
	entry *MigrationEntry, content string, output io.Writer) error {
	err := migrator.RecordDirty(ctx, entry.Migration)
	if err != nil {
		return fmt.Errorf("failed to record %s: %w", entry.FileName(), err)
	}

	transaction, err := migrator.Begin(ctx, false)
	if err != nil {
		return err
	}

	for _, statement := range driversshared.SplitSQLStatements(content) {
		err = transaction.Apply(ctx, statement)
		if err != nil {
			return fmt.Errorf("%s failed, and the statements before the failure stay applied. "+
				"Repair the database, and run migrate repair: %w",
				entry.FileName(), err)
		}
	}

	err = migrator.ClearDirty(ctx, entry.Migration)
	if err != nil {
		return fmt.Errorf("failed to record %s: %w", entry.FileName(), err)
	}

	_, _ = fmt.Fprintf(output, "Applied %s.\n", entry.FileName())

	return nil
}

func MigrationVerifyDirectory(set *MigrationSet) (string, func(), error) {
	directory, err := os.MkdirTemp("", "dbdiff-verify-")
	if err != nil {
		return "", nil, err
	}

	cleanup := func() {
		_ = os.RemoveAll(directory)
	}

	err = copyAppliedMigrations(set, directory)
	if err != nil {
		cleanup()

		return "", nil, err
	}

	return directory, cleanup, nil
}

func copyAppliedMigrations(set *MigrationSet, directory string) error {
	for _, entry := range set.Entries {
		if entry.State != MigrationApplied {
			continue
		}

		content, err := os.ReadFile(entry.Migration.Path)
		if err != nil {
			return err
		}

		path := filepath.Join(directory, filepath.Base(entry.Migration.Path))

		err = os.WriteFile(path, content, 0o600)
		if err != nil {
			return err
		}
	}

	return nil
}

func readStepAnswer(reader *bufio.Reader, output io.Writer) (string, error) {
	for {
		_, _ = fmt.Fprint(output, "  [a]pply  apply the [r]est  [q]uit ? ")

		line, err := reader.ReadString('\n')

		answer := strings.ToLower(strings.TrimSpace(line))
		if answer == "a" || answer == "r" || answer == "q" {
			return answer, nil
		}

		if errors.Is(err, io.EOF) {
			return "", fmt.Errorf("step needs a terminal. Use the command up to apply a migration with no prompt")
		}

		if err != nil {
			return "", err
		}

		_, _ = fmt.Fprintln(output, "  Answer a, r, or q.")
	}
}
