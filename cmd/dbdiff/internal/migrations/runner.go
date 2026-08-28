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

	"github.com/quantumsheep/dbdiff/drivers"
	coremigrations "github.com/quantumsheep/dbdiff/internal/migrations"
)

func LoadMigrationSet(ctx context.Context, migrator coremigrations.Migrator, directory string) (*coremigrations.MigrationSet, error) {
	files, err := coremigrations.ReadMigrationDirectory(directory)
	if err != nil {
		return nil, err
	}

	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	return coremigrations.NewMigrationSet(files, applied), nil
}

func RenderMigrationStatus(set *coremigrations.MigrationSet) string {
	if len(set.Entries) == 0 {
		return "The directory holds no migration.\n"
	}

	var builder strings.Builder

	for _, entry := range set.Entries {
		builder.WriteString(fmt.Sprintf("%-40s %s", entry.FileName(), entry.State))

		if entry.State == coremigrations.MigrationApplied {
			builder.WriteString("   ")
			builder.WriteString(entry.AppliedAt.UTC().Format(time.RFC3339))
		}

		builder.WriteString("\n")
	}

	return builder.String()
}

func RenderMigrationPreview(set *coremigrations.MigrationSet) (string, error) {
	err := set.RecordError()
	if err != nil {
		return "", err
	}

	var builder strings.Builder

	for _, entry := range set.Entries {
		if entry.State != coremigrations.MigrationOutOfOrder {
			continue
		}

		builder.WriteString(fmt.Sprintf("%s is out of order. The command up will refuse it.\n", entry.FileName()))
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
func RunMigrationPreview(ctx context.Context, migrator coremigrations.Migrator, set *coremigrations.MigrationSet, output io.Writer) error {
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
		return err
	}

	defer transaction.Rollback()

	for _, entry := range pending {
		content, err := entry.Content()
		if err != nil {
			return err
		}

		if !drivers.FileUsesTransaction(content) {
			fmt.Fprintf(output, "%s runs outside a transaction, so preview does not run it.\n",
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

func ApplyMigrations(ctx context.Context, migrator coremigrations.Migrator, set *coremigrations.MigrationSet,
	output io.Writer) error {
	return applyPendingMigrations(ctx, migrator, set, nil, output)
}

func StepMigration(ctx context.Context, migrator coremigrations.Migrator, set *coremigrations.MigrationSet,
	input io.Reader, output io.Writer) error {
	return applyPendingMigrations(ctx, migrator, set, bufio.NewReader(input), output)
}

func applyPendingMigrations(ctx context.Context, migrator coremigrations.Migrator, set *coremigrations.MigrationSet,
	reader *bufio.Reader, output io.Writer) error {
	err := set.ProblemError()
	if err != nil {
		return err
	}

	if len(set.Pending()) == 0 {
		fmt.Fprintln(output, "The database is up to date.")

		return nil
	}

	err = migrator.Lock(ctx)
	if err != nil {
		return err
	}

	defer migrator.Unlock(ctx)

	err = migrator.EnsureHistoryTable(ctx)
	if err != nil {
		return err
	}

	set, err = reloadMigrationSet(ctx, migrator, set)
	if err != nil {
		return err
	}

	err = set.ProblemError()
	if err != nil {
		return err
	}

	pending := set.Pending()
	if len(pending) == 0 {
		fmt.Fprintln(output, "The database is up to date.")

		return nil
	}

	applyTheRest := reader == nil

	for _, entry := range pending {
		if !applyTheRest {
			content, err := entry.Content()
			if err != nil {
				return err
			}

			fmt.Fprintf(output, "%s\n", entry.FileName())
			fmt.Fprintf(output, "  %s\n", indentMigrationStatement(strings.TrimSpace(content)))

			answer, err := readStepAnswer(reader, output)
			if err != nil {
				return err
			}

			if answer == "q" {
				fmt.Fprintln(output, "The run stopped. Every file that dbdiff applied stays applied.")

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

// Another process can apply a file between the first read of the state and the lock, so
// this second read builds the set again under the lock.
func reloadMigrationSet(ctx context.Context, migrator coremigrations.Migrator, set *coremigrations.MigrationSet) (*coremigrations.MigrationSet, error) {
	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	return coremigrations.NewMigrationSet(migrationFilesOf(set), applied), nil
}

func migrationFilesOf(set *coremigrations.MigrationSet) []*coremigrations.Migration {
	var files []*coremigrations.Migration

	for _, entry := range set.Entries {
		if entry.State == coremigrations.MigrationMissing {
			continue
		}

		files = append(files, entry.Migration)
	}

	return files
}

func applyOneMigration(ctx context.Context, migrator coremigrations.Migrator, entry *coremigrations.MigrationEntry,
	output io.Writer) error {
	content, err := entry.Content()
	if err != nil {
		return err
	}

	transaction, err := migrator.Begin(ctx, drivers.FileUsesTransaction(content))
	if err != nil {
		return err
	}

	for _, statement := range statementsOf(content) {
		err = transaction.Apply(ctx, statement)
		if err != nil {
			transaction.Rollback()

			return fmt.Errorf("%s failed: %w", entry.FileName(), err)
		}
	}

	err = transaction.Record(ctx, entry.Migration)
	if err != nil {
		transaction.Rollback()

		return err
	}

	err = transaction.Commit()
	if err != nil {
		return err
	}

	fmt.Fprintf(output, "Applied %s.\n", entry.FileName())

	return nil
}

func MigrationVerifyDirectory(set *coremigrations.MigrationSet) (string, func(), error) {
	directory, err := os.MkdirTemp("", "dbdiff-verify-")
	if err != nil {
		return "", nil, err
	}

	cleanup := func() {
		os.RemoveAll(directory)
	}

	err = copyAppliedMigrations(set, directory)
	if err != nil {
		cleanup()

		return "", nil, err
	}

	return directory, cleanup, nil
}

func copyAppliedMigrations(set *coremigrations.MigrationSet, directory string) error {
	for _, entry := range set.Entries {
		if entry.State != coremigrations.MigrationApplied {
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
		fmt.Fprint(output, "  [a]pply  apply the [r]est  [q]uit ? ")

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

		fmt.Fprintln(output, "  Answer a, r, or q.")
	}
}

func statementsOf(content string) []string {
	if drivers.FileUsesTransaction(content) {
		return []string{content}
	}

	return drivers.SplitSQLStatements(content)
}
