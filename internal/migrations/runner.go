package internalmigrations

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
	"github.com/samber/lo"
)

func LoadSet(ctx context.Context, migrator Migrator, directory string) (*Set, error) {
	files, err := ReadDirectory(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read the migrations directory: %w", err)
	}

	applied, err := migrator.AppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read the history table: %w", err)
	}

	return NewSet(files, applied), nil
}

type Runner struct {
	Migrator Migrator
	Set      *Set
	Output   io.Writer
}

func (r *Runner) Status() error {
	if len(r.Set.Entries) == 0 {
		_, _ = fmt.Fprintln(r.Output, "The directory holds no migration.")

		return nil
	}

	for _, entry := range r.Set.Entries {
		_, _ = fmt.Fprintf(r.Output, "%-40s %s", entry.Label(), entry.State)

		if entry.State == StateApplied {
			_, _ = fmt.Fprintf(r.Output, "   %s", entry.AppliedAt.UTC().Format(time.RFC3339))
		}

		_, _ = fmt.Fprintln(r.Output)
	}

	return r.Set.RecordError()
}

func (r *Runner) Preview() error {
	err := r.Set.RecordError()
	if err != nil {
		return err
	}

	var builder strings.Builder

	writeOutOfOrder(&builder, r.Set, "The commands up and step will refuse it.")

	pending := r.Set.Pending()
	if len(pending) == 0 {
		if builder.Len() == 0 {
			builder.WriteString("The database is up to date.\n")
		}

		_, _ = fmt.Fprint(r.Output, builder.String())

		return nil
	}

	for _, entry := range pending {
		content, err := entry.Content()
		if err != nil {
			return err
		}

		builder.WriteString(entry.Label())
		builder.WriteString("\n")
		builder.WriteString("  ")
		builder.WriteString(indentStatement(strings.TrimSpace(content)))
		builder.WriteString("\n")
	}

	_, _ = fmt.Fprint(r.Output, builder.String())

	return nil
}

func writeOutOfOrder(writer io.Writer, set *Set, consequence string) {
	for _, entry := range set.Entries {
		if entry.State != StateOutOfOrder {
			continue
		}

		_, _ = fmt.Fprintf(writer, "%s is out of order. %s\n", entry.Label(), consequence)
	}
}

func indentStatement(statement string) string {
	return strings.ReplaceAll(statement, "\n", "\n  ")
}

// One transaction holds every pending file, because a file can read an object of an
// earlier file.
func (r *Runner) PreviewInTransaction(ctx context.Context) error {
	err := r.Set.RecordError()
	if err != nil {
		return err
	}

	pending := r.Set.Pending()
	if len(pending) == 0 {
		return nil
	}

	if !r.Migrator.SupportsTransactionalDDL() {
		return fmt.Errorf("the database engine commits every DDL statement at once, so preview cannot roll the pending files back")
	}

	transaction, err := r.Migrator.Begin(ctx)
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
			_, _ = fmt.Fprintf(r.Output, "%s runs outside a transaction, so preview does not run it.\n",
				entry.Label())

			continue
		}

		err = transaction.Apply(ctx, content)
		if err != nil {
			return fmt.Errorf("%s failed: %w", entry.Label(), err)
		}
	}

	return nil
}

func (r *Runner) Up(ctx context.Context, toVersion string) error {
	return r.applyPending(ctx, nil, toVersion)
}

func (r *Runner) Step(ctx context.Context, input io.Reader) error {
	return r.applyPending(ctx, bufio.NewReader(input), "")
}

func pendingUpTo(set *Set, lastVersion string) ([]*Entry, error) {
	return upTo(set, set.Pending(), lastVersion)
}

// An empty version keeps every entry. A named version keeps the entries up to that
// version, and it must name a file of the directory.
func upTo(set *Set, entries []*Entry, lastVersion string) ([]*Entry, error) {
	if lastVersion == "" {
		return entries, nil
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

	var kept []*Entry

	for _, entry := range entries {
		if entry.Migration.Version <= lastVersion {
			kept = append(kept, entry)
		}
	}

	return kept, nil
}

func (r *Runner) applyPending(ctx context.Context, reader *bufio.Reader, lastVersion string) error {
	err := r.Set.ProblemError()
	if err != nil {
		return err
	}

	pendingBeforeLock, err := pendingUpTo(r.Set, lastVersion)
	if err != nil {
		return err
	}

	if len(pendingBeforeLock) == 0 {
		_, _ = fmt.Fprintln(r.Output, "The database is up to date.")

		return nil
	}

	locked, err := r.Migrator.TryLock(ctx)
	if err != nil {
		return fmt.Errorf("failed to take the migration lock: %w", err)
	}

	unlock := func() { _ = r.Migrator.Unlock(ctx) }

	if !locked {
		_, _ = fmt.Fprintln(r.Output, "Another process holds the migration lock. dbdiff waits for it.")

		unlock, err = r.lock(ctx)
		if err != nil {
			return err
		}
	}

	defer unlock()

	set, err := r.reloadWithHistoryTable(ctx)
	if err != nil {
		return err
	}

	err = set.ProblemError()
	if err != nil {
		return err
	}

	pending, err := pendingUpTo(set, lastVersion)
	if err != nil {
		return err
	}

	if len(pending) == 0 {
		_, _ = fmt.Fprintln(r.Output, "The database is up to date.")

		return nil
	}

	applyTheRest := reader == nil

	for _, entry := range pending {
		if !applyTheRest {
			content, err := entry.Content()
			if err != nil {
				return err
			}

			_, _ = fmt.Fprintf(r.Output, "%s\n", entry.Label())
			_, _ = fmt.Fprintf(r.Output, "  %s\n", indentStatement(strings.TrimSpace(content)))

			answer, err := readStepAnswer(ctx, reader, r.Output)
			if err != nil {
				return err
			}

			if answer == "q" {
				_, _ = fmt.Fprintln(r.Output, "The run stopped. Every file that dbdiff applied stays applied.")

				return nil
			}

			applyTheRest = answer == "r"
		}

		// Each file commits alone. A failure must not roll back an earlier file.
		err := r.applyOne(ctx, entry)
		if err != nil {
			return err
		}
	}

	return nil
}

// Repair makes the record agree with the files. A changed row takes the
// checksum of its file. A missing row and a dirty row go away. An out of order file
// needs a new generate, so repair does not touch it.
func (r *Runner) Repair(ctx context.Context) error {
	unlock, err := r.lock(ctx)
	if err != nil {
		return err
	}

	defer unlock()

	set, err := r.reload(ctx)
	if err != nil {
		return err
	}

	repaired := 0

	for _, entry := range set.Entries {
		switch entry.State {
		case StateChanged:
			err = r.Migrator.UpdateChecksum(ctx, entry.Migration)
			if err != nil {
				return fmt.Errorf("failed to update the checksum of %s: %w", entry.Label(), err)
			}

			_, _ = fmt.Fprintf(r.Output, "Updated the checksum of %s.\n", entry.Label())
			repaired++
		case StateMissing:
			err = r.Migrator.DeleteRecord(ctx, entry.Migration.Version)
			if err != nil {
				return fmt.Errorf("failed to delete the row of %s: %w", entry.Label(), err)
			}

			_, _ = fmt.Fprintf(r.Output, "Deleted the row of %s.\n", entry.Label())
			repaired++
		case StateDirty:
			err = r.Migrator.DeleteRecord(ctx, entry.Migration.Version)
			if err != nil {
				return fmt.Errorf("failed to delete the row of %s: %w", entry.Label(), err)
			}

			_, _ = fmt.Fprintf(r.Output, "Deleted the dirty row of %s. The next up runs the whole file again.\n",
				entry.Label())
			repaired++
		}
	}

	if repaired == 0 {
		_, _ = fmt.Fprintln(r.Output, "The record needs no repair.")
	}

	return nil
}

// Baseline records every file that the history table does not hold, and it
// runs no file. A baseline adopts a database that already holds the schema of the files.
// A named lastVersion stops the record after that version.
func (r *Runner) Baseline(ctx context.Context, lastVersion string) error {
	err := r.Set.RecordError()
	if err != nil {
		return err
	}

	unlock, err := r.lock(ctx)
	if err != nil {
		return err
	}

	defer unlock()

	set, err := r.reloadWithHistoryTable(ctx)
	if err != nil {
		return err
	}

	err = set.RecordError()
	if err != nil {
		return err
	}

	unrecorded := lo.Filter(set.Entries, func(entry *Entry, _ int) bool {
		return entry.State == StatePending || entry.State == StateOutOfOrder
	})

	unrecorded, err = upTo(set, unrecorded, lastVersion)
	if err != nil {
		return err
	}

	if len(unrecorded) == 0 {
		_, _ = fmt.Fprintln(r.Output, "The record holds every file.")

		return nil
	}

	transaction, err := r.Migrator.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to open the baseline transaction: %w", err)
	}

	for _, entry := range unrecorded {
		err = transaction.Record(ctx, entry.Migration)
		if err != nil {
			_ = transaction.Rollback()

			return fmt.Errorf("failed to record %s: %w", entry.Label(), err)
		}
	}

	err = transaction.Commit()
	if err != nil {
		return fmt.Errorf("failed to record the baseline: %w", err)
	}

	for _, entry := range unrecorded {
		_, _ = fmt.Fprintf(r.Output, "Recorded %s.\n", entry.Label())
	}

	return nil
}

func (r *Runner) lock(ctx context.Context) (func(), error) {
	err := r.Migrator.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to take the migration lock: %w", err)
	}

	return func() { _ = r.Migrator.Unlock(ctx) }, nil
}

func (r *Runner) reloadWithHistoryTable(ctx context.Context) (*Set, error) {
	err := r.Migrator.EnsureHistoryTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create the history table: %w", err)
	}

	return r.reload(ctx)
}

// Another process can apply a file between the first read of the state and the lock, so
// this second read builds the set again under the lock.
func (r *Runner) reload(ctx context.Context) (*Set, error) {
	applied, err := r.Migrator.AppliedMigrations(ctx)
	if err != nil {
		return nil, err
	}

	return NewSet(filesOf(r.Set), applied), nil
}

func filesOf(set *Set) []*Migration {
	var files []*Migration

	for _, entry := range set.Entries {
		if entry.State == StateMissing {
			continue
		}

		files = append(files, entry.Migration)
	}

	return files
}

func (r *Runner) applyOne(ctx context.Context, entry *Entry) error {
	content, err := entry.Content()
	if err != nil {
		return err
	}

	// An engine without transactional DDL commits each DDL statement at once, so a
	// rollback removes no schema change. The dirty row marks a half apply instead.
	if !driversshared.FileUsesTransaction(content) || !r.Migrator.SupportsTransactionalDDL() {
		return r.applyOneWithoutTransaction(ctx, entry, content)
	}

	transaction, err := r.Migrator.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to open the transaction of %s: %w", entry.Label(), err)
	}

	err = transaction.Apply(ctx, content)
	if err != nil {
		_ = transaction.Rollback()

		return fmt.Errorf("%s failed: %w", entry.Label(), err)
	}

	err = transaction.Record(ctx, entry.Migration)
	if err != nil {
		_ = transaction.Rollback()

		return fmt.Errorf("failed to record %s: %w", entry.Label(), err)
	}

	err = transaction.Commit()
	if err != nil {
		return fmt.Errorf("failed to record %s: %w", entry.Label(), err)
	}

	_, _ = fmt.Fprintf(r.Output, "Applied %s.\n", entry.Label())

	return nil
}

// The dirty row keeps a half apply visible, and its primary key stops a second process
// that runs the same file.
func (r *Runner) applyOneWithoutTransaction(ctx context.Context, entry *Entry, content string) error {
	err := r.Migrator.RecordDirty(ctx, entry.Migration)
	if err != nil {
		return fmt.Errorf("failed to record %s: %w", entry.Label(), err)
	}

	// The directive is the one signal that starts the split. A file without it runs in
	// one call.
	statements := []string{content}

	if !driversshared.FileUsesTransaction(content) {
		statements = r.Migrator.SplitStatements(content)
	}

	for _, statement := range statements {
		err = r.Migrator.Exec(ctx, statement)
		if err != nil {
			return fmt.Errorf("%s failed, and the statements before the failure stay applied. "+
				"Repair the database, and run migrate repair: %w",
				entry.Label(), err)
		}
	}

	err = r.Migrator.ClearDirty(ctx, entry.Migration)
	if err != nil {
		return fmt.Errorf("failed to record %s: %w", entry.Label(), err)
	}

	_, _ = fmt.Fprintf(r.Output, "Applied %s.\n", entry.Label())

	return nil
}

// Verify compares the database against a replay of the applied files. The diff function
// reads the replay directory as the desired schema. It returns the instructions that
// make the database equal to the replay.
func (r *Runner) Verify(
	ctx context.Context,
	diff func(ctx context.Context, replay driversshared.DataSource) (driversshared.Instructions, error),
) error {
	err := r.Set.RecordError()
	if err != nil {
		return err
	}

	writeOutOfOrder(r.Output, r.Set, "The replay does not hold it.")

	directory, err := os.MkdirTemp("", "dbdiff-verify-")
	if err != nil {
		return err
	}

	defer func() { _ = os.RemoveAll(directory) }()

	err = copyAppliedMigrations(r.Set, directory)
	if err != nil {
		return err
	}

	instructions, err := diff(ctx, driversshared.ParseDataSource(directory))
	if err != nil {
		return err
	}

	if len(instructions) == 0 {
		_, _ = fmt.Fprintln(r.Output, "The database holds the schema of the migrations.")

		return nil
	}

	_, _ = fmt.Fprintln(r.Output, instructions.String())

	return fmt.Errorf("the database holds %s that no migration made", changeCountText(len(instructions)))
}

func changeCountText(count int) string {
	if count == 1 {
		return "1 change"
	}

	return fmt.Sprintf("%d changes", count)
}

func copyAppliedMigrations(set *Set, directory string) error {
	for _, entry := range set.Entries {
		if entry.State != StateApplied {
			continue
		}

		content, err := entry.Content()
		if err != nil {
			return err
		}

		path := filepath.Join(directory, filepath.Base(entry.Migration.Path))

		err = os.WriteFile(path, []byte(content), 0o600)
		if err != nil {
			return err
		}
	}

	return nil
}

func readStepAnswer(ctx context.Context, reader *bufio.Reader, output io.Writer) (string, error) {
	for {
		_, _ = fmt.Fprint(output, "  [a]pply  apply the [r]est  [q]uit ? ")

		line, err := readLine(ctx, reader)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

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

type readResult struct {
	line string
	err  error
}

// A read of a terminal does not stop when the context stops. Without the goroutine, an
// interrupt signal at the prompt does nothing.
func readLine(ctx context.Context, reader *bufio.Reader) (string, error) {
	results := make(chan readResult, 1)

	go func() {
		line, err := reader.ReadString('\n')

		results <- readResult{
			line: line,
			err:  err,
		}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-results:
		return result.line, result.err
	}
}
