package driversshared

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Two runs can share one scratch server, so the name of a temporary database holds the time.
func ScratchDatabaseName(role string) string {
	return fmt.Sprintf("dbdiff_%s_%d", role, time.Now().UnixNano())
}

type SQLSource struct {
	Path  string
	Files []string

	BackslashEscapes bool
}

func NewSQLSource(path string) (*SQLSource, error) {
	information, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	files := []string{path}

	if information.IsDir() {
		files, err = collectSQLFiles(path)
		if err != nil {
			return nil, err
		}
	}

	sqlSource := &SQLSource{
		Path:  path,
		Files: files,
	}

	return sqlSource, nil
}

func (s *SQLSource) ApplyTo(ctx context.Context, db *sql.DB) error {
	for _, file := range s.Files {
		content, err := os.ReadFile(file)
		if err != nil {
			return err
		}

		err = applySQLContent(ctx, db, trimGooseDownSection(string(content)), s.BackslashEscapes)
		if err != nil {
			return fmt.Errorf("failed to apply %q: %w", file, err)
		}
	}

	return nil
}

func applySQLContent(ctx context.Context, db *sql.DB, content string, backslashEscapes bool) error {
	if strings.TrimSpace(content) == "" {
		return nil
	}

	if FileUsesTransaction(content) {
		_, err := db.ExecContext(ctx, content)

		return err
	}

	for _, statement := range SplitSQLStatements(content, backslashEscapes) {
		_, err := db.ExecContext(ctx, statement)
		if err != nil {
			return err
		}
	}

	return nil
}

func collectSQLFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	var files []string

	for _, entry := range entries {
		if !IsUpSQLFile(entry) {
			continue
		}

		files = append(files, filepath.Join(directory, entry.Name()))
	}

	slices.Sort(files)

	return files, nil
}

// A goose file holds the up migration and the down migration in one file, and the down
// part removes the schema that the up part built.
func trimGooseDownSection(content string) string {
	lines := strings.Split(content, "\n")

	for index, line := range lines {
		fields := strings.Fields(strings.TrimSpace(line))

		if len(fields) >= 3 && fields[0] == "--" && fields[1] == "+goose" &&
			strings.EqualFold(fields[2], "Down") {
			return strings.Join(lines[:index], "\n")
		}
	}

	return content
}

func IsUpSQLFile(entry os.DirEntry) bool {
	return !entry.IsDir() && hasSQLExtension(entry.Name()) && !isDownMigration(entry.Name())
}

func hasSQLExtension(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".sql")
}

func isDownMigration(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), ".down.sql")
}
