package driversshared

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type SQLSource struct {
	Path  string
	Files []string
}

func IsSQLSource(path string) bool {
	if path == "" || strings.Contains(path, "://") {
		return false
	}

	if HasSQLExtension(path) {
		return true
	}

	information, err := os.Stat(path)

	return err == nil && information.IsDir()
}

func NewSQLSource(path string) (*SQLSource, error) {
	information, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if !information.IsDir() {
		sqlSource := &SQLSource{
			Path:  path,
			Files: []string{path},
		}

		return sqlSource, nil
	}

	files, err := collectSQLFiles(path)
	if err != nil {
		return nil, err
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

		err = ApplySQLContent(ctx, db, TrimGooseDownSection(string(content)))
		if err != nil {
			return fmt.Errorf("failed to apply %q: %w", file, err)
		}
	}

	return nil
}

func ApplySQLContent(ctx context.Context, db *sql.DB, content string) error {
	if strings.TrimSpace(content) == "" {
		return nil
	}

	if FileUsesTransaction(content) {
		_, err := db.ExecContext(ctx, content)

		return err
	}

	for _, statement := range SplitSQLStatements(content) {
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
		if entry.IsDir() || !HasSQLExtension(entry.Name()) || IsDownMigration(entry.Name()) {
			continue
		}

		files = append(files, filepath.Join(directory, entry.Name()))
	}

	slices.Sort(files)

	return files, nil
}

// A goose file holds the up migration and the down migration in one file, and the down
// part removes the schema that the up part built.
func TrimGooseDownSection(content string) string {
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

func HasSQLExtension(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".sql")
}

func IsDownMigration(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), ".down.sql")
}
