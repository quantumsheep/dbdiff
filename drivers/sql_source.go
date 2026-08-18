package drivers

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

	if hasSQLExtension(path) {
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
		source := &SQLSource{
			Path:  path,
			Files: []string{path},
		}

		return source, nil
	}

	files, err := collectSQLFiles(path)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("the directory %q holds no .sql file", path)
	}

	source := &SQLSource{
		Path:  path,
		Files: files,
	}

	return source, nil
}

// Both engines run a whole file in one call. Never split the statements: a correct split
// needs a parser, because a function body of PostgreSQL holds a semicolon.
func (s *SQLSource) ApplyTo(ctx context.Context, db *sql.DB) error {
	for _, file := range s.Files {
		content, err := os.ReadFile(file)
		if err != nil {
			return err
		}

		if strings.TrimSpace(string(content)) == "" {
			continue
		}

		_, err = db.ExecContext(ctx, string(content))
		if err != nil {
			return fmt.Errorf("failed to apply %q: %w", file, err)
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
		if entry.IsDir() || !hasSQLExtension(entry.Name()) || isDownMigration(entry.Name()) {
			continue
		}

		files = append(files, filepath.Join(directory, entry.Name()))
	}

	slices.Sort(files)

	return files, nil
}

func hasSQLExtension(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".sql")
}

func isDownMigration(name string) bool {
	return strings.HasSuffix(strings.ToLower(name), ".down.sql")
}
