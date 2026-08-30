package driverssqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func (d *SQLiteDriver) OpenSide(ctx context.Context, path string, role string) (*sql.DB, error) {
	if !driversshared.IsSQLSource(path) {
		filePath := TrimSQLitePrefix(path)

		// sql.Open creates a file that is absent, and a wrong path then reads an empty
		// schema without an error. A file: name can hold options, so the check skips it.
		if filePath != ":memory:" && !strings.HasPrefix(filePath, "file:") {
			_, err := os.Stat(filePath)
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("the %s database %q does not exist", role, filePath)
			}

			if err != nil {
				return nil, err
			}
		}

		return sql.Open("sqlite3", filePath)
	}

	sqlSource, err := driversshared.NewSQLSource(path)
	if err != nil {
		return nil, err
	}

	directory, err := d.TemporaryDirectory()
	if err != nil {
		return nil, err
	}

	connection, err := sql.Open("sqlite3", filepath.Join(directory, role+".sqlite"))
	if err != nil {
		return nil, err
	}

	err = sqlSource.ApplyTo(ctx, connection)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	return connection, nil
}

func (d *SQLiteDriver) TemporaryDirectory() (string, error) {
	if d.temporaryDirectory != "" {
		return d.temporaryDirectory, nil
	}

	directory, err := os.MkdirTemp("", "dbdiff-sqlite-")
	if err != nil {
		return "", err
	}

	d.temporaryDirectory = directory

	return directory, nil
}

func (d *SQLiteDriver) RemoveTemporaryDirectory() error {
	if d.temporaryDirectory == "" {
		return nil
	}

	directory := d.temporaryDirectory
	d.temporaryDirectory = ""

	return os.RemoveAll(directory)
}
