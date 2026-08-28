package drivers

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
)

func (d *SQLiteDriver) OpenSide(ctx context.Context, path string, role string) (*sql.DB, error) {
	if !IsSQLSource(path) {
		return sql.Open("sqlite3", TrimSQLitePrefix(path))
	}

	sqlSource, err := NewSQLSource(path)
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
		connection.Close()
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
