package driversshared

import (
	"context"
	"database/sql"
)

func QueryAll[T any](ctx context.Context, db *sql.DB, query string, scan func(rows *sql.Rows) (T, error), arguments ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var values []T

	for rows.Next() {
		value, err := scan(rows)
		if err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return values, nil
}
