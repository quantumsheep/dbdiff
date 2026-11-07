package driverssqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteUniqueConstraint struct {
	Name string

	Columns []string

	// A key clause holds the quoted column name with its COLLATE clause and its DESC
	// keyword, which no PRAGMA reports. An empty list keeps the plain names of Columns.
	Keys []string

	Conflict string
}

func (c *SQLiteUniqueConstraint) Equal(other *SQLiteUniqueConstraint) bool {
	return c.Name == other.Name && c.Conflict == other.Conflict &&
		slices.Equal(c.Columns, other.Columns) &&
		slices.Equal(c.Keys, other.Keys)
}

func (c *SQLiteUniqueConstraint) Clause() string {
	keys := driversshared.QuoteIdentifiers(c.Columns)
	if len(c.Keys) == len(c.Columns) {
		keys = c.Keys
	}

	clause := fmt.Sprintf("%sUNIQUE (%s)",
		constraintNameClause(c.Name), strings.Join(keys, ", "))

	return clause + conflictClause(c.Conflict)
}

func conflictClause(resolution string) string {
	if resolution == "" {
		return ""
	}

	return " ON CONFLICT " + resolution
}

// This method sorts the keys, because SQLite gives no stable order.
func (d *SQLiteDriver) GetTableUniqueKeys(
	ctx context.Context,
	db *sql.DB,
	indexList []sqliteIndexListEntry,
) ([][]string, error) {
	var keys [][]string

	for _, index := range indexList {
		if index.Origin != "u" {
			continue
		}

		indexInfo, err := d.getIndexInfo(ctx, db, index.Name)
		if err != nil {
			return nil, err
		}

		var columnNames []string

		for _, entry := range indexInfo {
			if !entry.Name.Valid {
				return nil, fmt.Errorf("the index %s holds a key at the position %d that is no column", index.Name, entry.Position)
			}

			columnNames = append(columnNames, entry.Name.String)
		}

		keys = append(keys, columnNames)
	}

	slices.SortStableFunc(keys, slices.Compare)

	return keys, nil
}
