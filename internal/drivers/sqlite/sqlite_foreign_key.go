package driverssqlite

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteForeignKey struct {
	Name string

	Table    string
	From     []string
	To       []string
	OnUpdate string
	OnDelete string

	Deferrable string
}

func (fk *SQLiteForeignKey) Clause() string {
	fromColumns := strings.Join(driversshared.QuoteIdentifiers(fk.From), ", ")

	statement := fmt.Sprintf("%sFOREIGN KEY (%s) REFERENCES %s",
		constraintNameClause(fk.Name), fromColumns, driversshared.QuoteIdentifier(fk.Table))

	if len(fk.To) > 0 {
		statement += fmt.Sprintf(" (%s)", strings.Join(driversshared.QuoteIdentifiers(fk.To), ", "))
	}

	if fk.OnUpdate != "NO ACTION" && fk.OnUpdate != "" {
		statement += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
	}

	if fk.OnDelete != "NO ACTION" && fk.OnDelete != "" {
		statement += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
	}

	if fk.Deferrable != "" {
		statement += " " + fk.Deferrable
	}

	return statement
}

func (fk *SQLiteForeignKey) Equal(other *SQLiteForeignKey) bool {
	if fk.Name != other.Name || fk.Table != other.Table {
		return false
	}

	if fk.OnUpdate != other.OnUpdate || fk.OnDelete != other.OnDelete {
		return false
	}

	if fk.Deferrable != other.Deferrable {
		return false
	}

	if len(fk.From) != len(other.From) || len(fk.To) != len(other.To) {
		return false
	}

	for i := range fk.From {
		if fk.From[i] != other.From[i] {
			return false
		}
	}

	for i := range fk.To {
		if fk.To[i] != other.To[i] {
			return false
		}
	}

	return true
}

// PRAGMA foreign_key_list reports no name and no DEFERRABLE clause, so each key takes them
// from its clause in parsedForeignKeys.
func (d *SQLiteDriver) GetTableForeignKeys(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	parsedForeignKeys []*parsedConstraint,
) ([]*SQLiteForeignKey, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+driversshared.QuoteIdentifier(tableName)+");")
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	foreignKeysMap := make(map[int]*SQLiteForeignKey)

	for rows.Next() {
		var foreignKeyID, keyPosition int
		var table, from, onUpdate, onDelete, match string

		// PRAGMA foreign_key_list gives a NULL parent column when the key names no parent column.
		var to sql.NullString

		err := rows.Scan(&foreignKeyID, &keyPosition, &table, &from, &to, &onUpdate, &onDelete, &match)
		if err != nil {
			return nil, err
		}

		foreignKey, exists := foreignKeysMap[foreignKeyID]
		if !exists {
			foreignKey = &SQLiteForeignKey{
				Table:    table,
				From:     []string{},
				To:       []string{},
				OnUpdate: onUpdate,
				OnDelete: onDelete,
			}
			foreignKeysMap[foreignKeyID] = foreignKey
		}

		foreignKey.From = append(foreignKey.From, from)

		if to.Valid {
			foreignKey.To = append(foreignKey.To, to.String)
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	// SQLite gives the id 0 to the foreign key that the CREATE TABLE text declares last, so the
	// descending ids give the order of the text. Two keys on the same columns get their own
	// clauses only in that order.
	foreignKeysSet := make([]*SQLiteForeignKey, 0, len(foreignKeysMap))
	unmatchedForeignKeys := slices.Clone(parsedForeignKeys)

	for _, foreignKeyID := range slices.Backward(slices.Sorted(maps.Keys(foreignKeysMap))) {
		foreignKey := foreignKeysMap[foreignKeyID]

		position := slices.IndexFunc(unmatchedForeignKeys, func(parsedForeignKey *parsedConstraint) bool {
			return slices.Equal(parsedForeignKey.Columns, foreignKey.From)
		})
		if position >= 0 {
			foreignKey.Name = unmatchedForeignKeys[position].Name
			foreignKey.Deferrable = unmatchedForeignKeys[position].Deferrable
			unmatchedForeignKeys = slices.Delete(unmatchedForeignKeys, position, position+1)
		}

		foreignKeysSet = append(foreignKeysSet, foreignKey)
	}

	// PRAGMA foreign_key_list gives no stable order. Without this sort the output changes between two runs.
	slices.SortStableFunc(foreignKeysSet, func(first *SQLiteForeignKey, second *SQLiteForeignKey) int {
		return cmp.Or(
			strings.Compare(first.Table, second.Table),
			slices.Compare(first.From, second.From),
			slices.Compare(first.To, second.To),
			strings.Compare(first.OnUpdate, second.OnUpdate),
			strings.Compare(first.OnDelete, second.OnDelete),
			strings.Compare(first.Name, second.Name),
			strings.Compare(first.Deferrable, second.Deferrable),
		)
	})

	return foreignKeysSet, nil
}
