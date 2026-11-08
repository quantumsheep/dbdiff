package driverssqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type SQLiteIndex struct {
	Table string
	Name  string

	// A key holds the SQL text of one key part, so an expression key keeps its text.
	Keys  []string
	Where string

	Unique bool
}

func (i *SQLiteIndex) Equal(other *SQLiteIndex) bool {
	if identifierKey(i.Name) != identifierKey(other.Name) || identifierKey(i.Table) != identifierKey(other.Table) ||
		i.Unique != other.Unique {
		return false
	}

	if i.Where != other.Where {
		return false
	}

	return slices.Equal(i.Keys, other.Keys)
}

func (i *SQLiteIndex) CreateInstruction() *SQLiteCreateIndexInstruction {
	return &SQLiteCreateIndexInstruction{
		Unique:    i.Unique,
		Name:      i.Name,
		TableName: i.Table,
		Keys:      i.Keys,
		Where:     i.Where,
	}
}

const noQuote = rune(0)

func closingQuote(character rune) rune {
	switch character {
	case '\'', '"', '`':
		return character
	case '[':
		return ']'
	}

	return noQuote
}

// PRAGMA index_info gives no name for an expression key, and it gives no condition.
func parseIndexDefinition(definition string) ([]string, string) {
	definition = withoutComments(definition)

	opening, closing := keyListBounds(definition)
	if opening < 0 || closing < 0 {
		return nil, ""
	}

	keys := splitTopLevel(definition[opening+1 : closing])

	for position, key := range keys {
		keys[position] = strings.TrimSpace(key)
	}

	return keys, parseIndexCondition(definition[closing+1:])
}

func parseIndexCondition(text string) string {
	const whereKeyword = "WHERE"

	condition := strings.TrimSpace(text)
	if len(condition) <= len(whereKeyword) || !strings.EqualFold(condition[:len(whereKeyword)], whereKeyword) {
		return ""
	}

	return strings.TrimSpace(condition[len(whereKeyword):])
}

func (d *SQLiteDriver) GetTableIndexes(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	indexList []sqliteIndexListEntry,
) ([]*SQLiteIndex, error) {
	definitions, err := d.GetIndexDefinitions(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	var indexes []*SQLiteIndex

	for _, index := range indexList {
		// The origin "c" marks an index that a CREATE INDEX statement built. The column
		// definition already prints the index of a UNIQUE constraint or of a PRIMARY KEY.
		if index.Origin != "c" {
			continue
		}

		definitionKeys, condition := parseIndexDefinition(definitions[index.Name])

		keys, err := d.GetIndexKeys(ctx, db, index.Name, definitionKeys)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, &SQLiteIndex{
			Table:  tableName,
			Name:   index.Name,
			Unique: index.Unique,
			Keys:   keys,
			Where:  condition,
		})
	}

	// PRAGMA index_list gives no stable order.
	slices.SortStableFunc(indexes, func(first *SQLiteIndex, second *SQLiteIndex) int {
		return strings.Compare(first.Name, second.Name)
	})

	return indexes, nil
}

type sqliteIndexListEntry struct {
	Name   string
	Unique bool
	Origin string
}

func (d *SQLiteDriver) getIndexList(ctx context.Context, db *sql.DB, tableName string) ([]sqliteIndexListEntry, error) {
	return driversshared.QueryAll(ctx, db, "PRAGMA index_list("+driversshared.QuoteIdentifier(tableName)+");", func(rows *sql.Rows) (sqliteIndexListEntry, error) {
		var entry sqliteIndexListEntry
		var position int
		var isUnique int
		var partial int

		err := rows.Scan(&position, &entry.Name, &isUnique, &entry.Origin, &partial)
		entry.Unique = isUnique == 1

		return entry, err
	})
}

// PRAGMA index_info gives a NULL name for an expression key.
type sqliteIndexInfoEntry struct {
	Position int
	Name     sql.NullString
}

func (d *SQLiteDriver) getIndexInfo(ctx context.Context, db *sql.DB, indexName string) ([]sqliteIndexInfoEntry, error) {
	return driversshared.QueryAll(ctx, db, "PRAGMA index_info("+driversshared.QuoteIdentifier(indexName)+");", func(rows *sql.Rows) (sqliteIndexInfoEntry, error) {
		var entry sqliteIndexInfoEntry
		var columnID int

		err := rows.Scan(&entry.Position, &columnID, &entry.Name)

		return entry, err
	})
}

// An index that a UNIQUE constraint or a PRIMARY KEY builds holds no text.
func (d *SQLiteDriver) GetIndexDefinitions(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name", tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	definitions := make(map[string]string)

	for rows.Next() {
		var name, definition string

		err := rows.Scan(&name, &definition)
		if err != nil {
			return nil, err
		}

		definitions[name] = definition
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return definitions, nil
}

// PRAGMA index_info gives no name for an expression key, no direction, and no collation,
// so definitionKeys and indexKeyModifiers read those parts from the text.
func (d *SQLiteDriver) GetIndexKeys(
	ctx context.Context,
	db *sql.DB,
	indexName string,
	definitionKeys []string,
) ([]string, error) {
	indexInfo, err := d.getIndexInfo(ctx, db, indexName)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(indexInfo))

	for _, entry := range indexInfo {
		keyPosition := entry.Position
		definitionKeyFound := keyPosition >= 0 && keyPosition < len(definitionKeys)

		if entry.Name.Valid {
			modifiers := ""

			if definitionKeyFound {
				modifiers = indexKeyModifiers(definitionKeys[keyPosition])
			}

			keys = append(keys, driversshared.QuoteIdentifier(entry.Name.String)+modifiers)

			continue
		}

		if !definitionKeyFound {
			return nil, fmt.Errorf("the definition of the index %s holds no key at the position %d", indexName, keyPosition)
		}

		keys = append(keys, definitionKeys[keyPosition])
	}

	return keys, nil
}
