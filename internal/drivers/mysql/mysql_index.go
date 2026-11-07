package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
)

type MySQLIndex struct {
	Table string
	Name  string

	Kind string

	// A key holds the SQL text of one key part, so a prefix length, a DESC keyword, and
	// an expression key keep their text.
	Keys []string
}

func (i *MySQLIndex) Equal(other *MySQLIndex) bool {
	if i.Name != other.Name || i.Table != other.Table || i.Kind != other.Kind {
		return false
	}

	return slices.Equal(i.Keys, other.Keys)
}

func (i *MySQLIndex) UsesColumn(columnName string) bool {
	return slices.ContainsFunc(i.Keys, func(key string) bool {
		return strings.Contains(key, QuoteIdentifier(columnName))
	})
}

func (i *MySQLIndex) CreateInstruction() *MySQLCreateIndexInstruction {
	return &MySQLCreateIndexInstruction{
		Kind:      i.Kind,
		Name:      i.Name,
		TableName: i.Table,
		Keys:      i.Keys,
	}
}

func (i *MySQLIndex) DropInstruction() *MySQLDropIndexInstruction {
	return &MySQLDropIndexInstruction{
		Name:      i.Name,
		TableName: i.Table,
	}
}

func (d *MySQLDriver) GetTableIndexes(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) ([]string, []*MySQLIndex, error) {
	mariadb := d.detailsByConnection[db].mariadb

	expressionColumn := "NULL"
	if !mariadb {
		expressionColumn = "EXPRESSION"
	}

	// The STATISTICS table of MariaDB holds no EXPRESSION column, so the query of MariaDB
	// selects NULL in its place.
	query := fmt.Sprintf(`
		SELECT INDEX_NAME, NON_UNIQUE, INDEX_TYPE, COLUMN_NAME, SUB_PART, COLLATION, %s
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX;
	`, expressionColumn)

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, nil, err
	}

	defer func() { _ = rows.Close() }()

	var primaryKey []string
	var indexes []*MySQLIndex

	for rows.Next() {
		var indexName, indexType string
		var nonUnique int
		var columnName, collation, expression sql.NullString
		var subPart sql.NullInt64

		err := rows.Scan(&indexName, &nonUnique, &indexType, &columnName, &subPart, &collation, &expression)
		if err != nil {
			return nil, nil, err
		}

		if indexName == "PRIMARY" {
			primaryKey = append(primaryKey, columnName.String)
			continue
		}

		index, found := lo.Find(indexes, func(index *MySQLIndex) bool {
			return index.Name == indexName
		})
		if !found {
			index = &MySQLIndex{
				Table: tableName,
				Name:  indexName,
				Kind:  indexKind(indexType, nonUnique),
			}

			indexes = append(indexes, index)
		}

		if !mariadb {
			expression.String = unescapeCatalogText(expression.String)
		}

		index.Keys = append(index.Keys, indexKey(index.Kind,
			columnName, subPart, collation, expression))
	}

	err = rows.Err()
	if err != nil {
		return nil, nil, err
	}

	return primaryKey, indexes, nil
}

func indexKind(indexType string, nonUnique int) string {
	if indexType == "FULLTEXT" || indexType == "SPATIAL" {
		return indexType
	}

	if nonUnique == 0 {
		return "UNIQUE"
	}

	return ""
}

// A FULLTEXT index and a SPATIAL index report an internal prefix length, and the CREATE
// INDEX statement refuses it.
func indexKey(
	kind string,
	columnName sql.NullString,
	subPart sql.NullInt64,
	collation sql.NullString,
	expression sql.NullString,
) string {
	if expression.Valid && expression.String != "" {
		return "(" + expression.String + ")"
	}

	key := QuoteIdentifier(columnName.String)

	if subPart.Valid && kind != "FULLTEXT" && kind != "SPATIAL" {
		key += fmt.Sprintf("(%d)", subPart.Int64)
	}

	if collation.String == "D" {
		key += " DESC"
	}

	return key
}
