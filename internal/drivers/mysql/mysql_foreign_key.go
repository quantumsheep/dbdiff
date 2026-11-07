package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/samber/lo"
)

type MySQLForeignKey struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnUpdate          string
	OnDelete          string
}

func (k *MySQLForeignKey) Equal(other *MySQLForeignKey) bool {
	return k.Name == other.Name &&
		slices.Equal(k.Columns, other.Columns) &&
		k.ReferencedTable == other.ReferencedTable &&
		slices.Equal(k.ReferencedColumns, other.ReferencedColumns) &&
		sameReferentialAction(k.OnUpdate, other.OnUpdate) &&
		sameReferentialAction(k.OnDelete, other.OnDelete)
}

// InnoDB checks NO ACTION at once, the same as RESTRICT. MySQL stores the default rule as
// NO ACTION and MariaDB stores it as RESTRICT.
func sameReferentialAction(rule string, otherRule string) bool {
	return rule == otherRule || !withReferentialAction(rule) && !withReferentialAction(otherRule)
}

func (k *MySQLForeignKey) Clause() string {
	clause := fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		QuoteIdentifier(k.Name),
		strings.Join(QuoteIdentifiers(k.Columns), ", "),
		QuoteIdentifier(k.ReferencedTable),
		strings.Join(QuoteIdentifiers(k.ReferencedColumns), ", "))

	if withReferentialAction(k.OnDelete) {
		clause += " ON DELETE " + k.OnDelete
	}

	if withReferentialAction(k.OnUpdate) {
		clause += " ON UPDATE " + k.OnUpdate
	}

	return clause
}

func withReferentialAction(rule string) bool {
	return rule != "" && rule != "NO ACTION" && rule != "RESTRICT"
}

func (d *MySQLDriver) GetTableForeignKeys(
	ctx context.Context,
	db *sql.DB,
	tableName string,
) ([]*MySQLForeignKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT rc.CONSTRAINT_NAME, rc.UPDATE_RULE, rc.DELETE_RULE, rc.REFERENCED_TABLE_NAME,
			kcu.COLUMN_NAME, kcu.REFERENCED_COLUMN_NAME
		FROM information_schema.REFERENTIAL_CONSTRAINTS rc
		JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
			AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND kcu.TABLE_NAME = rc.TABLE_NAME
		WHERE rc.CONSTRAINT_SCHEMA = DATABASE() AND rc.TABLE_NAME = ?
		ORDER BY rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;
	`, tableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var foreignKeys []*MySQLForeignKey

	for rows.Next() {
		var constraintName, updateRule, deleteRule, referencedTable, columnName, referencedColumn string

		err := rows.Scan(&constraintName, &updateRule, &deleteRule, &referencedTable,
			&columnName, &referencedColumn)
		if err != nil {
			return nil, err
		}

		foreignKey, found := lo.Find(foreignKeys, func(foreignKey *MySQLForeignKey) bool {
			return foreignKey.Name == constraintName
		})
		if !found {
			foreignKey = &MySQLForeignKey{
				Name:            constraintName,
				ReferencedTable: referencedTable,
				OnUpdate:        updateRule,
				OnDelete:        deleteRule,
			}

			foreignKeys = append(foreignKeys, foreignKey)
		}

		foreignKey.Columns = append(foreignKey.Columns, columnName)
		foreignKey.ReferencedColumns = append(foreignKey.ReferencedColumns, referencedColumn)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return foreignKeys, nil
}
