package driverssqlite

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

// SQLite holds no ALTER statement for a virtual table, so a new definition prints a DROP
// statement and a CREATE statement. The module builds the shadow tables.
type SQLiteVirtualTable struct {
	Name string

	// The statement holds no semicolon.
	SQL string
}

func (t *SQLiteVirtualTable) CreateInstruction() *SQLiteCreateVirtualTableInstruction {
	return &SQLiteCreateVirtualTableInstruction{Definition: t.SQL}
}

func (t *SQLiteVirtualTable) DropInstruction() *driversshared.SQLDropTableInstruction {
	return &driversshared.SQLDropTableInstruction{Name: t.Name}
}

func (d *SQLiteDriver) GetVirtualTables(ctx context.Context, db *sql.DB) ([]*SQLiteVirtualTable, error) {
	tables, err := driversshared.QueryAll(ctx, db, `
		SELECT master.name, master.sql
		FROM sqlite_master AS master
		WHERE master.type = 'table' AND master.name NOT LIKE 'sqlite_%'
		AND EXISTS (
			SELECT 1 FROM pragma_table_list AS list
			WHERE list.schema = 'main' AND list.name = master.name AND list.type = 'virtual'
		)
		ORDER BY master.rowid;
	`, func(rows *sql.Rows) (*SQLiteVirtualTable, error) {
		table := &SQLiteVirtualTable{}

		err := rows.Scan(&table.Name, &table.SQL)

		return table, err
	})
	if err != nil {
		return nil, err
	}

	return lo.Reject(tables, func(table *SQLiteVirtualTable, _ int) bool {
		return d.isIgnoredTable(table.Name)
	}), nil
}

func (d *SQLiteDriver) DiffVirtualTables(ctx context.Context) ([]driversshared.Instruction, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection, d.GetVirtualTables, driversshared.ReplaceRules(
		func(table *SQLiteVirtualTable) string {
			return identifierKey(table.Name)
		},
		func(desired *SQLiteVirtualTable, current *SQLiteVirtualTable) bool {
			return slices.Equal(moduleArgumentTokens(desired.SQL), moduleArgumentTokens(current.SQL))
		},
		func(table *SQLiteVirtualTable) driversshared.Instruction {
			return table.CreateInstruction()
		},
		func(table *SQLiteVirtualTable) driversshared.Instruction {
			return table.DropInstruction()
		}))
	if err != nil {
		return nil, err
	}

	return append(additions, removals...), nil
}

// SQLite keeps the CREATE VIRTUAL TABLE statement as the user wrote it, and a new definition
// drops the table and its rows. The tokens skip the comments, the whitespace, the case of
// a bare word, and the table name, because none of them changes the table.
func moduleArgumentTokens(definition string) []string {
	text := withoutComments(definition)

	var tokens []string

	index := 0

	for index < len(text) {
		end := endOfQuotedTextOrComment(text, index)

		switch {
		case end > index:
			tokens = append(tokens, text[index:end])
		case isWordByte(text[index]):
			end = index

			for end < len(text) && isWordByte(text[end]) {
				end++
			}

			tokens = append(tokens, identifierKey(text[index:end]))
		case strings.ContainsRune(" \t\r\n\f\v", rune(text[index])):
			end = index + 1
		default:
			end = index + 1
			tokens = append(tokens, text[index:end])
		}

		index = end
	}

	usingPosition := slices.Index(tokens, "using")
	if usingPosition < 0 {
		return tokens
	}

	return tokens[usingPosition+1:]
}

func isWordByte(character byte) bool {
	return character == '_' || character == '$' || character >= 0x80 ||
		(character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
		(character >= '0' && character <= '9')
}
