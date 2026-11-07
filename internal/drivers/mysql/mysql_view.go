package driversmysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type MySQLView struct {
	Name       string
	Definition string

	// The two attributes stay empty for the defaults, DEFINER and NONE.
	SecurityType string
	CheckOption  string
}

func (v *MySQLView) createInstruction() *MySQLCreateViewInstruction {
	return &MySQLCreateViewInstruction{
		Name:         v.Name,
		Definition:   v.Definition,
		SecurityType: v.SecurityType,
		CheckOption:  v.CheckOption,
	}
}

func (v *MySQLView) Instructions() []driversshared.Instruction {
	return []driversshared.Instruction{v.createInstruction()}
}

func (v *MySQLView) Diff(other *MySQLView) ([]driversshared.Instruction, error) {
	if *v == *other {
		return nil, nil
	}

	instruction := v.createInstruction()
	instruction.OrReplace = true

	return []driversshared.Instruction{instruction}, nil
}

// MySQL checks the names that a view reads at the creation of the view, so a view comes
// after the views that it reads. MariaDB has no VIEW_TABLE_USAGE table, so the dependency
// reads the definition text. Two independent views keep their order in the input.
func sortViewsByDependency(views []*MySQLView) []*MySQLView {
	sorted := make([]*MySQLView, 0, len(views))
	visited := make(map[string]bool, len(views))

	var visit func(view *MySQLView)

	visit = func(view *MySQLView) {
		if visited[view.Name] {
			return
		}

		visited[view.Name] = true

		for _, otherView := range views {
			if otherView != view && strings.Contains(view.Definition, QuoteIdentifier(otherView.Name)) {
				visit(otherView)
			}
		}

		sorted = append(sorted, view)
	}

	for _, view := range views {
		visit(view)
	}

	return sorted
}

// A string literal can hold the qualifier text, so the removal skips each string literal.
// MySQL and MariaDB escape a quote of a literal with a backslash in every SQL mode.
func removeDatabaseQualifier(definition string, databaseQualifier string) string {
	var builder strings.Builder

	position := 0

	for position < len(definition) {
		if strings.HasPrefix(definition[position:], databaseQualifier) {
			position += len(databaseQualifier)

			continue
		}

		end := position + 1

		switch definition[position] {
		case '`':
			end = driversshared.EndOfQuotedText(definition, position)
		case '\'':
			end = endOfStringLiteral(definition, position)
		}

		builder.WriteString(definition[position:end])
		position = end
	}

	return builder.String()
}

func endOfStringLiteral(content string, index int) int {
	index++

	for index < len(content) {
		switch content[index] {
		case '\\':
			index += 2
		case '\'':
			return index + 1
		default:
			index++
		}
	}

	return len(content)
}

// The definition text qualifies every name with the database, and the two sides hold two
// database names. The removal of the qualifier keeps the two texts comparable.
func (d *MySQLDriver) GetViews(ctx context.Context, db *sql.DB) ([]*MySQLView, error) {
	databaseQualifier := QuoteIdentifier(d.detailsByConnection[db].databaseName) + "."

	return driversshared.QueryAll(ctx, db, `
		SELECT TABLE_NAME, VIEW_DEFINITION, SECURITY_TYPE, CHECK_OPTION
		FROM information_schema.VIEWS
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME;
	`, func(rows *sql.Rows) (*MySQLView, error) {
		var name string
		var definition sql.NullString
		var securityType string
		var checkOption string

		err := rows.Scan(&name, &definition, &securityType, &checkOption)
		if err != nil {
			return nil, err
		}

		// The catalog gives NULL when the connection misses the SHOW VIEW privilege.
		if !definition.Valid {
			return nil, fmt.Errorf("the view %q gives no definition. Grant the SHOW VIEW privilege", name)
		}

		view := &MySQLView{
			Name:       name,
			Definition: removeDatabaseQualifier(definition.String, databaseQualifier),
		}

		if securityType != "DEFINER" {
			view.SecurityType = securityType
		}

		if checkOption != "NONE" {
			view.CheckOption = checkOption
		}

		return view, nil
	})
}
