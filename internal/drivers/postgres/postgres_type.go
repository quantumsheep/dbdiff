package driverspostgres

import (
	"context"
	"database/sql"
	"slices"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresType struct {
	Name    string
	Values  []string
	Comment string
}

func (t *PostgresType) CreateInstruction() *PostgresCreateEnumTypeInstruction {
	return &PostgresCreateEnumTypeInstruction{
		Name:   t.Name,
		Labels: t.Values,
	}
}

// CREATE TYPE accepts no comment, so the comment takes its own statement.
func (t *PostgresType) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{t.CreateInstruction()}

	if t.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnTypeInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	return instructions
}

func (t *PostgresType) StartsWith(other *PostgresType) bool {
	if len(other.Values) > len(t.Values) {
		return false
	}

	return slices.Equal(t.Values[:len(other.Values)], other.Values)
}

// PostgreSQL adds a value to an enum, but it removes none and it moves none.
func (t *PostgresType) Diff(other *PostgresType) []driversshared.Instruction {
	if !slices.Equal(t.Values, other.Values) && !t.StartsWith(other) {
		instructions := []driversshared.Instruction{&PostgresDropTypeInstruction{Name: t.Name}}

		return append(instructions, t.Instructions()...)
	}

	var instructions []driversshared.Instruction

	for _, value := range t.Values[len(other.Values):] {
		instructions = append(instructions, &PostgresAlterTypeAddValueInstruction{
			Name:  t.Name,
			Value: value,
		})
	}

	if t.Comment != other.Comment {
		instructions = append(instructions, &PostgresCommentOnTypeInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	return instructions
}

func (d *PostgresDriver) DiffTypes(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresType]{
		Get: d.GetTypes,
		Key: func(enumType *PostgresType) string {
			return enumType.Name
		},
		Create: func(enumType *PostgresType) []driversshared.Instruction {
			return enumType.Instructions()
		},
		Change: func(target *PostgresType, source *PostgresType) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresType) driversshared.Instruction {
			return &PostgresDropTypeInstruction{Name: source.Name}
		},
	})
}

func (d *PostgresDriver) GetTypes(ctx context.Context, db *sql.DB) ([]*PostgresType, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT t.typname, e.enumlabel, coalesce(obj_description(t.oid, 'pg_type'), '')
		FROM pg_type t
		JOIN pg_enum e ON e.enumtypid = t.oid
		WHERE t.typnamespace = current_schema()::regnamespace
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
		)
		ORDER BY t.typname, e.enumsortorder
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var types []*PostgresType

	for rows.Next() {
		var typeName, value, comment string

		err := rows.Scan(&typeName, &value, &comment)
		if err != nil {
			return nil, err
		}

		lastType, _ := lo.Last(types)
		if lastType == nil || lastType.Name != typeName {
			lastType = &PostgresType{
				Name:    typeName,
				Comment: comment,
			}
			types = append(types, lastType)
		}

		lastType.Values = append(lastType.Values, value)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return types, nil
}
