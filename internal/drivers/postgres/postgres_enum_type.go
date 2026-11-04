package driverspostgres

import (
	"context"
	"database/sql"
	"slices"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresEnumType struct {
	Name    string
	Values  []string
	Comment string
}

func (t *PostgresEnumType) CreateInstruction() *PostgresCreateEnumTypeInstruction {
	return &PostgresCreateEnumTypeInstruction{
		Name:   t.Name,
		Labels: t.Values,
	}
}

// CREATE TYPE accepts no comment, so the comment takes its own statement.
func (t *PostgresEnumType) Instructions() []driversshared.Instruction {
	instructions := []driversshared.Instruction{t.CreateInstruction()}

	if t.Comment != "" {
		instructions = append(instructions, &PostgresCommentOnTypeInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	return instructions
}

func (t *PostgresEnumType) KeepsOrderOf(other *PostgresEnumType) bool {
	position := 0

	for _, value := range t.Values {
		if position < len(other.Values) && other.Values[position] == value {
			position++
		}
	}

	return position == len(other.Values)
}

// PostgreSQL adds a value to an enum, but it removes none and it moves none.
func (t *PostgresEnumType) Diff(other *PostgresEnumType) []driversshared.Instruction {
	if !t.KeepsOrderOf(other) {
		instructions := []driversshared.Instruction{&PostgresDropTypeInstruction{Name: t.Name}}

		return append(instructions, t.Instructions()...)
	}

	lastSourceValue, _ := lo.Last(other.Values)
	lastSourceIndex := slices.Index(t.Values, lastSourceValue)

	var instructions []driversshared.Instruction

	for index, value := range t.Values {
		if slices.Contains(other.Values, value) {
			continue
		}

		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:  t.Name,
			Value: value,
		}

		// A value after the last source value takes no position, so PostgreSQL adds it at the end.
		if index < lastSourceIndex {
			if index == 0 {
				instruction.Before = other.Values[0]
			} else {
				instruction.After = t.Values[index-1]
			}
		}

		instructions = append(instructions, instruction)
	}

	if t.Comment != other.Comment {
		instructions = append(instructions, &PostgresCommentOnTypeInstruction{
			Name: t.Name,
			Text: t.Comment,
		})
	}

	return instructions
}

func (d *PostgresDriver) DiffEnumTypes(ctx context.Context) (*driversshared.SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresEnumType]{
		Get: d.GetEnumTypes,
		Key: func(enumType *PostgresEnumType) string {
			return enumType.Name
		},
		Create: func(enumType *PostgresEnumType) []driversshared.Instruction {
			return enumType.Instructions()
		},
		Change: func(target *PostgresEnumType, source *PostgresEnumType) []driversshared.Instruction {
			return target.Diff(source)
		},
		Drop: func(source *PostgresEnumType) driversshared.Instruction {
			return &PostgresDropTypeInstruction{Name: source.Name}
		},
	})
}

func (d *PostgresDriver) GetEnumTypes(ctx context.Context, db *sql.DB) ([]*PostgresEnumType, error) {
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

	var types []*PostgresEnumType

	for rows.Next() {
		var typeName, value, comment string

		err := rows.Scan(&typeName, &value, &comment)
		if err != nil {
			return nil, err
		}

		lastType, _ := lo.Last(types)
		if lastType == nil || lastType.Name != typeName {
			lastType = &PostgresEnumType{
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
