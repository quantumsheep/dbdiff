package driverspostgres

import (
	"context"
	"database/sql"
	"slices"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type PostgresCompositeTypeAttribute struct {
	Name string
	Type string
}

type PostgresCompositeType struct {
	Name       string
	Attributes []*PostgresCompositeTypeAttribute
}

func (t *PostgresCompositeType) Equal(other *PostgresCompositeType) bool {
	if t.Name != other.Name {
		return false
	}

	return slices.EqualFunc(t.Attributes, other.Attributes, func(first, second *PostgresCompositeTypeAttribute) bool {
		return *first == *second
	})
}

func (t *PostgresCompositeType) CreateInstruction() *PostgresCreateCompositeTypeInstruction {
	return &PostgresCreateCompositeTypeInstruction{
		Name:       t.Name,
		Attributes: t.Attributes,
	}
}

func (t *PostgresCompositeType) DropInstruction() *PostgresDropTypeInstruction {
	return &PostgresDropTypeInstruction{Name: t.Name}
}

// One ALTER TYPE statement changes one attribute only, and the order of the attributes
// stays fixed. A recreation gives the wanted attribute list in every case.
func (t *PostgresCompositeType) Diff(other *PostgresCompositeType) []driversshared.Instruction {
	if t.Equal(other) {
		return nil
	}

	return []driversshared.Instruction{other.DropInstruction(), t.CreateInstruction()}
}

func (d *PostgresDriver) DiffCompositeTypes(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresCompositeType]{
		Get: d.GetCompositeTypes,
		Key: func(compositeType *PostgresCompositeType) string {
			return compositeType.Name
		},
		Create: func(compositeType *PostgresCompositeType) []driversshared.Instruction {
			return []driversshared.Instruction{compositeType.CreateInstruction()}
		},
		Change: func(desired *PostgresCompositeType, current *PostgresCompositeType) []driversshared.Instruction {
			return desired.Diff(current)
		},
		Drop: func(current *PostgresCompositeType) driversshared.Instruction {
			return current.DropInstruction()
		},
	})
}

func (d *PostgresDriver) GetCompositeTypes(ctx context.Context, db *sql.DB) ([]*PostgresCompositeType, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT t.typname, a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_type t
		JOIN pg_class c ON c.oid = t.typrelid AND c.relkind = 'c'
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		WHERE t.typnamespace = current_schema()::regnamespace
		AND t.typtype = 'c'
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
		)
		ORDER BY t.typname, a.attnum
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var compositeTypes []*PostgresCompositeType

	for rows.Next() {
		var typeName, attributeName, attributeType string

		err := rows.Scan(&typeName, &attributeName, &attributeType)
		if err != nil {
			return nil, err
		}

		lastCompositeType, _ := lo.Last(compositeTypes)
		if lastCompositeType == nil || lastCompositeType.Name != typeName {
			lastCompositeType = &PostgresCompositeType{Name: typeName}
			compositeTypes = append(compositeTypes, lastCompositeType)
		}

		attribute := &PostgresCompositeTypeAttribute{
			Name: attributeName,
			Type: attributeType,
		}
		lastCompositeType.Attributes = append(lastCompositeType.Attributes, attribute)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return compositeTypes, nil
}
