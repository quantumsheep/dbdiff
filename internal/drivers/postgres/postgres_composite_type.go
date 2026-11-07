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

// ADD ATTRIBUTE puts the attribute at the end. PostgreSQL refuses ALTER ATTRIBUTE TYPE
// while a column uses the type. Thus only the removals and the additions at the end can
// keep the type. The result is false if another change is necessary.
func (t *PostgresCompositeType) attributeChanges(other *PostgresCompositeType) ([]driversshared.Instruction, bool) {
	var instructions []driversshared.Instruction
	var keptAttributes []*PostgresCompositeTypeAttribute

	for _, currentAttribute := range other.Attributes {
		desiredAttribute, found := lo.Find(t.Attributes, func(attribute *PostgresCompositeTypeAttribute) bool {
			return attribute.Name == currentAttribute.Name
		})
		if !found {
			instructions = append(instructions, &PostgresAlterTypeDropAttributeInstruction{
				Name:          t.Name,
				AttributeName: currentAttribute.Name,
			})

			continue
		}

		if desiredAttribute.Type != currentAttribute.Type {
			return nil, false
		}

		keptAttributes = append(keptAttributes, currentAttribute)
	}

	if !slices.EqualFunc(keptAttributes, t.Attributes[:len(keptAttributes)], func(first, second *PostgresCompositeTypeAttribute) bool {
		return *first == *second
	}) {
		return nil, false
	}

	for _, attribute := range t.Attributes[len(keptAttributes):] {
		instructions = append(instructions, &PostgresAlterTypeAddAttributeInstruction{
			Name:      t.Name,
			Attribute: attribute,
		})
	}

	return instructions, true
}

// If only relations use the type, a recreation gives the wanted attribute list in every case.
func (d *PostgresDriver) diffCompositeType(
	ctx context.Context,
	desired *PostgresCompositeType,
	current *PostgresCompositeType,
) ([]driversshared.Instruction, error) {
	if desired.Equal(current) {
		return nil, nil
	}

	dependents, err := getTypeDependents(ctx, d.CurrentDatabaseConnection, current.Name)
	if err != nil {
		return nil, err
	}

	// DiffViewsAndMaterializedViews drops each marked relation before this section and
	// creates it again after the tables.
	onlyRelations := lo.EveryBy(dependents, func(dependent *PostgresTypeDependent) bool {
		return dependent.Kind == "relation"
	})
	if onlyRelations {
		for _, dependent := range dependents {
			d.markRecreated(dependent.TableName)
		}

		return []driversshared.Instruction{current.DropInstruction(), desired.CreateInstruction()}, nil
	}

	instructions, possible := desired.attributeChanges(current)
	if !possible {
		return nil, typeDependentError(dependents[0].Name, current.Name)
	}

	return instructions, nil
}

func (d *PostgresDriver) DiffCompositeTypes(ctx context.Context) (*SectionDiff, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection,
		d.GetCompositeTypes, driversshared.DiffRules[*PostgresCompositeType]{
			Key: func(compositeType *PostgresCompositeType) string {
				return compositeType.Name
			},
			Create: func(compositeType *PostgresCompositeType) []driversshared.Instruction {
				return []driversshared.Instruction{compositeType.CreateInstruction()}
			},
			Change: func(desired *PostgresCompositeType, current *PostgresCompositeType) ([]driversshared.Instruction, error) {
				return d.diffCompositeType(ctx, desired, current)
			},
			Drop: func(current *PostgresCompositeType) []driversshared.Instruction {
				return []driversshared.Instruction{current.DropInstruction()}
			},
		})
	if err != nil {
		return nil, err
	}

	return &SectionDiff{
		Additions: additions,
		Removals:  removals,
	}, nil
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
