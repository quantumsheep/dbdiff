package driverspostgres

import (
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresDomainConstraint struct {
	Name string
	Def  string
}

type PostgresDomain struct {
	Name        string
	BaseType    string
	NotNull     bool
	Default     sql.NullString
	Constraints []*PostgresDomainConstraint
}

func (d *PostgresDomain) ConstraintByName(name string) (*PostgresDomainConstraint, bool) {
	for _, constraint := range d.Constraints {
		if constraint.Name == name {
			return constraint, true
		}
	}

	return nil, false
}

func (d *PostgresDomain) CreateInstruction() *PostgresCreateDomainInstruction {
	return &PostgresCreateDomainInstruction{
		Name:        d.Name,
		BaseType:    d.BaseType,
		Default:     d.Default,
		NotNull:     d.NotNull,
		Constraints: d.Constraints,
	}
}

func (d *PostgresDomain) DropInstruction() *PostgresDropDomainInstruction {
	return &PostgresDropDomainInstruction{Name: d.Name}
}

// PostgreSQL changes no base type of a domain, so a new base type needs a recreation.
func (d *PostgresDomain) Diff(other *PostgresDomain) []driversshared.Instruction {
	if d.BaseType != other.BaseType {
		return []driversshared.Instruction{other.DropInstruction(), d.CreateInstruction()}
	}

	var instructions []driversshared.Instruction

	if d.Default != other.Default {
		if d.Default.Valid {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name:   d.Name,
				Action: &PostgresSetDomainDefaultAction{Expression: d.Default.String},
			})
		} else {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name:   d.Name,
				Action: &PostgresDropDomainDefaultAction{},
			})
		}
	}

	if d.NotNull != other.NotNull {
		if d.NotNull {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name:   d.Name,
				Action: &PostgresSetDomainNotNullAction{},
			})
		} else {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name:   d.Name,
				Action: &PostgresDropDomainNotNullAction{},
			})
		}
	}

	for _, targetConstraint := range d.Constraints {
		sourceConstraint, found := other.ConstraintByName(targetConstraint.Name)
		if !found {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name: d.Name,
				Action: &PostgresAddDomainConstraintAction{
					ConstraintName: targetConstraint.Name,
					Definition:     targetConstraint.Def,
				},
			})

			continue
		}

		if targetConstraint.Def != sourceConstraint.Def {
			instructions = append(instructions,
				&PostgresAlterDomainInstruction{
					Name: d.Name,
					Action: &PostgresDropDomainConstraintAction{
						ConstraintName: sourceConstraint.Name,
					},
				},
				&PostgresAlterDomainInstruction{
					Name: d.Name,
					Action: &PostgresAddDomainConstraintAction{
						ConstraintName: targetConstraint.Name,
						Definition:     targetConstraint.Def,
					},
				})
		}
	}

	for _, sourceConstraint := range other.Constraints {
		_, found := d.ConstraintByName(sourceConstraint.Name)
		if !found {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name: d.Name,
				Action: &PostgresDropDomainConstraintAction{
					ConstraintName: sourceConstraint.Name,
				},
			})
		}
	}

	return instructions
}
