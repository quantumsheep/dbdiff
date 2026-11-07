package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
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

	for _, desiredConstraint := range d.Constraints {
		currentConstraint, found := other.ConstraintByName(desiredConstraint.Name)
		if !found {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name: d.Name,
				Action: &PostgresAddDomainConstraintAction{
					ConstraintName: desiredConstraint.Name,
					Definition:     desiredConstraint.Def,
				},
			})

			continue
		}

		if desiredConstraint.Def != currentConstraint.Def {
			instructions = append(instructions,
				&PostgresAlterDomainInstruction{
					Name: d.Name,
					Action: &PostgresDropDomainConstraintAction{
						ConstraintName: currentConstraint.Name,
					},
				},
				&PostgresAlterDomainInstruction{
					Name: d.Name,
					Action: &PostgresAddDomainConstraintAction{
						ConstraintName: desiredConstraint.Name,
						Definition:     desiredConstraint.Def,
					},
				})
		}
	}

	for _, currentConstraint := range other.Constraints {
		_, found := d.ConstraintByName(currentConstraint.Name)
		if !found {
			instructions = append(instructions, &PostgresAlterDomainInstruction{
				Name: d.Name,
				Action: &PostgresDropDomainConstraintAction{
					ConstraintName: currentConstraint.Name,
				},
			})
		}
	}

	return instructions
}

func (d *PostgresDriver) DiffDomains(ctx context.Context) (*SectionDiff, error) {
	additions, removals, err := driversshared.DiffSides(ctx, d.DesiredDatabaseConnection, d.CurrentDatabaseConnection,
		d.GetDomains, driversshared.DiffRules[*PostgresDomain]{
			Key: func(domain *PostgresDomain) string {
				return domain.Name
			},
			Create: func(domain *PostgresDomain) []driversshared.Instruction {
				return []driversshared.Instruction{domain.CreateInstruction()}
			},
			Change: func(desired *PostgresDomain, current *PostgresDomain) ([]driversshared.Instruction, error) {
				if desired.BaseType != current.BaseType {
					instructions, err := d.replaceUsedType(ctx, "DOMAIN", desired.Name,
						[]driversshared.Instruction{desired.CreateInstruction()}, "")
					if err != nil || instructions != nil {
						return instructions, err
					}
				}

				return desired.Diff(current), nil
			},
			Drop: func(current *PostgresDomain) []driversshared.Instruction {
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

func (d *PostgresDriver) GetDomains(ctx context.Context, db *sql.DB) ([]*PostgresDomain, error) {
	// PostgreSQL 17 keeps the NOT NULL flag of a domain in pg_constraint too. This query
	// reads the check constraints only, because typnotnull holds that flag already.
	rows, err := db.QueryContext(ctx, `
		SELECT
			t.typname,
			format_type(t.typbasetype, t.typtypmod),
			t.typnotnull,
			t.typdefault,
			c.conname,
			pg_get_constraintdef(c.oid)
		FROM pg_type t
		LEFT JOIN pg_constraint c ON c.contypid = t.oid AND c.contype = 'c'
		WHERE t.typnamespace = current_schema()::regnamespace
		AND t.typtype = 'd'
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
		)
		ORDER BY t.typname, c.conname
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var domains []*PostgresDomain

	for rows.Next() {
		var domainName, baseType string
		var notNull bool
		var defaultValue, constraintName, constraintDef sql.NullString

		err := rows.Scan(&domainName, &baseType, &notNull, &defaultValue, &constraintName, &constraintDef)
		if err != nil {
			return nil, err
		}

		lastDomain, _ := lo.Last(domains)
		if lastDomain == nil || lastDomain.Name != domainName {
			lastDomain = &PostgresDomain{
				Name:     domainName,
				BaseType: baseType,
				NotNull:  notNull,
				Default:  defaultValue,
			}
			domains = append(domains, lastDomain)
		}

		if constraintName.Valid {
			constraint := &PostgresDomainConstraint{
				Name: constraintName.String,
				Def:  constraintDef.String,
			}
			lastDomain.Constraints = append(lastDomain.Constraints, constraint)
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return domains, nil
}
