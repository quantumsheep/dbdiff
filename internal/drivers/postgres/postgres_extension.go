package driverspostgres

import (
	"context"
	"database/sql"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type PostgresExtension struct {
	Name    string
	Version string
}

func (e *PostgresExtension) CreateInstruction() *PostgresCreateExtensionInstruction {
	return &PostgresCreateExtensionInstruction{Name: e.Name}
}

func (e *PostgresExtension) UpdateInstruction() *PostgresAlterExtensionInstruction {
	return &PostgresAlterExtensionInstruction{
		Name:       e.Name,
		NewVersion: e.Version,
	}
}

func (e *PostgresExtension) DropInstruction() *PostgresDropExtensionInstruction {
	return &PostgresDropExtensionInstruction{Name: e.Name}
}

func (d *PostgresDriver) DiffExtensions(ctx context.Context) (*SectionDiff, error) {
	return diffSection(ctx, d, sectionRules[*PostgresExtension]{
		Get: d.GetExtensions,
		Key: func(extension *PostgresExtension) string {
			return extension.Name
		},
		Create: func(extension *PostgresExtension) []driversshared.Instruction {
			return []driversshared.Instruction{extension.CreateInstruction()}
		},
		Change: func(desired *PostgresExtension, current *PostgresExtension) []driversshared.Instruction {
			if desired.Version == current.Version {
				return nil
			}

			return []driversshared.Instruction{desired.UpdateInstruction()}
		},
		Drop: func(current *PostgresExtension) driversshared.Instruction {
			return current.DropInstruction()
		},
	})
}

func (d *PostgresDriver) GetExtensions(ctx context.Context, db *sql.DB) ([]*PostgresExtension, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT extname, extversion
		FROM pg_extension
		WHERE extnamespace = quote_ident(current_schema())::regnamespace
		ORDER BY extname
	`, func(rows *sql.Rows) (*PostgresExtension, error) {
		extension := &PostgresExtension{}

		err := rows.Scan(&extension.Name, &extension.Version)

		return extension, err
	})
}
