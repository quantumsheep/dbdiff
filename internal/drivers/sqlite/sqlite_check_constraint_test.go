package driverssqlite

import (
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteCheckConstraint(t *testing.T) {
	t.Run("CreateTableWithCollationAndChecks", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE people (
				name TEXT COLLATE NOCASE,
				age INTEGER CHECK (age > 0),
				CHECK (length(name) < 100)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "people",
				Columns: []*SQLiteColumn{
					{
						Name:      "name",
						Type:      "TEXT",
						Collation: "NOCASE",
					},
					{
						Name:  "age",
						Type:  "INTEGER",
						Check: "(age > 0)",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{Expression: "(length(name) < 100)"},
				},
			},
		})
	})

	t.Run("ReorderedChecksGiveNoDiff", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE people (age INTEGER, CHECK (age > 0), CHECK (age < 200));`)
		driver.ExecOnDesired(`CREATE TABLE people (age INTEGER, CHECK (age < 200), CHECK (age > 0));`)
		driver.RequireInstructions(nil)
	})

	t.Run("AddTableCheckRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE people (name TEXT);
			INSERT INTO people (name) VALUES ('Ada');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE people (name TEXT, CHECK (length(name) < 100));
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("people",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_people_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "name",
							Type: "TEXT",
						},
					},
					CheckConstraints: []*SQLiteCheckConstraint{
						{Expression: "(length(name) < 100)"},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_people_temp",
					ColumnNames:       []string{"name"},
					SelectExpressions: []string{`"name"`},
					FromTableName:     "people",
				},
				&driversshared.SQLDropTableInstruction{Name: "people"},
				&SQLiteAlterTableInstruction{
					Name:   "_people_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "people"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("people", "")
		require.Equal(t, []map[string]any{
			{"name": "Ada"},
		}, rows)
	})

	t.Run("CreateTableWithANamedCheck", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE people (age INTEGER, CONSTRAINT age_is_positive CHECK (age > 0));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "people",
				Columns: []*SQLiteColumn{
					{
						Name: "age",
						Type: "INTEGER",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Name:       "age_is_positive",
						Expression: "(age > 0)",
					},
				},
			},
		})
	})

	t.Run("ModifyConstraintNameRecreatesTheTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_old CHECK (id > 0));
			INSERT INTO items (id) VALUES (5);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_new CHECK (id > 0));
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("items",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_items_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "id",
							Type: "INTEGER",
						},
					},
					CheckConstraints: []*SQLiteCheckConstraint{
						{
							Name:       "items_new",
							Expression: "(id > 0)",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_items_temp",
					ColumnNames:       []string{"id"},
					SelectExpressions: []string{`"id"`},
					FromTableName:     "items",
				},
				&driversshared.SQLDropTableInstruction{Name: "items"},
				&SQLiteAlterTableInstruction{
					Name:   "_items_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "items"},
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		rows := driver.FetchAllFromCurrent("items", "")
		require.Equal(t, []map[string]any{
			{"id": int64(5)},
		}, rows)
	})

	t.Run("EqualCheckConstraints", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_positive CHECK (id > 0));
		`)

		driver.ExecOnDesired(`
			CREATE TABLE items (id INTEGER, CONSTRAINT items_positive CHECK (id > 0));
		`)
		driver.RequireInstructions(nil)
	})
}
