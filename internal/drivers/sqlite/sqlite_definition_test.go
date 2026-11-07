package driverssqlite

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestSQLiteDefinition(t *testing.T) {
	t.Run("CreateTableWithoutSpacesBeforeTheParentheses", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				price INTEGER CHECK(price > 0),
				total INTEGER GENERATED ALWAYS AS(price * 2) VIRTUAL,
				parent INTEGER,
				CONSTRAINT fk_parent FOREIGN KEY(parent) REFERENCES parents(id),
				CHECK(id > 0)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "parents",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
				},
			},
			&SQLiteCreateTableInstruction{
				Name: "items",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:   "price",
						Type:   "INTEGER",
						Checks: "CHECK (price > 0)",
					},
					{
						Name:                "total",
						Type:                "INTEGER",
						GeneratedExpression: "(price * 2)",
					},
					{
						Name: "parent",
						Type: "INTEGER",
					},
				},
				ForeignKeys: []*SQLiteForeignKey{
					{
						Name:     "fk_parent",
						Table:    "parents",
						From:     []string{"parent"},
						To:       []string{"id"},
						OnUpdate: "NO ACTION",
						OnDelete: "NO ACTION",
					},
				},
				CheckConstraints: []*SQLiteCheckConstraint{
					{
						Expression: "(id > 0)",
					},
				},
			},
		})
	})

	t.Run("CreateTableAndIndexWithComments", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE notes (
				id INTEGER PRIMARY KEY, -- the key (one, two
				body TEXT CHECK (body <> ''), /* a comment, with ( */
				score INTEGER -- a last comment )
			);
			CREATE INDEX notes_score ON notes (score DESC, -- a key comment, (
				lower(body)) WHERE score > 0 -- a condition comment
			;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "notes",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:   "body",
						Type:   "TEXT",
						Checks: "CHECK (body <> '')",
					},
					{
						Name: "score",
						Type: "INTEGER",
					},
				},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "notes_score",
				TableName: "notes",
				Keys:      []string{`"score" DESC`, "lower(body)"},
				Where:     "score > 0",
			},
		})
	})

	t.Run("CreateTableAndIndexWithABackslashStringAfterAWord", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE codes (
				name TEXT CHECK (name NOT LIKE '%x\%' ESCAPE'\'),
				code INTEGER
			);
			CREATE INDEX codes_name ON codes (name) WHERE name LIKE 'a\%' ESCAPE'\';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "codes",
				Columns: []*SQLiteColumn{
					{
						Name:   "name",
						Type:   "TEXT",
						Checks: `CHECK (name NOT LIKE '%x\%' ESCAPE'\')`,
					},
					{
						Name: "code",
						Type: "INTEGER",
					},
				},
			},
			&SQLiteCreateIndexInstruction{
				Name:      "codes_name",
				TableName: "codes",
				Keys:      []string{`"name"`},
				Where:     `name LIKE 'a\%' ESCAPE'\'`,
			},
		})
	})
}
