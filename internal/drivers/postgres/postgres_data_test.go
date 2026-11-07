package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestPostgresData(t *testing.T) {
	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"3", "'Carol'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Robert'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "4",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
			{"id": int64(3), "name": "Carol"},
		}, rows)
	})

	t.Run("CompareRowsOfATableWithAGeneratedPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INT NOT NULL, code INT GENERATED ALWAYS AS (id * 10) STORED, name TEXT, PRIMARY KEY (code));`

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');`)
		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'Alicia'), (3, 'Cara');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "code",
							Expression: "10",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "code",
							Expression: "30",
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "code": int64(10), "name": "Alice"},
			{"id": int64(2), "code": int64(20), "name": "Bob"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY code"))
	})

	t.Run("CompareRowsOfANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
			INSERT INTO users (id, name) VALUES (2, 'Bob'), (1, 'Alice');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "name",
						Type: "text",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "users_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"1", "'Alice'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})

	t.Run("CompareRowsWithAGeneratedColumnAndAnIdentityColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE items (
				id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				price integer,
				total integer GENERATED ALWAYS AS (price * 2) STORED
			);
		`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO items (id, price) OVERRIDING SYSTEM VALUE VALUES (1, 10), (2, 20);`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO items (id, price) OVERRIDING SYSTEM VALUE VALUES (1, 15), (3, 30);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresInsertOverridingInstruction{
				TableName:   "items",
				ColumnNames: []string{"id", "price"},
				Expressions: []string{"3", "30"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "items",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "price",
						Expression: "15",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "items",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})
	})

	t.Run("CompareRowsOfAPartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE measurements (
				id integer PRIMARY KEY,
				value integer
			) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO measurements (id, value) VALUES (1, 5);`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO measurements (id, value) VALUES (1, 5), (2, 7);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "measurements",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "7"},
			},
		})
	})

	t.Run("CompareRowsWithAJSONValueAndAByteaValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE documents (
				id integer PRIMARY KEY,
				body jsonb,
				raw bytea
			);
		`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO documents (id, body, raw) VALUES (1, '{"a": 1}', '\x01');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO documents (id, body, raw) VALUES (1, '{"a": 2}', '\x02'), (2, '{"b": 3}', NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "documents",
				ColumnNames: []string{"id", "body", "raw"},
				Expressions: []string{"2", `'{"b": 3}'`, "NULL"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "documents",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: `'{"a": 2}'`,
					},
					{
						ColumnName: "raw",
						Expression: `'\x02'`,
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})
	})

	t.Run("CompareRowsDeletesAChildRowFirst", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE parents (id integer PRIMARY KEY);
			CREATE TABLE children (
				id integer PRIMARY KEY,
				parent integer REFERENCES parents(id)
			);
		`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`
			INSERT INTO parents (id) VALUES (1);
			INSERT INTO children (id, parent) VALUES (10, 1);
		`)

		driver.ExecOnDesired(schema)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDeleteInstruction{
				TableName: "children",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "10",
						},
					},
				},
			},
			&driversshared.SQLDeleteInstruction{
				TableName: "parents",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})
	})

	t.Run("CompareRowsOfATableWithoutAPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE logs (message TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO logs (message) VALUES ('stop');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO logs (message) VALUES ('start');`)
		diff := driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnCurrent(diff)

		// The rows of a table without a primary key stay different, so each diff holds the note.
		driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLCommentInstruction{
				Text: `The table "logs" holds no primary key, so dbdiff compares no row of it.`,
			},
		})

		rows := driver.FetchAllFromCurrent("logs", "")

		require.Equal(t, []map[string]any{
			{"message": "stop"},
		}, rows)
	})

	t.Run("CompareRowsWithAQuoteAndWithNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INT PRIMARY KEY, body TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO notes (id, body) VALUES (1, 'plain'), (2, 'not empty');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO notes (id, body) VALUES (1, 'it''s a note'), (2, NULL), (3, NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "notes",
				ColumnNames: []string{"id", "body"},
				Expressions: []string{"3", "NULL"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: "'it''s a note'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "notes",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "body",
						Expression: "NULL",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("notes", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "it's a note"},
			{"id": int64(2), "body": nil},
			{"id": int64(3), "body": nil},
		}, rows)
	})

	t.Run("CompareRowsWithATimestampValue", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE events (id integer PRIMARY KEY, at timestamptz);`

		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO events (id, at) VALUES (1, '2024-01-01T05:00:00+02:00');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "events",
				ColumnNames: []string{"id", "at"},
				Expressions: []string{"1", "'2024-01-01 03:00:00+00:00'"},
			},
		})
	})

	t.Run("CompareRowsWithSpecialFloatValues", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE readings (id integer PRIMARY KEY, value double precision);`

		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO readings (id, value) VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-Infinity'), (4, 1.5);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"1", "'NaN'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "'Infinity'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"3", "'-Infinity'"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"4", "1.5"},
			},
		})
	})

	t.Run("CompareRowsOfATableWithTheKeyOnAnotherColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE items (a integer PRIMARY KEY, b integer);`)
		driver.ExecOnCurrent(`INSERT INTO items (a, b) VALUES (1, 2);`)

		driver.ExecOnDesired(`CREATE TABLE items (a integer, b integer PRIMARY KEY);`)
		driver.ExecOnDesired(`INSERT INTO items (a, b) VALUES (1, 2);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "b"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "items_pkey"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "items_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (b)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "a"},
				},
			},
			&driversshared.SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the current schema, so dbdiff compares no row of it.`,
			},
		})
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)
		driver.CompareData = true

		// A new NOT NULL column needs an empty table, so the current table holds no row here.
		driver.ExecOnCurrent(`CREATE TABLE items (label TEXT);`)

		driver.ExecOnDesired(`CREATE TABLE items (code INT PRIMARY KEY, label TEXT);`)
		driver.ExecOnDesired(`INSERT INTO items (code, label) VALUES (1, 'first');`)

		diff := driver.RequireInstructions([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:    "code",
							Type:    "integer",
							NotNull: true,
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "items_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (code)",
						},
					},
				},
			},
			&driversshared.SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the current schema, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnCurrent(diff)

		rows := driver.FetchAllFromCurrent("items", "ORDER BY code")

		require.Equal(t, []map[string]any(nil), rows)

		// The two tables now hold the same primary key, so the second diff compares the rows.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "items",
				ColumnNames: []string{"code", "label"},
				Expressions: []string{"1", "'first'"},
			},
		})
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)
		driver.RequireDiffApplies(nil)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})
}
