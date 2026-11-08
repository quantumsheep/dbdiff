package driverssqlite

import (
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteData(t *testing.T) {
	t.Run("CompareRows", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

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

	t.Run("CompareRowsOfATableNameInOtherCase", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`
			CREATE TABLE Users (id INTEGER PRIMARY KEY, name TEXT);
			INSERT INTO Users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (4, 'Dave');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert'), (3, 'Carol');
		`)
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
				TableName: "Users",
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

	t.Run("CompareRowsOfANewTable", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
			INSERT INTO users (id, name) VALUES (2, 'Bob'), (1, 'Alice');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTableInstruction{
				ForeignKeys: []*SQLiteForeignKey{},
				Name:        "users",
				Columns: []*SQLiteColumn{
					{
						Name:       "id",
						Type:       "INTEGER",
						PrimaryKey: true,
					},
					{
						Name:    "name",
						Type:    "TEXT",
						NotNull: true,
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

	t.Run("CompareRowsWithAGeneratedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				price INTEGER,
				total INTEGER GENERATED ALWAYS AS (price * 2) VIRTUAL
			);
		`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO items (id, price) VALUES (1, 10), (2, 20);`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO items (id, price) VALUES (1, 15), (3, 30);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
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

		rows := driver.FetchAllFromCurrent("items", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "price": int64(15), "total": int64(30)},
			{"id": int64(3), "price": int64(30), "total": int64(60)},
		}, rows)
	})

	t.Run("CompareRowsWithADateColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE events (id INTEGER PRIMARY KEY, at DATE);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO events (id, at) VALUES (1, 'alpha');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO events (id, at) VALUES (1, 'omega'), (2, '2024-01-01');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "events",
				ColumnNames: []string{"id", "at"},
				Expressions: []string{"2", "'2024-01-01'"},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "events",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "at",
						Expression: "'omega'",
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
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE children (
				id INTEGER PRIMARY KEY,
				parent INTEGER REFERENCES parents(id)
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
		driver := NewTestSQLiteDriver(t)
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
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);`

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

	t.Run("CompareRowsWithAnInfiniteValue", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL);`

		driver.ExecOnCurrent(schema)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO readings (id, value) VALUES (1, 9e999), (2, -9e999), (3, 1.5);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"1", "9e999"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"2", "-9e999"},
			},
			&driversshared.SQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "value"},
				Expressions: []string{"3", "1.5"},
			},
		})
	})

	t.Run("CompareRowsOfATableWithTheKeyOnAnotherColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE items (a INTEGER PRIMARY KEY, b INTEGER);`)
		driver.ExecOnCurrent(`INSERT INTO items (a, b) VALUES (1, 2);`)

		driver.ExecOnDesired(`CREATE TABLE items (a INTEGER, b INTEGER PRIMARY KEY);`)
		driver.ExecOnDesired(`INSERT INTO items (a, b) VALUES (1, 2);`)
		driver.RequireDiffApplies(slices.Concat(
			recreationPrologue(),
			recreated("items",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_items_temp",
					Columns: []*SQLiteColumn{
						{
							Name: "a",
							Type: "INTEGER",
						},
						{
							Name:       "b",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_items_temp",
					ColumnNames:       []string{"a", "b"},
					SelectExpressions: []string{`"a"`, `"b"`},
					FromTableName:     "items",
				},
				&driversshared.SQLDropTableInstruction{Name: "items"},
				&SQLiteAlterTableInstruction{
					Name:   "_items_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "items"},
				},
			),
			[]driversshared.Instruction{
				&driversshared.SQLCommentInstruction{
					Text: `The table "items" holds another primary key in the current schema, so dbdiff compares no row of it.`,
				},
			},
			recreationEpilogue("items"),
		))
	})

	t.Run("CompareRowsOfATableWithAnotherPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.GuessRenames = true
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE items (identifier INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO items (identifier, label) VALUES (1, 'old');`)

		driver.ExecOnDesired(`CREATE TABLE items (code INTEGER PRIMARY KEY, label TEXT);`)
		driver.ExecOnDesired(`INSERT INTO items (code, label) VALUES (1, 'first');`)
		diff := driver.RequireInstructions([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "items",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "identifier",
					NewColumnName: "code",
				},
			},
			&driversshared.SQLCommentInstruction{
				Text: `The table "items" holds another primary key in the current schema, so dbdiff compares no row of it.`,
			},
		})

		driver.ExecOnCurrent(diff)

		rows := driver.FetchAllFromCurrent("items", "ORDER BY code")

		require.Equal(t, []map[string]any{
			{"code": int64(1), "label": "old"},
		}, rows)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLUpdateInstruction{
				TableName: "items",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "label",
						Expression: "'first'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&driversshared.SQLEqualityCondition{
							ColumnName: "code",
							Expression: "1",
						},
					},
				},
			},
		})

		rows = driver.FetchAllFromCurrent("items", "ORDER BY code")

		require.Equal(t, []map[string]any{
			{"code": int64(1), "label": "first"},
		}, rows)
	})

	t.Run("NullPrimaryKey", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		schema := `CREATE TABLE users (email TEXT PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (email, name) VALUES (NULL, 'Bob');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (email, name) VALUES (NULL, 'Alice');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{&driversshared.SQLIsNullCondition{ColumnName: "email"}},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "")

		require.Equal(t, []map[string]any{{"email": nil, "name": "Alice"}}, rows)
	})

	t.Run("CompareRowsWithAnAddedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)

		driver.ExecOnDesired(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, color TEXT);`)
		driver.ExecOnDesired(`INSERT INTO users (id, name, color) VALUES (1, 'Alice', 'red');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &SQLiteAddColumnAction{
					Column: &SQLiteColumn{
						Name: "color",
						Type: "TEXT",
					},
				},
			},
			&driversshared.SQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*driversshared.SQLSetClause{
					{
						ColumnName: "color",
						Expression: "'red'",
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

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "color": "red"},
		}, rows)
	})

	t.Run("CompareRowsWithARenamedColumn", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)
		driver.CompareData = true

		driver.ExecOnCurrent(`CREATE TABLE users (id INTEGER PRIMARY KEY, Name TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, Name) VALUES (1, 'Alice'), (2, 'Bob');`)

		driver.ExecOnDesired(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Robert');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteAlterTableInstruction{
				Name: "users",
				Action: &driversshared.SQLRenameColumnAction{
					ColumnName:    "Name",
					NewColumnName: "name",
				},
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
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
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
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice"},
			{"id": int64(2), "name": "Robert"},
		}, rows)
	})

	t.Run("CompareNoRowWithoutTheDataFlag", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (2, 'Bob');`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`INSERT INTO users (id, name) VALUES (1, 'Alice');`)
		diff := driver.RequireInstructions(nil)

		driver.ExecOnCurrent(diff)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "Bob"},
		}, rows)
	})
}
