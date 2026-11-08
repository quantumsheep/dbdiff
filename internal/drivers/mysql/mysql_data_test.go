package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLData(t *testing.T) {
	t.Run("DataComparison", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), age int, PRIMARY KEY (id));
			INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (3, 'Cara', NULL);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), age int, PRIMARY KEY (id));
			INSERT INTO users (id, name, age) VALUES (1, 'Alicia', 30), (2, 'Bob', 40);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name", "age"},
				Expressions: []string{"3", "'Cara'", "NULL"},
			},
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&MySQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "age": int64(30)},
			{"id": int64(3), "name": "Cara", "age": nil},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

	t.Run("DataWithAnAddedColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), color varchar(20), PRIMARY KEY (id));
			INSERT INTO users (id, name, color) VALUES (1, 'Alice', 'red');
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alice');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "color",
						Type: "varchar(20)",
					},
				},
			},
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "color",
						Expression: "'red'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "Alice", "color": "red"},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("DataOfANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE readings (
				id int NOT NULL,
				taken_at datetime,
				payload varbinary(4),
				price decimal(5,2),
				PRIMARY KEY (id)
			);
			INSERT INTO readings VALUES (1, '2030-01-02 03:04:05', X'DEADBEEF', 12.50);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "readings",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "taken_at",
						Type: "datetime",
					},
					{
						Name: "payload",
						Type: "varbinary(4)",
					},
					{
						Name: "price",
						Type: "decimal(5,2)",
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLInsertInstruction{
				TableName:   "readings",
				ColumnNames: []string{"id", "taken_at", "payload", "price"},
				Expressions: []string{"1", "'2030-01-02 03:04:05'", "X'deadbeef'", "12.50"},
			},
		})
	})

	t.Run("DataOfATableWithAGeneratedPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		schema := "CREATE TABLE users (id int NOT NULL, code int GENERATED ALWAYS AS (id * 10) STORED NOT NULL, name varchar(100), PRIMARY KEY (code));"

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');")
		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent("INSERT INTO users (id, name) VALUES (1, 'Alicia'), (3, 'Cara');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLInsertInstruction{
				TableName:   "users",
				ColumnNames: []string{"id", "name"},
				Expressions: []string{"2", "'Bob'"},
			},
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "code",
							Expression: "10",
						},
					},
				},
			},
			&MySQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
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

	t.Run("DataOfATableWithAForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		schema := `
			CREATE TABLE b_parent (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE a_child (
				id int NOT NULL,
				parent_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES b_parent (id)
			);
		`

		driver.ExecOnDesired(schema)
		driver.ExecOnCurrent(schema)
		driver.ExecOnDesired(`
			INSERT INTO b_parent VALUES (1);
			INSERT INTO a_child VALUES (10, 1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLInsertInstruction{
				TableName:   "a_child",
				ColumnNames: []string{"id", "parent_id"},
				Expressions: []string{"10", "1"},
			},
			&MySQLInsertInstruction{
				TableName:   "b_parent",
				ColumnNames: []string{"id"},
				Expressions: []string{"1"},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(10), "parent_id": int64(1)},
		}, driver.FetchAllFromCurrent("a_child", "ORDER BY id"))
		require.Equal(t, []map[string]any{
			{"id": int64(1)},
		}, driver.FetchAllFromCurrent("b_parent", "ORDER BY id"))
	})

	t.Run("DataOfATableWithNoPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired("CREATE TABLE notes (body text);")
		driver.ExecOnCurrent("CREATE TABLE notes (body text);")
		driver.ExecOnDesired("INSERT INTO notes VALUES ('hello');")

		driver.RequireInstructions([]driversshared.Instruction{
			&driversshared.SQLCommentInstruction{
				Text: `The table "notes" holds no primary key, so dbdiff compares no row of it.`,
			},
		})
	})
}

func TestMariaDBData(t *testing.T) {
	t.Run("DataComparison", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)
		driver.CompareData = true

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alice');
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));
			INSERT INTO users (id, name) VALUES (1, 'Alicia'), (2, 'Bob');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLUpdateInstruction{
				TableName: "users",
				SetClauses: []*MySQLSetClause{
					{
						ColumnName: "name",
						Expression: "'Alice'",
					},
				},
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "1",
						},
					},
				},
			},
			&MySQLDeleteInstruction{
				TableName: "users",
				Condition: &driversshared.SQLConjunctionCondition{
					Conditions: []driversshared.Condition{
						&MySQLEqualityCondition{
							ColumnName: "id",
							Expression: "2",
						},
					},
				},
			},
		})
	})
}
