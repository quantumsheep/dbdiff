package driverspostgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestPostgresColumn(t *testing.T) {
	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, name TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "name",
							Type: "text",
						},
					},
				},
			},
		})
	})

	t.Run("DropColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "name"},
				},
			},
		})
	})

	t.Run("AlterColumnType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, name VARCHAR(50));`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, name TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "name",
						DataType:   "text",
					},
				},
			},
		})
	})

	t.Run("AlterColumnTypeWithAutomaticCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, score INT);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, score) VALUES (1, 42);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, score BIGINT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "bigint",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("AlterColumnTypeWithoutAutomaticCast", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, score TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, score) VALUES (1, '42');`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, score INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "integer",
						UsingCast:  true,
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
		}, rows)
	})

	t.Run("AlterColumnTypeWithADefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, score TEXT DEFAULT '0');`)
		driver.ExecOnCurrent(`INSERT INTO users (id, score) VALUES (1, '42');`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, score INT DEFAULT 0);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{
						ColumnName: "score",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "score",
						DataType:   "integer",
						UsingCast:  true,
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "score",
						Expression: "0",
					},
				},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users (id) VALUES (2);`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "score": int64(42)},
			{"id": int64(2), "score": int64(0)},
		}, rows)
	})

	t.Run("AlterColumnTypeToArray", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, tags INT[]);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, tags) VALUES (1, ARRAY[5, 6]);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, tags BIGINT[]);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "tags",
						DataType:   "bigint[]",
						UsingCast:  true,
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "tags": "{5,6}"},
		}, rows)
	})

	t.Run("CreateTableWithIdentityColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY, name TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:     "id",
						Type:     "integer",
						NotNull:  true,
						Identity: "ALWAYS",
					},
					{
						Name: "name",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithSerialColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
						Default: sql.NullString{
							String: "nextval('users_id_seq'::regclass)",
							Valid:  true,
						},
						Serial:             "serial",
						SerialSequenceName: "users_id_seq",
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
		})

		driver.ExecOnCurrent(`INSERT INTO users (name) VALUES ('alice');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "alice"},
		}, rows)
	})

	t.Run("AddSerialColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (name TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE users (name TEXT, id BIGSERIAL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:    "id",
							Type:    "bigint",
							NotNull: true,
							Default: sql.NullString{
								String: "nextval('users_id_seq'::regclass)",
								Valid:  true,
							},
							Serial:             "bigserial",
							SerialSequenceName: "users_id_seq",
						},
					},
				},
			},
		})
	})

	t.Run("EqualSerialColumns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id SERIAL PRIMARY KEY);`)
		driver.ExecOnDesired(`CREATE TABLE users (id SERIAL PRIMARY KEY);`)
		driver.RequireInstructions(nil)
	})

	t.Run("CreateTableWithGeneratedColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "measures",
				Columns: []*PostgresColumn{
					{
						Name: "value",
						Type: "integer",
					},
					{
						Name:                "doubled",
						Type:                "integer",
						GeneratedExpression: "(value * 2)",
					},
				},
			},
		})
	})

	t.Run("AddIdentityToColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT PRIMARY KEY, name TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b');`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
			&PostgresRestartOwnedSequenceInstruction{
				TableName:  "users",
				ColumnName: "id",
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users (name) VALUES ('c');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")
		require.Equal(t, []map[string]any{
			{
				"id":   int64(1),
				"name": "a",
			},
			{
				"id":   int64(2),
				"name": "b",
			},
			{
				"id":   int64(3),
				"name": "c",
			},
		}, rows)
	})

	t.Run("AddIdentityWithOptionsToColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b');`)
		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 5) PRIMARY KEY,
				name TEXT
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
						Options:    "START WITH 100 INCREMENT BY 5",
					},
				},
			},
			&PostgresRestartOwnedSequenceInstruction{
				TableName:  "users",
				ColumnName: "id",
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users (name) VALUES ('c'), ('d');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")
		require.Equal(t, []map[string]any{
			{
				"id":   int64(1),
				"name": "a",
			},
			{
				"id":   int64(2),
				"name": "b",
			},
			{
				"id":   int64(100),
				"name": "c",
			},
			{
				"id":   int64(105),
				"name": "d",
			},
		}, rows)
	})

	t.Run("AddIdentityToAnEmptyColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
			&PostgresRestartOwnedSequenceInstruction{
				TableName:  "users",
				ColumnName: "id",
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users DEFAULT VALUES;`)

		rows := driver.FetchAllFromCurrent("users", "")
		require.Equal(t, []map[string]any{
			{
				"id": int64(1),
			},
		}, rows)
	})

	t.Run("ChangeIdentityGeneration", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT GENERATED BY DEFAULT AS IDENTITY);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityAction{
						ColumnName: "id",
						Identity:   "BY DEFAULT",
					},
				},
			},
		})
	})

	t.Run("DropIdentityFromColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropIdentityAction{ColumnName: "id"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "id"},
				},
			},
		})
	})

	t.Run("ModifyGeneratedExpression", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);
			CREATE INDEX measures_doubled_idx ON measures (doubled);
			CREATE VIEW doubled_measures AS SELECT doubled FROM measures;
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 3) STORED);
			CREATE INDEX measures_doubled_idx ON measures (doubled);
			CREATE VIEW doubled_measures AS SELECT doubled FROM measures;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetExpressionAction{
						ColumnName: "doubled",
						Expression: "(value * 3)",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("doubled_measures", "")
		require.Equal(t, []map[string]any{
			{
				"doubled": int64(15),
			},
		}, rows)
	})

	t.Run("GeneratedColumnBecomesAPlainColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);
			CREATE VIEW doubled_measures AS SELECT doubled FROM measures;
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE measures (value INT, doubled INT DEFAULT 0);
			CREATE VIEW doubled_measures AS SELECT doubled FROM measures;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropExpressionAction{ColumnName: "doubled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "doubled",
						Expression: "0",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":   int64(5),
				"doubled": int64(10),
			},
		}, rows)
	})

	t.Run("PlainColumnBecomesAGeneratedColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, doubled INT CONSTRAINT doubled_positive CHECK (doubled > 0));
			CREATE INDEX measures_doubled_idx ON measures (doubled);
			COMMENT ON COLUMN measures.doubled IS 'two times the value';
			ALTER TABLE measures ALTER COLUMN doubled SET STATISTICS 500;
			CREATE VIEW doubled_measures AS SELECT doubled FROM measures;
			CREATE VIEW measure_values AS SELECT value FROM measures;
			INSERT INTO measures (value, doubled) VALUES (5, 7);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE measures (
				value INT,
				doubled INT GENERATED ALWAYS AS (value * 2) STORED CONSTRAINT doubled_positive CHECK (doubled > 0)
			);
			CREATE INDEX measures_doubled_idx ON measures (doubled);
			COMMENT ON COLUMN measures.doubled IS 'two times the value';
			ALTER TABLE measures ALTER COLUMN doubled SET STATISTICS 500;
			CREATE VIEW doubled_measures AS SELECT doubled FROM measures;
			CREATE VIEW measure_values AS SELECT value FROM measures;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "doubled_measures"},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "doubled"},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "doubled",
							Type:                "integer",
							GeneratedExpression: "(value * 2)",
							Comment:             "two times the value",
							StatisticsTarget: sql.NullInt64{
								Int64: 500,
								Valid: true,
							},
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName:       "doubled",
						StatisticsTarget: 500,
					},
				},
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "measures",
				ColumnName: "doubled",
				Text:       "two times the value",
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "doubled_positive",
							Type: "c",
							Def:  "CHECK ((doubled > 0))",
						},
					},
				},
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measures_doubled_idx ON measures USING btree (doubled)",
			},
			&PostgresCreateViewInstruction{
				Name:  "doubled_measures",
				Query: " SELECT doubled\n   FROM measures;",
			},
		})

		rows := driver.FetchAllFromCurrent("doubled_measures", "")
		require.Equal(t, []map[string]any{
			{
				"doubled": int64(10),
			},
		}, rows)
	})

	t.Run("GeneratedColumnChangesItsTypeAndItsExpression", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, label INT GENERATED ALWAYS AS (value) STORED);
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`CREATE TABLE measures (value INT, label TEXT GENERATED ALWAYS AS ((value + 1)::text) STORED);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "label"},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "label",
							Type:                "text",
							GeneratedExpression: "((value + 1))::text",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value": int64(5),
				"label": "6",
			},
		}, rows)
	})

	t.Run("GeneratedColumnChangesOnlyItsType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`CREATE TABLE measures (value INT, doubled BIGINT GENERATED ALWAYS AS (value * 2) STORED);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "doubled"},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "doubled",
							Type:                "bigint",
							GeneratedExpression: "(value * 2)",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":   int64(5),
				"doubled": int64(10),
			},
		}, rows)
	})

	t.Run("ChangeTheTypeOfAColumnThatAGeneratedColumnReads", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, tripled INT GENERATED ALWAYS AS (value * 3) STORED);
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`CREATE TABLE measures (value BIGINT, tripled INT GENERATED ALWAYS AS (value * 3) STORED);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "tripled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   "bigint",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "tripled",
							Type:                "integer",
							GeneratedExpression: "(value * 3)",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":   int64(5),
				"tripled": int64(15),
			},
		}, rows)
	})

	t.Run("ChangeTheTypeOfAColumnThatAGeneratedColumnWithAnIndexAndAViewReads", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		dependents := `
			CREATE INDEX measures_tripled ON measures (tripled);
			CREATE VIEW tripled_measures AS SELECT tripled FROM measures;
			COMMENT ON COLUMN measures.tripled IS 'Three times the value';
		`
		driver.ExecOnCurrent(`CREATE TABLE measures (value INT, tripled INT GENERATED ALWAYS AS (value * 3) STORED);` +
			dependents + `INSERT INTO measures (value) VALUES (5);`)
		driver.ExecOnDesired(`CREATE TABLE measures (value BIGINT, tripled INT GENERATED ALWAYS AS (value * 3) STORED);` +
			dependents)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "tripled_measures"},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "tripled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   "bigint",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "tripled",
							Type:                "integer",
							GeneratedExpression: "(value * 3)",
							Comment:             "Three times the value",
						},
					},
				},
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "measures",
				ColumnName: "tripled",
				Text:       "Three times the value",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measures_tripled ON measures USING btree (tripled)",
			},
			&PostgresCreateViewInstruction{
				Name:  "tripled_measures",
				Query: " SELECT tripled\n   FROM measures;",
			},
		})

		rows := driver.FetchAllFromCurrent("tripled_measures", "")
		require.Equal(t, []map[string]any{
			{
				"tripled": int64(15),
			},
		}, rows)
	})

	t.Run("ChangeTheTypeOfAColumnThatTwoGeneratedColumnsRead", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (
				doubled INT GENERATED ALWAYS AS (value * 2) STORED,
				value INT,
				tripled INT GENERATED ALWAYS AS (value * 3) STORED
			);
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE measures (
				doubled INT GENERATED ALWAYS AS (value * 2) STORED,
				value BIGINT,
				tripled INT GENERATED ALWAYS AS (value * 3) STORED
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "doubled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "tripled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   "bigint",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "doubled",
							Type:                "integer",
							GeneratedExpression: "(value * 2)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "tripled",
							Type:                "integer",
							GeneratedExpression: "(value * 3)",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"doubled": int64(10),
				"value":   int64(5),
				"tripled": int64(15),
			},
		}, rows)
	})

	t.Run("DropAGeneratedColumnThatReadsAColumnWithANewType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, tripled INT GENERATED ALWAYS AS (value * 3) STORED);
			CREATE INDEX measures_tripled ON measures (tripled);
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`CREATE TABLE measures (value BIGINT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "tripled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   "bigint",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value": int64(5),
			},
		}, rows)
	})

	t.Run("DropTheExpressionOfAGeneratedColumnThatReadsAColumnWithANewType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures (value INT, tripled INT GENERATED ALWAYS AS (value * 3) STORED);
			INSERT INTO measures (value) VALUES (5);
		`)
		driver.ExecOnDesired(`CREATE TABLE measures (value BIGINT, tripled INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropExpressionAction{ColumnName: "tripled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value",
						DataType:   "bigint",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":   int64(5),
				"tripled": int64(15),
			},
		}, rows)
	})

	t.Run("ChangeTheTypeOfAColumnWithACommaInItsNameThatAGeneratedColumnReads", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measures ("value, raw" INT, tripled INT GENERATED ALWAYS AS ("value, raw" * 3) STORED);
			INSERT INTO measures ("value, raw") VALUES (5);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE measures ("value, raw" BIGINT, tripled INT GENERATED ALWAYS AS ("value, raw" * 3) STORED);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "tripled"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "value, raw",
						DataType:   "bigint",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "tripled",
							Type:                "integer",
							GeneratedExpression: "(\"value, raw\" * 3)",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value, raw": int64(5),
				"tripled":    int64(15),
			},
		}, rows)
	})

	t.Run("RebuiltColumnWithAPolicyAndATrigger", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		setup := `
			CREATE FUNCTION touch() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
		`
		dependents := `
			ALTER TABLE measures ENABLE ROW LEVEL SECURITY;
			CREATE POLICY positive_doubled ON measures USING (doubled > 0);
			CREATE TRIGGER touch_doubled BEFORE UPDATE OF doubled ON measures FOR EACH ROW EXECUTE FUNCTION touch();
		`
		driver.ExecOnCurrent(setup + `CREATE TABLE measures (value INT, doubled INT);` + dependents)
		driver.ExecOnCurrent(`INSERT INTO measures (value, doubled) VALUES (5, 7);`)
		driver.ExecOnDesired(setup +
			`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);` + dependents)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropPolicyInstruction{
				Name:      "positive_doubled",
				TableName: "measures",
			},
			&PostgresDropTriggerInstruction{
				Name:      "touch_doubled",
				TableName: "measures",
			},
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "doubled"},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "doubled",
							Type:                "integer",
							GeneratedExpression: "(value * 2)",
						},
					},
				},
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER touch_doubled BEFORE UPDATE OF doubled ON measures " +
					"FOR EACH ROW EXECUTE FUNCTION touch()",
			},
			&PostgresCreatePolicyInstruction{
				Name:       "positive_doubled",
				TableName:  "measures",
				Permissive: "PERMISSIVE",
				Command:    "ALL",
				Roles:      []string{"public"},
				Using:      "(doubled > 0)",
			},
		})

		rows := driver.FetchAllFromCurrent("measures", "")
		require.Equal(t, []map[string]any{
			{
				"value":   int64(5),
				"doubled": int64(10),
			},
		}, rows)
	})

	t.Run("ViewOfAnotherSchemaBlocksARebuiltColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		otherSchema := driver.currentSchema + "_other"

		driver.ExecOnCurrent(fmt.Sprintf(`
			CREATE TABLE measures (value INT, doubled INT);
			CREATE SCHEMA %[1]s;
			CREATE VIEW %[1]s.doubled_measures AS SELECT doubled FROM measures;
		`, otherSchema))
		t.Cleanup(func() {
			_, err := driver.conn.ExecContext(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE;`, otherSchema))
			require.NoError(t, err)
		})
		driver.ExecOnDesired(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);`)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, fmt.Sprintf(
			`the view %s.doubled_measures uses the column "doubled" of the table "measures". `+
				`dbdiff must drop and add this column. dbdiff cannot drop and create this object again. `+
				`Drop this object and run dbdiff again. After you apply the diff, create the object again`,
			otherSchema))
	})

	t.Run("NeedsRebuild", func(t *testing.T) {
		plain := &PostgresColumn{Type: "integer"}
		doubled := &PostgresColumn{
			Type:                "integer",
			GeneratedExpression: "(value * 2)",
		}
		tripled := &PostgresColumn{
			Type:                "integer",
			GeneratedExpression: "(value * 3)",
		}
		bigDoubled := &PostgresColumn{
			Type:                "bigint",
			GeneratedExpression: "(value * 2)",
		}

		testCases := []struct {
			name          string
			desired       *PostgresColumn
			current       *PostgresColumn
			serverVersion int
			expected      bool
		}{
			{"PlainColumnsOnPostgres17", plain, plain, 170000, false},
			{"SameExpressionOnPostgres12", doubled, doubled, 120000, false},
			{"GeneratedToPlainOnPostgres12", plain, doubled, 120000, true},
			{"GeneratedToPlainOnPostgres13", plain, doubled, 130000, false},
			{"PlainToGeneratedOnPostgres17", doubled, plain, 170000, true},
			{"NewExpressionOnPostgres16", tripled, doubled, 160000, true},
			{"NewExpressionOnPostgres17", tripled, doubled, 170000, false},
			{"NewTypeOfAGeneratedColumnOnPostgres17", bigDoubled, doubled, 170000, true},
		}

		for _, testCase := range testCases {
			require.Equal(t, testCase.expected,
				testCase.desired.NeedsRebuild(testCase.current, testCase.serverVersion), testCase.name)
		}
	})

	t.Run("CreateTableWithColumnCollation", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE people (name TEXT COLLATE "C", other TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "people",
				Columns: []*PostgresColumn{
					{
						Name:      "name",
						Type:      "text",
						Collation: "C",
					},
					{
						Name: "other",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("AlterColumnCollation", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE people (name TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE people (name TEXT COLLATE "C");`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "people",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "name",
						DataType:   "text",
						Collation:  "C",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:            "id",
						Type:            "integer",
						NotNull:         true,
						Identity:        "ALWAYS",
						IdentityOptions: "START WITH 100 INCREMENT BY 5",
					},
				},
			},
		})
	})

	t.Run("AlterTwoIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "START WITH 100 INCREMENT BY 5",
					},
				},
			},
		})
	})

	t.Run("MakeAColumnSerial", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id integer PRIMARY KEY, name text);`)
		driver.ExecOnCurrent(`INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b');`)
		driver.ExecOnDesired(`CREATE TABLE users (id serial PRIMARY KEY, name text);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateOwnedSequenceInstruction{
				Name:       "users_id_seq",
				DataType:   "integer",
				TableName:  "users",
				ColumnName: "id",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "id",
						Expression: "nextval('users_id_seq'::regclass)",
					},
				},
			},
			&PostgresRestartOwnedSequenceInstruction{
				TableName:  "users",
				ColumnName: "id",
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users (name) VALUES ('c');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")
		require.Equal(t, []map[string]any{
			{
				"id":   int64(1),
				"name": "a",
			},
			{
				"id":   int64(2),
				"name": "b",
			},
			{
				"id":   int64(3),
				"name": "c",
			},
		}, rows)
	})

	t.Run("MakeASmallintColumnSmallserial", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id smallint NOT NULL);`)
		driver.ExecOnDesired(`CREATE TABLE users (id smallserial);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateOwnedSequenceInstruction{
				Name:       "users_id_seq",
				DataType:   "smallint",
				TableName:  "users",
				ColumnName: "id",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "id",
						Expression: "nextval('users_id_seq'::regclass)",
					},
				},
			},
			&PostgresRestartOwnedSequenceInstruction{
				TableName:  "users",
				ColumnName: "id",
			},
		})
	})

	t.Run("MakeASerialColumnPlain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id serial);`)
		driver.ExecOnDesired(`CREATE TABLE users (id integer);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropDefaultAction{ColumnName: "id"},
				},
			},
			&PostgresDropSequenceInstruction{Name: "users_id_seq"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropNotNullAction{ColumnName: "id"},
				},
			},
		})
	})

	t.Run("ResetIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5));
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "START WITH 1 INCREMENT BY 1",
					},
				},
			},
		})
	})

	t.Run("AlterIdentityOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT GENERATED ALWAYS AS IDENTITY (INCREMENT BY 5));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetIdentityOptionsAction{
						ColumnName: "id",
						Options:    "INCREMENT BY 5",
					},
				},
			},
		})
	})

	t.Run("AlterColumnNotNull", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, name TEXT NOT NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "name"},
				},
			},
		})
	})

	t.Run("AlterColumnDefault", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, name TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, name TEXT DEFAULT 'anon');`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "name",
						Expression: "'anon'::text",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithAColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "docs",
				Columns: []*PostgresColumn{
					{
						Name:    "body",
						Type:    "text",
						Storage: "MAIN",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})
	})

	t.Run("AlterColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})
	})

	t.Run("ResetColumnStorage", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.ExecOnDesired(`CREATE TABLE docs (body TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "DEFAULT",
					},
				},
			},
		})
	})

	t.Run("ColumnStorageAfterATypeChange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (body VARCHAR(200));
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE MAIN;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "body",
						DataType:   "text",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "MAIN",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithAStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "docs",
				Columns: []*PostgresColumn{
					{
						Name:             "body",
						Type:             "text",
						StatisticsTarget: sql.NullInt64{Int64: 500, Valid: true},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName:       "body",
						StatisticsTarget: 500,
					},
				},
			},
		})
	})

	t.Run("AlterColumnStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 100;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName:       "body",
						StatisticsTarget: 500,
					},
				},
			},
		})
	})

	t.Run("ResetColumnStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 500;
		`)
		driver.ExecOnDesired(`CREATE TABLE docs (body TEXT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName:       "body",
						StatisticsTarget: -1,
					},
				},
			},
		})
	})

	t.Run("AddColumnWithAStorageAndAStatisticsTarget", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE docs (id INT);`)
		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT, body TEXT);
			ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTERNAL;
			ALTER TABLE docs ALTER COLUMN body SET STATISTICS 250;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:             "body",
							Type:             "text",
							Storage:          "EXTERNAL",
							StatisticsTarget: sql.NullInt64{Int64: 250, Valid: true},
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageAction{
						ColumnName: "body",
						Storage:    "EXTERNAL",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStatisticsAction{
						ColumnName:       "body",
						StatisticsTarget: 250,
					},
				},
			},
		})
	})

	t.Run("RebuiltColumnThatAForeignKeyOfAnotherTableReferences", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE accounts (name TEXT, code TEXT UNIQUE);
			CREATE TABLE orders (id INT, account_code TEXT REFERENCES accounts (code));
			INSERT INTO accounts VALUES ('a', 'A');
			INSERT INTO orders VALUES (1, 'A');
		`)
		driver.ExecOnDesired(`
			CREATE TABLE accounts (name TEXT, code TEXT GENERATED ALWAYS AS (upper(name)) STORED UNIQUE);
			CREATE TABLE orders (id INT, account_code TEXT REFERENCES accounts (code));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "orders_account_code_fkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{
						ColumnName: "code",
					},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "code",
							Type:                "text",
							GeneratedExpression: "upper(name)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_code_key",
							Type: "u",
							Def:  "UNIQUE (code)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "orders_account_code_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (account_code) REFERENCES accounts(code)",
							ReferencedTable: "accounts",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("orders", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "account_code": "A"},
		}, rows)
	})
}
