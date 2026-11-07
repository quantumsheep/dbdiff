package driverspostgres

import (
	"database/sql"
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
						Name:               "id",
						Type:               "integer",
						NotNull:            true,
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
							Name:               "id",
							Type:               "bigint",
							NotNull:            true,
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
		})
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

		driver.ExecOnCurrent(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 2) STORED);`)
		driver.ExecOnCurrent(`INSERT INTO measures (value) VALUES (5);`)
		driver.ExecOnDesired(`CREATE TABLE measures (value INT, doubled INT GENERATED ALWAYS AS (value * 3) STORED);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "measures",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "doubled"},
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name:                "doubled",
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
				"doubled": int64(15),
			},
		}, rows)
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

		driver.ExecOnCurrent(`CREATE TABLE users (id integer);`)
		driver.ExecOnDesired(`CREATE TABLE users (id serial);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{ColumnName: "id"},
				},
			},
			&PostgresCreateOwnedSequenceInstruction{
				Name:       "users_id_seq",
				TableName:  "users",
				ColumnName: "id",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "id",
						Expression: `nextval('"users_id_seq"'::regclass)`,
					},
				},
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
}
