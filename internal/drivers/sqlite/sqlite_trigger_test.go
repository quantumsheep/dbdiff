package driverssqlite

import (
	"slices"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestSQLiteTrigger(t *testing.T) {
	t.Run("RecreateTableWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age TEXT
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;

			INSERT INTO users (id, age) VALUES (1, '30');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id INTEGER PRIMARY KEY,
				age INTEGER
			);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
		`)
		driver.RequireDiffApplies(slices.Concat(
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{},
			},
			recreated("users",
				&SQLiteCreateTableInstruction{
					ForeignKeys: []*SQLiteForeignKey{},
					Name:        "_users_temp",
					Columns: []*SQLiteColumn{
						{
							Name:       "id",
							Type:       "INTEGER",
							PrimaryKey: true,
						},
						{
							Name: "age",
							Type: "INTEGER",
						},
					},
				},
				&driversshared.SQLInsertSelectInstruction{
					TableName:         "_users_temp",
					ColumnNames:       []string{"id", "age"},
					SelectExpressions: []string{`"id"`, `"age"`},
					FromTableName:     "users",
				},
				&driversshared.SQLDropTableInstruction{Name: "users"},
				&SQLiteAlterTableInstruction{
					Name:   "_users_temp",
					Action: &driversshared.SQLRenameTableAction{NewName: "users"},
				},
				&SQLiteCreateTriggerInstruction{
					Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
				},
			),
			[]driversshared.Instruction{
				&SQLitePragmaForeignKeysInstruction{Enabled: true},
			},
		))

		triggers, err := driver.GetTriggers(t.Context(), driver.currentConnection, "users")
		require.NoError(t, err)
		require.Len(t, triggers, 1)
		require.Equal(t, "users_insert", triggers[0].Name)
	})

	t.Run("Triggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 999; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
			CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN SELECT 4; END;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
			CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END;
			CREATE TRIGGER users_delete AFTER DELETE ON users BEGIN SELECT 3; END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
			},
			&SQLiteDropTriggerInstruction{Name: "users_update"},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_update AFTER UPDATE ON users BEGIN SELECT 2; END",
			},
			&SQLiteDropTriggerInstruction{Name: "users_audit"},
		})
	})

	t.Run("CreateTableWithTriggers", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END;
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
						Name: "name",
						Type: "TEXT",
					},
				},
			},
			&SQLiteCreateTriggerInstruction{
				Definition: "CREATE TRIGGER users_insert AFTER INSERT ON users BEGIN SELECT 1; END",
			},
		})
	})

	t.Run("CreateViewTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users_view (id, name) VALUES (1, 'alice');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": "alice"},
		}, rows)
	})

	t.Run("DropViewTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteDropTriggerInstruction{Name: "users_view_insert"},
		})
	})

	t.Run("RecreateViewWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT name, id FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "users_view"},
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT name, id FROM users",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})

		driver.ExecOnCurrent(`INSERT INTO users_view (id, name) VALUES (2, 'bob');`)

		rows := driver.FetchAllFromCurrent("users", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(2), "name": "bob"},
		}, rows)
	})

	t.Run("CreateViewWithTrigger", func(t *testing.T) {
		driver := NewTestSQLiteDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
			CREATE VIEW users_view AS SELECT id, name FROM users;
			CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&SQLiteCreateViewInstruction{
				Definition: "CREATE VIEW users_view AS SELECT id, name FROM users",
			},
			&SQLiteCreateTriggerInstruction{
				Definition: `CREATE TRIGGER users_view_insert INSTEAD OF INSERT ON users_view
				BEGIN INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); END`,
			},
		})
	})
}
