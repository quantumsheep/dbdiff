package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLTrigger(t *testing.T) {
	t.Run("CreateTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, updated_count int DEFAULT 0, PRIMARY KEY (id));
			CREATE TRIGGER trg_users_update BEFORE UPDATE ON users FOR EACH ROW
			BEGIN
				SET NEW.updated_count = OLD.updated_count + 1;
			END;
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, updated_count int DEFAULT 0, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (id) VALUES (1);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_update",
				Timing:    "BEFORE",
				Event:     "UPDATE",
				TableName: "users",
				Statement: "BEGIN\n\t\t\t\tSET NEW.updated_count = OLD.updated_count + 1;\n\t\t\tEND",
			},
		})

		driver.ExecOnCurrent("UPDATE users SET id = 1 WHERE id = 1;")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "updated_count": int64(1)},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("MoveATriggerToAnotherTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE a (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TABLE b (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_note BEFORE INSERT ON a FOR EACH ROW SET NEW.note = 'a';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE a (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TABLE b (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_note BEFORE INSERT ON b FOR EACH ROW SET NEW.note = 'b';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_note",
			},
			&MySQLCreateTriggerInstruction{
				Name:      "trg_note",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "a",
				Statement: "SET NEW.note = 'a'",
			},
		})

		driver.ExecOnCurrent("INSERT INTO a (id) VALUES (1); INSERT INTO b (id) VALUES (1);")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "note": "a"},
		}, driver.FetchAllFromCurrent("a", ""))
		require.Equal(t, []map[string]any{
			{"id": int64(1), "note": nil},
		}, driver.FetchAllFromCurrent("b", ""))
	})

	t.Run("NewTableTakesTheTriggerNameOfADroppedTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE a (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_note BEFORE INSERT ON a FOR EACH ROW BEGIN SET NEW.note = 'a'; END;
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE b (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_note BEFORE INSERT ON b FOR EACH ROW SET NEW.note = 'b';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_note",
			},
			&MySQLCreateTableInstruction{
				Name: "a",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "note",
						Type: "varchar(20)",
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLCreateTriggerInstruction{
				Name:      "trg_note",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "a",
				Statement: "BEGIN SET NEW.note = 'a'; END",
			},
			&MySQLDropTableInstruction{
				Name: "b",
			},
		})
	})

	t.Run("ChangeTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'new';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'old';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_users_insert",
			},
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_insert",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "users",
				Statement: "SET NEW.note = 'new'",
			},
		})
	})

	t.Run("DropTrigger", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TRIGGER trg_users_delete AFTER DELETE ON users FOR EACH ROW SET @deleted = 1;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTriggerInstruction{
				Name: "trg_users_delete",
			},
		})
	})
}

func TestMariaDBTrigger(t *testing.T) {
	t.Run("Trigger", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));
			CREATE TRIGGER trg_users_insert BEFORE INSERT ON users FOR EACH ROW SET NEW.note = 'new';
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, note varchar(20), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTriggerInstruction{
				Name:      "trg_users_insert",
				Timing:    "BEFORE",
				Event:     "INSERT",
				TableName: "users",
				Statement: "SET NEW.note = 'new'",
			},
		})
	})
}
