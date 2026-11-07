package driversmysql

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLColumn(t *testing.T) {
	t.Run("AddColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(255)",
					},
				},
			},
		})
	})

	t.Run("RemoveColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, email varchar(255), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropColumnAction{
					ColumnName: "email",
				},
			},
		})
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age bigint, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (id, age) VALUES (1, 42);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "age",
						Type: "bigint",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(42)},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("RenameColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, full_name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (id, name) VALUES (1, 'Alice');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "name",
					NewColumnName: "full_name",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "full_name": "Alice"},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("ExpressionDefaultAndOnUpdate", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE events (
				id int NOT NULL,
				updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "events",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "updated_at",
						Type: "timestamp",
						Default: sql.NullString{
							String: "CURRENT_TIMESTAMP",
							Valid:  true,
						},
						DefaultIsExpression: true,
						OnUpdate:            "CURRENT_TIMESTAMP",
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("GeneratedColumns", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				doubled int GENERATED ALWAYS AS (net + net) VIRTUAL,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE prices (id int NOT NULL, net int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int",
						GeneratedExpression: "(`net` * 2)",
						GeneratedStored:     true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "doubled",
						Type:                "int",
						GeneratedExpression: "(`net` + `net`)",
					},
				},
			},
		})
	})

	t.Run("ChangeColumnToGenerated", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE prices (id int NOT NULL, net int, gross int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLDropColumnAction{
					ColumnName: "gross",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int",
						GeneratedExpression: "(`net` * 2)",
						GeneratedStored:     true,
					},
				},
			},
		})
	})

	t.Run("ColumnComment", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL COMMENT 'the identifier', PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
						Comment: "the identifier",
					},
				},
			},
		})
	})

	t.Run("BitAndBinaryDefaults", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE flags (
				id int NOT NULL,
				flag bit(1) NOT NULL DEFAULT b'1',
				payload varbinary(4) DEFAULT 0xDEAD,
				note varchar(9) DEFAULT 'b''1''',
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE flags (id int NOT NULL, PRIMARY KEY (id));
			INSERT INTO flags VALUES (1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "flags",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:    "flag",
						Type:    "bit(1)",
						NotNull: true,
						Default: sql.NullString{
							String: "b'1'",
							Valid:  true,
						},
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "flags",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "payload",
						Type: "varbinary(4)",
						Default: sql.NullString{
							String: "0xDEAD",
							Valid:  true,
						},
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "flags",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "note",
						Type: "varchar(9)",
						Default: sql.NullString{
							String: "b'1'",
							Valid:  true,
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "flag": "\x01", "payload": "\xde\xad", "note": "b'1'"},
		}, driver.FetchAllFromCurrent("flags", "ORDER BY id"))
	})
}

func TestMariaDBColumn(t *testing.T) {
	t.Run("CreateTableWithDefaults", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL DEFAULT 'anonymous',
				age int DEFAULT 18,
				created_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "users",
				Columns: []*MySQLColumn{
					{
						Name:          "id",
						Type:          "int(11)",
						NotNull:       true,
						AutoIncrement: true,
					},
					{
						Name:    "name",
						Type:    "varchar(100)",
						NotNull: true,
						Default: sql.NullString{
							String: "'anonymous'",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
					{
						Name: "age",
						Type: "int(11)",
						Default: sql.NullString{
							String: "18",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
					{
						Name: "created_at",
						Type: "timestamp",
						Default: sql.NullString{
							String: "current_timestamp()",
							Valid:  true,
						},
						DefaultIsExpression: true,
						OnUpdate:            "current_timestamp()",
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("GeneratedColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) STORED,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent("CREATE TABLE prices (id int NOT NULL, net int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int(11)",
						GeneratedExpression: "`net` * 2",
						GeneratedStored:     true,
					},
				},
			},
		})
	})

	t.Run("BitDefault", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE flags (id int NOT NULL, flag bit(1) NOT NULL DEFAULT b'1', PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE flags (id int NOT NULL, PRIMARY KEY (id));
			INSERT INTO flags VALUES (1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "flags",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:    "flag",
						Type:    "bit(1)",
						NotNull: true,
						Default: sql.NullString{
							String: "b'1'",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "flag": "\x01"},
		}, driver.FetchAllFromCurrent("flags", "ORDER BY id"))
	})
}
