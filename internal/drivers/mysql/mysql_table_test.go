package driversmysql

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLTable(t *testing.T) {
	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (
				id int NOT NULL AUTO_INCREMENT,
				name varchar(100) NOT NULL,
				age int DEFAULT 18,
				PRIMARY KEY (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "users",
				Columns: []*MySQLColumn{
					{
						Name:          "id",
						Type:          "int",
						NotNull:       true,
						AutoIncrement: true,
					},
					{
						Name:    "name",
						Type:    "varchar(100)",
						NotNull: true,
					},
					{
						Name: "age",
						Type: "int",
						Default: sql.NullString{
							String: "18",
							Valid:  true,
						},
					},
				},
				PrimaryKey: []string{"id"},
			},
		})
	})

	t.Run("DropTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnCurrent("CREATE TABLE old_users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTableInstruction{
				Name: "old_users",
			},
		})
	})

	t.Run("ChangePrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE members (id int NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id, tenant_id));")
		driver.ExecOnCurrent("CREATE TABLE members (id int NOT NULL, tenant_id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "members",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "members",
				Action: &MySQLAddPrimaryKeyAction{
					Columns: []string{"id", "tenant_id"},
				},
			},
		})
	})

	t.Run("RemoveAutoIncrementAndPrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE logs (id int NOT NULL, message text);")
		driver.ExecOnCurrent("CREATE TABLE logs (id int NOT NULL AUTO_INCREMENT, message text, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "logs",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name:   "logs",
				Action: &MySQLDropPrimaryKeyAction{},
			},
		})
	})

	t.Run("RenameAndRetypeAPrimaryKeyColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (user_id bigint NOT NULL, name varchar(20), PRIMARY KEY (user_id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (7, 'Alice');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "users",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLDropColumnAction{
					ColumnName: "id",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:    "user_id",
						Type:    "bigint",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddPrimaryKeyAction{
					Columns: []string{"user_id"},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"user_id": int64(0), "name": "Alice"},
		}, driver.FetchAllFromCurrent("users", ""))
	})

	t.Run("CreateTableWithEngineAndCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id)) ENGINE = MyISAM DEFAULT COLLATE = utf8mb4_bin;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "logs",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
				Engine:     "MyISAM",
				Collation:  "utf8mb4_bin",
			},
		})
	})

	t.Run("ChangeEngine", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id)) ENGINE = MyISAM;")
		driver.ExecOnCurrent("CREATE TABLE logs (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "logs",
				Action: &MySQLEngineAction{
					Engine: "MyISAM",
				},
			},
		})
	})

	t.Run("ChangeTableCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE messages (id int NOT NULL, body varchar(100), PRIMARY KEY (id)) DEFAULT COLLATE = utf8mb4_bin;")
		driver.ExecOnCurrent("CREATE TABLE messages (id int NOT NULL, body varchar(100), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO messages (id, body) VALUES (1, 'hello');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLConvertToCharacterSetAction{
					CharacterSet: "utf8mb4",
					Collation:    "utf8mb4_bin",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": "hello"},
		}, driver.FetchAllFromCurrent("messages", ""))
	})

	t.Run("ChangeTableCollationWithAColumnCollation", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE messages (id int NOT NULL, body varchar(100) COLLATE utf8mb4_general_ci, PRIMARY KEY (id)) DEFAULT COLLATE = utf8mb4_bin;")
		driver.ExecOnCurrent("CREATE TABLE messages (id int NOT NULL, body varchar(100) COLLATE utf8mb4_general_ci, PRIMARY KEY (id));")

		// The conversion rewrites the collation of the body column, so the column needs
		// its definition again in the same run.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLConvertToCharacterSetAction{
					CharacterSet: "utf8mb4",
					Collation:    "utf8mb4_bin",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "messages",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:      "body",
						Type:      "varchar(100)",
						Collation: "utf8mb4_general_ci",
					},
				},
			},
		})
	})

	t.Run("CreatePartitionedTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "metrics",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
				Partition:  "PARTITION BY HASH (`id`)\nPARTITIONS 4",
			},
		})
	})

	t.Run("AddPartitioning", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 2;")
		driver.ExecOnCurrent("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO metrics (id) VALUES (1), (2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "metrics",
				Action: &MySQLPartitionAction{
					Clause: "PARTITION BY KEY (id)\nPARTITIONS 2",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		}, driver.FetchAllFromCurrent("metrics", "ORDER BY id"))
	})

	t.Run("RemovePartitioning", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "metrics",
				Action: &MySQLRemovePartitioningAction{},
			},
		})
	})
}

func TestMariaDBTable(t *testing.T) {
	t.Run("PartitionedTable", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE metrics (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateTableInstruction{
				Name: "metrics",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int(11)",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
				Partition:  "PARTITION BY HASH (`id`)\nPARTITIONS 4",
			},
		})
	})
}
