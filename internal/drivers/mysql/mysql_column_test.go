package driversmysql

import (
	"database/sql"
	"fmt"
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

	t.Run("RenameColumnWithACheckAndAnIndex", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE orders (
				id int NOT NULL,
				quantity int,
				PRIMARY KEY (id),
				KEY idx_quantity (quantity),
				CONSTRAINT quantity_positive CHECK (quantity > 0)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE orders (
				id int NOT NULL,
				qty int,
				PRIMARY KEY (id),
				KEY idx_quantity (qty),
				CONSTRAINT quantity_positive CHECK (qty > 0)
			);
			INSERT INTO orders VALUES (1, 3);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "orders",
				Action: &MySQLDropCheckConstraintAction{
					Name: "quantity_positive",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "orders",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "qty",
					NewColumnName: "quantity",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "orders",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "quantity_positive",
						Expression: "(`quantity` > 0)",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "quantity": int64(3)},
		}, driver.FetchAllFromCurrent("orders", "ORDER BY id"))
	})

	t.Run("RenameAnAutoIncrementPrimaryKeyColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (user_id int NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (user_id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users (name) VALUES ('Alice');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "id",
					NewColumnName: "user_id",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"user_id": int64(1), "name": "Alice"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY user_id"))
	})

	t.Run("RenameAColumnThatAGeneratedColumnReads", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE prices (
				id int NOT NULL,
				net_price int,
				gross int GENERATED ALWAYS AS (net_price * 2) VIRTUAL,
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) VIRTUAL,
				PRIMARY KEY (id)
			);
			INSERT INTO prices (id, net) VALUES (1, 5);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLDropColumnAction{
					ColumnName: "gross",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "net",
					NewColumnName: "net_price",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:                "gross",
						Type:                "int",
						GeneratedExpression: "(`net_price` * 2)",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "net_price": int64(5), "gross": int64(10)},
		}, driver.FetchAllFromCurrent("prices", "ORDER BY id"))
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

	t.Run("ChangeGeneratedColumnStorageKeepsDependents", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		schema := `
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) %s,
				PRIMARY KEY (id),
				KEY idx_gross (gross),
				CONSTRAINT gross_positive CHECK (gross >= 0)
			);
		`

		driver.ExecOnDesired(fmt.Sprintf(schema, "STORED"))
		driver.ExecOnCurrent(fmt.Sprintf(schema, "VIRTUAL"))
		driver.ExecOnCurrent("INSERT INTO prices (id, net) VALUES (1, 5);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLDropCheckConstraintAction{
					Name: "gross_positive",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "idx_gross",
				TableName: "prices",
			},
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
			&MySQLCreateIndexInstruction{
				Name:      "idx_gross",
				TableName: "prices",
				Keys:      []string{"`gross`"},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "gross_positive",
						Expression: "(`gross` >= 0)",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "net": int64(5), "gross": int64(10)},
		}, driver.FetchAllFromCurrent("prices", "ORDER BY id"))
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
	t.Run("RenameColumnWithACheckAndAnIndex", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE orders (
				id int NOT NULL,
				quantity int,
				PRIMARY KEY (id),
				KEY idx_quantity (quantity),
				CONSTRAINT quantity_positive CHECK (quantity > 0)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE orders (
				id int NOT NULL,
				qty int,
				PRIMARY KEY (id),
				KEY idx_quantity (qty),
				CONSTRAINT quantity_positive CHECK (qty > 0)
			);
			INSERT INTO orders VALUES (1, 3);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "orders",
				Action: &MySQLDropCheckConstraintAction{
					Name: "quantity_positive",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "orders",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "qty",
					NewColumnName: "quantity",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "orders",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "quantity_positive",
						Expression: "`quantity` > 0",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "quantity": int64(3)},
		}, driver.FetchAllFromCurrent("orders", "ORDER BY id"))
	})

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

	t.Run("ChangeGeneratedColumnStorageKeepsDependents", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		schema := `
			CREATE TABLE prices (
				id int NOT NULL,
				net int,
				gross int GENERATED ALWAYS AS (net * 2) %s,
				PRIMARY KEY (id),
				KEY idx_gross (gross),
				CONSTRAINT gross_positive CHECK (gross >= 0)
			);
		`

		driver.ExecOnDesired(fmt.Sprintf(schema, "STORED"))
		driver.ExecOnCurrent(fmt.Sprintf(schema, "VIRTUAL"))
		driver.ExecOnCurrent("INSERT INTO prices (id, net) VALUES (1, 5);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLDropCheckConstraintAction{
					Name: "gross_positive",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "idx_gross",
				TableName: "prices",
			},
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
						Type:                "int(11)",
						GeneratedExpression: "`net` * 2",
						GeneratedStored:     true,
					},
				},
			},
			&MySQLCreateIndexInstruction{
				Name:      "idx_gross",
				TableName: "prices",
				Keys:      []string{"`gross`"},
			},
			&MySQLAlterTableInstruction{
				Name: "prices",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:       "gross_positive",
						Expression: "`gross` >= 0",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "net": int64(5), "gross": int64(10)},
		}, driver.FetchAllFromCurrent("prices", "ORDER BY id"))
	})
}
