package driversmysql

import (
	"database/sql"
	"fmt"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestMySQLColumn(t *testing.T) {
	t.Run("AddColumnInTheMiddle", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, name varchar(20), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (1, 'a@b.c');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "name",
						Type: "varchar(20)",
					},
					After: "id",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": nil, "email": "a@b.c"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

	t.Run("AddColumnFirst", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (code int, id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "code",
						Type: "int",
					},
					First: true,
				},
			},
		})
	})

	t.Run("ChangeTypeAndMoveAColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, age bigint, name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(20), age int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (1, 'Alice', 30);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "age",
						Type: "bigint",
					},
					After: "id",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "age": int64(30), "name": "Alice"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

	t.Run("MoveTheFirstColumnToTheEnd", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (a int, b int, c int, id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, a int, b int, c int, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					After: "c",
				},
			},
		})
	})

	t.Run("ChangeColumnOrder", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, email varchar(50), name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(20), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (1, 'Alice', 'a@b.c');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(50)",
					},
					After: "id",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "a@b.c", "name": "Alice"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

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

	t.Run("RenameAndRetypeAnAutoIncrementPrimaryKeyColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (user_id bigint NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (user_id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (5, 'Alice'), (9, 'Bob');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "id",
					NewColumnName: "user_id",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:          "user_id",
						Type:          "bigint",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"user_id": int64(5), "name": "Alice"},
			{"user_id": int64(9), "name": "Bob"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY user_id"))
	})

	t.Run("ReplaceTheAutoIncrementPrimaryKeyWithANewColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, user_id bigint NOT NULL AUTO_INCREMENT, " +
			"name varchar(20), PRIMARY KEY (user_id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL AUTO_INCREMENT, name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (5, 'Alice'), (9, 'Bob');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name:   "users",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:          "user_id",
						Type:          "bigint",
						NotNull:       true,
						AutoIncrement: true,
					},
					After:      "id",
					PrimaryKey: true,
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(5), "user_id": int64(1), "name": "Alice"},
			{"id": int64(9), "user_id": int64(2), "name": "Bob"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY user_id"))
	})

	t.Run("DropTheAutoIncrementPrimaryKeyColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (uid bigint NOT NULL, a int, PRIMARY KEY (uid));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL AUTO_INCREMENT, a int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 1);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name:   "t",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLDropColumnAction{
					ColumnName: "id",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name:    "uid",
						Type:    "bigint",
						NotNull: true,
					},
					First: true,
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddPrimaryKeyAction{
					Columns: []string{"uid"},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"uid": int64(0), "a": int64(1)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY uid"))
	})

	t.Run("MoveAutoIncrementToAnotherColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (uid int NOT NULL AUTO_INCREMENT, PRIMARY KEY (uid));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL AUTO_INCREMENT, uid int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 1), (9, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name:   "t",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLDropColumnAction{
					ColumnName: "id",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddPrimaryKeyAction{
					Columns: []string{"uid"},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:          "uid",
						Type:          "int",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"uid": int64(1)},
			{"uid": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY uid"))
	})

	t.Run("AddAnAutoIncrementColumnToACompositePrimaryKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, uid bigint NOT NULL AUTO_INCREMENT, a int, " +
			"PRIMARY KEY (id, uid), KEY k (uid));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL AUTO_INCREMENT, a int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 1), (9, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name:   "t",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLActionList{
					Actions: []driversshared.AlterTableAction{
						&MySQLAddColumnAction{
							Column: &MySQLColumn{
								Name:          "uid",
								Type:          "bigint",
								NotNull:       true,
								AutoIncrement: true,
							},
							After: "id",
						},
						&MySQLAddPrimaryKeyAction{
							Columns: []string{"id", "uid"},
						},
						&MySQLAddIndexAction{
							Name: "k",
							Keys: []string{"`uid`"},
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(5), "uid": int64(1), "a": int64(1)},
			{"id": int64(9), "uid": int64(2), "a": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("AddAnAutoIncrementColumnToACompositePrimaryKeyWithRepeatedValues", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, a int NOT NULL, uid bigint NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (a, uid), KEY k (uid));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 7), (2, 7), (3, 8);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "t",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLActionList{
					Actions: []driversshared.AlterTableAction{
						&MySQLAddColumnAction{
							Column: &MySQLColumn{
								Name:          "uid",
								Type:          "bigint",
								NotNull:       true,
								AutoIncrement: true,
							},
						},
						&MySQLAddPrimaryKeyAction{
							Columns: []string{"a", "uid"},
						},
						&MySQLAddIndexAction{
							Name: "k",
							Keys: []string{"`uid`"},
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "a": int64(7), "uid": int64(1)},
			{"id": int64(2), "a": int64(7), "uid": int64(2)},
			{"id": int64(3), "a": int64(8), "uid": int64(3)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("AddAnAutoIncrementColumnWithAUniqueKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, seq bigint NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), UNIQUE KEY u (seq));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5), (9);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "seq",
						Type: "bigint",
					},
				},
			},
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "u",
				TableName: "t",
				Keys:      []string{"`seq`"},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:          "seq",
						Type:          "bigint",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(5), "seq": int64(1)},
			{"id": int64(9), "seq": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("RemoveAutoIncrementFromAColumnWithAUniqueKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, a int);")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL AUTO_INCREMENT, a int, UNIQUE KEY u (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 1), (9, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "u",
				TableName: "t",
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(5), "a": int64(1)},
			{"id": int64(9), "a": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("AddAutoIncrementToAColumnWithANewUniqueKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, seq bigint NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (id), UNIQUE KEY u (seq));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, seq bigint NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 10), (9, 20);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "u",
				TableName: "t",
				Keys:      []string{"`seq`"},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:          "seq",
						Type:          "bigint",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(5), "seq": int64(10)},
			{"id": int64(9), "seq": int64(20)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("MoveAndAddAutoIncrementToAColumnWithANewUniqueKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (seq bigint NOT NULL AUTO_INCREMENT, id int NOT NULL, " +
			"PRIMARY KEY (id), UNIQUE KEY u (seq));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, seq bigint NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 10), (9, 20);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "seq",
						Type:    "bigint",
						NotNull: true,
					},
					First: true,
				},
			},
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "u",
				TableName: "t",
				Keys:      []string{"`seq`"},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:          "seq",
						Type:          "bigint",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"seq": int64(10), "id": int64(5)},
			{"seq": int64(20), "id": int64(9)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("ReplaceTheOnlyColumn", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (b varchar(10));")
		driver.ExecOnCurrent("CREATE TABLE t (a int);")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1), (2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "b",
						Type: "varchar(10)",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLDropColumnAction{
					ColumnName: "a",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"b": nil},
			{"b": nil},
		}, driver.FetchAllFromCurrent("t", ""))
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
	t.Run("MoveAColumnWithAColumnCheck", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (a int CHECK (a > 0), id int NOT NULL, b int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, b int, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2, 3);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "a",
						Type: "int(11)",
					},
					First: true,
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:        "a",
						Expression:  "`a` > 0",
						ColumnLevel: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"a": int64(3), "id": int64(1), "b": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("CommentOnAJSONColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE docs (id int NOT NULL, body json COMMENT 'the body', PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE docs (id int NOT NULL, body json, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`INSERT INTO docs VALUES (1, '{"a": 1}');`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "docs",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:      "body",
						Type:      "longtext",
						Collation: "utf8mb4_bin",
						Comment:   "the body",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "docs",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:        "body",
						Expression:  "json_valid(`body`)",
						ColumnLevel: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "body": `{"a": 1}`},
		}, driver.FetchAllFromCurrent("docs", "ORDER BY id"))
	})

	t.Run("AddColumnInTheMiddle", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, name varchar(20), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (1, 'a@b.c');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "name",
						Type: "varchar(20)",
					},
					After: "id",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "name": nil, "email": "a@b.c"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

	t.Run("ChangeColumnOrder", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE users (id int NOT NULL, email varchar(50), name varchar(20), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, name varchar(20), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO users VALUES (1, 'Alice', 'a@b.c');")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "users",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "email",
						Type: "varchar(50)",
					},
					After: "id",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "email": "a@b.c", "name": "Alice"},
		}, driver.FetchAllFromCurrent("users", "ORDER BY id"))
	})

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

	t.Run("BinaryDefaultThatIsNotUTF8", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE payloads (
				id int NOT NULL,
				payload varbinary(4) DEFAULT X'BEEF',
				note varbinary(4) DEFAULT 'AB',
				PRIMARY KEY (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE payloads (id int NOT NULL, PRIMARY KEY (id));
			INSERT INTO payloads VALUES (1);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "payloads",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "payload",
						Type: "varbinary(4)",
						Default: sql.NullString{
							String: "0xBEEF",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "payloads",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "note",
						Type: "varbinary(4)",
						Default: sql.NullString{
							String: "'AB'",
							Valid:  true,
						},
						DefaultIsExpression: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "payload": "\xbe\xef", "note": "AB"},
		}, driver.FetchAllFromCurrent("payloads", "ORDER BY id"))
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

	t.Run("RenameAColumnWithAColumnCheck", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, aa int CHECK (aa > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "a",
					NewColumnName: "aa",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "aa",
						Type: "int(11)",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:        "aa",
						Expression:  "`aa` > 0",
						ColumnLevel: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "aa": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("RenameAndMoveAColumnWithAColumnCheck", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (aa int CHECK (aa > 0), id int NOT NULL, b int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, b int, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2, 3);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLRenameColumnAction{
					ColumnName:    "a",
					NewColumnName: "aa",
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "aa",
						Type: "int(11)",
					},
					First: true,
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:        "aa",
						Expression:  "`aa` > 0",
						ColumnLevel: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"aa": int64(3), "id": int64(1), "b": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("ChangeAColumnCheckThatKeepsTheNameOfARenamedColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, aa int CHECK (aa > 1), PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("ALTER TABLE t RENAME COLUMN a TO aa;")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "aa",
						Type: "int(11)",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddCheckConstraintAction{
					CheckConstraint: &MySQLCheckConstraint{
						Name:        "aa",
						Expression:  "`aa` > 1",
						ColumnLevel: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "aa": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("RemoveAColumnCheckThatKeepsTheNameOfARenamedColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, aa int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int CHECK (a > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("ALTER TABLE t RENAME COLUMN a TO aa;")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name: "aa",
						Type: "int(11)",
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "aa": int64(2)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("ChangeAColumnCheckWithNoKnownColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, aa int, PRIMARY KEY (id));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int CHECK (1 > 0), PRIMARY KEY (id));")
		driver.ExecOnCurrent("ALTER TABLE t RENAME COLUMN a TO aa;")

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err, `the check "a" of table "t" names no column. Its expression reads no column `+
			`or more than one column. Change the check manually`)
	})

	t.Run("MoveAndAddAutoIncrementToAColumnWithANewUniqueKey", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (seq bigint NOT NULL AUTO_INCREMENT, id int NOT NULL, " +
			"PRIMARY KEY (id), UNIQUE KEY u (seq));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, seq bigint NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (5, 10), (9, 20);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:    "seq",
						Type:    "bigint(20)",
						NotNull: true,
					},
					First: true,
				},
			},
			&MySQLCreateIndexInstruction{
				Kind:      "UNIQUE",
				Name:      "u",
				TableName: "t",
				Keys:      []string{"`seq`"},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLModifyColumnAction{
					Column: &MySQLColumn{
						Name:          "seq",
						Type:          "bigint(20)",
						NotNull:       true,
						AutoIncrement: true,
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"seq": int64(10), "id": int64(5)},
			{"seq": int64(20), "id": int64(9)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("AddAnAutoIncrementColumnToACompositePrimaryKeyWithRepeatedValues", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (id int NOT NULL, a int NOT NULL, uid bigint NOT NULL AUTO_INCREMENT, " +
			"PRIMARY KEY (a, uid), KEY k (uid));")
		driver.ExecOnCurrent("CREATE TABLE t (id int NOT NULL, a int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1, 7), (2, 7), (3, 8);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name:   "t",
				Action: &MySQLDropPrimaryKeyAction{},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLActionList{
					Actions: []driversshared.AlterTableAction{
						&MySQLAddColumnAction{
							Column: &MySQLColumn{
								Name:          "uid",
								Type:          "bigint(20)",
								NotNull:       true,
								AutoIncrement: true,
							},
						},
						&MySQLAddPrimaryKeyAction{
							Columns: []string{"a", "uid"},
						},
						&MySQLAddIndexAction{
							Name: "k",
							Keys: []string{"`uid`"},
						},
					},
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"id": int64(1), "a": int64(7), "uid": int64(1)},
			{"id": int64(2), "a": int64(7), "uid": int64(2)},
			{"id": int64(3), "a": int64(8), "uid": int64(3)},
		}, driver.FetchAllFromCurrent("t", "ORDER BY id"))
	})

	t.Run("ReplaceTheOnlyColumn", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired("CREATE TABLE t (b varchar(10));")
		driver.ExecOnCurrent("CREATE TABLE t (a int);")
		driver.ExecOnCurrent("INSERT INTO t VALUES (1), (2);")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLAddColumnAction{
					Column: &MySQLColumn{
						Name: "b",
						Type: "varchar(10)",
					},
				},
			},
			&MySQLAlterTableInstruction{
				Name: "t",
				Action: &MySQLDropColumnAction{
					ColumnName: "a",
				},
			},
		})

		require.Equal(t, []map[string]any{
			{"b": nil},
			{"b": nil},
		}, driver.FetchAllFromCurrent("t", ""))
	})
}
