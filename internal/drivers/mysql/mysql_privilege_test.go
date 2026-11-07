package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMySQLPrivilege(t *testing.T) {
	t.Run("RevokeGrantOption", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%' WITH GRANT OPTION;
			GRANT SELECT ON posts TO 'dbdiff_reader'@'%' WITH GRANT OPTION;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLDropTableInstruction{
				Name: "posts",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"GRANT OPTION"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT", "GRANT OPTION"},
				TableName:  "posts",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("GrantTablePrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT, UPDATE ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent("CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT", "UPDATE"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("RevokeAndNarrowPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE posts (id int NOT NULL, PRIMARY KEY (id));
			GRANT SELECT, DELETE ON users TO 'dbdiff_reader'@'%';
			GRANT SELECT ON posts TO 'dbdiff_reader'@'%';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLRevokeInstruction{
				Privileges: []string{"DELETE"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				TableName:  "posts",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("GrantColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(
			"CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"id", "name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("NarrowColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, email) ON users TO 'dbdiff_reader'@'%';
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"email"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("RevokeColumnPrivileges", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired(
			"CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE users (id int NOT NULL, name varchar(50), email varchar(50), PRIMARY KEY (id));
			GRANT SELECT (id, name) ON users TO 'dbdiff_reader'@'%';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLRevokeInstruction{
				Privileges: []string{"SELECT"},
				Columns:    []string{"id", "name"},
				TableName:  "users",
				Grantee:    "'dbdiff_reader'@'%'",
			},
		})
	})

	t.Run("DatabasePrivilegesAndGrantOption", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)
		driver.ComparePrivileges = true
		driver.CreateTestUser("dbdiff_reader")

		driver.ExecOnDesired("GRANT SELECT ON * TO 'dbdiff_reader'@'%' WITH GRANT OPTION;")

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLGrantInstruction{
				Privileges:      []string{"SELECT"},
				Grantee:         "'dbdiff_reader'@'%'",
				WithGrantOption: true,
			},
		})
	})
}
