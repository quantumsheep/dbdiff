package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestPostgresSequence(t *testing.T) {
	t.Run("CreateSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE SEQUENCE counter;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateSequenceInstruction{
				Name:      "counter",
				DataType:  "bigint",
				Increment: 1,
				Min:       1,
				Max:       9223372036854775807,
				Start:     1,
				Cache:     1,
			},
		})
	})

	t.Run("DropSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDropSequenceInstruction{Name: "counter"},
		})
	})

	t.Run("AlterSequenceCache", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnDesired(`CREATE SEQUENCE counter CACHE 20;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Cache: sql.NullInt64{
					Int64: 20,
					Valid: true,
				},
			},
		})
	})

	t.Run("AlterSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnDesired(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Increment: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
				Max: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceRestartOnHigherMinimum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnCurrent(`SELECT nextval('counter');`)

		driver.ExecOnDesired(`CREATE SEQUENCE counter MINVALUE 100 START WITH 100;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Min: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Start: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Restart: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceRestartOnLowerMaximum", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnCurrent(`SELECT nextval('counter') FROM generate_series(1, 10);`)

		driver.ExecOnDesired(`CREATE SEQUENCE counter MAXVALUE 5;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Max: sql.NullInt64{
					Int64: 5,
					Valid: true,
				},
				Restart: sql.NullInt64{
					Int64: 1,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceNoRestartWithinRange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE SEQUENCE counter;`)
		driver.ExecOnCurrent(`SELECT nextval('counter');`)

		driver.ExecOnDesired(`CREATE SEQUENCE counter INCREMENT BY 2 MAXVALUE 100 CYCLE;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "counter",
				Increment: sql.NullInt64{
					Int64: 2,
					Valid: true,
				},
				Max: sql.NullInt64{
					Int64: 100,
					Valid: true,
				},
				Cycle: sql.NullBool{
					Bool:  true,
					Valid: true,
				},
			},
		})
	})

	t.Run("SequenceOfSerialColumnIsIgnored", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE users (id SERIAL);`)

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
				},
			},
		})
	})

	t.Run("SequenceOwnedByAColumnWithCustomOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT PRIMARY KEY);`)
		driver.ExecOnDesired(`
			CREATE SEQUENCE user_ids START 1000 INCREMENT 5;
			CREATE TABLE users (id INT PRIMARY KEY DEFAULT nextval('user_ids'));
			ALTER SEQUENCE user_ids OWNED BY users.id;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateSequenceInstruction{
				Name:      "user_ids",
				DataType:  "bigint",
				Increment: 5,
				Min:       1,
				Max:       9223372036854775807,
				Start:     1000,
				Cache:     1,
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "id",
						Expression: "nextval('user_ids'::regclass)",
					},
				},
			},
			&PostgresAlterSequenceOwnerInstruction{
				Name:       "user_ids",
				TableName:  "users",
				ColumnName: "id",
			},
		})
	})

	t.Run("CreateATableThatOwnsASequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE SEQUENCE user_ids START 1000;
			CREATE TABLE users (id INT DEFAULT nextval('user_ids'));
			ALTER SEQUENCE user_ids OWNED BY users.id;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateSequenceInstruction{
				Name:      "user_ids",
				DataType:  "bigint",
				Increment: 1,
				Min:       1,
				Max:       9223372036854775807,
				Start:     1000,
				Cache:     1,
			},
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
						Default: sql.NullString{
							String: "nextval('user_ids'::regclass)",
							Valid:  true,
						},
					},
				},
			},
			&PostgresAlterSequenceOwnerInstruction{
				Name:       "user_ids",
				TableName:  "users",
				ColumnName: "id",
			},
		})
	})

	t.Run("SerialSequenceWithCustomOptions", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id SERIAL);`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id SERIAL);
			ALTER SEQUENCE users_id_seq INCREMENT 5;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "users_id_seq",
				Increment: sql.NullInt64{
					Int64: 5,
					Valid: true,
				},
			},
		})
	})

	t.Run("SerialSequenceTakesTheDefaultOptionsAgain", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id SERIAL);
			ALTER SEQUENCE users_id_seq INCREMENT 5;
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id SERIAL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceInstruction{
				Name: "users_id_seq",
				Increment: sql.NullInt64{
					Int64: 1,
					Valid: true,
				},
			},
		})
	})

	t.Run("DropATableThatOwnsASequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE SEQUENCE user_ids START 1000;
			CREATE TABLE users (id INT DEFAULT nextval('user_ids'));
			ALTER SEQUENCE user_ids OWNED BY users.id;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceOwnerInstruction{Name: "user_ids"},
			&driversshared.SQLDropTableInstruction{Name: "users"},
			&PostgresDropSequenceInstruction{Name: "user_ids"},
		})
	})

	t.Run("DropAColumnAndKeepItsSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE SEQUENCE user_ids START 1000;
			CREATE TABLE users (id INT DEFAULT nextval('user_ids'), name TEXT);
			ALTER SEQUENCE user_ids OWNED BY users.id;
		`)
		driver.ExecOnDesired(`
			CREATE SEQUENCE user_ids START 1000;
			CREATE TABLE users (name TEXT);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceOwnerInstruction{Name: "user_ids"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "id"},
				},
			},
		})
	})

	t.Run("SerialSequenceStaysAfterItsColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id SERIAL, name TEXT);`)
		driver.ExecOnDesired(`
			CREATE SEQUENCE users_id_seq AS integer;
			CREATE TABLE users (name TEXT);
			CREATE TABLE accounts (id INT DEFAULT nextval('users_id_seq'));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceOwnerInstruction{Name: "users_id_seq"},
			&PostgresCreateTableInstruction{
				Name: "accounts",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
						Default: sql.NullString{
							String: "nextval('users_id_seq'::regclass)",
							Valid:  true,
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "id"},
				},
			},
		})
	})

	t.Run("SequenceWithoutAnOwnerBecomesASerialSequence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE SEQUENCE users_id_seq AS integer;
			CREATE TABLE users (id INT NOT NULL DEFAULT nextval('users_id_seq'));
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id SERIAL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceOwnerInstruction{
				Name:       "users_id_seq",
				TableName:  "users",
				ColumnName: "id",
			},
		})

		require.Equal(t, sql.NullString{
			String: driver.currentSchema + ".users_id_seq",
			Valid:  true,
		}, driver.serialSequenceOfCurrent("users", "id"))
	})

	t.Run("SerialSequenceBecomesASequenceWithoutAnOwner", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id SERIAL);`)
		driver.ExecOnDesired(`
			CREATE SEQUENCE users_id_seq AS integer;
			CREATE TABLE users (id INT NOT NULL DEFAULT nextval('users_id_seq'));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterSequenceOwnerInstruction{Name: "users_id_seq"},
		})

		require.Equal(t, sql.NullString{}, driver.serialSequenceOfCurrent("users", "id"))
	})
}
