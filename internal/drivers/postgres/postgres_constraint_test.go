package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestPostgresConstraint(t *testing.T) {
	t.Run("ConstraintsPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT PRIMARY KEY);`)

		driver.ExecOnDesired(`DROP TABLE users; CREATE TABLE users (id INT, CONSTRAINT pk_users PRIMARY KEY (id));`)
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
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "pk_users",
							Type: "p",
							Def:  "PRIMARY KEY (id)",
						},
					},
				},
			},
		})
	})

	t.Run("ConstraintsUnique", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (email TEXT);`)
		driver.ExecOnDesired(`CREATE TABLE users (email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "uq_email",
							Type: "u",
							Def:  "UNIQUE (email)",
						},
					},
				},
			},
		})
	})

	t.Run("ConstraintsForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT, CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles(id));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "fk_role",
							Type: "f",
							Def:  "FOREIGN KEY (role_id) REFERENCES roles(id)",
						},
					},
				},
			},
		})
	})

	t.Run("ConstraintsCheck", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE items (
				price INT,
				CONSTRAINT positive_price CHECK (price > 0),
				CONSTRAINT small_price CHECK (price < 10)
			);
		`)
		driver.ExecOnDesired(`CREATE TABLE items (price INT, CONSTRAINT positive_price CHECK (price >= 0));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "positive_price"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "positive_price",
							Type: "c",
							Def:  "CHECK ((price >= 0))",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "items",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "small_price"},
				},
			},
		})
	})

	t.Run("DropColumnWithPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, code INT, CONSTRAINT pk_users PRIMARY KEY (code));`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "pk_users"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "code"},
				},
			},
		})
	})

	t.Run("DropColumnWithUniqueConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_email UNIQUE (email));`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_email"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnKeepsOtherConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (
				id INT NOT NULL,
				email TEXT,
				CONSTRAINT pk_users PRIMARY KEY (id),
				CONSTRAINT uq_email UNIQUE (email)
			);
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT NOT NULL, CONSTRAINT pk_users PRIMARY KEY (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_email"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})

	t.Run("DropColumnOfCompositeConstraint", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT, email TEXT, CONSTRAINT uq_users UNIQUE (id, email));`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT, CONSTRAINT uq_users UNIQUE (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{ConstraintName: "uq_users"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "uq_users",
							Type: "u",
							Def:  "UNIQUE (id)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "email"},
				},
			},
		})
	})
}
