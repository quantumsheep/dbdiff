package driverspostgres

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
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
							Name:            "fk_role",
							Type:            "f",
							Def:             "FOREIGN KEY (role_id) REFERENCES roles(id)",
							ReferencedTable: "roles",
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
					&PostgresDropConstraintAction{ConstraintName: "small_price"},
				},
			},
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

	t.Run("ChangeAReferencedPrimaryKeyAndItsForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE accounts (id INT, region TEXT NOT NULL, PRIMARY KEY (id));
			CREATE TABLE orders (id INT, account_id INT, region TEXT, FOREIGN KEY (account_id) REFERENCES accounts (id));
			INSERT INTO accounts VALUES (1, 'eu');
			INSERT INTO orders VALUES (1, 1, 'eu');
		`)
		driver.ExecOnDesired(`
			CREATE TABLE accounts (id INT, region TEXT NOT NULL, PRIMARY KEY (id, region));
			CREATE TABLE orders (
				id INT,
				account_id INT,
				region TEXT,
				FOREIGN KEY (account_id, region) REFERENCES accounts (id, region)
			);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "orders_account_id_fkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "accounts_pkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (id, region)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "orders_account_id_region_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (account_id, region) REFERENCES accounts(id, region)",
							ReferencedTable: "accounts",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("orders", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1), "account_id": int64(1), "region": "eu"},
		}, rows)
	})

	t.Run("ChangeAReferencedPrimaryKeyAndKeepItsForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE accounts (id INT, region TEXT NOT NULL, PRIMARY KEY (id));
			CREATE TABLE orders (account_id INT REFERENCES accounts (id));
		`)
		driver.ExecOnDesired(`
			CREATE TABLE accounts (id INT, region TEXT NOT NULL, PRIMARY KEY (id, region), UNIQUE (id));
			CREATE TABLE orders (account_id INT REFERENCES accounts (id));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "orders_account_id_fkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_id_key",
							Type: "u",
							Def:  "UNIQUE (id)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "accounts_pkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (id, region)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "orders_account_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (account_id) REFERENCES accounts(id)",
							ReferencedTable: "accounts",
						},
					},
				},
			},
		})
	})

	t.Run("AddASelfReferenceAndAPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE nodes (id INT NOT NULL, parent_id INT);`)
		driver.ExecOnDesired(`CREATE TABLE nodes (id INT PRIMARY KEY, parent_id INT REFERENCES nodes (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "nodes",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "nodes_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (id)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "nodes",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "nodes_parent_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (parent_id) REFERENCES nodes(id)",
							ReferencedTable: "nodes",
						},
					},
				},
			},
		})
	})

	t.Run("ChangeAReferencedUniqueIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE accounts (id INT, code TEXT);
			CREATE UNIQUE INDEX accounts_code ON accounts (code);
			CREATE TABLE orders (account_code TEXT REFERENCES accounts (code));
		`)
		driver.ExecOnDesired(`
			CREATE TABLE accounts (id INT, code TEXT);
			CREATE UNIQUE INDEX accounts_code ON accounts (code) INCLUDE (id);
			CREATE TABLE orders (account_code TEXT REFERENCES accounts (code));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "orders_account_code_fkey",
					},
				},
			},
			&driversshared.SQLDropIndexInstruction{
				Name: "accounts_code",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX accounts_code ON accounts USING btree (code) INCLUDE (id)",
			},
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "orders_account_code_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (account_code) REFERENCES accounts(code)",
							ReferencedTable: "accounts",
						},
					},
				},
			},
		})
	})

	t.Run("RenameAPrimaryKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE accounts (id INT, CONSTRAINT accounts_pkey PRIMARY KEY (id));
			INSERT INTO accounts VALUES (1), (2);
		`)
		driver.ExecOnDesired(`CREATE TABLE accounts (id INT, CONSTRAINT accounts_pk PRIMARY KEY (id));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "accounts_pkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_pk",
							Type: "p",
							Def:  "PRIMARY KEY (id)",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("accounts", "ORDER BY id")

		require.Equal(t, []map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		}, rows)
	})

	t.Run("AddAConstraintComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));`)
		driver.ExecOnDesired(`
			CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));
			COMMENT ON CONSTRAINT positive_age ON users IS 'The age must be positive';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "positive_age",
				Text:      "The age must be positive",
			},
		})
	})

	t.Run("ChangeAConstraintComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));
			COMMENT ON CONSTRAINT positive_age ON users IS 'Old text';
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));
			COMMENT ON CONSTRAINT positive_age ON users IS 'New text';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "positive_age",
				Text:      "New text",
			},
		})
	})

	t.Run("RemoveAConstraintComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));
			COMMENT ON CONSTRAINT positive_age ON users IS 'Old text';
		`)
		driver.ExecOnDesired(`CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "positive_age",
				Text:      "",
			},
		})
	})

	t.Run("AChangedConstraintKeepsItsComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 0));
			COMMENT ON CONSTRAINT positive_age ON users IS 'The age must be valid';
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (age INT, CONSTRAINT positive_age CHECK (age > 1));
			COMMENT ON CONSTRAINT positive_age ON users IS 'The age must be valid';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "positive_age",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:    "positive_age",
							Type:    "c",
							Def:     "CHECK ((age > 1))",
							Comment: "The age must be valid",
						},
					},
				},
			},
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "positive_age",
				Text:      "The age must be valid",
			},
		})
	})

	t.Run("ChangeAForeignKeyComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT, CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles (id));
		`)
		driver.ExecOnDesired(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (role_id INT, CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles (id));
			COMMENT ON CONSTRAINT fk_role ON users IS 'The role of the user';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "fk_role",
				Text:      "The role of the user",
			},
		})
	})

	t.Run("AReAddedForeignKeyKeepsItsComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE accounts (id INT, region TEXT NOT NULL, PRIMARY KEY (id));
			CREATE TABLE orders (account_id INT REFERENCES accounts (id));
			COMMENT ON CONSTRAINT orders_account_id_fkey ON orders IS 'The account of the order';
		`)
		driver.ExecOnDesired(`
			CREATE TABLE accounts (id INT, region TEXT NOT NULL, PRIMARY KEY (id, region), UNIQUE (id));
			CREATE TABLE orders (account_id INT REFERENCES accounts (id));
			COMMENT ON CONSTRAINT orders_account_id_fkey ON orders IS 'The account of the order';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "orders_account_id_fkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_id_key",
							Type: "u",
							Def:  "UNIQUE (id)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "accounts_pkey",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "accounts",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "accounts_pkey",
							Type: "p",
							Def:  "PRIMARY KEY (id, region)",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "orders",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "orders_account_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (account_id) REFERENCES accounts(id)",
							ReferencedTable: "accounts",
							Comment:         "The account of the order",
						},
					},
				},
			},
			&PostgresCommentOnConstraintInstruction{
				TableName: "orders",
				Name:      "orders_account_id_fkey",
				Text:      "The account of the order",
			},
		})
	})

	t.Run("CreateATableWithConstraintComments", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE roles (id INT PRIMARY KEY);`)
		driver.ExecOnDesired(`
			CREATE TABLE roles (id INT PRIMARY KEY);
			CREATE TABLE users (
				role_id INT,
				CONSTRAINT positive_role CHECK (role_id > 0),
				CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles (id)
			);
			COMMENT ON CONSTRAINT positive_role ON users IS 'The role must be positive';
			COMMENT ON CONSTRAINT fk_role ON users IS 'The role of the user';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name: "role_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name:    "positive_role",
						Type:    "c",
						Def:     "CHECK ((role_id > 0))",
						Comment: "The role must be positive",
					},
				},
			},
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "positive_role",
				Text:      "The role must be positive",
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "fk_role",
							Type:            "f",
							Def:             "FOREIGN KEY (role_id) REFERENCES roles(id)",
							ReferencedTable: "roles",
							Comment:         "The role of the user",
						},
					},
				},
			},
			&PostgresCommentOnConstraintInstruction{
				TableName: "users",
				Name:      "fk_role",
				Text:      "The role of the user",
			},
		})
	})
}
