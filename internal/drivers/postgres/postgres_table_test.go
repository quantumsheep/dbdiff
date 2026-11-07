package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestPostgresTable(t *testing.T) {
	t.Run("CreateTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE simple (id INT, name TEXT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "simple",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "name",
						Type: "text",
					},
				},
			},
		})
	})

	t.Run("DropTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("CreateTableWithArrayColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE tags (id INT, labels TEXT[]);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "tags",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "labels",
						Type: "text[]",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithEnumColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TYPE mood AS ENUM ('sad', 'ok');
			CREATE TABLE users (id INT, mood mood);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateEnumTypeInstruction{
				Name:   "mood",
				Labels: []string{"sad", "ok"},
			},
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "mood",
						Type: "mood",
					},
				},
			},
		})
	})

	t.Run("CreateTableWithSeveralConstraints", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE items (
				id INT PRIMARY KEY,
				code TEXT,
				price INT,
				CONSTRAINT z_price_is_positive CHECK (price > 0),
				CONSTRAINT a_code_is_unique UNIQUE (code)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "items",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "code",
						Type: "text",
					},
					{
						Name: "price",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "a_code_is_unique",
						Type: "u",
						Def:  "UNIQUE (code)",
					},
					{
						Name: "items_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
					{
						Name: "z_price_is_positive",
						Type: "c",
						Def:  "CHECK ((price > 0))",
					},
				},
			},
		})
	})

	t.Run("CreateAPartitionWithItsOwnObjects", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE measurements (id INT PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
			COMMENT ON TABLE measurements_low IS 'low range';
			CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
			CREATE TRIGGER trg BEFORE INSERT ON measurements_low FOR EACH ROW EXECUTE FUNCTION touch();
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDisableFunctionBodyCheckInstruction{},
			&PostgresCreateFunctionInstruction{
				Definition: "CREATE OR REPLACE FUNCTION touch()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function$ BEGIN RETURN NEW; END; $function$",
			},
			&PostgresCreateTableInstruction{
				Name: "measurements",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "measurements_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
				PartitionKey: "RANGE (id)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "measurements_low",
				ParentName: "measurements",
				Bound:      "FOR VALUES FROM (0) TO (100)",
			},
			&PostgresCommentOnTableInstruction{
				Name: "measurements_low",
				Text: "low range",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER trg BEFORE INSERT ON measurements_low FOR EACH ROW EXECUTE FUNCTION touch()",
			},
		})
	})

	t.Run("CreatePartitionedTableWithAForeignKey", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT, zoo_id INT REFERENCES zoo(id)) PARTITION BY RANGE (id);
			CREATE TABLE animal_low PARTITION OF animal FOR VALUES FROM (0) TO (100);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zoo",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "zoo_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "animal",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "zoo_id",
						Type: "integer",
					},
				},
				PartitionKey: "RANGE (id)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "animal_low",
				ParentName: "animal",
				Bound:      "FOR VALUES FROM (0) TO (100)",
			},
			&PostgresAlterTableInstruction{
				Name: "animal",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "animal_zoo_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (zoo_id) REFERENCES zoo(id)",
							ReferencedTable: "zoo",
						},
					},
				},
			},
		})
	})

	t.Run("CreateTablesWithAForeignKeyCycle", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT PRIMARY KEY, org_id INT);
			CREATE TABLE orgs (id INT PRIMARY KEY, owner_id INT REFERENCES users(id));
			ALTER TABLE users ADD CONSTRAINT users_org_id_fkey FOREIGN KEY (org_id) REFERENCES orgs(id);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "org_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "users_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "orgs",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "owner_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "orgs_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "users_org_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (org_id) REFERENCES orgs(id)",
							ReferencedTable: "orgs",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "orgs",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "orgs_owner_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (owner_id) REFERENCES users(id)",
							ReferencedTable: "users",
						},
					},
				},
			},
		})
	})

	t.Run("CreateTablesInForeignKeyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT REFERENCES zoo(id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zoo",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "zoo_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "animal",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
					{
						Name: "zoo_id",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "animal_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "animal",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "animal_zoo_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (zoo_id) REFERENCES zoo(id)",
							ReferencedTable: "zoo",
						},
					},
				},
			},
		})
	})

	t.Run("DropTablesInForeignKeyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE animal (id INT PRIMARY KEY);
			CREATE TABLE zoo (id INT PRIMARY KEY, animal_id INT REFERENCES animal(id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "zoo"},
			&driversshared.SQLDropTableInstruction{Name: "animal"},
		})
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT);`)
		driver.ExecOnDesired(`
			CREATE TABLE zoo (id INT PRIMARY KEY);
			CREATE TABLE animal (id INT PRIMARY KEY, zoo_id INT REFERENCES zoo(id));
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zoo",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "zoo_pkey",
						Type: "p",
						Def:  "PRIMARY KEY (id)",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "animal",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name:            "animal_zoo_id_fkey",
							Type:            "f",
							Def:             "FOREIGN KEY (zoo_id) REFERENCES zoo(id)",
							ReferencedTable: "zoo",
						},
					},
				},
			},
		})
	})

	t.Run("CreatePartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL, label TEXT)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
			CREATE TABLE events_default PARTITION OF events DEFAULT;
			CREATE INDEX events_label ON events (label);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "events",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "bigint",
					},
					{
						Name:    "created",
						Type:    "date",
						NotNull: true,
					},
					{
						Name: "label",
						Type: "text",
					},
				},
				PartitionKey: "RANGE (created)",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX events_label ON events USING btree (label)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_2024",
				ParentName: "events",
				Bound:      "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_default",
				ParentName: "events",
				Bound:      "DEFAULT",
			},
		})
	})

	t.Run("DropPartitionedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "events"},
		})
	})

	t.Run("AddPartitionToExistingTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		driver.ExecOnDesired(`
			CREATE TABLE events (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE events_2024 PARTITION OF events
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
			CREATE TABLE events_2025 PARTITION OF events
				FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTablePartitionInstruction{
				Name:       "events_2025",
				ParentName: "events",
				Bound:      "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')",
			},
		})
	})

	t.Run("CreateTableWithComments", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			COMMENT ON TABLE users IS 'the people';
			COMMENT ON COLUMN users.id IS 'the key';
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "users",
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						Comment: "the key",
					},
				},
			},
			&PostgresCommentOnTableInstruction{
				Name: "users",
				Text: "the people",
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "users",
				ColumnName: "id",
				Text:       "the key",
			},
		})
	})

	t.Run("ModifyComments", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT, name TEXT);
			COMMENT ON TABLE users IS 'the old comment';
			COMMENT ON COLUMN users.id IS 'the old key';
			COMMENT ON COLUMN users.name IS 'this one goes away';
		`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT, name TEXT);
			COMMENT ON TABLE users IS 'the new comment';
			COMMENT ON COLUMN users.id IS 'the new key';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnTableInstruction{
				Name: "users",
				Text: "the new comment",
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "users",
				ColumnName: "id",
				Text:       "the new key",
			},
			&PostgresCommentOnColumnInstruction{
				TableName:  "users",
				ColumnName: "name",
			},
		})
	})

	t.Run("CreateTableWithRowLevelSecurity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE docs (id INT, secret BOOLEAN);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
			ALTER TABLE docs FORCE ROW LEVEL SECURITY;
			CREATE POLICY docs_read ON docs FOR SELECT USING (NOT secret);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "docs",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
					{
						Name: "secret",
						Type: "boolean",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "ENABLE"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "FORCE"},
				},
			},
			&PostgresCreatePolicyInstruction{
				Name:       "docs_read",
				TableName:  "docs",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(NOT secret)",
			},
		})
	})

	t.Run("DisableRowLevelSecurity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE docs (id INT);
			ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
		`)
		driver.ExecOnDesired(`CREATE TABLE docs (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "docs",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "DISABLE"},
				},
			},
		})
	})

	t.Run("CreatePartitionThatSortsBeforeItsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE zebra (id BIGINT, created DATE NOT NULL)
				PARTITION BY RANGE (created);
			CREATE TABLE alpha PARTITION OF zebra
				FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zebra",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "bigint",
					},
					{
						Name:    "created",
						Type:    "date",
						NotNull: true,
					},
				},
				PartitionKey: "RANGE (created)",
			},
			&PostgresCreateTablePartitionInstruction{
				Name:       "alpha",
				ParentName: "zebra",
				Bound:      "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
			},
		})
	})

	t.Run("CreateTableThatInheritsAnotherTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE zparent (a INT);
			CREATE TABLE achild (b INT) INHERITS (zparent);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zparent",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name: "achild",
				Columns: []*PostgresColumn{
					{
						Name: "b",
						Type: "integer",
					},
				},
				Inherits: []string{"zparent"},
			},
		})
	})

	t.Run("CreateTableThatInheritsAColumnAndACheck", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE zparent (a INT CONSTRAINT a_positive CHECK (a > 0));
			CREATE TABLE achild () INHERITS (zparent);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "zparent",
				Columns: []*PostgresColumn{
					{
						Name: "a",
						Type: "integer",
					},
				},
				Constraints: []*PostgresConstraint{
					{
						Name: "a_positive",
						Type: "c",
						Def:  "CHECK ((a > 0))",
					},
				},
			},
			&PostgresCreateTableInstruction{
				Name:     "achild",
				Inherits: []string{"zparent"},
			},
		})
	})

	t.Run("AddAColumnAndACheckToAnInheritsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (a INT);
			CREATE TABLE children (x INT) INHERITS (parents);
			INSERT INTO children (a, x) VALUES (1, 2);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (a INT, b INT CONSTRAINT b_positive CHECK (b > 0));
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "b",
							Type: "integer",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "b_positive",
							Type: "c",
							Def:  "CHECK ((b > 0))",
						},
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("children", "")
		require.Equal(t, []map[string]any{
			{
				"a": int64(1),
				"b": nil,
				"x": int64(2),
			},
		}, rows)
	})

	t.Run("ChildSetsItsOwnDefaultOnAnInheritedColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE parents (a INT DEFAULT 0);
			CREATE TABLE children (x INT) INHERITS (parents);
		`
		driver.ExecOnCurrent(schema)
		driver.ExecOnDesired(schema + `ALTER TABLE children ALTER COLUMN a SET DEFAULT 5;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "a",
						Expression: "5",
					},
				},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO children (x) VALUES (1);`)

		rows := driver.FetchAllFromCurrent("children", "")
		require.Equal(t, []map[string]any{
			{
				"a": int64(5),
				"x": int64(1),
			},
		}, rows)
	})

	t.Run("ParentChangesADefaultThatTheChildOverrides", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (a INT DEFAULT 0);
			CREATE TABLE children (x INT) INHERITS (parents);
			ALTER TABLE children ALTER COLUMN a SET DEFAULT 5;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (a INT DEFAULT 1);
			CREATE TABLE children (x INT) INHERITS (parents);
			ALTER TABLE children ALTER COLUMN a SET DEFAULT 5;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "parents",
				Only: true,
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "a",
						Expression: "1",
					},
				},
			},
		})
	})

	t.Run("ParentAddsAColumnThatTheChildOverrides", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (a INT);
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (a INT, b INT DEFAULT 1);
			CREATE TABLE children (x INT) INHERITS (parents);
			ALTER TABLE children ALTER COLUMN b SET DEFAULT 7;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "b",
							Type: "integer",
							Default: sql.NullString{
								String: "1",
								Valid:  true,
							},
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "b",
						Expression: "7",
					},
				},
			},
		})
	})

	t.Run("ChildStopsInheritingAndLosesTheInheritedObjects", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (a INT CONSTRAINT a_positive CHECK (a > 0));
			CREATE TABLE children (x INT) INHERITS (parents);
			INSERT INTO children (a, x) VALUES (1, 2);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (a INT CONSTRAINT a_positive CHECK (a > 0));
			CREATE TABLE children (x INT);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresNoInheritAction{
						ParentName: "parents",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "a_positive",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{
						ColumnName: "a",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("children", "")
		require.Equal(t, []map[string]any{
			{
				"x": int64(2),
			},
		}, rows)
	})

	t.Run("CreateAChildWithItsOwnAttributesOnInheritedColumns", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		parent := `CREATE TABLE parents (id INT, a INT DEFAULT 1 CONSTRAINT a_positive CHECK (a > 0));`
		driver.ExecOnCurrent(parent)
		driver.ExecOnDesired(parent + `
			CREATE TABLE children (x INT) INHERITS (parents);
			ALTER TABLE children ALTER COLUMN a SET DEFAULT 9;
			ALTER TABLE children ALTER COLUMN id SET NOT NULL;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "children",
				Columns: []*PostgresColumn{
					{
						Name: "x",
						Type: "integer",
					},
				},
				Inherits: []string{"parents"},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{
						ColumnName: "id",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetDefaultAction{
						ColumnName: "a",
						Expression: "9",
					},
				},
			},
		})

		driver.ExecOnCurrent(`INSERT INTO children (id, x) VALUES (1, 2);`)

		rows := driver.FetchAllFromCurrent("children", "")
		require.Equal(t, []map[string]any{
			{
				"id": int64(1),
				"a":  int64(9),
				"x":  int64(2),
			},
		}, rows)
	})

	t.Run("ChildStartsToInheritAParentThatAddsAColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (id INT NOT NULL CONSTRAINT id_positive CHECK (id > 0));
			CREATE TABLE children (id INT, x INT);
			INSERT INTO children (id, x) VALUES (1, 2);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (id INT NOT NULL CONSTRAINT id_positive CHECK (id > 0), a INT);
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "a",
							Type: "integer",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{
						ColumnName: "id",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddColumnAction{
						Column: &PostgresColumn{
							Name: "a",
							Type: "integer",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresAddConstraintAction{
						Constraint: &PostgresConstraint{
							Name: "id_positive",
							Type: "c",
							Def:  "CHECK ((id > 0))",
						},
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresInheritAction{
						ParentName: "parents",
					},
				},
			},
		})

		rows := driver.FetchAllFromCurrent("children", "")
		require.Equal(t, []map[string]any{
			{
				"id": int64(1),
				"x":  int64(2),
				"a":  nil,
			},
		}, rows)
	})

	t.Run("ParentSetsNotNullOnAColumnOfItsChildren", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (a INT);
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (a INT NOT NULL);
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetNotNullAction{
						ColumnName: "a",
					},
				},
			},
		})
	})

	t.Run("DropAColumnAndACheckOfAnInheritsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE parents (a INT, b INT CONSTRAINT b_positive CHECK (b > 0));
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.ExecOnDesired(`
			CREATE TABLE parents (a INT);
			CREATE TABLE children (x INT) INHERITS (parents);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&PostgresDropConstraintAction{
						ConstraintName: "b_positive",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "parents",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{
						ColumnName: "b",
					},
				},
			},
		})
	})

	t.Run("CreateUnloggedTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE UNLOGGED TABLE cache (id INT);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "cache",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
				Unlogged: true,
			},
		})
	})

	t.Run("AlterTablePersistence", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE cache (id INT);`)
		driver.ExecOnDesired(`CREATE UNLOGGED TABLE cache (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "cache",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetPersistenceAction{Persistence: "UNLOGGED"},
				},
			},
		})
	})

	t.Run("CreateTableWithAReplicaIdentity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE events (id INT);
			ALTER TABLE events REPLICA IDENTITY FULL;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "events",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "FULL"},
				},
			},
		})
	})

	t.Run("AlterReplicaIdentity", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE events (id INT);`)
		driver.ExecOnDesired(`
			CREATE TABLE events (id INT);
			ALTER TABLE events REPLICA IDENTITY NOTHING;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "NOTHING"},
				},
			},
		})
	})

	t.Run("ReplicaIdentityUsingAnIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);`)
		driver.ExecOnDesired(`
			CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);
			CREATE UNIQUE INDEX events_code_key ON events (code);
			ALTER TABLE events REPLICA IDENTITY USING INDEX events_code_key;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateIndexInstruction{
				Definition: "CREATE UNIQUE INDEX events_code_key ON events USING btree (code)",
			},
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{
						Mode:      "USING INDEX",
						IndexName: "events_code_key",
					},
				},
			},
		})
	})

	t.Run("ReplicaIdentityBeforeAnIndexRemoval", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);
			CREATE UNIQUE INDEX events_code_key ON events (code);
			ALTER TABLE events REPLICA IDENTITY USING INDEX events_code_key;
		`)
		driver.ExecOnDesired(`CREATE TABLE events (id INT NOT NULL, code TEXT NOT NULL);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "events",
				Actions: []driversshared.AlterTableAction{
					&PostgresReplicaIdentityAction{Mode: "DEFAULT"},
				},
			},
			&driversshared.SQLDropIndexInstruction{Name: "events_code_key"},
		})
	})

	t.Run("CreateTableWithStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: "tuned",
				Columns: []*PostgresColumn{
					{
						Name: "id",
						Type: "integer",
					},
				},
				StorageParameters: []string{"fillfactor=70"},
			},
		})
	})

	t.Run("AlterStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)
		driver.ExecOnDesired(`CREATE TABLE tuned (id INT) WITH (fillfactor = 90);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "tuned",
				Actions: []driversshared.AlterTableAction{
					&PostgresSetStorageParametersAction{
						Parameters: []string{"fillfactor=90"},
					},
				},
			},
		})
	})

	t.Run("ResetStorageParameters", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE tuned (id INT) WITH (fillfactor = 70);`)
		driver.ExecOnDesired(`CREATE TABLE tuned (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "tuned",
				Actions: []driversshared.AlterTableAction{
					&PostgresResetStorageParametersAction{
						Names: []string{"fillfactor"},
					},
				},
			},
		})
	})

	t.Run("AddAndRemoveAnInheritsParent", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE old_parents (note text);
			CREATE TABLE new_parents (note text);
			CREATE TABLE children (id integer) INHERITS (old_parents);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE old_parents (note text);
			CREATE TABLE new_parents (note text);
			CREATE TABLE children (id integer) INHERITS (new_parents);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresInheritAction{ParentName: "new_parents"},
				},
			},
			&PostgresAlterTableInstruction{
				Name: "children",
				Actions: []driversshared.AlterTableAction{
					&PostgresNoInheritAction{ParentName: "old_parents"},
				},
			},
		})
	})

	t.Run("DropTableBeforeType", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TYPE mood AS ENUM ('sad');
			CREATE TABLE events (id INT, feeling mood);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropTableInstruction{Name: "events"},
			&PostgresDropTypeInstruction{Name: "mood"},
		})
	})

	t.Run("ChangeAPartitionBound", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (200);
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresDetachPartitionInstruction{
				ParentName:    "measurements",
				PartitionName: "measurements_low",
			},
			&PostgresAttachPartitionInstruction{
				ParentName:    "measurements",
				PartitionName: "measurements_low",
				Bound:         "FOR VALUES FROM (0) TO (200)",
			},
		})
	})

	t.Run("ModifyTheOwnObjectsOfAPartition", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE measurements (id integer PRIMARY KEY, value integer) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
			CREATE FUNCTION touch() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
		`

		driver.ExecOnCurrent(schema + `
			CREATE INDEX measurements_low_old ON measurements_low (id, value);
		`)

		driver.ExecOnDesired(schema + `
			CREATE INDEX measurements_value ON measurements (value);
			CREATE TRIGGER parent_trigger BEFORE INSERT ON measurements
				FOR EACH ROW EXECUTE FUNCTION touch();
			COMMENT ON TABLE measurements_low IS 'low range';
			ALTER TABLE measurements_low ENABLE ROW LEVEL SECURITY;
			CREATE POLICY low_read ON measurements_low FOR SELECT USING (value > 0);
			CREATE INDEX measurements_low_id ON measurements_low (id);
			CREATE TRIGGER low_trigger BEFORE INSERT ON measurements_low
				FOR EACH ROW EXECUTE FUNCTION touch();
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "measurements_low_old"},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measurements_value ON measurements USING btree (value)",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER parent_trigger BEFORE INSERT ON measurements FOR EACH ROW EXECUTE FUNCTION touch()",
			},
			&PostgresAlterTableInstruction{
				Name: "measurements_low",
				Actions: []driversshared.AlterTableAction{
					&PostgresRowLevelSecurityAction{Mode: "ENABLE"},
				},
			},
			&PostgresCommentOnTableInstruction{
				Name: "measurements_low",
				Text: "low range",
			},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measurements_low_id ON measurements_low USING btree (id)",
			},
			&PostgresCreateTriggerInstruction{
				Definition: "CREATE TRIGGER low_trigger BEFORE INSERT ON measurements_low FOR EACH ROW EXECUTE FUNCTION touch()",
			},
			&PostgresCreatePolicyInstruction{
				Name:       "low_read",
				TableName:  "measurements_low",
				Permissive: "PERMISSIVE",
				Command:    "SELECT",
				Roles:      []string{"public"},
				Using:      "(value > 0)",
			},
		})
	})

	t.Run("ReplaceAPartitionIndexWithAParentIndex", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE measurements (id integer PRIMARY KEY, value integer) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`

		driver.ExecOnCurrent(schema + `
			CREATE INDEX measurements_low_value ON measurements_low (value);
		`)

		driver.ExecOnDesired(schema + `
			CREATE INDEX measurements_value ON measurements (value);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropIndexInstruction{Name: "measurements_low_value"},
			&PostgresCreateIndexInstruction{
				Definition: "CREATE INDEX measurements_value ON measurements USING btree (value)",
			},
		})
	})

	t.Run("ChangeATableToAPartition", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low (id integer PRIMARY KEY);
		`)

		driver.ExecOnDesired(`
			CREATE TABLE measurements (id integer PRIMARY KEY) PARTITION BY RANGE (id);
			CREATE TABLE measurements_low PARTITION OF measurements FOR VALUES FROM (0) TO (100);
		`)

		_, err := driver.Diff(t.Context(), driver.current, driver.desired, driversshared.DiffOptions{})
		require.EqualError(t, err,
			`the table "measurements_low" changes from a table to a partition, and dbdiff cannot diff this change`)
	})

	t.Run("TableNameThatNeedsQuotes", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnDesired(`CREATE TABLE "order ""list""" (id INT NOT NULL);`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateTableInstruction{
				Name: `order "list"`,
				Columns: []*PostgresColumn{
					{
						Name:    "id",
						Type:    "integer",
						NotNull: true,
					},
				},
			},
		})
	})
}
