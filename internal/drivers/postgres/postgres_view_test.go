package driverspostgres

import (
	"fmt"
	"strings"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestPostgresView(t *testing.T) {
	t.Run("CreateViewOfAMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE VIEW user_identifiers AS SELECT id FROM all_users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateMaterializedViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateViewInstruction{
				Name:  "user_identifiers",
				Query: " SELECT id\n   FROM all_users;",
			},
		})
	})

	t.Run("DropViewOfAMaterializedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE MATERIALIZED VIEW all_users AS SELECT id FROM users;
			CREATE VIEW user_identifiers AS SELECT id FROM all_users;
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_identifiers"},
			&PostgresDropMaterializedViewInstruction{Name: "all_users"},
		})
	})

	t.Run("ModifyViewComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		schema := `
			CREATE TABLE users (id INT);
			CREATE VIEW all_users AS SELECT id FROM users;
		`
		driver.ExecOnCurrent(schema)
		driver.ExecOnCurrent(`COMMENT ON VIEW all_users IS 'the old comment';`)

		driver.ExecOnDesired(schema)
		driver.ExecOnDesired(`COMMENT ON VIEW all_users IS 'the new comment';`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCommentOnViewInstruction{
				Name: "all_users",
				Text: "the new comment",
			},
		})
	})

	t.Run("CreateViewWithComment", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE VIEW all_users AS SELECT id FROM users;
			COMMENT ON VIEW all_users IS 'every person';
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "all_users",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCommentOnViewInstruction{
				Name: "all_users",
				Text: "every person",
			},
		})
	})

	t.Run("CreateViewWithACheckOption", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE base (a INT);`)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0 WITH CASCADED CHECK OPTION;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
				CheckOption: "CASCADED",
			},
		})
	})

	t.Run("ModifyViewCheckOption", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0;
		`)

		driver.ExecOnDesired(`
			CREATE TABLE base (a INT);
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0 WITH LOCAL CHECK OPTION;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "positive"},
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
				CheckOption: "LOCAL",
			},
		})
	})

	t.Run("ModifyAViewOfAnotherOwner", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		// information_schema.views gives no definition of a view that the connecting role
		// does not own, so the diff connects as a role that owns no view.
		driver.ExecOnCurrent(`
			DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbdiff_view_owner') THEN
					CREATE ROLE dbdiff_view_owner;
				END IF;
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbdiff_view_reader') THEN
					CREATE ROLE dbdiff_view_reader LOGIN PASSWORD 'password';
				END IF;
			END
			$$;
		`)

		for _, schema := range []string{driver.currentSchema, driver.desiredSchema} {
			_, err := driver.conn.ExecContext(t.Context(), fmt.Sprintf(`
				GRANT USAGE ON SCHEMA %[1]s TO dbdiff_view_reader;
				CREATE TABLE %[1]s.base (a integer);
				GRANT SELECT ON ALL TABLES IN SCHEMA %[1]s TO dbdiff_view_reader;
			`, schema))
			require.NoError(t, err)
		}

		driver.ExecOnCurrent(`
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 0;
			ALTER VIEW positive OWNER TO dbdiff_view_owner;
			GRANT SELECT ON positive TO dbdiff_view_reader;
		`)

		driver.ExecOnDesired(`
			CREATE VIEW positive AS SELECT a FROM base WHERE a > 1 WITH LOCAL CHECK OPTION;
			ALTER VIEW positive OWNER TO dbdiff_view_owner;
			GRANT SELECT ON positive TO dbdiff_view_reader;
		`)

		readerConnectionString := strings.Replace(postgresTestConnectionString,
			"user:password", "dbdiff_view_reader:password", 1)
		driver.current = driversshared.ConnectionStringDataSource{
			ConnectionString: fmt.Sprintf("%s&search_path=%s", readerConnectionString, driver.currentSchema),
		}
		driver.desired = driversshared.ConnectionStringDataSource{
			ConnectionString: fmt.Sprintf("%s&search_path=%s", readerConnectionString, driver.desiredSchema),
		}

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "positive"},
			&PostgresCreateViewInstruction{
				Name:        "positive",
				Query:       " SELECT a\n   FROM base\n  WHERE (a > 1);",
				CheckOption: "LOCAL",
			},
		})
	})

	t.Run("Views", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT); CREATE VIEW user_ids AS SELECT id FROM users;`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "user_ids",
				Query: " SELECT id\n   FROM users;",
			},
		})
	})

	t.Run("DropViewBeforeTable", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE VIEW user_ids AS SELECT id FROM users;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_ids"},
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("DropViewBeforeColumn", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.ExecOnDesired(`CREATE TABLE users (id INT);`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&driversshared.SQLDropColumnAction{ColumnName: "label"},
				},
			},
		})
	})

	t.Run("RecreateViewOnColumnTypeChange", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT, label TEXT);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT, label VARCHAR);
			CREATE VIEW user_labels AS SELECT label FROM users;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "user_labels"},
			&PostgresAlterTableInstruction{
				Name: "users",
				Actions: []driversshared.AlterTableAction{
					&PostgresAlterColumnTypeAction{
						ColumnName: "label",
						DataType:   "character varying",
					},
				},
			},
			&PostgresCreateViewInstruction{
				Name:  "user_labels",
				Query: " SELECT label\n   FROM users;",
			},
		})
	})

	t.Run("CreateViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`CREATE TABLE users (id INT);`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
		})
	})

	t.Run("DropViewsInDependencyOrder", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&driversshared.SQLDropTableInstruction{Name: "users"},
		})
	})

	t.Run("RecreateTheViewsThatReadARecreatedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users WHERE id > 0;
			CREATE VIEW view_a AS SELECT id FROM view_b;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users\n  WHERE (id > 0);",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
		})
	})

	t.Run("RecreateAViewThatCountsTheRowsOfARecreatedView", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE VIEW view_a AS SELECT count(*) AS total FROM view_b;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users WHERE id > 0;
			CREATE VIEW view_a AS SELECT count(*) AS total FROM view_b;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users\n  WHERE (id > 0);",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_a",
				Query: " SELECT count(*) AS total\n   FROM view_b;",
			},
		})
	})

	t.Run("RecreateAChainOfViewsAndMaterializedViews", func(t *testing.T) {
		driver := NewTestPostgresDriver(t)

		driver.ExecOnCurrent(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users;
			CREATE MATERIALIZED VIEW view_a AS SELECT id FROM view_b;
			CREATE VIEW view_c AS SELECT id FROM view_a;
		`)
		driver.ExecOnDesired(`
			CREATE TABLE users (id INT);
			CREATE VIEW view_b AS SELECT id FROM users WHERE id > 0;
			CREATE MATERIALIZED VIEW view_a AS SELECT id FROM view_b;
			CREATE VIEW view_c AS SELECT id FROM view_a;
		`)
		driver.RequireDiffApplies([]driversshared.Instruction{
			&driversshared.SQLDropViewInstruction{Name: "view_c"},
			&PostgresDropMaterializedViewInstruction{Name: "view_a"},
			&driversshared.SQLDropViewInstruction{Name: "view_b"},
			&PostgresCreateViewInstruction{
				Name:  "view_b",
				Query: " SELECT id\n   FROM users\n  WHERE (id > 0);",
			},
			&PostgresCreateMaterializedViewInstruction{
				Name:  "view_a",
				Query: " SELECT id\n   FROM view_b;",
			},
			&PostgresCreateViewInstruction{
				Name:  "view_c",
				Query: " SELECT id\n   FROM view_a;",
			},
		})
	})
}
