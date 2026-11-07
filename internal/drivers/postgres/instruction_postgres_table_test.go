package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func commentInstruction(text string) driversshared.Instruction {
	return &driversshared.SQLCommentInstruction{
		Text: text,
	}
}

func TestPostgresTableInstructions(t *testing.T) {
	t.Run("PostgresInsertOverriding", func(t *testing.T) {
		instruction := &PostgresInsertOverridingInstruction{
			TableName:   "users",
			ColumnNames: []string{"id", "name"},
			Expressions: []string{"1", "'Alice'"},
		}

		require.Equal(t,
			`INSERT INTO "users" ("id", "name") OVERRIDING SYSTEM VALUE VALUES (1, 'Alice');`,
			instruction.String())
	})

	t.Run("PostgresAlterTableAddColumn", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []driversshared.AlterTableAction{
				&PostgresAddColumnAction{
					Column: &PostgresColumn{
						Name: "email",
						Type: "text",
					},
				},
			},
		}

		require.Equal(t, `ALTER TABLE "users" ADD COLUMN "email" text;`, instruction.String())
	})

	t.Run("PostgresAlterTableTwoActions", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []driversshared.AlterTableAction{
				&PostgresDropConstraintAction{ConstraintName: "users_pkey"},
				&PostgresSetNotNullAction{ColumnName: "id"},
			},
		}

		require.Equal(t,
			`ALTER TABLE "users" DROP CONSTRAINT "users_pkey", ALTER COLUMN "id" SET NOT NULL;`,
			instruction.String())
	})

	t.Run("PostgresAlterColumnTypeAction", func(t *testing.T) {
		action := &PostgresAlterColumnTypeAction{
			ColumnName: "age",
			DataType:   "integer",
		}

		require.Equal(t, `ALTER COLUMN "age" TYPE integer`, action.TableActionClause())
	})

	t.Run("PostgresAlterColumnTypeActionWithACast", func(t *testing.T) {
		action := &PostgresAlterColumnTypeAction{
			ColumnName: "age",
			DataType:   "integer",
			UsingCast:  true,
		}

		require.Equal(t,
			`ALTER COLUMN "age" TYPE integer USING "age"::integer`,
			action.TableActionClause())
	})

	t.Run("PostgresSetNotNullAction", func(t *testing.T) {
		action := &PostgresSetNotNullAction{ColumnName: "name"}

		require.Equal(t, `ALTER COLUMN "name" SET NOT NULL`, action.TableActionClause())
	})

	t.Run("PostgresDropNotNullAction", func(t *testing.T) {
		action := &PostgresDropNotNullAction{ColumnName: "name"}

		require.Equal(t, `ALTER COLUMN "name" DROP NOT NULL`, action.TableActionClause())
	})

	t.Run("PostgresSetDefaultAction", func(t *testing.T) {
		action := &PostgresSetDefaultAction{
			ColumnName: "age",
			Expression: "0",
		}

		require.Equal(t, `ALTER COLUMN "age" SET DEFAULT 0`, action.TableActionClause())
	})

	t.Run("PostgresDropDefaultAction", func(t *testing.T) {
		action := &PostgresDropDefaultAction{ColumnName: "age"}

		require.Equal(t, `ALTER COLUMN "age" DROP DEFAULT`, action.TableActionClause())
	})

	t.Run("PostgresCreateTablePartitionedByRange", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "events",
			Columns: []*PostgresColumn{
				{
					Name: "created",
					Type: "date",
				},
			},
			PartitionKey: "RANGE (created)",
		}

		require.Equal(t, `CREATE TABLE "events" (
	"created" date
) PARTITION BY RANGE (created);`, instruction.String())
	})

	t.Run("PostgresCreateTablePartition", func(t *testing.T) {
		instruction := &PostgresCreateTablePartitionInstruction{
			Name:       "events_2024",
			ParentName: "events",
			Bound:      "FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
		}

		require.Equal(t,
			`CREATE TABLE "events_2024" PARTITION OF "events" FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');`,
			instruction.String())
	})

	t.Run("PostgresCreateTableDefaultPartition", func(t *testing.T) {
		instruction := &PostgresCreateTablePartitionInstruction{
			Name:       "events_default",
			ParentName: "events",
			Bound:      "DEFAULT",
		}

		require.Equal(t,
			`CREATE TABLE "events_default" PARTITION OF "events" DEFAULT;`,
			instruction.String())
	})

	t.Run("PostgresCreateMaterializedView", func(t *testing.T) {
		instruction := &PostgresCreateMaterializedViewInstruction{
			Name:  "active_users",
			Query: " SELECT id\n   FROM users;",
		}

		require.Equal(t, `CREATE MATERIALIZED VIEW "active_users" AS  SELECT id
   FROM users;`, instruction.String())
	})

	t.Run("PostgresDropMaterializedView", func(t *testing.T) {
		instruction := &PostgresDropMaterializedViewInstruction{Name: "active_users"}

		require.Equal(t, `DROP MATERIALIZED VIEW "active_users";`, instruction.String())
	})

	t.Run("PostgresCreateTableWithACollation", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "people",
			Columns: []*PostgresColumn{
				{
					Name:      "name",
					Type:      "text",
					Collation: "C",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "people" (
	"name" text COLLATE "C"
);`, instruction.String())
	})

	t.Run("PostgresCreateTableWithASerialColumn", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
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
					Serial: "serial",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" serial NOT NULL
);`, instruction.String())
	})

	t.Run("PostgresAlterColumnTypeActionWithACollation", func(t *testing.T) {
		action := &PostgresAlterColumnTypeAction{
			ColumnName: "name",
			DataType:   "text",
			Collation:  "C",
		}

		require.Equal(t,
			`ALTER COLUMN "name" TYPE text COLLATE "C"`,
			action.TableActionClause())
	})

	t.Run("PostgresCommentOnTable", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{
			Name: "users",
			Text: "the people of the site",
		}

		require.Equal(t,
			`COMMENT ON TABLE "users" IS 'the people of the site';`,
			instruction.String())
	})

	t.Run("PostgresCommentOnTableThatRemovesTheComment", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{Name: "users"}

		require.Equal(t, `COMMENT ON TABLE "users" IS NULL;`, instruction.String())
	})

	t.Run("PostgresCommentOnColumn", func(t *testing.T) {
		instruction := &PostgresCommentOnColumnInstruction{
			TableName:  "users",
			ColumnName: "id",
			Text:       "the key",
		}

		require.Equal(t,
			`COMMENT ON COLUMN "users"."id" IS 'the key';`,
			instruction.String())
	})

	t.Run("PostgresCommentOnView", func(t *testing.T) {
		instruction := &PostgresCommentOnViewInstruction{
			Name: "active_users",
			Text: "the people that signed in",
		}

		require.Equal(t,
			`COMMENT ON VIEW "active_users" IS 'the people that signed in';`,
			instruction.String())
	})

	t.Run("PostgresCommentOnMaterializedView", func(t *testing.T) {
		instruction := &PostgresCommentOnMaterializedViewInstruction{
			Name: "daily_totals",
			Text: "the totals of one day",
		}

		require.Equal(t,
			`COMMENT ON MATERIALIZED VIEW "daily_totals" IS 'the totals of one day';`,
			instruction.String())
	})

	t.Run("PostgresCommentOnIndex", func(t *testing.T) {
		instruction := &PostgresCommentOnIndexInstruction{
			Name: "users_email",
			Text: "the lookup key of the sign in",
		}

		require.Equal(t,
			`COMMENT ON INDEX "users_email" IS 'the lookup key of the sign in';`,
			instruction.String())
	})

	t.Run("PostgresCommentWithAQuote", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{
			Name: "users",
			Text: "the site's people",
		}

		require.Equal(t,
			`COMMENT ON TABLE "users" IS 'the site''s people';`,
			instruction.String())
	})

	t.Run("PostgresEnableRowLevelSecurityAction", func(t *testing.T) {
		action := &PostgresRowLevelSecurityAction{Mode: "ENABLE"}

		require.Equal(t, `ENABLE ROW LEVEL SECURITY`, action.TableActionClause())
	})

	t.Run("PostgresNoForceRowLevelSecurityAction", func(t *testing.T) {
		action := &PostgresRowLevelSecurityAction{Mode: "NO FORCE"}

		require.Equal(t, `NO FORCE ROW LEVEL SECURITY`, action.TableActionClause())
	})

	t.Run("PostgresCreatePolicy", func(t *testing.T) {
		instruction := &PostgresCreatePolicyInstruction{
			Name:       "docs_read",
			TableName:  "docs",
			Permissive: "PERMISSIVE",
			Command:    "SELECT",
			Roles:      []string{"public"},
			Using:      "(NOT secret)",
		}

		require.Equal(t,
			`CREATE POLICY "docs_read" ON "docs" AS PERMISSIVE FOR SELECT TO public USING (NOT secret);`,
			instruction.String())
	})

	t.Run("PostgresCreatePolicyWithACheck", func(t *testing.T) {
		instruction := &PostgresCreatePolicyInstruction{
			Name:       "docs_write",
			TableName:  "docs",
			Permissive: "PERMISSIVE",
			Command:    "INSERT",
			Roles:      []string{"public"},
			WithCheck:  "(owner = CURRENT_USER)",
		}

		require.Equal(t,
			`CREATE POLICY "docs_write" ON "docs" AS PERMISSIVE FOR INSERT TO public WITH CHECK (owner = CURRENT_USER);`,
			instruction.String())
	})

	t.Run("PostgresDropPolicy", func(t *testing.T) {
		instruction := &PostgresDropPolicyInstruction{
			Name:      "docs_read",
			TableName: "docs",
		}

		require.Equal(t, `DROP POLICY "docs_read" ON "docs";`, instruction.String())
	})

	t.Run("PostgresCreateTableThatInherits", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "child",
			Columns: []*PostgresColumn{
				{
					Name: "a",
					Type: "integer",
				},
			},
			Inherits: []string{"parent"},
		}

		require.Equal(t, `CREATE TABLE "child" (
	"a" integer
) INHERITS ("parent");`, instruction.String())
	})

	t.Run("PostgresDetachPartition", func(t *testing.T) {
		instruction := &PostgresDetachPartitionInstruction{
			ParentName:    "measurements",
			PartitionName: "measurements_low",
		}

		require.Equal(t,
			`ALTER TABLE "measurements" DETACH PARTITION "measurements_low";`,
			instruction.String())
	})

	t.Run("PostgresAttachPartition", func(t *testing.T) {
		instruction := &PostgresAttachPartitionInstruction{
			ParentName:    "measurements",
			PartitionName: "measurements_low",
			Bound:         "FOR VALUES FROM (0) TO (200)",
		}

		require.Equal(t,
			`ALTER TABLE "measurements" ATTACH PARTITION "measurements_low" FOR VALUES FROM (0) TO (200);`,
			instruction.String())
	})

	t.Run("PostgresAlterTableInherit", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "children",
			Actions: []driversshared.AlterTableAction{
				&PostgresInheritAction{ParentName: "parents"},
			},
		}

		require.Equal(t, `ALTER TABLE "children" INHERIT "parents";`, instruction.String())
	})

	t.Run("PostgresAlterTableNoInherit", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "children",
			Actions: []driversshared.AlterTableAction{
				&PostgresNoInheritAction{ParentName: "parents"},
			},
		}

		require.Equal(t, `ALTER TABLE "children" NO INHERIT "parents";`, instruction.String())
	})

	t.Run("PostgresAlterTableSetIdentityOptions", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []driversshared.AlterTableAction{
				&PostgresSetIdentityOptionsAction{
					ColumnName: "id",
					Options:    "START WITH 100 INCREMENT BY 5 NO CYCLE",
				},
			},
		}

		require.Equal(t,
			`ALTER TABLE "users" ALTER COLUMN "id" SET START WITH 100 SET INCREMENT BY 5 SET NO CYCLE;`,
			instruction.String())
	})

	t.Run("PostgresCreateTableWithIdentityOptions", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
			Columns: []*PostgresColumn{
				{
					Name:            "id",
					Type:            "integer",
					NotNull:         true,
					Identity:        "ALWAYS",
					IdentityOptions: "START WITH 100 INCREMENT BY 5",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 5)
);`, instruction.String())
	})

	t.Run("PostgresCreateUnloggedTable", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "cache",
			Columns: []*PostgresColumn{
				{
					Name: "id",
					Type: "integer",
				},
			},
			Unlogged: true,
		}

		require.Equal(t, `CREATE UNLOGGED TABLE "cache" (
	"id" integer
);`, instruction.String())
	})

	t.Run("PostgresSetLoggedAction", func(t *testing.T) {
		action := &PostgresSetPersistenceAction{Persistence: "LOGGED"}

		require.Equal(t, `SET LOGGED`, action.TableActionClause())
	})

	t.Run("PostgresSetStorageAction", func(t *testing.T) {
		action := &PostgresSetStorageAction{
			ColumnName: "body",
			Storage:    "MAIN",
		}

		require.Equal(t, `ALTER COLUMN "body" SET STORAGE MAIN`, action.TableActionClause())
	})

	t.Run("PostgresSetStorageDefaultAction", func(t *testing.T) {
		action := &PostgresSetStorageAction{
			ColumnName: "body",
			Storage:    "DEFAULT",
		}

		require.Equal(t, `ALTER COLUMN "body" SET STORAGE DEFAULT`, action.TableActionClause())
	})

	t.Run("PostgresSetStatisticsAction", func(t *testing.T) {
		action := &PostgresSetStatisticsAction{
			ColumnName:       "body",
			StatisticsTarget: 500,
		}

		require.Equal(t, `ALTER COLUMN "body" SET STATISTICS 500`, action.TableActionClause())
	})

	t.Run("PostgresResetStatisticsAction", func(t *testing.T) {
		action := &PostgresSetStatisticsAction{
			ColumnName:       "body",
			StatisticsTarget: -1,
		}

		require.Equal(t, `ALTER COLUMN "body" SET STATISTICS -1`, action.TableActionClause())
	})

	t.Run("PostgresTriggerEnableAction", func(t *testing.T) {
		action := &PostgresTriggerEnableAction{
			Mode:        "ENABLE ALWAYS",
			TriggerName: "set_timestamp",
		}

		require.Equal(t, `ENABLE ALWAYS TRIGGER "set_timestamp"`, action.TableActionClause())
	})

	t.Run("PostgresTriggerDisableAction", func(t *testing.T) {
		action := &PostgresTriggerEnableAction{
			Mode:        "DISABLE",
			TriggerName: "set_timestamp",
		}

		require.Equal(t, `DISABLE TRIGGER "set_timestamp"`, action.TableActionClause())
	})

	t.Run("PostgresReplicaIdentityAction", func(t *testing.T) {
		action := &PostgresReplicaIdentityAction{Mode: "FULL"}

		require.Equal(t, `REPLICA IDENTITY FULL`, action.TableActionClause())
	})

	t.Run("PostgresReplicaIdentityUsingIndexAction", func(t *testing.T) {
		action := &PostgresReplicaIdentityAction{
			Mode:      "USING INDEX",
			IndexName: "users_email_key",
		}

		require.Equal(t, `REPLICA IDENTITY USING INDEX "users_email_key"`,
			action.TableActionClause())
	})

	t.Run("PostgresCreateTableWithStorageParameters", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "tuned",
			Columns: []*PostgresColumn{
				{
					Name: "id",
					Type: "integer",
				},
			},
			StorageParameters: []string{"fillfactor=70", "autovacuum_enabled=false"},
		}

		require.Equal(t, `CREATE TABLE "tuned" (
	"id" integer
) WITH (fillfactor=70, autovacuum_enabled=false);`, instruction.String())
	})

	t.Run("PostgresSetStorageParametersAction", func(t *testing.T) {
		action := &PostgresSetStorageParametersAction{
			Parameters: []string{"fillfactor=70"},
		}

		require.Equal(t, `SET (fillfactor=70)`, action.TableActionClause())
	})

	t.Run("PostgresResetStorageParametersAction", func(t *testing.T) {
		action := &PostgresResetStorageParametersAction{
			Names: []string{"fillfactor", "autovacuum_enabled"},
		}

		require.Equal(t, `RESET (fillfactor, autovacuum_enabled)`, action.TableActionClause())
	})

	t.Run("PostgresCreateViewWithACheckOption", func(t *testing.T) {
		instruction := &PostgresCreateViewInstruction{
			Name:        "positive",
			Query:       " SELECT a\n   FROM base\n  WHERE (a > 0);",
			CheckOption: "CASCADED",
		}

		require.Equal(t, `CREATE VIEW "positive" AS  SELECT a
   FROM base
  WHERE (a > 0) WITH CASCADED CHECK OPTION;`, instruction.String())
	})

	t.Run("PostgresCreateRule", func(t *testing.T) {
		instruction := &PostgresCreateRuleInstruction{
			Definition: "CREATE RULE no_delete AS\n    ON DELETE TO base DO INSTEAD NOTHING;",
		}

		require.Equal(t, `CREATE RULE no_delete AS
    ON DELETE TO base DO INSTEAD NOTHING;`, instruction.String())
	})

	t.Run("PostgresDropRule", func(t *testing.T) {
		instruction := &PostgresDropRuleInstruction{
			Name:      "no_delete",
			TableName: "base",
		}

		require.Equal(t, `DROP RULE "no_delete" ON "base";`, instruction.String())
	})

	t.Run("PostgresAddIdentityAction", func(t *testing.T) {
		action := &PostgresAddIdentityAction{
			ColumnName: "id",
			Identity:   "ALWAYS",
		}

		require.Equal(t,
			`ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY`,
			action.TableActionClause())
	})

	t.Run("PostgresAddIdentityActionWithOptions", func(t *testing.T) {
		action := &PostgresAddIdentityAction{
			ColumnName: "id",
			Identity:   "BY DEFAULT",
			Options:    "START WITH 100 INCREMENT BY 5",
		}

		require.Equal(t,
			`ALTER COLUMN "id" ADD GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 5)`,
			action.TableActionClause())
	})

	t.Run("PostgresSetIdentityAction", func(t *testing.T) {
		action := &PostgresSetIdentityAction{
			ColumnName: "id",
			Identity:   "BY DEFAULT",
		}

		require.Equal(t,
			`ALTER COLUMN "id" SET GENERATED BY DEFAULT`,
			action.TableActionClause())
	})

	t.Run("PostgresDropIdentityAction", func(t *testing.T) {
		action := &PostgresDropIdentityAction{ColumnName: "id"}

		require.Equal(t, `ALTER COLUMN "id" DROP IDENTITY`, action.TableActionClause())
	})

	t.Run("PostgresAddConstraintAction", func(t *testing.T) {
		action := &PostgresAddConstraintAction{
			Constraint: &PostgresConstraint{
				Name: "users_pkey",
				Def:  "PRIMARY KEY (id)",
			},
		}

		require.Equal(t,
			`ADD CONSTRAINT "users_pkey" PRIMARY KEY (id)`,
			action.TableActionClause())
	})

	t.Run("PostgresDropConstraintAction", func(t *testing.T) {
		action := &PostgresDropConstraintAction{ConstraintName: "users_pkey"}

		require.Equal(t, `DROP CONSTRAINT "users_pkey"`, action.TableActionClause())
	})

	t.Run("PostgresCreateTable", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
			Columns: []*PostgresColumn{
				{
					Name:    "id",
					Type:    "integer",
					NotNull: true,
				},
			},
			Constraints: []*PostgresConstraint{
				{
					Name: "users_pkey",
					Def:  "PRIMARY KEY (id)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL,
	CONSTRAINT "users_pkey" PRIMARY KEY (id)
);`, instruction.String())
	})

	t.Run("PostgresCreateTableWithAnIdentityColumn", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
			Columns: []*PostgresColumn{
				{
					Name:     "id",
					Type:     "integer",
					NotNull:  true,
					Identity: "ALWAYS",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "users" (
	"id" integer NOT NULL GENERATED ALWAYS AS IDENTITY
);`, instruction.String())
	})

	t.Run("PostgresCreateTableWithAGeneratedColumn", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "measures",
			Columns: []*PostgresColumn{
				{
					Name: "value",
					Type: "integer",
				},
				{
					Name:                "doubled",
					Type:                "integer",
					GeneratedExpression: "(value * 2)",
				},
			},
		}

		require.Equal(t, `CREATE TABLE "measures" (
	"value" integer,
	"doubled" integer GENERATED ALWAYS AS ((value * 2)) STORED
);`, instruction.String())
	})

	t.Run("PostgresCreateIndex", func(t *testing.T) {
		instruction := &PostgresCreateIndexInstruction{
			Definition: `CREATE INDEX idx_users_name ON users USING btree (name)`,
		}

		require.Equal(t,
			`CREATE INDEX idx_users_name ON users USING btree (name);`,
			instruction.String())
	})

	t.Run("PostgresCreateTrigger", func(t *testing.T) {
		instruction := &PostgresCreateTriggerInstruction{
			Definition: "CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp()",
		}

		require.Equal(t,
			"CREATE TRIGGER set_timestamp BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_timestamp();",
			instruction.String())
	})

	t.Run("PostgresDropTrigger", func(t *testing.T) {
		instruction := &PostgresDropTriggerInstruction{
			Name:      "set_timestamp",
			TableName: "users",
		}

		require.Equal(t,
			`DROP TRIGGER "set_timestamp" ON "users";`,
			instruction.String())
	})

	t.Run("PostgresCreateView", func(t *testing.T) {
		instruction := &PostgresCreateViewInstruction{
			Name:  "user_ids",
			Query: " SELECT id\n   FROM users;",
		}

		require.Equal(t, "CREATE VIEW \"user_ids\" AS  SELECT id\n   FROM users;",
			instruction.String())
	})

	t.Run("ColumnDefinitionIsNotAnInstruction", func(t *testing.T) {
		// A fragment must never satisfy Instruction. Go cannot express that negatively at
		// compile time, so this check runs at run time.
		fragments := map[string]any{
			"PostgresColumn":     &PostgresColumn{},
			"PostgresConstraint": &PostgresConstraint{},
		}

		for name, fragment := range fragments {
			_, isInstruction := fragment.(driversshared.Instruction)
			require.False(t, isInstruction, name)
		}

		postgresColumn := &PostgresColumn{
			Name:    "id",
			Type:    "integer",
			NotNull: true,
		}
		require.Equal(t, `"id" integer NOT NULL`, postgresColumn.Definition())

		constraint := &PostgresConstraint{
			Name: "users_pkey",
			Def:  "PRIMARY KEY (id)",
		}
		require.Equal(t, `CONSTRAINT "users_pkey" PRIMARY KEY (id)`, constraint.Clause())
	})
}

func TestPostgresTableInstructionComments(t *testing.T) {
	t.Run("PostgresInsertOverriding", func(t *testing.T) {
		instruction := &PostgresInsertOverridingInstruction{
			TableName: "users",
		}

		require.Equal(t, `Change the rows of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresAlterTable", func(t *testing.T) {
		instruction := &PostgresAlterTableInstruction{
			Name: "posts",
		}

		require.Equal(t, `Modify the table "posts"`, instruction.Comment())
	})

	t.Run("PostgresCreateTable", func(t *testing.T) {
		instruction := &PostgresCreateTableInstruction{
			Name: "users",
		}

		require.Equal(t, `Create the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCreateTablePartition", func(t *testing.T) {
		instruction := &PostgresCreateTablePartitionInstruction{
			Name:       "measurements_2026",
			ParentName: "measurements",
		}

		require.Equal(t, `Create the table "measurements_2026"`, instruction.Comment())
	})

	t.Run("PostgresCreateIndex", func(t *testing.T) {
		instruction := &PostgresCreateIndexInstruction{
			Definition: "CREATE UNIQUE INDEX posts_slug ON posts USING btree (slug)",
		}

		require.Equal(t, `Create the index "posts_slug" of the table "posts"`, instruction.Comment())
	})

	t.Run("PostgresCreateTrigger", func(t *testing.T) {
		instruction := &PostgresCreateTriggerInstruction{
			Definition: "CREATE TRIGGER sync_updated_at BEFORE UPDATE ON scim FOR EACH ROW EXECUTE FUNCTION sync_updated_at()",
		}

		require.Equal(t, `Create the trigger "sync_updated_at" of the table "scim"`, instruction.Comment())
	})

	t.Run("PostgresDropTrigger", func(t *testing.T) {
		instruction := &PostgresDropTriggerInstruction{
			Name:      "sync_created_at",
			TableName: "scim",
		}

		require.Equal(t, `Drop the trigger "sync_created_at" of the table "scim"`, instruction.Comment())
	})

	t.Run("PostgresCreatePolicy", func(t *testing.T) {
		instruction := &PostgresCreatePolicyInstruction{
			Name:      "read_own_row",
			TableName: "users",
		}

		require.Equal(t, `Create the policy "read_own_row" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresDropPolicy", func(t *testing.T) {
		instruction := &PostgresDropPolicyInstruction{
			Name:      "read_every_row",
			TableName: "users",
		}

		require.Equal(t, `Drop the policy "read_every_row" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCreateRule", func(t *testing.T) {
		instruction := &PostgresCreateRuleInstruction{
			Definition: "CREATE RULE notify_me AS\n    ON UPDATE TO users DO\n NOTIFY users;",
		}

		require.Equal(t, `Create the rule "notify_me" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresDropRule", func(t *testing.T) {
		instruction := &PostgresDropRuleInstruction{
			Name:      "notify_them",
			TableName: "users",
		}

		require.Equal(t, `Drop the rule "notify_them" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnTable", func(t *testing.T) {
		instruction := &PostgresCommentOnTableInstruction{
			Name: "users",
			Text: "the people of the site",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnColumn", func(t *testing.T) {
		instruction := &PostgresCommentOnColumnInstruction{
			TableName:  "users",
			ColumnName: "id",
			Text:       "the key",
		}

		require.Equal(t, `Modify the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnView", func(t *testing.T) {
		instruction := &PostgresCommentOnViewInstruction{
			Name: "active_users",
			Text: "the people that signed in",
		}

		require.Equal(t, `Modify the view "active_users"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnMaterializedView", func(t *testing.T) {
		instruction := &PostgresCommentOnMaterializedViewInstruction{
			Name: "daily_totals",
			Text: "the totals of one day",
		}

		require.Equal(t, `Modify the materialized view "daily_totals"`, instruction.Comment())
	})

	t.Run("PostgresCommentOnIndex", func(t *testing.T) {
		instruction := &PostgresCommentOnIndexInstruction{
			Name: "users_email",
			Text: "the lookup key of the sign in",
		}

		require.Equal(t, `Modify the index "users_email"`, instruction.Comment())
	})

	t.Run("PostgresCreateMaterializedView", func(t *testing.T) {
		instruction := &PostgresCreateMaterializedViewInstruction{
			Name:  "sales_of_the_month",
			Query: "SELECT 1",
		}

		require.Equal(t, `Create the materialized view "sales_of_the_month"`, instruction.Comment())
	})

	t.Run("PostgresDropMaterializedView", func(t *testing.T) {
		instruction := &PostgresDropMaterializedViewInstruction{
			Name: "sales_of_the_year",
		}

		require.Equal(t, `Drop the materialized view "sales_of_the_year"`, instruction.Comment())
	})

	t.Run("PostgresCreateView", func(t *testing.T) {
		instruction := &PostgresCreateViewInstruction{
			Name:  "recent_posts",
			Query: "SELECT id FROM posts",
		}

		require.Equal(t, `Create the view "recent_posts"`, instruction.Comment())
	})

	t.Run("PostgresDetachPartition", func(t *testing.T) {
		instruction := &PostgresDetachPartitionInstruction{
			ParentName:    "measurements",
			PartitionName: "measurements_low",
		}

		require.Equal(t, `Detach the partition of the table "measurements"`, instruction.Comment())
	})

	t.Run("PostgresAttachPartition", func(t *testing.T) {
		instruction := &PostgresAttachPartitionInstruction{
			ParentName:    "measurements",
			PartitionName: "measurements_low",
		}

		require.Equal(t, `Attach the partition of the table "measurements"`, instruction.Comment())
	})
}

func TestPostgresTableAnnotateInstructions(t *testing.T) {
	t.Run("OneCommentForAGroupOfOneObject", func(t *testing.T) {
		dropColumn := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []driversshared.AlterTableAction{
				&driversshared.SQLDropColumnAction{
					ColumnName: "age",
				},
			},
		}
		dropConstraint := &PostgresAlterTableInstruction{
			Name: "users",
			Actions: []driversshared.AlterTableAction{
				&PostgresDropConstraintAction{
					ConstraintName: "users_age_check",
				},
			},
		}

		require.Equal(t, driversshared.Instructions{
			commentInstruction(`Modify the table "users"`),
			dropColumn,
			dropConstraint,
		}, driversshared.AnnotateInstructions([]driversshared.Instruction{dropColumn, dropConstraint}))
	})
}
