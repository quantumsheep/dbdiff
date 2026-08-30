package driverspostgres

import (
	"database/sql"
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/stretchr/testify/require"
)

func TestInstructions(t *testing.T) {
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
					Serial:  "serial",
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

	t.Run("PostgresCommentOnRoutine", func(t *testing.T) {
		instruction := &PostgresCommentOnRoutineInstruction{
			Name:      "add",
			Arguments: "integer, integer",
			Text:      "the sum of the two values",
		}

		require.Equal(t,
			`COMMENT ON ROUTINE "add"(integer, integer) IS 'the sum of the two values';`,
			instruction.String())
	})

	t.Run("PostgresCommentOnType", func(t *testing.T) {
		instruction := &PostgresCommentOnTypeInstruction{
			Name: "mood",
			Text: "the state of a person",
		}

		require.Equal(t,
			`COMMENT ON TYPE "mood" IS 'the state of a person';`,
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
			ColumnName: "body",
			Source:     500,
		}

		require.Equal(t, `ALTER COLUMN "body" SET STATISTICS 500`, action.TableActionClause())
	})

	t.Run("PostgresResetStatisticsAction", func(t *testing.T) {
		action := &PostgresSetStatisticsAction{
			ColumnName: "body",
			Source:     -1,
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

	t.Run("PostgresCreateStatistics", func(t *testing.T) {
		instruction := &PostgresCreateStatisticsInstruction{
			Definition: "CREATE STATISTICS st_ab ON a, b FROM t",
		}

		require.Equal(t, `CREATE STATISTICS st_ab ON a, b FROM t;`, instruction.String())
	})

	t.Run("PostgresDropStatistics", func(t *testing.T) {
		instruction := &PostgresDropStatisticsInstruction{Name: "st_ab"}

		require.Equal(t, `DROP STATISTICS "st_ab";`, instruction.String())
	})

	t.Run("PostgresGrant", func(t *testing.T) {
		instruction := &PostgresGrantInstruction{
			Privileges: []string{"SELECT", "INSERT"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "app_writer",
		}

		require.Equal(t,
			`GRANT SELECT, INSERT ON TABLE "users" TO "app_writer";`,
			instruction.String())
	})

	t.Run("PostgresRevoke", func(t *testing.T) {
		instruction := &PostgresRevokeInstruction{
			Privileges: []string{"UPDATE"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "app_writer",
		}

		require.Equal(t,
			`REVOKE UPDATE ON TABLE "users" FROM "app_writer";`,
			instruction.String())
	})

	t.Run("PostgresSetOwner", func(t *testing.T) {
		instruction := &PostgresSetOwnerInstruction{
			ObjectType: "TABLE",
			ObjectName: "users",
			Owner:      "app_owner",
		}

		require.Equal(t,
			`ALTER TABLE "users" OWNER TO "app_owner";`,
			instruction.String())
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
	"doubled" integer GENERATED ALWAYS AS (value * 2) STORED
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

	t.Run("PostgresCreateEnumType", func(t *testing.T) {
		instruction := &PostgresCreateEnumTypeInstruction{
			Name:   "mood",
			Labels: []string{"sad", "happy"},
		}

		require.Equal(t, `CREATE TYPE "mood" AS ENUM ('sad', 'happy');`, instruction.String())
	})

	t.Run("PostgresAlterTypeAddValue", func(t *testing.T) {
		instruction := &PostgresAlterTypeAddValueInstruction{
			Name:  "mood",
			Value: "calm",
		}

		require.Equal(t, `ALTER TYPE "mood" ADD VALUE 'calm';`, instruction.String())
	})

	t.Run("PostgresDropType", func(t *testing.T) {
		instruction := &PostgresDropTypeInstruction{Name: "mood"}

		require.Equal(t, `DROP TYPE "mood";`, instruction.String())
	})

	t.Run("PostgresCreateCompositeType", func(t *testing.T) {
		instruction := &PostgresCreateCompositeTypeInstruction{
			Name: "address",
			Attributes: []*PostgresCompositeTypeAttribute{
				{
					Name: "street",
					Type: "text",
				},
				{
					Name: "number",
					Type: "integer",
				},
			},
		}

		require.Equal(t, `CREATE TYPE "address" AS (
	"street" text,
	"number" integer
);`, instruction.String())
	})

	t.Run("PostgresCreateDomain", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive",
			BaseType: "integer",
		}

		require.Equal(t, `CREATE DOMAIN "positive" AS integer;`, instruction.String())
	})

	t.Run("PostgresCreateDomainWithEveryPart", func(t *testing.T) {
		instruction := &PostgresCreateDomainInstruction{
			Name:     "positive",
			BaseType: "integer",
			Default: sql.NullString{
				String: "1",
				Valid:  true,
			},
			NotNull: true,
			Constraints: []*PostgresDomainConstraint{
				{
					Name: "positive_check",
					Def:  "CHECK ((VALUE > 0))",
				},
			},
		}

		require.Equal(t,
			`CREATE DOMAIN "positive" AS integer DEFAULT 1 NOT NULL CONSTRAINT "positive_check" CHECK ((VALUE > 0));`,
			instruction.String())
	})

	t.Run("PostgresDropDomain", func(t *testing.T) {
		instruction := &PostgresDropDomainInstruction{Name: "positive"}

		require.Equal(t, `DROP DOMAIN "positive";`, instruction.String())
	})

	t.Run("PostgresAlterDomainSetDefault", func(t *testing.T) {
		instruction := &PostgresAlterDomainInstruction{
			Name:   "positive",
			Action: &PostgresSetDomainDefaultAction{Expression: "5"},
		}

		require.Equal(t, `ALTER DOMAIN "positive" SET DEFAULT 5;`, instruction.String())
	})

	t.Run("PostgresDropDomainDefaultAction", func(t *testing.T) {
		action := &PostgresDropDomainDefaultAction{}

		require.Equal(t, "DROP DEFAULT", action.DomainActionClause())
	})

	t.Run("PostgresSetDomainNotNullAction", func(t *testing.T) {
		action := &PostgresSetDomainNotNullAction{}

		require.Equal(t, "SET NOT NULL", action.DomainActionClause())
	})

	t.Run("PostgresDropDomainNotNullAction", func(t *testing.T) {
		action := &PostgresDropDomainNotNullAction{}

		require.Equal(t, "DROP NOT NULL", action.DomainActionClause())
	})

	t.Run("PostgresAddDomainConstraintAction", func(t *testing.T) {
		action := &PostgresAddDomainConstraintAction{
			ConstraintName: "positive_check",
			Definition:     "CHECK ((VALUE > 0))",
		}

		require.Equal(t,
			`ADD CONSTRAINT "positive_check" CHECK ((VALUE > 0))`,
			action.DomainActionClause())
	})

	t.Run("PostgresDropDomainConstraintAction", func(t *testing.T) {
		action := &PostgresDropDomainConstraintAction{ConstraintName: "positive_check"}

		require.Equal(t, `DROP CONSTRAINT "positive_check"`, action.DomainActionClause())
	})

	t.Run("PostgresCreateFunction", func(t *testing.T) {
		instruction := &PostgresCreateFunctionInstruction{
			Definition: "CREATE FUNCTION add(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql",
		}

		require.Equal(t,
			"CREATE FUNCTION add(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql;",
			instruction.String())
	})

	t.Run("PostgresDropFunction", func(t *testing.T) {
		instruction := &PostgresDropFunctionInstruction{
			Name:      "add",
			Arguments: "integer",
		}

		require.Equal(t, `DROP FUNCTION "add"(integer);`, instruction.String())
	})

	t.Run("PostgresCreateProcedure", func(t *testing.T) {
		instruction := &PostgresCreateProcedureInstruction{
			Definition: "CREATE PROCEDURE audit(entry text) LANGUAGE sql AS $$ SELECT 1 $$",
		}

		require.Equal(t,
			"CREATE PROCEDURE audit(entry text) LANGUAGE sql AS $$ SELECT 1 $$;",
			instruction.String())
	})

	t.Run("PostgresDropProcedure", func(t *testing.T) {
		instruction := &PostgresDropProcedureInstruction{
			Name:      "audit",
			Arguments: "entry text",
		}

		require.Equal(t, `DROP PROCEDURE "audit"(entry text);`, instruction.String())
	})

	t.Run("PostgresCreateAggregate", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:               "total",
			Arguments:          "integer",
			TransitionFunction: "int4pl",
			StateType:          "integer",
		}

		require.Equal(t,
			`CREATE AGGREGATE "total"(integer) (SFUNC = int4pl, STYPE = integer);`,
			instruction.String())
	})

	t.Run("PostgresCreateAggregateWithEveryOption", func(t *testing.T) {
		instruction := &PostgresCreateAggregateInstruction{
			Name:                "total",
			Arguments:           "integer",
			TransitionFunction:  "int4pl",
			TransitionSpace:     128,
			StateType:           "integer",
			FinalFunctionExtra:  true,
			FinalFunctionModify: "s",
			FinalFunction: sql.NullString{
				String: "int4out",
				Valid:  true,
			},
			CombineFunction: sql.NullString{
				String: "int4pl",
				Valid:  true,
			},
			SerializeFunction: sql.NullString{
				String: "numeric_avg_serialize",
				Valid:  true,
			},
			DeserializeFunction: sql.NullString{
				String: "numeric_avg_deserialize",
				Valid:  true,
			},
			InitialCondition: sql.NullString{
				String: "0",
				Valid:  true,
			},
			MovingTransitionFunction: sql.NullString{
				String: "int4pl",
				Valid:  true,
			},
			MovingInverseTransitionFunction: sql.NullString{
				String: "int4mi",
				Valid:  true,
			},
			MovingStateType: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			MovingTransitionSpace: 256,
			MovingFinalFunction: sql.NullString{
				String: "int4out",
				Valid:  true,
			},
			MovingFinalFunctionExtra:  true,
			MovingFinalFunctionModify: "w",
			MovingInitialCondition: sql.NullString{
				String: "0",
				Valid:  true,
			},
			SortOperator: sql.NullString{
				String: ">",
				Valid:  true,
			},
			Parallel: "s",
		}

		require.Equal(t,
			`CREATE AGGREGATE "total"(integer) (SFUNC = int4pl, STYPE = integer, SSPACE = 128, `+
				`FINALFUNC = int4out, FINALFUNC_EXTRA, FINALFUNC_MODIFY = SHAREABLE, COMBINEFUNC = int4pl, `+
				`SERIALFUNC = numeric_avg_serialize, DESERIALFUNC = numeric_avg_deserialize, INITCOND = '0', `+
				`MSFUNC = int4pl, MINVFUNC = int4mi, MSTYPE = integer, MSSPACE = 256, MFINALFUNC = int4out, `+
				`MFINALFUNC_EXTRA, MFINALFUNC_MODIFY = READ_WRITE, MINITCOND = '0', SORTOP = OPERATOR(>), `+
				`PARALLEL = SAFE);`,
			instruction.String())
	})

	t.Run("PostgresDropAggregate", func(t *testing.T) {
		instruction := &PostgresDropAggregateInstruction{
			Name:      "total",
			Arguments: "integer",
		}

		require.Equal(t, `DROP AGGREGATE "total"(integer);`, instruction.String())
	})

	t.Run("PostgresCreateOperator", func(t *testing.T) {
		instruction := &PostgresCreateOperatorInstruction{
			Name:     "===",
			Function: "int4eq",
			LeftArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
		}

		require.Equal(t,
			`CREATE OPERATOR === (FUNCTION = "int4eq", LEFTARG = integer, RIGHTARG = integer);`,
			instruction.String())
	})

	t.Run("PostgresCreateOperatorWithEveryOption", func(t *testing.T) {
		instruction := &PostgresCreateOperatorInstruction{
			Name:     "===",
			Function: "int4eq",
			LeftArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			Commutator: sql.NullString{
				String: "===",
				Valid:  true,
			},
			Negator: sql.NullString{
				String: "!==",
				Valid:  true,
			},
			RestrictFunction: sql.NullString{
				String: "eqsel",
				Valid:  true,
			},
			JoinFunction: sql.NullString{
				String: "eqjoinsel",
				Valid:  true,
			},
			CanHash:  true,
			CanMerge: true,
		}

		require.Equal(t,
			`CREATE OPERATOR === (FUNCTION = "int4eq", LEFTARG = integer, RIGHTARG = integer, `+
				`COMMUTATOR = ===, NEGATOR = !==, RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES);`,
			instruction.String())
	})

	t.Run("PostgresDropOperatorWithoutALeftArgument", func(t *testing.T) {
		instruction := &PostgresDropOperatorInstruction{
			Name: "===",
			RightArgument: sql.NullString{
				String: "integer",
				Valid:  true,
			},
		}

		require.Equal(t, `DROP OPERATOR === (NONE, integer);`, instruction.String())
	})

	t.Run("PostgresCreateExtension", func(t *testing.T) {
		instruction := &PostgresCreateExtensionInstruction{Name: "pgcrypto"}

		require.Equal(t, `CREATE EXTENSION "pgcrypto";`, instruction.String())
	})

	t.Run("PostgresAlterExtension", func(t *testing.T) {
		instruction := &PostgresAlterExtensionInstruction{
			Name:       "pgcrypto",
			NewVersion: "1.3",
		}

		require.Equal(t, `ALTER EXTENSION "pgcrypto" UPDATE TO '1.3';`, instruction.String())
	})

	t.Run("PostgresDropExtension", func(t *testing.T) {
		instruction := &PostgresDropExtensionInstruction{Name: "pgcrypto"}

		require.Equal(t, `DROP EXTENSION "pgcrypto";`, instruction.String())
	})

	t.Run("PostgresCreateOwnedSequence", func(t *testing.T) {
		instruction := &PostgresCreateOwnedSequenceInstruction{
			Name:       "users_id_seq",
			TableName:  "users",
			ColumnName: "id",
		}

		require.Equal(t,
			`CREATE SEQUENCE "users_id_seq" OWNED BY "users"."id";`,
			instruction.String())
	})

	t.Run("PostgresCreateSequence", func(t *testing.T) {
		instruction := &PostgresCreateSequenceInstruction{
			Name:      "counter",
			DataType:  "bigint",
			Increment: 1,
			Min:       1,
			Max:       9223372036854775807,
			Start:     1,
			Cache:     1,
		}

		require.Equal(t,
			`CREATE SEQUENCE "counter" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START WITH 1 CACHE 1 NO CYCLE;`,
			instruction.String())
	})

	t.Run("PostgresCreateSequenceWithCycle", func(t *testing.T) {
		instruction := &PostgresCreateSequenceInstruction{
			Name:      "counter",
			DataType:  "bigint",
			Increment: 1,
			Min:       1,
			Max:       10,
			Start:     1,
			Cache:     1,
			Cycle:     true,
		}

		require.Equal(t,
			`CREATE SEQUENCE "counter" AS bigint INCREMENT BY 1 MINVALUE 1 MAXVALUE 10 START WITH 1 CACHE 1 CYCLE;`,
			instruction.String())
	})

	t.Run("PostgresAlterSequenceOneClause", func(t *testing.T) {
		instruction := &PostgresAlterSequenceInstruction{
			Name: "counter",
			Increment: sql.NullInt64{
				Int64: 2,
				Valid: true,
			},
		}

		require.Equal(t, `ALTER SEQUENCE "counter" INCREMENT BY 2;`, instruction.String())
	})

	t.Run("PostgresAlterSequenceEveryClauseInOrder", func(t *testing.T) {
		instruction := &PostgresAlterSequenceInstruction{
			Name: "counter",
			DataType: sql.NullString{
				String: "integer",
				Valid:  true,
			},
			Increment: sql.NullInt64{
				Int64: 2,
				Valid: true,
			},
			Min: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
			Max: sql.NullInt64{
				Int64: 50,
				Valid: true,
			},
			Start: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
			Cycle: sql.NullBool{
				Bool:  true,
				Valid: true,
			},
			Restart: sql.NullInt64{
				Int64: 5,
				Valid: true,
			},
		}

		require.Equal(t,
			`ALTER SEQUENCE "counter" AS integer INCREMENT BY 2 MINVALUE 5 MAXVALUE 50 START WITH 5 CYCLE RESTART WITH 5;`,
			instruction.String())
	})

	t.Run("PostgresDropSequence", func(t *testing.T) {
		instruction := &PostgresDropSequenceInstruction{Name: "counter"}

		require.Equal(t, `DROP SEQUENCE "counter";`, instruction.String())
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
