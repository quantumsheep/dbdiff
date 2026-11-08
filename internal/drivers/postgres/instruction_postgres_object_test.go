package driverspostgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresObjectInstructions(t *testing.T) {
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

	t.Run("PostgresGrantWithGrantOption", func(t *testing.T) {
		instruction := &PostgresGrantInstruction{
			Privileges:      []string{"SELECT"},
			ObjectType:      "TABLE",
			ObjectName:      "users",
			Grantee:         "app_writer",
			WithGrantOption: true,
		}

		require.Equal(t,
			`GRANT SELECT ON TABLE "users" TO "app_writer" WITH GRANT OPTION;`,
			instruction.String())
	})

	t.Run("PostgresRevokeGrantOptionFor", func(t *testing.T) {
		instruction := &PostgresRevokeInstruction{
			Privileges:     []string{"UPDATE"},
			ObjectType:     "TABLE",
			ObjectName:     "users",
			Column:         "email",
			Grantee:        "app_writer",
			GrantOptionFor: true,
		}

		require.Equal(t,
			`REVOKE GRANT OPTION FOR UPDATE ("email") ON TABLE "users" FROM "app_writer";`,
			instruction.String())
	})

	t.Run("PostgresGrantDefaultPrivileges", func(t *testing.T) {
		instruction := &PostgresGrantDefaultPrivilegesInstruction{
			Role:            "app owner",
			Schema:          "sales",
			Privileges:      []string{"INSERT", "SELECT"},
			ObjectType:      "TABLES",
			Grantee:         "app_writer",
			WithGrantOption: true,
		}

		require.Equal(t,
			`ALTER DEFAULT PRIVILEGES FOR ROLE "app owner" IN SCHEMA "sales" `+
				`GRANT INSERT, SELECT ON TABLES TO "app_writer" WITH GRANT OPTION;`,
			instruction.String())
	})

	t.Run("PostgresRevokeDefaultPrivileges", func(t *testing.T) {
		instruction := &PostgresRevokeDefaultPrivilegesInstruction{
			Role:       "app_owner",
			Schema:     "sales",
			Privileges: []string{"EXECUTE"},
			ObjectType: "FUNCTIONS",
			Grantee:    "public",
		}

		require.Equal(t,
			`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "sales" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;`,
			instruction.String())
	})

	t.Run("PostgresRevokeDefaultPrivilegesGrantOptionFor", func(t *testing.T) {
		instruction := &PostgresRevokeDefaultPrivilegesInstruction{
			Role:           "app_owner",
			Schema:         "sales",
			Privileges:     []string{"USAGE"},
			ObjectType:     "TYPES",
			Grantee:        "app_writer",
			GrantOptionFor: true,
		}

		require.Equal(t,
			`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "sales" `+
				`REVOKE GRANT OPTION FOR USAGE ON TYPES FROM "app_writer";`,
			instruction.String())
	})

	t.Run("PostgresGrantToPublic", func(t *testing.T) {
		instruction := &PostgresGrantInstruction{
			Privileges: []string{"SELECT"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "public",
		}

		require.Equal(t,
			`GRANT SELECT ON TABLE "users" TO PUBLIC;`,
			instruction.String())
	})

	t.Run("PostgresRevokeFromPublic", func(t *testing.T) {
		instruction := &PostgresRevokeInstruction{
			Privileges: []string{"SELECT"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "public",
		}

		require.Equal(t,
			`REVOKE SELECT ON TABLE "users" FROM PUBLIC;`,
			instruction.String())
	})

	t.Run("PostgresGrantOnAFunction", func(t *testing.T) {
		instruction := &PostgresGrantInstruction{
			Privileges: []string{"EXECUTE"},
			ObjectType: "FUNCTION",
			ObjectName: "add_one",
			Arguments:  "integer",
			Grantee:    "app_writer",
		}

		require.Equal(t,
			`GRANT EXECUTE ON FUNCTION "add_one"(integer) TO "app_writer";`,
			instruction.String())
	})

	t.Run("PostgresRevokeOnAProcedureWithNoArguments", func(t *testing.T) {
		instruction := &PostgresRevokeInstruction{
			Privileges: []string{"EXECUTE"},
			ObjectType: "PROCEDURE",
			ObjectName: "record_run",
			Arguments:  "",
			Grantee:    "public",
		}

		require.Equal(t,
			`REVOKE EXECUTE ON PROCEDURE "record_run"() FROM PUBLIC;`,
			instruction.String())
	})

	t.Run("PostgresSetOwnerOfAFunction", func(t *testing.T) {
		instruction := &PostgresSetOwnerInstruction{
			ObjectType: "FUNCTION",
			ObjectName: "add_one",
			Arguments:  "integer, text",
			Owner:      "app_owner",
		}

		require.Equal(t,
			`ALTER FUNCTION "add_one"(integer, text) OWNER TO "app_owner";`,
			instruction.String())
	})

	t.Run("PostgresSetOwnerOfAnAggregateWithNoArguments", func(t *testing.T) {
		instruction := &PostgresSetOwnerInstruction{
			ObjectType: "AGGREGATE",
			ObjectName: "row_total",
			Arguments:  "",
			Owner:      "app_owner",
		}

		require.Equal(t,
			`ALTER AGGREGATE "row_total"(*) OWNER TO "app_owner";`,
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
			DataType:   "integer",
			TableName:  "users",
			ColumnName: "id",
		}

		require.Equal(t,
			`CREATE SEQUENCE "users_id_seq" AS integer OWNED BY "users"."id";`,
			instruction.String())
	})

	t.Run("PostgresAlterSequenceOwner", func(t *testing.T) {
		instruction := &PostgresAlterSequenceOwnerInstruction{
			Name:       "user_ids",
			TableName:  "users",
			ColumnName: "id",
		}

		require.Equal(t, `ALTER SEQUENCE "user_ids" OWNED BY "users"."id";`, instruction.String())
	})

	t.Run("PostgresAlterSequenceOwnerToNone", func(t *testing.T) {
		instruction := &PostgresAlterSequenceOwnerInstruction{Name: "user_ids"}

		require.Equal(t, `ALTER SEQUENCE "user_ids" OWNED BY NONE;`, instruction.String())
	})

	t.Run("PostgresRestartOwnedSequence", func(t *testing.T) {
		instruction := &PostgresRestartOwnedSequenceInstruction{
			TableName:  "user's",
			ColumnName: "Id",
		}

		require.Equal(t,
			`SELECT setval(pg_get_serial_sequence('"user''s"', 'Id'), `+
				`greatest(max("Id") + 1, (SELECT seqstart FROM pg_sequence `+
				`WHERE seqrelid = pg_get_serial_sequence('"user''s"', 'Id')::regclass)), false) `+
				`FROM "user's" HAVING max("Id") IS NOT NULL;`,
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
}

func TestPostgresObjectInstructionComments(t *testing.T) {
	t.Run("PostgresCreateExtension", func(t *testing.T) {
		instruction := &PostgresCreateExtensionInstruction{
			Name: "pgcrypto",
		}

		require.Equal(t, `Create the extension "pgcrypto"`, instruction.Comment())
	})

	t.Run("PostgresAlterExtension", func(t *testing.T) {
		instruction := &PostgresAlterExtensionInstruction{
			Name:       "postgis",
			NewVersion: "3.4.0",
		}

		require.Equal(t, `Modify the extension "postgis"`, instruction.Comment())
	})

	t.Run("PostgresDropExtension", func(t *testing.T) {
		instruction := &PostgresDropExtensionInstruction{
			Name: "hstore",
		}

		require.Equal(t, `Drop the extension "hstore"`, instruction.Comment())
	})

	t.Run("PostgresCreateOwnedSequence", func(t *testing.T) {
		instruction := &PostgresCreateOwnedSequenceInstruction{
			Name: "users_id_seq",
		}

		require.Equal(t, `Create the sequence "users_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresAlterSequenceOwner", func(t *testing.T) {
		instruction := &PostgresAlterSequenceOwnerInstruction{Name: "user_ids"}

		require.Equal(t, `Modify the owner column of the sequence "user_ids"`, instruction.Comment())
	})

	t.Run("PostgresRestartOwnedSequence", func(t *testing.T) {
		instruction := &PostgresRestartOwnedSequenceInstruction{
			TableName:  "users",
			ColumnName: "id",
		}

		require.Equal(t, `Restart the sequence of the column "id" of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresCreateSequence", func(t *testing.T) {
		instruction := &PostgresCreateSequenceInstruction{
			Name:     "users_id_seq",
			DataType: "bigint",
		}

		require.Equal(t, `Create the sequence "users_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresAlterSequence", func(t *testing.T) {
		instruction := &PostgresAlterSequenceInstruction{
			Name: "posts_id_seq",
		}

		require.Equal(t, `Modify the sequence "posts_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresDropSequence", func(t *testing.T) {
		instruction := &PostgresDropSequenceInstruction{
			Name: "audit_id_seq",
		}

		require.Equal(t, `Drop the sequence "audit_id_seq"`, instruction.Comment())
	})

	t.Run("PostgresCreateStatistics", func(t *testing.T) {
		instruction := &PostgresCreateStatisticsInstruction{
			Definition: "CREATE STATISTICS users_city_state (ndistinct) ON city, state FROM users",
		}

		require.Equal(t, `Create the statistics object "users_city_state" of the table "users"`,
			instruction.Comment())
	})

	t.Run("PostgresDropStatistics", func(t *testing.T) {
		instruction := &PostgresDropStatisticsInstruction{
			Name: "users_city_zip",
		}

		require.Equal(t, `Drop the statistics object "users_city_zip"`, instruction.Comment())
	})

	t.Run("PostgresGrant", func(t *testing.T) {
		instruction := &PostgresGrantInstruction{
			Privileges: []string{"SELECT"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "reader",
		}

		require.Equal(t, `Change the privileges of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresRevoke", func(t *testing.T) {
		instruction := &PostgresRevokeInstruction{
			Privileges: []string{"UPDATE"},
			ObjectType: "TABLE",
			ObjectName: "users",
			Grantee:    "reader",
		}

		require.Equal(t, `Change the privileges of the table "users"`, instruction.Comment())
	})

	t.Run("PostgresGrantDefaultPrivileges", func(t *testing.T) {
		instruction := &PostgresGrantDefaultPrivilegesInstruction{
			Role:       "app_owner",
			Schema:     "sales",
			Privileges: []string{"SELECT"},
			ObjectType: "TABLES",
			Grantee:    "reader",
		}

		require.Equal(t, `Change the default privileges of the schema "sales"`, instruction.Comment())
	})

	t.Run("PostgresRevokeDefaultPrivileges", func(t *testing.T) {
		instruction := &PostgresRevokeDefaultPrivilegesInstruction{
			Role:       "app_owner",
			Schema:     "sales",
			Privileges: []string{"SELECT"},
			ObjectType: "TABLES",
			Grantee:    "reader",
		}

		require.Equal(t, `Change the default privileges of the schema "sales"`, instruction.Comment())
	})

	t.Run("PostgresSetOwner", func(t *testing.T) {
		instruction := &PostgresSetOwnerInstruction{
			ObjectType: "MATERIALIZED VIEW",
			ObjectName: "sales_of_the_month",
			Owner:      "application",
		}

		require.Equal(t, `Change the owner of the materialized view "sales_of_the_month"`,
			instruction.Comment())
	})
}
