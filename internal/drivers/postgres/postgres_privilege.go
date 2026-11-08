package driverspostgres

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

// A role belongs to the server and not to the schema. The server of the current schema holds
// other role names in most cases. A GRANT statement of an absent role fails.
type PostgresPrivilege struct {
	ObjectType string
	ObjectName string
	Arguments  string
	Column     string
	Grantee    string
	Privileges []string
}

func (p *PostgresPrivilege) Key() string {
	return p.ObjectType + " " + privilegeObjectKey(p.ObjectType, p.ObjectName, p.Arguments) + " " +
		p.Column + " " + p.Grantee
}

func (d *PostgresDriver) isPrivilegeRecreated(privilege *PostgresPrivilege) bool {
	if d.isRecreated(privilege.ObjectType, privilege.ObjectName, privilege.Arguments) {
		return true
	}

	return privilege.Column != "" && d.recreatedObjectNames[columnKey(privilege.ObjectName, privilege.Column)]
}

// An identifier cannot hold the character NUL, so the key of a column matches no relation name.
func columnKey(tableName string, columnName string) string {
	return tableName + "\x00" + columnName
}

// A dropped column loses its privileges. DiffTables calls this function, so DiffPrivileges
// revokes nothing on a dropped column and gives the grants back to a column that comes back.
func (d *PostgresDriver) markDroppedColumns(instructions []driversshared.Instruction) {
	for _, instruction := range instructions {
		alterTableInstruction, isAlterTable := instruction.(*PostgresAlterTableInstruction)
		if !isAlterTable {
			continue
		}

		for _, action := range alterTableInstruction.Actions {
			dropColumnAction, isDropColumn := action.(*driversshared.SQLDropColumnAction)
			if isDropColumn {
				d.markRecreated(columnKey(alterTableInstruction.Name, dropColumnAction.ColumnName))
			}
		}
	}
}

func (p *PostgresPrivilege) GrantInstruction(privileges []string) *PostgresGrantInstruction {
	return &PostgresGrantInstruction{
		Privileges: privileges,
		ObjectType: p.ObjectType,
		ObjectName: p.ObjectName,
		Arguments:  p.Arguments,
		Column:     p.Column,
		Grantee:    p.Grantee,
	}
}

func (p *PostgresPrivilege) RevokeInstruction(privileges []string) *PostgresRevokeInstruction {
	return &PostgresRevokeInstruction{
		Privileges: privileges,
		ObjectType: p.ObjectType,
		ObjectName: p.ObjectName,
		Arguments:  p.Arguments,
		Column:     p.Column,
		Grantee:    p.Grantee,
	}
}

type PostgresOwner struct {
	ObjectType string
	ObjectName string
	Arguments  string
	Owner      string
}

func (o *PostgresOwner) Key() string {
	return privilegeObjectKey(o.ObjectType, o.ObjectName, o.Arguments)
}

func (o *PostgresOwner) SetInstruction() *PostgresSetOwnerInstruction {
	return &PostgresSetOwnerInstruction{
		ObjectType: o.ObjectType,
		ObjectName: o.ObjectName,
		Arguments:  o.Arguments,
		Owner:      o.Owner,
	}
}

func isRoutineObjectType(objectType string) bool {
	return objectType == "FUNCTION" || objectType == "PROCEDURE" || objectType == "AGGREGATE"
}

// A GRANT statement names an aggregate with the keyword FUNCTION, and an ALTER statement
// names it with the keyword AGGREGATE.
func privilegeGrantObjectType(ownerObjectType string) string {
	if ownerObjectType == "AGGREGATE" {
		return "FUNCTION"
	}

	return ownerObjectType
}

// The key of a routine is the match key of DiffFunctions, and droppedFunctionKeys holds
// that key. DiffAggregates puts the key of a recreated aggregate in recreatedObjectNames.
// The key of a relation is its name, and recreatedObjectNames holds that name.
func privilegeObjectKey(objectType string, objectName string, arguments string) string {
	if !isRoutineObjectType(objectType) {
		return objectName
	}

	routine := &PostgresFunction{
		Name:          objectName,
		ArgumentTypes: arguments,
		Kind:          privilegeGrantObjectType(objectType),
	}

	return routine.MatchKey()
}

func (d *PostgresDriver) isRecreated(objectType string, objectName string, arguments string) bool {
	key := privilegeObjectKey(objectType, objectName, arguments)

	return d.droppedFunctionKeys[key] || d.recreatedObjectNames[key]
}

// The default privileges of a routine give EXECUTE to PUBLIC, and the default privileges of
// a type give USAGE to PUBLIC. The default privileges of a relation give nothing to PUBLIC.
func defaultPublicPrivilege(ownerObjectType string) string {
	switch ownerObjectType {
	case "FUNCTION", "PROCEDURE", "AGGREGATE":
		return "EXECUTE"
	case "TYPE", "DOMAIN":
		return "USAGE"
	}

	return ""
}

// PostgreSQL names a view and a materialized view with the keyword TABLE in a GRANT.
func privilegeObjectType(relkind string) string {
	if relkind == "S" {
		return "SEQUENCE"
	}

	return "TABLE"
}

// The keyword of an ALTER statement names the exact kind of the object, unlike the
// keyword of a GRANT statement.
func ownerObjectType(relkind string) string {
	switch relkind {
	case "S":
		return "SEQUENCE"
	case "v":
		return "VIEW"
	case "m":
		return "MATERIALIZED VIEW"
	}

	return "TABLE"
}

// Two equal sets must compare equal, so the order stays fixed.
func sortedPrivileges(privileges []string) []string {
	sorted := slices.Clone(privileges)
	slices.Sort(sorted)

	return sorted
}

func joinPrivileges(privileges []string) string {
	return strings.Join(privileges, ", ")
}

func (d *PostgresDriver) DiffPrivileges(ctx context.Context) (*SectionDiff, error) {
	if !d.ComparePrivileges {
		return &SectionDiff{}, nil
	}

	var additions []driversshared.Instruction

	desiredOwners, err := d.GetOwners(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentOwners, err := d.GetOwners(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, desiredOwner := range desiredOwners {
		currentOwner, found := lo.Find(currentOwners, func(owner *PostgresOwner) bool {
			return owner.Key() == desiredOwner.Key()
		})
		if found && currentOwner.Owner == desiredOwner.Owner &&
			!d.isRecreated(desiredOwner.ObjectType, desiredOwner.ObjectName, desiredOwner.Arguments) {
			continue
		}

		additions = append(additions, desiredOwner.SetInstruction())
	}

	desiredPrivileges, err := d.GetPrivileges(ctx, d.DesiredDatabaseConnection)
	if err != nil {
		return nil, err
	}

	currentPrivileges, err := d.GetPrivileges(ctx, d.CurrentDatabaseConnection)
	if err != nil {
		return nil, err
	}

	// A new object and an object that the diff creates again hold the default privileges.
	currentPrivileges = lo.Reject(currentPrivileges, func(privilege *PostgresPrivilege, _ int) bool {
		return d.isPrivilegeRecreated(privilege)
	})

	currentObjectKeys := lo.Map(currentOwners, func(owner *PostgresOwner, _ int) string {
		return owner.Key()
	})

	for _, desiredOwner := range desiredOwners {
		publicPrivilege := defaultPublicPrivilege(desiredOwner.ObjectType)
		if publicPrivilege == "" {
			continue
		}

		if !slices.Contains(currentObjectKeys, desiredOwner.Key()) ||
			d.isRecreated(desiredOwner.ObjectType, desiredOwner.ObjectName, desiredOwner.Arguments) {
			currentPrivileges = append(currentPrivileges, &PostgresPrivilege{
				ObjectType: privilegeGrantObjectType(desiredOwner.ObjectType),
				ObjectName: desiredOwner.ObjectName,
				Arguments:  desiredOwner.Arguments,
				Grantee:    "public",
				Privileges: []string{publicPrivilege},
			})
		}
	}

	for _, desiredPrivilege := range desiredPrivileges {
		currentPrivilege, found := lo.Find(currentPrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == desiredPrivilege.Key()
		})
		if !found {
			additions = append(additions,
				desiredPrivilege.GrantInstruction(desiredPrivilege.Privileges))

			continue
		}

		granted := lo.Without(desiredPrivilege.Privileges, currentPrivilege.Privileges...)
		if len(granted) > 0 {
			additions = append(additions, desiredPrivilege.GrantInstruction(granted))
		}

		revoked := lo.Without(currentPrivilege.Privileges, desiredPrivilege.Privileges...)
		if len(revoked) > 0 {
			additions = append(additions, desiredPrivilege.RevokeInstruction(revoked))
		}
	}

	desiredObjectKeys := lo.Map(desiredOwners, func(owner *PostgresOwner, _ int) string {
		return owner.Key()
	})

	for _, currentPrivilege := range currentPrivileges {
		// The diff drops an object that the desired schema does not hold, and the drop removes
		// every privilege of the object.
		currentObjectKey := privilegeObjectKey(
			currentPrivilege.ObjectType, currentPrivilege.ObjectName, currentPrivilege.Arguments)
		if !slices.Contains(desiredObjectKeys, currentObjectKey) {
			continue
		}

		_, found := lo.Find(desiredPrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == currentPrivilege.Key()
		})
		if !found {
			additions = append(additions,
				currentPrivilege.RevokeInstruction(currentPrivilege.Privileges))
		}
	}

	return &SectionDiff{Additions: additions}, nil
}

func (d *PostgresDriver) GetOwners(ctx context.Context, db *sql.DB) ([]*PostgresOwner, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.relname, c.relkind, pg_get_userbyid(c.relowner)
		FROM pg_class c
		WHERE c.relnamespace = quote_ident(current_schema())::regnamespace
		AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
		AND NOT c.relispartition
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i')
		)
		AND c.relname <> $1
		ORDER BY c.relname
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var owners []*PostgresOwner

	for rows.Next() {
		var name, relkind, owner string

		err := rows.Scan(&name, &relkind, &owner)
		if err != nil {
			return nil, err
		}

		if slices.Contains(d.IgnoreTables, name) {
			continue
		}

		owners = append(owners, &PostgresOwner{
			ObjectType: ownerObjectType(relkind),
			ObjectName: name,
			Owner:      owner,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	routineOwners, err := getRoutineOwners(ctx, db)
	if err != nil {
		return nil, err
	}

	typeOwners, err := getTypeOwners(ctx, db)
	if err != nil {
		return nil, err
	}

	return slices.Concat(owners, routineOwners, typeOwners), nil
}

// PostgreSQL gives the privileges of the owner with the object. The grantee 0 is PUBLIC, and
// the name public cannot name a role.
func (d *PostgresDriver) GetPrivileges(ctx context.Context, db *sql.DB) ([]*PostgresPrivilege, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.relname,
			c.relkind,
			CASE acl.grantee WHEN 0 THEN 'public' ELSE pg_get_userbyid(acl.grantee)::text END AS grantee_name,
			array_to_string(array_agg(acl.privilege_type ORDER BY acl.privilege_type), ',')
		FROM pg_class c, aclexplode(c.relacl) acl
		WHERE c.relnamespace = quote_ident(current_schema())::regnamespace
		AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
		AND acl.grantee <> c.relowner
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i')
		)
		AND c.relname <> $1
		GROUP BY c.relname, c.relkind, acl.grantee
		ORDER BY c.relname, grantee_name
	`, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var privileges []*PostgresPrivilege

	for rows.Next() {
		var name, relkind, grantee, granted string

		err := rows.Scan(&name, &relkind, &grantee, &granted)
		if err != nil {
			return nil, err
		}

		if slices.Contains(d.IgnoreTables, name) {
			continue
		}

		privileges = append(privileges, &PostgresPrivilege{
			ObjectType: privilegeObjectType(relkind),
			ObjectName: name,
			Grantee:    grantee,
			Privileges: sortedPrivileges(strings.Split(granted, ",")),
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	routinePrivileges, err := getRoutinePrivileges(ctx, db)
	if err != nil {
		return nil, err
	}

	typePrivileges, err := getTypePrivileges(ctx, db)
	if err != nil {
		return nil, err
	}

	columnPrivileges, err := d.getColumnPrivileges(ctx, db)
	if err != nil {
		return nil, err
	}

	return slices.Concat(privileges, columnPrivileges, routinePrivileges, typePrivileges), nil
}

func getRoutineOwners(ctx context.Context, db *sql.DB) ([]*PostgresOwner, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			p.proname,
			CASE p.prokind WHEN 'p' THEN 'PROCEDURE' WHEN 'a' THEN 'AGGREGATE' ELSE 'FUNCTION' END,
			oidvectortypes(p.proargtypes),
			pg_get_userbyid(p.proowner)
		FROM pg_proc p
		WHERE p.pronamespace = quote_ident(current_schema())::regnamespace
		AND p.prokind IN ('f', 'p', 'a')
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, oidvectortypes(p.proargtypes)
	`, func(rows *sql.Rows) (*PostgresOwner, error) {
		owner := &PostgresOwner{}

		err := rows.Scan(&owner.ObjectName, &owner.ObjectType, &owner.Arguments, &owner.Owner)

		return owner, err
	})
}

// A NULL ACL holds the default privileges, and the default privileges of a routine give
// EXECUTE to PUBLIC. acldefault gives these privileges, so a REVOKE FROM PUBLIC shows.
func getRoutinePrivileges(ctx context.Context, db *sql.DB) ([]*PostgresPrivilege, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			p.proname,
			CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END,
			oidvectortypes(p.proargtypes) AS argument_types,
			CASE acl.grantee WHEN 0 THEN 'public' ELSE pg_get_userbyid(acl.grantee)::text END AS grantee_name,
			array_to_string(array_agg(acl.privilege_type ORDER BY acl.privilege_type), ',')
		FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
		WHERE p.pronamespace = quote_ident(current_schema())::regnamespace
		AND p.prokind IN ('f', 'p', 'a')
		AND acl.grantee <> p.proowner
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		GROUP BY p.oid, p.proname, p.prokind, acl.grantee
		ORDER BY p.proname, argument_types, grantee_name
	`, func(rows *sql.Rows) (*PostgresPrivilege, error) {
		privilege := &PostgresPrivilege{}

		var granted string

		err := rows.Scan(&privilege.ObjectName, &privilege.ObjectType, &privilege.Arguments, &privilege.Grantee, &granted)
		if err != nil {
			return nil, err
		}

		privilege.Privileges = sortedPrivileges(strings.Split(granted, ","))

		return privilege, nil
	})
}

// The filter keeps the types that DiffEnumTypes, DiffDomains, and DiffCompositeTypes read.
// The row type of a table and the array type of a type belong to another object.
const managedTypeFilter = `
	t.typnamespace = quote_ident(current_schema())::regnamespace
	AND (t.typtype IN ('e', 'd') OR (t.typtype = 'c' AND EXISTS (
		SELECT 1 FROM pg_class c WHERE c.oid = t.typrelid AND c.relkind = 'c'
	)))
	AND NOT EXISTS (
		SELECT 1 FROM pg_depend d WHERE d.objid = t.oid AND d.deptype = 'e'
	)
`

func getTypeOwners(ctx context.Context, db *sql.DB) ([]*PostgresOwner, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			t.typname,
			CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END,
			pg_get_userbyid(t.typowner)
		FROM pg_type t
		WHERE `+managedTypeFilter+`
		ORDER BY t.typname
	`, func(rows *sql.Rows) (*PostgresOwner, error) {
		owner := &PostgresOwner{}

		err := rows.Scan(&owner.ObjectName, &owner.ObjectType, &owner.Owner)

		return owner, err
	})
}

// A NULL ACL holds the default privileges, and the default privileges of a type give USAGE
// to PUBLIC. acldefault gives these privileges, so a REVOKE FROM PUBLIC shows.
func getTypePrivileges(ctx context.Context, db *sql.DB) ([]*PostgresPrivilege, error) {
	return driversshared.QueryAll(ctx, db, `
		SELECT
			t.typname,
			CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END,
			CASE acl.grantee WHEN 0 THEN 'public' ELSE pg_get_userbyid(acl.grantee)::text END AS grantee_name,
			array_to_string(array_agg(acl.privilege_type ORDER BY acl.privilege_type), ',')
		FROM pg_type t, aclexplode(coalesce(t.typacl, acldefault('T', t.typowner))) acl
		WHERE `+managedTypeFilter+`
		AND acl.grantee <> t.typowner
		GROUP BY t.typname, t.typtype, acl.grantee
		ORDER BY t.typname, grantee_name
	`, func(rows *sql.Rows) (*PostgresPrivilege, error) {
		privilege := &PostgresPrivilege{}

		var granted string

		err := rows.Scan(&privilege.ObjectName, &privilege.ObjectType, &privilege.Grantee, &granted)
		if err != nil {
			return nil, err
		}

		privilege.Privileges = sortedPrivileges(strings.Split(granted, ","))

		return privilege, nil
	})
}

func (d *PostgresDriver) getColumnPrivileges(ctx context.Context, db *sql.DB) ([]*PostgresPrivilege, error) {
	privileges, err := driversshared.QueryAll(ctx, db, `
		SELECT
			c.relname,
			c.relkind,
			a.attname,
			CASE acl.grantee WHEN 0 THEN 'public' ELSE pg_get_userbyid(acl.grantee)::text END AS grantee_name,
			array_to_string(array_agg(acl.privilege_type ORDER BY acl.privilege_type), ',')
		FROM pg_class c
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped,
		aclexplode(a.attacl) acl
		WHERE c.relnamespace = quote_ident(current_schema())::regnamespace
		AND c.relkind IN ('r', 'p', 'v', 'm')
		AND acl.grantee <> c.relowner
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i')
		)
		AND c.relname <> $1
		GROUP BY c.relname, c.relkind, a.attnum, a.attname, acl.grantee
		ORDER BY c.relname, a.attnum, grantee_name
	`, func(rows *sql.Rows) (*PostgresPrivilege, error) {
		privilege := &PostgresPrivilege{}

		var relkind, granted string

		err := rows.Scan(&privilege.ObjectName, &relkind, &privilege.Column, &privilege.Grantee, &granted)
		if err != nil {
			return nil, err
		}

		privilege.ObjectType = privilegeObjectType(relkind)
		privilege.Privileges = sortedPrivileges(strings.Split(granted, ","))

		return privilege, nil
	}, driversshared.MigrationHistoryTableName)
	if err != nil {
		return nil, err
	}

	return lo.Reject(privileges, func(privilege *PostgresPrivilege, _ int) bool {
		return slices.Contains(d.IgnoreTables, privilege.ObjectName)
	}), nil
}
