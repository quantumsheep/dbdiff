package driverspostgres

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

// A role belongs to the server and not to the schema. A source server holds other role
// names in most cases, and a GRANT statement of an absent role fails.
type PostgresPrivilege struct {
	ObjectType string
	ObjectName string
	Grantee    string
	Privileges []string
}

func (p *PostgresPrivilege) Key() string {
	return p.ObjectType + " " + p.ObjectName + " " + p.Grantee
}

func (p *PostgresPrivilege) GrantInstruction(privileges []string) *PostgresGrantInstruction {
	return &PostgresGrantInstruction{
		Privileges: privileges,
		ObjectType: p.ObjectType,
		ObjectName: p.ObjectName,
		Grantee:    p.Grantee,
	}
}

func (p *PostgresPrivilege) RevokeInstruction(privileges []string) *PostgresRevokeInstruction {
	return &PostgresRevokeInstruction{
		Privileges: privileges,
		ObjectType: p.ObjectType,
		ObjectName: p.ObjectName,
		Grantee:    p.Grantee,
	}
}

type PostgresOwner struct {
	ObjectType string
	ObjectName string
	Owner      string
}

func (o *PostgresOwner) SetInstruction() *PostgresSetOwnerInstruction {
	return &PostgresSetOwnerInstruction{
		ObjectType: o.ObjectType,
		ObjectName: o.ObjectName,
		Owner:      o.Owner,
	}
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

	targetOwners, err := d.GetOwners(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceOwners, err := d.GetOwners(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetOwner := range targetOwners {
		sourceOwner, found := lo.Find(sourceOwners, func(owner *PostgresOwner) bool {
			return owner.ObjectName == targetOwner.ObjectName
		})
		if found && sourceOwner.Owner == targetOwner.Owner &&
			!d.recreatedObjectNames[targetOwner.ObjectName] {
			continue
		}

		additions = append(additions, targetOwner.SetInstruction())
	}

	targetPrivileges, err := d.GetPrivileges(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourcePrivileges, err := d.GetPrivileges(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	for _, targetPrivilege := range targetPrivileges {
		sourcePrivilege, found := lo.Find(sourcePrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == targetPrivilege.Key()
		})

		// The diff drops and creates the object again, and the new object holds the
		// default privileges, so every grant of the target comes back.
		if !found || d.recreatedObjectNames[targetPrivilege.ObjectName] {
			additions = append(additions,
				targetPrivilege.GrantInstruction(targetPrivilege.Privileges))

			continue
		}

		granted := lo.Without(targetPrivilege.Privileges, sourcePrivilege.Privileges...)
		if len(granted) > 0 {
			additions = append(additions, targetPrivilege.GrantInstruction(granted))
		}

		revoked := lo.Without(sourcePrivilege.Privileges, targetPrivilege.Privileges...)
		if len(revoked) > 0 {
			additions = append(additions, targetPrivilege.RevokeInstruction(revoked))
		}
	}

	targetObjectNames := lo.Map(targetOwners, func(owner *PostgresOwner, _ int) string {
		return owner.ObjectName
	})

	for _, sourcePrivilege := range sourcePrivileges {
		// The diff drops an object that the target does not hold, and the drop removes
		// every privilege of the object.
		if !slices.Contains(targetObjectNames, sourcePrivilege.ObjectName) ||
			d.recreatedObjectNames[sourcePrivilege.ObjectName] {
			continue
		}

		_, found := lo.Find(targetPrivileges, func(privilege *PostgresPrivilege) bool {
			return privilege.Key() == sourcePrivilege.Key()
		})
		if !found {
			additions = append(additions,
				sourcePrivilege.RevokeInstruction(sourcePrivilege.Privileges))
		}
	}

	return &SectionDiff{Additions: additions}, nil
}

func (d *PostgresDriver) GetOwners(ctx context.Context, db *sql.DB) ([]*PostgresOwner, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.relname, c.relkind, pg_get_userbyid(c.relowner)
		FROM pg_class c
		WHERE c.relnamespace = current_schema()::regnamespace
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

	return owners, nil
}

// PostgreSQL gives the privileges of the owner with the object.
func (d *PostgresDriver) GetPrivileges(ctx context.Context, db *sql.DB) ([]*PostgresPrivilege, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			c.relname,
			c.relkind,
			pg_get_userbyid(acl.grantee),
			array_to_string(array_agg(acl.privilege_type ORDER BY acl.privilege_type), ',')
		FROM pg_class c, aclexplode(c.relacl) acl
		WHERE c.relnamespace = current_schema()::regnamespace
		AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
		AND acl.grantee <> c.relowner
		AND acl.grantee <> 0
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			WHERE d.objid = c.oid AND d.deptype IN ('e', 'a', 'i')
		)
		AND c.relname <> $1
		GROUP BY c.relname, c.relkind, acl.grantee
		ORDER BY c.relname, pg_get_userbyid(acl.grantee)
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

	return privileges, nil
}
