package driverspostgres

import (
	"slices"
	"strings"
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

func missingPrivileges(first []string, second []string) []string {
	var missing []string

	for _, privilege := range first {
		if !slices.Contains(second, privilege) {
			missing = append(missing, privilege)
		}
	}

	return missing
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
