package driversmysql

import (
	"context"
	"database/sql"
	"slices"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
	"github.com/samber/lo"
)

type MySQLPrivilegeSet struct {
	// Grantee holds the quoted account text of the catalog, for example 'reader'@'%'.
	Grantee string

	// An empty table name marks the privileges of the whole database.
	TableName string

	Privileges []string

	// A set of column privileges holds one privilege and the columns of that privilege.
	Columns []string

	Grantable bool
}

func (s *MySQLPrivilegeSet) sameTarget(other *MySQLPrivilegeSet) bool {
	return s.Grantee == other.Grantee && s.TableName == other.TableName &&
		(s.Columns == nil || s.Privileges[0] == other.Privileges[0])
}

func (s *MySQLPrivilegeSet) items() []string {
	if s.Columns != nil {
		return s.Columns
	}

	return s.Privileges
}

func (s *MySQLPrivilegeSet) withItems(items []string) *MySQLPrivilegeSet {
	set := *s

	if s.Columns != nil {
		set.Columns = items
	} else {
		set.Privileges = items
	}

	return &set
}

func (s *MySQLPrivilegeSet) grantInstruction(withGrantOption bool) *MySQLGrantInstruction {
	return &MySQLGrantInstruction{
		Privileges:      s.Privileges,
		Columns:         s.Columns,
		TableName:       s.TableName,
		Grantee:         s.Grantee,
		WithGrantOption: withGrantOption,
	}
}

func (s *MySQLPrivilegeSet) revokeInstruction() *MySQLRevokeInstruction {
	return &MySQLRevokeInstruction{
		Privileges: s.Privileges,
		Columns:    s.Columns,
		TableName:  s.TableName,
		Grantee:    s.Grantee,
	}
}

func (d *MySQLDriver) DiffPrivileges(ctx context.Context) ([]driversshared.Instruction, error) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction
	var allDesiredSets []*MySQLPrivilegeSet
	var allCurrentSets []*MySQLPrivilegeSet

	for _, getSets := range []func(context.Context, *sql.DB) ([]*MySQLPrivilegeSet, error){
		d.GetPrivileges,
		d.GetColumnPrivileges,
	} {
		desiredSets, err := getSets(ctx, d.DesiredDatabaseConnection)
		if err != nil {
			return nil, err
		}

		currentSets, err := getSets(ctx, d.CurrentDatabaseConnection)
		if err != nil {
			return nil, err
		}

		setAdditions, setRemovals := diffPrivilegeSets(desiredSets, currentSets)
		additions = append(additions, setAdditions...)
		removals = append(removals, setRemovals...)
		allDesiredSets = append(allDesiredSets, desiredSets...)
		allCurrentSets = append(allCurrentSets, currentSets...)
	}

	removals = append(removals, columnGrantOptionRevokes(allDesiredSets, allCurrentSets)...)

	return append(additions, removals...), nil
}

// MySQL keeps one grant option for each grantee and table, and the column privileges share
// it. The revoke of a column privilege keeps the grant option. MySQL refuses a second
// REVOKE GRANT OPTION, and the diff of a table set revokes the grant option already.
func columnGrantOptionRevokes(
	desiredSets []*MySQLPrivilegeSet,
	currentSets []*MySQLPrivilegeSet,
) []driversshared.Instruction {
	var revokedTargets []*MySQLPrivilegeSet

	for _, currentSet := range currentSets {
		target := &MySQLPrivilegeSet{
			Grantee:    currentSet.Grantee,
			TableName:  currentSet.TableName,
			Privileges: []string{"GRANT OPTION"},
		}

		keptInDesired := lo.ContainsBy(desiredSets, func(desiredSet *MySQLPrivilegeSet) bool {
			return target.sameTarget(desiredSet) && desiredSet.Grantable
		})
		revokedByTableSet := lo.ContainsBy(currentSets, func(otherSet *MySQLPrivilegeSet) bool {
			return otherSet.Columns == nil && target.sameTarget(otherSet)
		})

		skipped := currentSet.Columns == nil || !currentSet.Grantable || keptInDesired || revokedByTableSet ||
			lo.ContainsBy(revokedTargets, target.sameTarget)
		if skipped {
			continue
		}

		revokedTargets = append(revokedTargets, target)
	}

	return lo.Map(revokedTargets, func(target *MySQLPrivilegeSet, _ int) driversshared.Instruction {
		return target.revokeInstruction()
	})
}

func diffPrivilegeSets(
	desiredSets []*MySQLPrivilegeSet,
	currentSets []*MySQLPrivilegeSet,
) ([]driversshared.Instruction, []driversshared.Instruction) {
	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	for _, desiredSet := range desiredSets {
		currentSet, found := lo.Find(currentSets, desiredSet.sameTarget)
		if !found {
			additions = append(additions, desiredSet.grantInstruction(desiredSet.Grantable))

			continue
		}

		missing := lo.Without(desiredSet.items(), currentSet.items()...)

		// A new GRANT OPTION needs a whole GRANT statement, so the statement repeats the
		// privileges that the grantee holds already.
		if desiredSet.Grantable && !currentSet.Grantable {
			additions = append(additions, desiredSet.grantInstruction(true))
		} else if len(missing) > 0 {
			additions = append(additions, desiredSet.withItems(missing).grantInstruction(desiredSet.Grantable))
		}

		if desiredSet.Columns == nil && !desiredSet.Grantable && currentSet.Grantable {
			removals = append(removals, desiredSet.withItems([]string{"GRANT OPTION"}).revokeInstruction())
		}

		extra := lo.Without(currentSet.items(), desiredSet.items()...)
		if len(extra) > 0 {
			removals = append(removals, desiredSet.withItems(extra).revokeInstruction())
		}
	}

	for _, currentSet := range currentSets {
		if lo.ContainsBy(desiredSets, currentSet.sameTarget) {
			continue
		}

		removedSet := currentSet
		if currentSet.Columns == nil && currentSet.Grantable {
			removedSet = currentSet.withItems(append(slices.Clone(currentSet.Privileges), "GRANT OPTION"))
		}

		removals = append(removals, removedSet.revokeInstruction())
	}

	return additions, removals
}

// The read skips a grant of a dropped table, because the grant tables keep such a row and
// the diff can revoke nothing on an absent table.
func (d *MySQLDriver) GetPrivileges(ctx context.Context, db *sql.DB) ([]*MySQLPrivilegeSet, error) {
	sets, err := d.getPrivilegeSets(ctx, db, `
		SELECT GRANTEE, '' AS TABLE_NAME, '' AS COLUMN_NAME, PRIVILEGE_TYPE, IS_GRANTABLE
		FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY GRANTEE, PRIVILEGE_TYPE;
	`)
	if err != nil {
		return nil, err
	}

	tableSets, err := d.getPrivilegeSets(ctx, db, `
		SELECT p.GRANTEE, p.TABLE_NAME, '' AS COLUMN_NAME, p.PRIVILEGE_TYPE, p.IS_GRANTABLE
		FROM information_schema.TABLE_PRIVILEGES p
		WHERE p.TABLE_SCHEMA = DATABASE()
		AND EXISTS (
			SELECT 1 FROM information_schema.TABLES t
			WHERE t.TABLE_SCHEMA = p.TABLE_SCHEMA AND t.TABLE_NAME = p.TABLE_NAME
		)
		ORDER BY p.GRANTEE, p.TABLE_NAME, p.PRIVILEGE_TYPE;
	`)
	if err != nil {
		return nil, err
	}

	return append(sets, tableSets...), nil
}

// The read skips a grant of a dropped table, because the grant tables keep such a row and
// the diff can revoke nothing on an absent table.
func (d *MySQLDriver) GetColumnPrivileges(ctx context.Context, db *sql.DB) ([]*MySQLPrivilegeSet, error) {
	return d.getPrivilegeSets(ctx, db, `
		SELECT p.GRANTEE, p.TABLE_NAME, p.COLUMN_NAME, p.PRIVILEGE_TYPE, p.IS_GRANTABLE
		FROM information_schema.COLUMN_PRIVILEGES p
		WHERE p.TABLE_SCHEMA = DATABASE()
		AND EXISTS (
			SELECT 1 FROM information_schema.TABLES t
			WHERE t.TABLE_SCHEMA = p.TABLE_SCHEMA AND t.TABLE_NAME = p.TABLE_NAME
		)
		ORDER BY p.GRANTEE, p.TABLE_NAME, p.PRIVILEGE_TYPE, p.COLUMN_NAME;
	`)
}

// An empty column name marks a privilege of a table or of the whole database.
func (d *MySQLDriver) getPrivilegeSets(ctx context.Context, db *sql.DB, query string) ([]*MySQLPrivilegeSet, error) {
	rowSets, err := driversshared.QueryAll(ctx, db, query, func(rows *sql.Rows) (*MySQLPrivilegeSet, error) {
		var columnName, privilege, grantable string

		set := &MySQLPrivilegeSet{}

		err := rows.Scan(&set.Grantee, &set.TableName, &columnName, &privilege, &grantable)
		if err != nil {
			return nil, err
		}

		set.Privileges = []string{privilege}
		set.Grantable = grantable == "YES"

		if columnName != "" {
			set.Columns = []string{columnName}
		}

		return set, nil
	})
	if err != nil {
		return nil, err
	}

	var sets []*MySQLPrivilegeSet

	for _, rowSet := range rowSets {
		if slices.Contains(d.IgnoreTables, rowSet.TableName) {
			continue
		}

		set, found := lo.Find(sets, rowSet.sameTarget)
		if !found {
			sets = append(sets, rowSet)

			continue
		}

		if set.Columns != nil {
			set.Columns = append(set.Columns, rowSet.Columns...)
		} else {
			set.Privileges = append(set.Privileges, rowSet.Privileges...)
		}

		set.Grantable = set.Grantable || rowSet.Grantable
	}

	return sets, nil
}
