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
	Grantable  bool
}

type MySQLColumnPrivilegeSet struct {
	Grantee   string
	TableName string
	Privilege string
	Columns   []string
	Grantable bool
}

func (d *MySQLDriver) DiffPrivileges(ctx context.Context) ([]driversshared.Instruction, error) {
	targetSets, err := d.GetPrivileges(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceSets, err := d.GetPrivileges(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	findSet := func(sets []*MySQLPrivilegeSet, grantee string, tableName string) (*MySQLPrivilegeSet, bool) {
		return lo.Find(sets, func(set *MySQLPrivilegeSet) bool {
			return set.Grantee == grantee && set.TableName == tableName
		})
	}

	var additions []driversshared.Instruction
	var removals []driversshared.Instruction

	for _, targetSet := range targetSets {
		sourceSet, found := findSet(sourceSets, targetSet.Grantee, targetSet.TableName)
		if !found {
			additions = append(additions, &MySQLGrantInstruction{
				Privileges:      targetSet.Privileges,
				TableName:       targetSet.TableName,
				Grantee:         targetSet.Grantee,
				WithGrantOption: targetSet.Grantable,
			})

			continue
		}

		missing := lo.Filter(targetSet.Privileges, func(privilege string, _ int) bool {
			return !lo.Contains(sourceSet.Privileges, privilege)
		})

		// A new GRANT OPTION needs a whole GRANT statement, so the statement repeats the
		// privileges that the grantee holds already.
		if targetSet.Grantable && !sourceSet.Grantable {
			additions = append(additions, &MySQLGrantInstruction{
				Privileges:      targetSet.Privileges,
				TableName:       targetSet.TableName,
				Grantee:         targetSet.Grantee,
				WithGrantOption: true,
			})
		} else if len(missing) > 0 {
			additions = append(additions, &MySQLGrantInstruction{
				Privileges:      missing,
				TableName:       targetSet.TableName,
				Grantee:         targetSet.Grantee,
				WithGrantOption: targetSet.Grantable,
			})
		}

		if !targetSet.Grantable && sourceSet.Grantable {
			removals = append(removals, &MySQLRevokeInstruction{
				Privileges: []string{"GRANT OPTION"},
				TableName:  targetSet.TableName,
				Grantee:    targetSet.Grantee,
			})
		}

		extra := lo.Filter(sourceSet.Privileges, func(privilege string, _ int) bool {
			return !lo.Contains(targetSet.Privileges, privilege)
		})
		if len(extra) > 0 {
			removals = append(removals, &MySQLRevokeInstruction{
				Privileges: extra,
				TableName:  targetSet.TableName,
				Grantee:    targetSet.Grantee,
			})
		}
	}

	for _, sourceSet := range sourceSets {
		_, found := findSet(targetSets, sourceSet.Grantee, sourceSet.TableName)
		if found {
			continue
		}

		privileges := sourceSet.Privileges
		if sourceSet.Grantable {
			privileges = append(slices.Clone(privileges), "GRANT OPTION")
		}

		removals = append(removals, &MySQLRevokeInstruction{
			Privileges: privileges,
			TableName:  sourceSet.TableName,
			Grantee:    sourceSet.Grantee,
		})
	}

	targetColumnSets, err := d.GetColumnPrivileges(ctx, d.TargetDatabaseConnection)
	if err != nil {
		return nil, err
	}

	sourceColumnSets, err := d.GetColumnPrivileges(ctx, d.SourceDatabaseConnection)
	if err != nil {
		return nil, err
	}

	findColumnSet := func(sets []*MySQLColumnPrivilegeSet, grantee string, tableName string, privilege string) (*MySQLColumnPrivilegeSet, bool) {
		return lo.Find(sets, func(set *MySQLColumnPrivilegeSet) bool {
			return set.Grantee == grantee && set.TableName == tableName && set.Privilege == privilege
		})
	}

	for _, targetSet := range targetColumnSets {
		sourceSet, found := findColumnSet(sourceColumnSets, targetSet.Grantee, targetSet.TableName, targetSet.Privilege)
		if !found {
			additions = append(additions, &MySQLGrantInstruction{
				Privileges:      []string{targetSet.Privilege},
				Columns:         targetSet.Columns,
				TableName:       targetSet.TableName,
				Grantee:         targetSet.Grantee,
				WithGrantOption: targetSet.Grantable,
			})

			continue
		}

		missingColumns := lo.Filter(targetSet.Columns, func(column string, _ int) bool {
			return !lo.Contains(sourceSet.Columns, column)
		})

		if targetSet.Grantable && !sourceSet.Grantable {
			additions = append(additions, &MySQLGrantInstruction{
				Privileges:      []string{targetSet.Privilege},
				Columns:         targetSet.Columns,
				TableName:       targetSet.TableName,
				Grantee:         targetSet.Grantee,
				WithGrantOption: true,
			})
		} else if len(missingColumns) > 0 {
			additions = append(additions, &MySQLGrantInstruction{
				Privileges:      []string{targetSet.Privilege},
				Columns:         missingColumns,
				TableName:       targetSet.TableName,
				Grantee:         targetSet.Grantee,
				WithGrantOption: targetSet.Grantable,
			})
		}

		extraColumns := lo.Filter(sourceSet.Columns, func(column string, _ int) bool {
			return !lo.Contains(targetSet.Columns, column)
		})
		if len(extraColumns) > 0 {
			removals = append(removals, &MySQLRevokeInstruction{
				Privileges: []string{targetSet.Privilege},
				Columns:    extraColumns,
				TableName:  targetSet.TableName,
				Grantee:    targetSet.Grantee,
			})
		}
	}

	for _, sourceSet := range sourceColumnSets {
		_, found := findColumnSet(targetColumnSets, sourceSet.Grantee, sourceSet.TableName, sourceSet.Privilege)
		if found {
			continue
		}

		removals = append(removals, &MySQLRevokeInstruction{
			Privileges: []string{sourceSet.Privilege},
			Columns:    sourceSet.Columns,
			TableName:  sourceSet.TableName,
			Grantee:    sourceSet.Grantee,
		})
	}

	return append(additions, removals...), nil
}

// The read skips a grant of a dropped table, because the grant tables keep such a row and
// the diff can revoke nothing on an absent table.
func (d *MySQLDriver) GetPrivileges(ctx context.Context, db *sql.DB) ([]*MySQLPrivilegeSet, error) {
	sets, err := d.getPrivilegeSets(ctx, db, `
		SELECT GRANTEE, '' AS TABLE_NAME, PRIVILEGE_TYPE, IS_GRANTABLE
		FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY GRANTEE, PRIVILEGE_TYPE;
	`)
	if err != nil {
		return nil, err
	}

	tableSets, err := d.getPrivilegeSets(ctx, db, `
		SELECT p.GRANTEE, p.TABLE_NAME, p.PRIVILEGE_TYPE, p.IS_GRANTABLE
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

func (d *MySQLDriver) getPrivilegeSets(ctx context.Context, db *sql.DB, query string) ([]*MySQLPrivilegeSet, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var sets []*MySQLPrivilegeSet

	for rows.Next() {
		var grantee, tableName, privilege, grantable string

		err := rows.Scan(&grantee, &tableName, &privilege, &grantable)
		if err != nil {
			return nil, err
		}

		set, found := lo.Find(sets, func(set *MySQLPrivilegeSet) bool {
			return set.Grantee == grantee && set.TableName == tableName
		})
		if !found {
			set = &MySQLPrivilegeSet{
				Grantee:   grantee,
				TableName: tableName,
			}

			sets = append(sets, set)
		}

		set.Privileges = append(set.Privileges, privilege)

		if grantable == "YES" {
			set.Grantable = true
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return sets, nil
}

// The read skips a grant of a dropped table, because the grant tables keep such a row and
// the diff can revoke nothing on an absent table.
func (d *MySQLDriver) GetColumnPrivileges(ctx context.Context, db *sql.DB) ([]*MySQLColumnPrivilegeSet, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT p.GRANTEE, p.TABLE_NAME, p.COLUMN_NAME, p.PRIVILEGE_TYPE, p.IS_GRANTABLE
		FROM information_schema.COLUMN_PRIVILEGES p
		WHERE p.TABLE_SCHEMA = DATABASE()
		AND EXISTS (
			SELECT 1 FROM information_schema.TABLES t
			WHERE t.TABLE_SCHEMA = p.TABLE_SCHEMA AND t.TABLE_NAME = p.TABLE_NAME
		)
		ORDER BY p.GRANTEE, p.TABLE_NAME, p.PRIVILEGE_TYPE, p.COLUMN_NAME;
	`)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var sets []*MySQLColumnPrivilegeSet

	for rows.Next() {
		var grantee, tableName, columnName, privilege, grantable string

		err := rows.Scan(&grantee, &tableName, &columnName, &privilege, &grantable)
		if err != nil {
			return nil, err
		}

		set, found := lo.Find(sets, func(set *MySQLColumnPrivilegeSet) bool {
			return set.Grantee == grantee && set.TableName == tableName && set.Privilege == privilege
		})
		if !found {
			set = &MySQLColumnPrivilegeSet{
				Grantee:   grantee,
				TableName: tableName,
				Privilege: privilege,
			}

			sets = append(sets, set)
		}

		set.Columns = append(set.Columns, columnName)

		if grantable == "YES" {
			set.Grantable = true
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return sets, nil
}
