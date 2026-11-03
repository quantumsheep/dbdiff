package internalmigrations

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

type statementExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type SQLConnection interface {
	statementExecutor
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

// The two existence queries take the name of the history table as their one argument.
type SQLDialect struct {
	QuoteIdentifier func(name string) string
	Placeholder     func(position int) string

	CreateHistoryTableStatement string
	HistoryTableExistsQuery     string
	DirtyColumnExistsQuery      string

	DirtyColumnType string
	TrueLiteral     string
	FalseLiteral    string

	// If this field is true, the applied_at column holds RFC3339 text.
	StoresTimeAsText bool
}

type SQLMigrator struct {
	Connection SQLConnection
	Dialect    SQLDialect
}

func (m *SQLMigrator) historyTable() string {
	return m.Dialect.QuoteIdentifier(driversshared.MigrationHistoryTableName)
}

func (m *SQLMigrator) column(name string) string {
	return m.Dialect.QuoteIdentifier(name)
}

func (m *SQLMigrator) now() any {
	now := time.Now().UTC()

	if m.Dialect.StoresTimeAsText {
		return now.Format(time.RFC3339)
	}

	return now
}

func (m *SQLMigrator) EnsureHistoryTable(ctx context.Context) error {
	_, err := m.Connection.ExecContext(ctx, m.Dialect.CreateHistoryTableStatement)
	if err != nil {
		return err
	}

	return m.ensureDirtyColumn(ctx)
}

// An older dbdiff created the table without the dirty column.
func (m *SQLMigrator) ensureDirtyColumn(ctx context.Context) error {
	found, err := m.exists(ctx, m.Dialect.DirtyColumnExistsQuery)
	if err != nil {
		return err
	}

	if found {
		return nil
	}

	statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s NOT NULL DEFAULT %s;",
		m.historyTable(), m.column("dirty"), m.Dialect.DirtyColumnType, m.Dialect.FalseLiteral)

	_, err = m.Connection.ExecContext(ctx, statement)

	return err
}

func (m *SQLMigrator) exists(ctx context.Context, query string) (bool, error) {
	row := m.Connection.QueryRowContext(ctx, query, driversshared.MigrationHistoryTableName)

	found := false

	err := row.Scan(&found)
	if err != nil {
		return false, err
	}

	return found, nil
}

// A read must stay a read, so this method creates no table. The command status calls it
// against a database that never ran a migration.
func (m *SQLMigrator) AppliedMigrations(ctx context.Context) ([]AppliedMigration, error) {
	found, err := m.exists(ctx, m.Dialect.HistoryTableExistsQuery)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	// A read must stay a read, so an old table keeps its columns and the query selects
	// a constant in the place of the absent one.
	dirtyColumn := m.column("dirty")

	dirtyFound, err := m.exists(ctx, m.Dialect.DirtyColumnExistsQuery)
	if err != nil {
		return nil, err
	}

	if !dirtyFound {
		dirtyColumn = m.Dialect.FalseLiteral
	}

	statement := fmt.Sprintf("SELECT %s, %s, %s, %s, %s FROM %s ORDER BY %s;",
		m.column("version"), m.column("name"), m.column("checksum"), m.column("applied_at"), dirtyColumn,
		m.historyTable(), m.column("version"))

	rows, err := m.Connection.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rows.Close() }()

	var applied []AppliedMigration

	for rows.Next() {
		row, err := m.scanAppliedMigration(rows)
		if err != nil {
			return nil, err
		}

		applied = append(applied, row)
	}

	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return applied, nil
}

func (m *SQLMigrator) scanAppliedMigration(rows *sql.Rows) (AppliedMigration, error) {
	var row AppliedMigration

	if !m.Dialect.StoresTimeAsText {
		err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.AppliedAt, &row.Dirty)

		return row, err
	}

	var appliedAt string

	err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &appliedAt, &row.Dirty)
	if err != nil {
		return row, err
	}

	row.AppliedAt, err = time.Parse(time.RFC3339, appliedAt)

	return row, err
}

func (m *SQLMigrator) insertRecord(ctx context.Context, executor statementExecutor, migration *Migration,
	dirtyLiteral string) error {
	statement := fmt.Sprintf("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, %s, %s, %s, %s);",
		m.historyTable(),
		m.column("version"), m.column("name"), m.column("checksum"), m.column("applied_at"), m.column("dirty"),
		m.Dialect.Placeholder(1), m.Dialect.Placeholder(2), m.Dialect.Placeholder(3), m.Dialect.Placeholder(4),
		dirtyLiteral)

	_, err := executor.ExecContext(ctx, statement, migration.Version, migration.Name, migration.Checksum, m.now())

	return err
}

func (m *SQLMigrator) RecordDirty(ctx context.Context, migration *Migration) error {
	return m.insertRecord(ctx, m.Connection, migration, m.Dialect.TrueLiteral)
}

func (m *SQLMigrator) ClearDirty(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s = %s;",
		m.historyTable(), m.column("dirty"), m.Dialect.FalseLiteral, m.column("version"), m.Dialect.Placeholder(1))

	_, err := m.Connection.ExecContext(ctx, statement, migration.Version)

	return err
}

func (m *SQLMigrator) UpdateChecksum(ctx context.Context, migration *Migration) error {
	statement := fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s = %s;",
		m.historyTable(), m.column("checksum"), m.Dialect.Placeholder(1), m.column("version"), m.Dialect.Placeholder(2))

	_, err := m.Connection.ExecContext(ctx, statement, migration.Checksum, migration.Version)

	return err
}

func (m *SQLMigrator) DeleteRecord(ctx context.Context, version string) error {
	statement := fmt.Sprintf("DELETE FROM %s WHERE %s = %s;",
		m.historyTable(), m.column("version"), m.Dialect.Placeholder(1))

	_, err := m.Connection.ExecContext(ctx, statement, version)

	return err
}

func (m *SQLMigrator) Exec(ctx context.Context, statement string) error {
	_, err := m.Connection.ExecContext(ctx, statement)

	return err
}

func (m *SQLMigrator) Begin(ctx context.Context) (Transaction, error) {
	transaction, err := m.Connection.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &SQLTransaction{
		Migrator:    m,
		Transaction: transaction,
	}, nil
}

type SQLTransaction struct {
	Migrator    *SQLMigrator
	Transaction *sql.Tx
}

func (t *SQLTransaction) Apply(ctx context.Context, statement string) error {
	_, err := t.Transaction.ExecContext(ctx, statement)

	return err
}

func (t *SQLTransaction) Record(ctx context.Context, migration *Migration) error {
	return t.Migrator.insertRecord(ctx, t.Transaction, migration, t.Migrator.Dialect.FalseLiteral)
}

func (t *SQLTransaction) Commit() error {
	return t.Transaction.Commit()
}

func (t *SQLTransaction) Rollback() error {
	return t.Transaction.Rollback()
}
