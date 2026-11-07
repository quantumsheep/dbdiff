package driversmysql

import (
	"testing"

	driversshared "github.com/quantumsheep/dbdiff/internal/drivers/shared"
)

func TestMySQLForeignKey(t *testing.T) {
	t.Run("CreateTablesWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLCreateTableInstruction{
				Name: "authors",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLCreateTableInstruction{
				Name: "books",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
					{
						Name: "author_id",
						Type: "int",
					},
				},
				PrimaryKey: []string{"id"},
				ForeignKeys: []*MySQLForeignKey{
					{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "CASCADE",
					},
				},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("AddForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
		})
	})

	t.Run("AddForeignKeyToANewTable", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE publishers (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));
			ALTER TABLE books ADD CONSTRAINT fk_books_publisher
				FOREIGN KEY (publisher_id) REFERENCES publishers (id);
		`)
		driver.ExecOnCurrent("CREATE TABLE books (id int NOT NULL, publisher_id int, PRIMARY KEY (id));")

		// The name order puts the change of the kept table before the creation of the
		// referenced table, so the enforcement of the foreign keys goes off.
		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_publisher",
						Columns:           []string{"publisher_id"},
						ReferencedTable:   "publishers",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "NO ACTION",
						OnDelete:          "NO ACTION",
					},
				},
			},
			&MySQLCreateTableInstruction{
				Name: "publishers",
				Columns: []*MySQLColumn{
					{
						Name:    "id",
						Type:    "int",
						NotNull: true,
					},
				},
				PrimaryKey: []string{"id"},
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})

	t.Run("RemoveForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLDropForeignKeyAction{
					Name: "fk_books_author",
				},
			},
			&MySQLDropIndexInstruction{
				Name:      "fk_books_author",
				TableName: "books",
			},
		})
	})

	t.Run("DropTableWithForeignKey", func(t *testing.T) {
		driver := NewTestMySQLDriver(t)

		driver.ExecOnDesired("CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));")
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLSetForeignKeyChecksInstruction{},
			&MySQLDropTableInstruction{
				Name: "books",
			},
			&MySQLSetForeignKeyChecksInstruction{
				Enabled: true,
			},
		})
	})
}

func TestMariaDBForeignKey(t *testing.T) {
	t.Run("ForeignKey", func(t *testing.T) {
		driver := NewTestMariaDBDriver(t)

		driver.ExecOnDesired(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (
				id int NOT NULL,
				author_id int,
				PRIMARY KEY (id),
				CONSTRAINT fk_books_author FOREIGN KEY (author_id) REFERENCES authors (id)
			);
		`)
		driver.ExecOnCurrent(`
			CREATE TABLE authors (id int NOT NULL, PRIMARY KEY (id));
			CREATE TABLE books (id int NOT NULL, author_id int, PRIMARY KEY (id));
		`)

		driver.RequireDiffApplies([]driversshared.Instruction{
			&MySQLAlterTableInstruction{
				Name: "books",
				Action: &MySQLAddForeignKeyAction{
					ForeignKey: &MySQLForeignKey{
						Name:              "fk_books_author",
						Columns:           []string{"author_id"},
						ReferencedTable:   "authors",
						ReferencedColumns: []string{"id"},
						OnUpdate:          "RESTRICT",
						OnDelete:          "RESTRICT",
					},
				},
			},
		})
	})
}
