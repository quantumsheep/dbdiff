<div align="center">

# dbdiff

**dbdiff reads the schema of two databases. Then it prints the SQL statements that make the first schema equal to the second one.**

![Tests](https://github.com/quantumsheep/dbdiff/actions/workflows/test.yaml/badge.svg)
![Release](https://img.shields.io/github/v/release/quantumsheep/dbdiff?label=release)
![Go Reference](https://pkg.go.dev/badge/github.com/quantumsheep/dbdiff.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

</div>

```console
$ dbdiff diff current.sqlite final.sqlite
ALTER TABLE "users" ADD COLUMN "created_at" TEXT;
CREATE INDEX "users_email" ON "users" ("email");
DROP TABLE "audit";
CREATE VIEW active_users AS SELECT id, email FROM users;
```

- **Three engines.** dbdiff supports SQLite, PostgreSQL, and MySQL with MariaDB.
- **Databases and files.** Each side is a database, a `.sql` file, or a directory of `.sql` migration files.
- **Write on request.** `dbdiff diff` prints the statements and changes no database. `dbdiff migrate up` applies migration files to a database.
- **Rows.** The `--data` flag adds the comparison of the rows.

## Installation

Download a binary from the [releases](https://github.com/quantumsheep/dbdiff/releases) page, or install with Go:

```bash
go install github.com/quantumsheep/dbdiff/cmd/dbdiff@latest
```

> [!NOTE]
> The SQLite driver is a C binding. If the build fails with an undefined symbol, set `CGO_ENABLED=1` before the build.

## Usage


| Command                          | Result                                                            |
| -------------------------------- | ----------------------------------------------------------------- |
| `dbdiff diff <source> <target>`  | Print the statements that make the source equal to the target     |
| `dbdiff migrate generate <name>` | Write the next migration file                                     |
| `dbdiff migrate status`          | List each migration and its state                                 |
| `dbdiff migrate preview`         | Print the statements that `up` will run                           |
| `dbdiff migrate up`              | Apply every pending migration                                     |
| `dbdiff migrate step`            | Apply every pending migration, and ask before each file           |
| `dbdiff migrate verify`          | Compare the database against the replay of the applied migrations |
| `dbdiff migrate repair`          | Make the history table agree with the migration files             |
| `dbdiff migrate baseline`        | Record every migration file as applied, and run no file           |


The source holds the current schema. The target holds the final schema. The output changes the source into the target.

```bash
dbdiff diff current.sqlite final.sqlite
dbdiff diff current.sqlite schema.sql
dbdiff diff current.sqlite ./migrations
dbdiff diff --driver postgres --schema app \
  postgres://user:password@localhost:5432/source \
  postgres://user:password@localhost:5432/target
dbdiff diff \
  mysql://user:password@localhost:3306/source \
  mysql://user:password@localhost:3306/target
```

dbdiff writes one SQL statement per line to the standard output. It changes no database. To apply the statements, send them to the client of the engine:

```bash
dbdiff diff current.sqlite final.sqlite | sqlite3 current.sqlite
```

> [!CAUTION]
> Read the output before you apply it. A statement can delete a table, a column, or a row of the source database. dbdiff holds no rollback.

### Flags of `diff`


| Flag           | Value                             | Purpose                                                                                       |
| -------------- | --------------------------------- | --------------------------------------------------------------------------------------------- |
| `--driver`     | `sqlite3`, `postgres`, or `mysql` | Select the database engine. The default value comes from the arguments.                       |
| `--schema`     | A schema name                     | Name the schema that the postgres driver reads. The default is the schema of the search path. |
| `--data`       | none                              | Add the comparison of the rows.                                                               |
| `--comments`   | none                              | Print a comment before each object that the output changes.                                   |
| `--privileges` | none                              | Add the comparison of the owner and the privileges (postgres and mysql).                      |
| `--exit-code`  | none                              | Exit with the code 1 when the schemas differ, like `diff(1)`.                                 |
| `--ignore-table` | A table name                    | Ignore the table with this name. Repeat the flag for each table.                              |


### Driver detection

If you give no `--driver` flag, dbdiff reads the engine from the arguments. A `postgres://` URL or a keyword connection string selects `postgres`. A `mysql://` URL or a `mariadb://` URL selects `mysql`. A plain path or a `sqlite://` path selects `sqlite3`. A `.sql` file or a directory takes the engine of the other side. If both sides name SQL text, or if the two sides name different engines, give the `--driver` flag.

## SQL files

An argument names SQL text when the path ends in `.sql`, or when the path is a directory. dbdiff applies the SQL to a temporary database, and then it compares that database. A directory gives its top-level `.sql` files in the order of the names, without the `.down.sql` files. A directory that holds no `.sql` file gives an empty schema.

With the postgres driver, the temporary server downloads on the first run. It takes the major version of the database of the other side, or the `version` key of `dbdiff.yaml`.

With the mysql driver, dbdiff builds the temporary database on the server of the other side, so that other side must be a database. The connection needs the privilege to create and to drop a database.

dbdiff applies each file in one call. PostgreSQL refuses `CREATE INDEX CONCURRENTLY` and other statements of that kind inside such a call. Write the line `-- dbdiff:no-transaction` in such a file. dbdiff then runs one call for each statement. The line `-- atlas:txmode none` of Atlas gives the same result.

## Data comparison

The `--data` flag adds the comparison of the rows:

```bash
dbdiff diff --data current.sqlite final.sqlite
```

The output holds an `INSERT` statement for a key of the target only, an `UPDATE` statement for a key with a different row, and a `DELETE` statement for a key of the source only. The comparison needs the primary key of the table. A table with no primary key, or with a different primary key in the source, gets a comment line and no row statement.

## Migrations

`dbdiff migrate` reads `dbdiff.yaml` from the working directory. The `--config` flag names another path. A flag overrides the key with the same name.

```yaml
driver: postgres
source: ./migrations   # the directory of the migration files
target: ./schema.sql   # the wanted schema of `migrate generate`
schema: app
version: 17.11.0       # the version of the real PostgreSQL server
ignore:
  tables:              # the tables that the diff ignores
    - example
```

`migrate generate` compares the target against the replay of the migration files, and it writes the difference as a new file:

```console
$ dbdiff migrate generate add_created_at
migrations/20260822143000_add_created_at.sql
```

The seven other commands change or read a database, and they name that database with the `--target` flag. Do not put a production connection string in a file that goes into git. Give the flag, or set the `DBDIFF_TARGET` variable:

```bash
export DBDIFF_TARGET=postgres://user:password@localhost:5432/production
dbdiff migrate up
```

`migrate up` applies each pending file in its own transaction and records it in the table `dbdiff_migrations`. On MySQL a transaction rolls no DDL statement back, so `migrate up` marks each file with a dirty row instead. If a file fails halfway, repair the database, then run `migrate repair`. A checksum detects a file that changed after its migration ran. `migrate up` also accepts `--to <version>`, which stops the run after that version. `migrate preview` accepts `--run`, which applies every pending file in one transaction and rolls it back. `migrate preview --run` refuses the mysql driver, because MySQL commits every DDL statement at once.

`migrate baseline` records every migration file as applied, and it runs no file. Use it to adopt dbdiff on a database that already holds the schema of the files. `migrate baseline` also accepts `--to <version>`, which stops the record after that version.

A file that holds the line `-- dbdiff:no-transaction` runs outside a transaction, one call per statement.

> [!WARNING]
> The directive costs the atomicity guarantee of the file. If such a file fails halfway, repair the database, then run `migrate repair`.

### Run dbdiff from Go

The package `drivers` runs a diff:

```go
import "github.com/quantumsheep/dbdiff/drivers"

driver, err := drivers.NewDriver(drivers.SQLiteDriverName)

statements, err := driver.Diff(ctx,
	drivers.NewConnectionStringDataSource("app.sqlite"),
	drivers.NewFileDataSource("schema.sql"),
)
```

The package `migrations` applies the migrations, for example at the start of a server:

```go
import (
	"github.com/quantumsheep/dbdiff/drivers"
	"github.com/quantumsheep/dbdiff/migrations"
)

driver, err := drivers.NewDriver(drivers.PostgresDriverName)

migrator := driver.Migrator(
	migrations.WithTargetDataSource(
		drivers.NewConnectionStringDataSource(os.Getenv("DATABASE_URL")),
	),
	migrations.WithMigrationDirectory("migrations"),
)

err = migrator.Up(ctx)
```

## Supported objects


| Object                        | SQLite                                         | PostgreSQL             | MySQL / MariaDB                                |
| ----------------------------- | ---------------------------------------------- | ---------------------- | ---------------------------------------------- |
| Tables                        | ✅                                              | ✅                      | ✅                                              |
| Identity columns              | ➖                                              | ✅                      | ➖                                              |
| Table options                 | ✅ (WITHOUT ROWID, STRICT)                      | ✅ (storage parameters) | ✅ (ENGINE, collation)                          |
| Virtual tables                | ✅                                              | ➖                      | ➖                                              |
| Generated columns             | ✅                                              | ✅                      | ✅                                              |
| Column storage and statistics | ➖                                              | ✅                      | ➖                                              |
| Indexes                       | ✅                                              | ✅                      | ✅                                              |
| Constraints                   | ✅ (foreign keys, primary keys, unique, checks) | ✅                      | ✅ (foreign keys, primary keys, unique, checks) |
| Triggers                      | ✅ (of a table and of a view)                   | ✅ (with the mode)      | ✅                                              |
| Views                         | ✅                                              | ✅                      | ✅                                              |
| Materialized views            | ➖                                              | ✅                      | ➖                                              |
| Rules                         | ➖                                              | ✅                      | ➖                                              |
| Extended statistics           | ➖                                              | ✅                      | ➖                                              |
| Partitioned tables            | ➖                                              | ✅                      | ✅                                              |
| Replica identity              | ➖                                              | ✅                      | ➖                                              |
| Sequences                     | ➖                                              | ✅                      | ✅ (MariaDB)                                    |
| Enum types                    | ➖                                              | ✅                      | ➖ (the column type covers it)                  |
| Domains                       | ➖                                              | ✅                      | ➖                                              |
| Composite types               | ➖                                              | ✅                      | ➖                                              |
| Functions                     | ➖                                              | ✅                      | ✅                                              |
| Procedures                    | ➖                                              | ✅                      | ✅                                              |
| Events                        | ➖                                              | ➖                      | ✅                                              |
| Aggregates                    | ➖                                              | ✅                      | ➖                                              |
| Operators                     | ➖                                              | ✅                      | ➖                                              |
| Casts                         | ➖                                              | ✅                      | ➖                                              |
| Extensions                    | ➖                                              | ✅                      | ➖                                              |
| Comments                      | ➖                                              | ✅                      | ✅ (of a column)                                |
| Row level security            | ➖                                              | ✅                      | ➖                                              |
| Privileges                    | ➖                                              | ✅ (`--privileges`)     | ✅ (`--privileges`)                             |
| Data                          | ✅                                              | ✅                      | ✅                                              |


✅ dbdiff compares this object. ➖ the engine holds no such object. A table covers its columns. The mysql driver covers MySQL and MariaDB.

## Limits

- The PostgreSQL driver compares one schema per run. It prints no `CREATE SCHEMA` statement, and it detects no object that moved between schemas.
- The MySQL driver compares one database per run, and the connection URL names that database.
- With the MySQL driver, a diff of two SQL sources does not work. One side must be a database, because dbdiff builds a SQL source on the server of that side.
- The body of a trigger, a procedure, or a function holds semicolons. The `mysql` client splits its input at each semicolon, so give such a diff to the client with a `DELIMITER` command, or apply it with `dbdiff migrate up`.
- The `--privileges` flag of the mysql driver covers the grants of the database and of its tables. It reads no global grant, no column grant, and no routine grant, and it creates no user.
- The mysql driver ignores the STARTS clause when it compares two events, because MySQL writes the creation time into an event without that clause.
- The `--privileges` flag covers tables, views, materialized views, and sequences. It reads no default privilege of `ALTER DEFAULT PRIVILEGES`.
- dbdiff generates no down migration. Use an up migration as a down migration.
- The name `dbdiff_migrations` is reserved. `dbdiff diff` hides a table with that name.
- `migrate up` sends a whole migration file to the engine in one call. An error names the file, and not the position of the statement.

## Development

```bash
docker compose up -d                      # Start PostgreSQL, MySQL, and MariaDB
go build -o ./bin/dbdiff ./cmd/dbdiff     # Build the binary
go test ./...                             # Run the tests
```

To run the tests without the containers:

```bash
DBDIFF_TEST_SKIP_POSTGRES=1 DBDIFF_TEST_SKIP_MYSQL=1 go test -short ./...
```

## License

dbdiff uses the MIT license. Read the [LICENSE](LICENSE) file.