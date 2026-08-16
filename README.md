# dbdiff

A tool for comparing database schemas and data. It supports multiple database engines and provides a simple way to generate migration scripts.

## Installation

### From Releases

You can download precompiled binaries from the [releases](https://github.com/quantumsheep/dbdiff/releases) assets.

### Manually

You can install dbdiff using Go:

```bash
go install github.com/quantumsheep/dbdiff/cmd/dbdiff@latest
```

## Usage

To compare two databases, use the following command:

```bash
dbdiff <source_db_connection_string> <target_db_connection_string>
```

The first argument is the source. It holds the wanted schema. The second argument is the target. The output holds the SQL statements that make the target equal to the source.

The `--driver` flag selects the database engine. The default value is `sqlite3`:

```bash
dbdiff --driver postgres postgres://user:password@localhost:5432/source postgres://user:password@localhost:5432/target
```

The `--schema` flag names the schema that the postgres driver reads. The driver reads that schema in the source database and in the target database. If you give no value, the driver reads the schema of the search path:

```bash
dbdiff --driver postgres --schema app postgres://user:password@localhost:5432/source postgres://user:password@localhost:5432/target
```

The sqlite3 driver holds no schema. A `--schema` value with that driver gives an error.

The `--data` flag adds the comparison of the rows. The default value is off, so the output holds the schema only:

```bash
dbdiff --data source.sqlite target.sqlite
```

The data section comes after the schema section, because a new row needs its table and its column. It covers each table that the source and the target both hold, and it uses the primary key of the table. A table with no primary key gets a comment line, and no row statement.

## Supported Databases

| Name       | Tables | Indexes | Constraints | Triggers | Views | Sequences | Enum types | Domains | Composite types | Functions | Aggregates | Operators | Extensions | Data |
|------------|--------|---------|-------------|----------|-------|-----------|------------|---------|-----------------|-----------|------------|-----------|------------|------|
| SQLite     | ✅      | ✅       | ✅ (foreign keys) | ✅        | ✅     | ➖         | ➖          | ➖       | ➖               | ➖         | ➖          | ➖         | ➖          | ✅    |
| PostgreSQL | ✅      | ✅       | ✅           | ✅        | ✅     | ✅         | ✅          | ✅       | ✅               | ✅         | ✅          | ✅         | ✅          | ✅    |
| MySQL      | ❌      | ❌       | ❌           | ❌        | ❌     | ❌         | ❌          | ❌       | ❌               | ❌         | ❌          | ❌         | ❌          | ❌    |

✅ supported, ❌ not supported, ➖ the engine holds no such object. A table covers its columns.

The SQLite driver compares a partial index and an index that an expression builds.

The PostgreSQL driver compares one schema. The `--schema` flag selects it. Without that flag, the search path of the connection string selects it, and the default schema is `public`.
