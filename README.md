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

## Supported Databases

| Name       | Tables | Indexes | Constraints | Triggers | Views | Sequences | Enum types | Functions | Extensions | Data |
|------------|--------|---------|-------------|----------|-------|-----------|------------|-----------|------------|------|
| SQLite     | ✅      | ✅       | ✅ (foreign keys) | ✅        | ✅     | ➖         | ➖          | ➖         | ➖          | ❌    |
| PostgreSQL | ✅      | ✅       | ✅           | ✅        | ✅     | ✅         | ✅          | ✅         | ✅          | ❌    |
| MySQL      | ❌      | ❌       | ❌           | ❌        | ❌     | ❌         | ❌          | ❌         | ❌          | ❌    |

✅ supported, ❌ not supported, ➖ the engine holds no such object. A table covers its columns.

The PostgreSQL driver compares one schema. The search path of the connection string selects it, and the default schema is `public`.
