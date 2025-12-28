# dbdiff

A tool for comparing database schemas and data. It supports multiple database engines and provides a simple way to generate migration scripts.

## Installation

You can install dbdiff using Go:

```bash
go install github.com/quantumsheep/dbdiff/cmd/dbdiff@latest
```

## Usage

To compare two databases, use the following command:

```bash
dbdiff <source_db_connection_string> <target_db_connection_string>
```

This will output the differences between the two databases in SQL format.

## Supported Databases

| Name       | Tables | Indexes | Triggers | Data |
|------------|--------|---------|----------|------|
| SQLite     | ✅      | ✅       | ✅        | ❌    |
| PostgreSQL | ❌      | ❌       | ❌        | ❌    |
| MySQL      | ❌      | ❌       | ❌        | ❌    |
