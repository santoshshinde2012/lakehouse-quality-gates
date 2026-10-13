# ADR 0002: DuckDB as the default engine, reusing lakehouse_client's connection

- Status: accepted

## Context
The lakehouse already ships `lakehouse_client.duckdb_connect()`, which attaches the Iceberg REST catalog with
vended credentials and needs no S3 keys or JVM. Soda's DuckDB data source YAML only accepts a database path,
but `DuckDBDataSourceImpl.from_existing_cursor(con, name)` accepts a live connection.

## Decision
`DuckDBProvider` gets a connection from an injected factory (by default the repo's `duckdb_connect`, imported
by path through `DQ_LDL_REPO_PATH`), runs `USE "lakehouse"`, and hands it to Soda. Contracts address tables
as `lakehouse_duckdb/<schema>/<table>`, because Soda's DuckDB dialect has a single prefix (schema). A
four-part name makes Soda's `information_schema` lookup filter on the wrong level, and the schema check
then comes back NOT_EVALUATED. We hit exactly that while building this.

## Consequences
- One catalog-attach implementation (the repo's) and no duplicated credential logic.
- The `duckdb-local` engine uses the same provider with a CSV-backed factory, so CI exercises the real
  Soda and DuckDB code path.
