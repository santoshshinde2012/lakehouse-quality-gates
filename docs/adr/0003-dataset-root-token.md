# ADR 0003: Engine-neutral contracts via a `{{ dataset_root }}` token

- Status: accepted

## Context

The same table is `lakehouse_duckdb/silver/orders` on DuckDB and `lakehouse_trino/lakehouse/silver/orders`
on Trino. Soda 4.26 does not resolve `${var.*}` inside the `dataset:` key: we tried it, and it fails with
"Variable 'DATASET_ROOT' was used and not declared".

## Decision

`FileContractRepository` replaces `{{ dataset_root }}` (also inside `valid_reference_data.dataset`) before
Soda reads the file, and writes the rendered copy to `reports/rendered/`. Unknown tokens raise an error.
Soda's own `${var.*}` placeholders are left untouched.

## Consequences

One contract per table for every engine. Rendered files can be inspected, and `soda-dq list -e trino`
shows the names.
