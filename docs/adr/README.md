# Architecture decision records

Each ADR records one decision, its context and its consequences. Status values: *proposed*, *accepted*,
*superseded*.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-soda-core-v4-proprietary-licence.md) | Use Soda Core v4 despite its non-OSI licence (amended: the licence is ELv2) | accepted |
| [0002](0002-duckdb-existing-cursor.md) | Hand Soda the lakehouse's DuckDB connection through an existing cursor | accepted |
| [0003](0003-dataset-root-token.md) | One `{{ dataset_root }}` token in contracts, rendered per engine | accepted |
| [0004](0004-separate-gated-dag.md) | A separate gated Airflow DAG next to the original; write-audit-publish later | accepted |
| [0005](0005-ports-and-adapters.md) | Ports and adapters with a single composition root | accepted |
| [0006](0006-write-audit-publish.md) | Write-audit-publish with Iceberg audit branches, DuckDB pinned-snapshot audit and Spark fast-forward | accepted |

New ADR: copy the shape of an existing file, number it next, and add it to this table.
