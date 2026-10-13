# ADR 0005: Ports and adapters with a single composition root

- Status: accepted

## Decision

`domain` (dataclasses and `Protocol` ports, standard library only) ← `application` (policy and use cases) ←
`adapters` (Soda, DuckDB, Trino, files, Slack). The only module that instantiates concrete classes is
`container.py`. Configuration comes from `pydantic-settings` (`DQ_*`) and logging from `structlog` (JSON in
containers).

## Consequences

The application logic is unit-tested with in-memory fakes and no mocking library. Integration tests run real
Soda on DuckDB. A new sink, notifier or engine means one adapter plus one line of wiring.
