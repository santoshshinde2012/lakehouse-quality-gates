# Contributing

1. `make venv env && uv run pre-commit install`
2. Make the change. Follow the dependency rule in [docs/architecture/components.md](docs/architecture/components.md): domain imports
   nothing, application imports domain, and wiring happens only in `container.py`.
3. New behaviour needs tests. Use fakes in `tests/conftest.py` for logic, and `tests/integration` for anything
   that touches Soda or DuckDB.
4. `make test-all` must pass (lint, mypy, T0 + T1 tests, contract validation, demo); CI runs the same. When you touch the
   runner, the DAG or the lakehouse integration, also run `make e2e-live` and `make e2e-full`. Every tier is described in
   [running and testing](docs/guides/running-and-testing.md).
5. Contract changes: update [docs/guides/contracts.md](docs/guides/contracts.md) and, when behaviour changes, `fixtures/bad_batch`.
6. Add an entry under a new *Unreleased* heading at the top of `CHANGELOG.md`; record significant decisions as an ADR in `docs/adr/`.
7. Docs changes: write each fact on one page and link to it from the others. Diagrams live in
   `docs/diagrams/*.mmd`; text that must appear twice is a `region` copied by an `include` (see
   `scripts/doc_snippets.py`). Run `make docs`, then `make docs-check` (links, anchors, markdownlint, sync, no
   prose written twice). The [documentation hub](docs/README.md) lists every page.
