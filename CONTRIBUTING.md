# Contributing

1. `uv sync --all-extras && uv run pre-commit install`
2. Make the change. Follow the dependency rule in [docs/architecture/components.md](docs/architecture/components.md): domain imports
   nothing, application imports domain, and wiring happens only in `container.py`.
3. New behaviour needs tests. Use fakes in `tests/conftest.py` for logic, and `tests/integration` for anything
   that touches Soda or DuckDB.
4. `make check validate demo` must pass. CI runs the same targets.
5. Contract changes: update [docs/guides/contracts.md](docs/guides/contracts.md) and, when behaviour changes, `fixtures/bad_batch`.
6. Add an entry under a new *Unreleased* heading at the top of `CHANGELOG.md`; record significant decisions as an ADR in `docs/adr/`.
7. Docs changes: keep diagrams in `docs/diagrams/*.mmd` (run `make diagrams`), and run `make docs-check`
   (links, anchors, markdownlint). The [documentation hub](docs/README.md) lists every page.
