# Contributing

1. `uv sync --all-extras && uv run pre-commit install`
2. Make the change. Follow the dependency rule in [docs/architecture.md](docs/architecture.md): domain imports
   nothing, application imports domain, and wiring happens only in `container.py`.
3. New behaviour needs tests. Use fakes in `tests/conftest.py` for logic, and `tests/integration` for anything
   that touches Soda or DuckDB.
4. `make check validate demo` must pass. CI runs the same targets.
5. Contract changes: update `docs/contracts.md` and, when behaviour changes, `fixtures/bad_batch`.
6. Add an entry under *Unreleased* in `CHANGELOG.md`; record significant decisions as an ADR in `docs/adr/`.
