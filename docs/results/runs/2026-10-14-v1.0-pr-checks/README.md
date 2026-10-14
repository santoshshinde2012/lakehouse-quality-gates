# Final PR validation

Run on 14 October 2026 for branch `codex/harden-quality-gates-wap`, based on `2200222` plus the PR branch working tree. This run follows the CLI reporting fix.

The complete, unchanged output of `make test-all` is preserved in [test-all.log](test-all.log). It records lint and formatting checks, strict type checking, **216 unit and integration tests passed with 8 live/full tests deselected**, **97% line coverage**, contract syntax validation, and the good/bad fixture demos. The good batch passed all **53 contract checks: 14 bronze, 22 silver and 17 gold**; the bad batch blocked every layer as expected.

The live workflows were not rerun for the CLI reporting change. Their earlier passing evidence remains in the separate hardening run: [live 9/9](../2026-10-14-v1.0-hardening/e2e-live.log), [full 15/15](../2026-10-14-v1.0-hardening/e2e-full.log), and [WAP 11/11](../2026-10-14-v1.0-hardening/e2e-wap.log). That run's original 214-test log and all raw live evidence remain unchanged.

Reproduce the local PR checks with:

```bash
make test-all
```
