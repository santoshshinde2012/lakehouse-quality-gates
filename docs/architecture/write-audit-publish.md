# Write-audit-publish (proposed)

> [!IMPORTANT]
> **Not implemented yet.** Today a gate stops *promotion*, but Spark has already overwritten the layer it checks
> (`CREATE OR REPLACE`), so readers can see a bad layer until it is rolled back. Write-audit-publish closes that gap.

**Goal:** readers of `main` never see a batch that has not passed its gate.

## Sequence

How Iceberg branches would keep a layer hidden from readers until its gate passes.

<!-- diagram:wap-sequence -->
```mermaid
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
%% Title: Write-audit-publish (proposed)
%% Purpose: How Iceberg branches would hide a layer until its gate passes
%% Last updated: 2026-10-13
%% Theme: docs/diagrams/THEME.md (shared init line above). Edit here, then run: make diagrams
sequenceDiagram
  autonumber
  participant AF as Airflow DAG
  participant SP as Spark job
  participant LK as Lakekeeper (Iceberg)
  participant DQ as soda-dq gate
  participant PB as Publisher (Spark)
  AF->>SP: run layer, spark.wap.branch = audit_RUN
  SP->>LK: commit snapshot to branch audit_RUN
  Note over LK: main is unchanged: readers still<br/>see the last published snapshot
  AF->>DQ: soda-dq verify LAYER --branch audit_RUN
  DQ->>LK: read FOR VERSION AS OF 'audit_RUN'
  DQ-->>AF: GateReport
  alt passed or passed with warnings
    AF->>PB: publish
    PB->>LK: CALL system.fast_forward(t, 'main', 'audit_RUN')
    Note over PB,LK: Spark publishes because<br/>Trino 483 cannot fast-forward branches
  else blocked
    DQ-->>AF: exit 1, task fails, downstream skipped
    Note over LK: branch is never published<br/>and expires by retention
  end
```
<!-- /diagram -->

**How to read it:**

- This is a **proposal**; nothing on this page is implemented yet.
- Numbers give the order of steps; notes mark the key decisions.
- Readers of `main` never see the `audit_RUN` branch: a blocked batch is simply never published.
- Publishing runs on Spark because Trino 483 cannot create or fast-forward branches.

## Design

1. **Table properties** (once per table, Spark): `ALTER TABLE t SET TBLPROPERTIES ('write.wap.enabled'='true')`.
2. **Write** (Spark job): `spark.conf.set("spark.wap.branch", f"audit_{run_id}")`, then the existing
   writes. On Spark 4.1 + Iceberg 1.12 a session WAP branch can create the branch (Iceberg PR #15917).
   Alternative: `INSERT INTO t.branch_audit_<run_id>`. `spark.wap.id` and `spark.wap.branch` are mutually
   exclusive (Iceberg issue #11528).
3. **Audit** (`soda-dq`): a new `--branch audit_<run_id>` option. `FileContractRepository` renders the
   dataset to a branch-aware form: DuckDB and Trino read `FOR VERSION AS OF 'audit_<run_id>'` (Trino 483
   supports reading branches); `soda-sparkdf` can read `t.branch_audit_<run_id>` in Spark instead. Reference
   datasets in the same run read the same branch.
4. **Publish** (new `Publisher` port, `SparkFastForwardPublisher` adapter, Airflow task after each gate):
   `CALL lakehouse.system.fast_forward('silver.orders', 'main', 'audit_<run_id>')`, one call per table.
5. **Rollback:** if a gate blocks, do nothing: `main` is untouched and the branch expires through
   retention (`ALTER TABLE t CREATE BRANCH ... RETAIN 7 DAYS`, Lakekeeper keeps referenced snapshots).
   If a published batch is later found bad, `CALL system.rollback_to_snapshot` or `set_current_snapshot`.
6. **Multi-table:** each table is fast-forwarded separately; there is no atomic multi-table publish on this
   catalog. Publish order follows dependencies (customers before orders), and gold is published last.

**Effort:** 3 to 5 days (Spark write changes 1 d, publisher port and DAG task 1 d, branch-aware dataset
rendering 0.5 to 1 d, live tests and docs 1 to 2 d). Requires changes in `local-data-lakehouse`'s Spark
jobs, so it needs agreement from that repo's owners.

---

[← Observability](observability.md) · [Architecture index](README.md) · [Design principles →](design-principles.md)
