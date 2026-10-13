"""Write the benchmark charts (Mermaid) and tables into docs/benchmarks/results.md from the raw results.

Reads the newest ``docs/results/benchmarks/<date>-v<version>/`` folder (or ``--data DIR``) and rewrites the block
between ``<!-- bench-charts:begin -->`` and ``<!-- bench-charts:end -->``. Every section has a heading, a takeaway
computed from the data, a chart (``xychart-beta`` or ``pie``), the table behind it and a link to the raw file.
``--check`` exits 1 when the block is stale; ``make docs-check`` and the unit tests run it.

    python scripts/bench_charts.py [--data docs/results/benchmarks/2026-10-14-v0.4.1] [--check]
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "docs" / "benchmarks" / "results.md"
BENCH = ROOT / "docs" / "results" / "benchmarks"
BEGIN, END = "<!-- bench-charts:begin -->", "<!-- bench-charts:end -->"
ENGINES = {"duckdb": "host DuckDB", "trino": "Trino", "container": "container (ldl-soda)"}
LAYERS = ("bronze", "silver", "gold")
COLOURS = "indigo, amber, teal"  # the order of plotColorPalette in docs/diagrams/THEME.md

Row = dict[str, str]
SHORT = {"duplicate_in_file": "dup in file", "future_timestamp": "future ts", "malformed_email": "bad email",
         "missing_customer": "no customer", "negative_amount": "neg amount", "orphan_customer": "orphan",
         "unknown_status": "bad status"}  # fmt: skip


def latest_data(root: Path = ROOT) -> Path:
    runs = sorted(
        p for p in (root / "docs" / "results" / "benchmarks").glob("20*-v*") if (p / "perf_raw.csv").exists()
    )
    return runs[-1] if runs else root / "docs" / "results" / "benchmarks"


def read(path: Path) -> list[Row]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def theme(root: Path = ROOT) -> str:
    text = (root / "docs" / "diagrams" / "THEME.md").read_text(encoding="utf-8")
    match = re.search(r"^%%\{init:.*\}%%$", text, re.M)
    return match.group(0) if match else ""


def human(rows: int) -> str:
    return f"{rows // 1_000_000}M" if rows >= 1_000_000 else f"{rows // 1000}k"


def num(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def p50(values: Sequence[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def nearest_rank(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, int(round(q * len(ordered) + 0.5)) - 1))] if ordered else 0.0


def table(header: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def xychart(theme_line: str, title: str, x: Sequence[str], y_label: str, series: Sequence[tuple[str, Sequence[float]]],
            kind: str = "line", y_max: float | None = None) -> str:  # fmt: skip
    top = y_max if y_max is not None else max([v for _, vals in series for v in vals] + [1.0]) * 1.15
    body = [theme_line, "xychart-beta", f'  title "{title}"', "  x-axis [" + ", ".join(f'"{c}"' for c in x) + "]",
            f'  y-axis "{y_label}" 0 --> {num(top, 2)}']  # fmt: skip
    body += [f"  {kind} [" + ", ".join(num(v, 3) for v in vals) + "]" for _, vals in series]
    return "```mermaid\n" + "\n".join(body) + "\n```"


PIE = (
    "'pie1':'#4f46e5','pie2':'#a5b4fc','pie3':'#d97706','pie4':'#fcd34d','pie5':'#0d9488','pie6':'#5eead4',"
    "'pieStrokeColor':'#ffffff','pieOuterStrokeColor':'#cbd5e1','pieSectionTextColor':'#0f172a',"
)


def pie(theme_line: str, title: str, parts: Sequence[tuple[str, float]]) -> str:
    theme_line = theme_line.replace("'xyChart':", PIE + "'xyChart':")
    body = [theme_line, f"pie showData title {title}"] + [f'  "{k}" : {num(v, 2)}' for k, v in parts]
    return "```mermaid\n" + "\n".join(body) + "\n```"


def section(heading: str, takeaway: str, chart: str, tbl: str, raw: str) -> str:
    return f"### {heading}\n\n**{takeaway}**\n\n{chart}\n\n{tbl}\n\nRaw data: {raw}\n"


class Charts:
    def __init__(self, data: Path, root: Path = ROOT) -> None:
        self.data = data
        self.root = root
        self.theme = theme(root)
        self.rel = Path("..") / data.relative_to(root / "docs")
        self.perf = read(data / "perf_raw.csv")
        self.pipe = read(data / "pipeline_raw.csv")
        self.acc = read(data / "accuracy.csv")
        self.spark = read(data / "spark_reference.csv")
        self.tools = read(data / "tools.csv")
        self.af_tasks = read(data / "airflow_tasks.csv")
        self.af_runs = read(data / "airflow_runs.csv")
        specs = data / "specs.json"
        self.specs: dict[str, object] = json.loads(specs.read_text()) if specs.exists() else {}
        self.sizes = sorted({int(r["rows"]) for r in self.perf})

    def link(self, name: str) -> str:
        return f"[`{name}`]({(self.rel / name).as_posix()})"

    def warm(self, rows: int, engine: str, layer: str, key: str = "gate_s") -> list[float]:
        return [float(r[key]) for r in self.perf if int(r["rows"]) == rows and r["engine"] == engine
                and r["layer"] == layer and r["phase"] == "warm" and r[key]]  # fmt: skip

    def status(self, rows: int, engine: str, layer: str) -> str:
        states = {
            r["status"]
            for r in self.perf
            if int(r["rows"]) == rows and r["engine"] == engine and r["layer"] == layer
        }
        return "passed" if states == {"passed"} else "/".join(sorted(states))

    def medallion_p50(self, rows: int, engine: str) -> float:
        return sum(p50(self.warm(rows, engine, layer)) for layer in LAYERS)

    # ---- headline -------------------------------------------------------------------------------------
    def headline(self) -> dict[str, str]:
        big = self.sizes[-1] if self.sizes else 0
        one_m = 1_000_000 if 1_000_000 in self.sizes else big
        duck = [p50(self.warm(s, "duckdb", layer)) for s in self.sizes if s <= one_m for layer in LAYERS]
        acc_ok = sum(r["verdict_ok"] == "True" for r in self.acc)
        return {
            "duckdb_p50_range_1m": f"{num(min(duck))}–{num(max(duck))} s" if duck else "n/a",
            "medallion_duckdb_big": f"{num(self.medallion_p50(big, 'duckdb'))} s",
            "medallion_trino_big": f"{num(self.medallion_p50(big, 'trino'))} s",
            "accuracy": f"{acc_ok}/{len(self.acc)}",
            "big": human(big),
        }

    def headline_table(self) -> str:
        big = self.sizes[-1]
        h = self.headline()
        warm95 = max(
            nearest_rank(self.warm(big, e, layer), 0.95) for e in ("duckdb", "container") for layer in LAYERS
        )
        mem = {e: max(p50(self.warm(big, e, layer, "max_rss_bytes")) for layer in LAYERS) / 2**20
               for e in ("duckdb", "container")}  # fmt: skip
        over = [100 * float(r["gate_s"]) / float(r["spark_s"]) for r in self.spark]
        per_layer: dict[str, list[float]] = defaultdict(list)
        for r in self.spark:
            per_layer[r["layer"]].append(100 * float(r["gate_s"]) / float(r["spark_s"]))
        med = [p50(v) for v in per_layer.values()] or [0.0]
        tool_rows = sorted({r["tool"] for r in self.tools})
        tbig = max((int(r["rows"]) for r in self.tools), default=0)
        tools = ", ".join(f"{t} {num(p50([float(r['run_s']) for r in self.tools if r['tool'] == t and int(r['rows']) == tbig and r['run_s']]))} s"
                          for t in sorted(tool_rows, key=lambda t: p50([float(r["run_s"]) for r in self.tools
                                                                          if r["tool"] == t and int(r["rows"]) == tbig and r["run_s"]])))  # fmt: skip
        rows = [
            ["Gate latency, host DuckDB, warm p50 per layer",
             f"{h['duckdb_p50_range_1m']} up to 1M orders"],
            [f"All three gates at {h['big']} orders, warm p50", f"{h['medallion_duckdb_big']} host DuckDB · "
             f"{h['medallion_trino_big']} Trino (4 GiB heap)"],
            [f"Worst warm p95, DuckDB host or container ({h['big']})", f"{num(warm95)} s"],
            ["Gate time as a share of its Spark job", f"{num(min(med), 1)}–{num(max(med), 1)} % per layer"],
            [f"Peak gate memory at {h['big']} orders", f"{num(mem['container'], 0)} MiB (container), "
             f"{num(mem['duckdb'], 0)} MiB (host)"],
            ["Detection accuracy (7 defect types × 3 rates + clean)", f"{h['accuracy']} verdicts"],
            [f"Same six checks at {human(tbig)} rows", tools],
        ]  # fmt: skip
        del over
        return table(["Headline", "Value"], rows)

    def headline_chart(self) -> str:
        x = [human(s) for s in self.sizes]
        series = [(ENGINES[e], [self.medallion_p50(s, e) for s in self.sizes]) for e in ENGINES]
        return xychart(self.theme, "All three gates, warm p50 (s)", x, "seconds", series)

    # ---- sections -------------------------------------------------------------------------------------
    def latency(self) -> str:
        x = [human(s) for s in self.sizes]
        series = [(ENGINES[e], [self.medallion_p50(s, e) for s in self.sizes]) for e in ENGINES]
        rows = []
        for s in self.sizes:
            for layer in LAYERS:
                cells: list[object] = [human(s), layer]
                for e in ENGINES:
                    w = self.warm(s, e, layer)
                    cold = [float(r["gate_s"]) for r in self.perf if int(r["rows"]) == s and r["engine"] == e
                            and r["layer"] == layer and r["phase"] == "cold"]  # fmt: skip
                    flag = "" if self.status(s, e, layer) == "passed" else f" ⚠ {self.status(s, e, layer)}"
                    cells.append(
                        f"{num(p50(w))} / {num(nearest_rank(w, 0.95))} ({num(cold[0]) if cold else '–'}){flag}"
                    )
                rows.append(cells)
        big = self.sizes[-1]
        ratio = self.medallion_p50(big, "trino") / max(self.medallion_p50(big, "duckdb"), 1e-9)
        take = (f"All three gates on host DuckDB take {num(self.medallion_p50(big, 'duckdb'))} s at {human(big)} orders "
                f"(warm p50); Trino takes {num(ratio, 1)}x as long.")  # fmt: skip
        tbl = table(["orders", "layer", *[f"{v} p50 / p95 (cold)" for v in ENGINES.values()]], rows)
        legend = f"Lines, in order {COLOURS}: {', '.join(ENGINES.values())}. Sum of the bronze, silver and gold warm p50."
        chart = xychart(self.theme, "All three gates vs orders, warm p50 (s)", x, "seconds", series)
        return section(
            "Gate latency vs batch size", take, legend + "\n\n" + chart, tbl, self.link("perf_raw.csv")
        )

    def per_layer(self) -> str:
        big = self.sizes[-1]
        short = {"duckdb": "DuckDB", "trino": "Trino", "container": "Cont."}
        x = [f"{short[e]} {layer[:2]}" for e in ENGINES for layer in LAYERS]
        vals = [p50(self.warm(big, e, layer)) for e in ENGINES for layer in LAYERS]
        names = [f"{ENGINES[e]} {layer}" for e in ENGINES for layer in LAYERS]
        slow = max(zip(vals, names, strict=True))
        rows = [[ENGINES[e], *[num(p50(self.warm(big, e, layer))) for layer in LAYERS]] for e in ENGINES]
        take = f"At {human(big)} orders the slowest single gate is {slow[1]} at {num(slow[0])} s (warm p50)."
        chart = xychart(
            self.theme, f"Warm p50 per gate at {human(big)} orders (s)", x, "seconds", [("p50", vals)], "bar"
        )
        return section(f"Per-layer latency at {human(big)} orders", take,
                       "Bars: engine (DuckDB host, Trino, container) and layer (br = bronze, si = silver, go = gold).\n\n" + chart,
                       table(["engine", *LAYERS], rows), self.link("perf_raw.csv"))  # fmt: skip

    def memory(self) -> str:
        x = [human(s) for s in self.sizes]

        def peak(s: int, e: str) -> float:
            return max(p50(self.warm(s, e, layer, "max_rss_bytes")) for layer in LAYERS) / 2**20

        series = [(ENGINES[e], [peak(s, e) for s in self.sizes]) for e in ENGINES]

        def queries(s: int, e: str, layer: str) -> str:
            q = self.warm(s, e, layer, "queries")
            return str(int(q[0])) if q else "–"

        rows = [[human(s), *[f"{num(peak(s, e), 0)}" for e in ENGINES],
                 " / ".join(queries(s, "duckdb", layer) for layer in LAYERS)] for s in self.sizes]  # fmt: skip
        big = self.sizes[-1]
        take = (f"Client memory peaks at {num(peak(big, 'container'), 0)} MiB (container) and "
                f"{num(peak(big, 'duckdb'), 0)} MiB (host DuckDB) at {human(big)}; Trino's client stays near "
                f"{num(peak(big, 'trino'), 0)} MiB because the server does the work.")  # fmt: skip
        legend = (
            f"Lines, in order {COLOURS}: {', '.join(ENGINES.values())}. Largest per-layer median peak RSS."
        )
        chart = xychart(self.theme, "Client peak memory vs orders (MiB)", x, "MiB", series)
        return section("Memory vs batch size", take, legend + "\n\n" + chart,
                       table(["orders", *[f"{v} MiB" for v in ENGINES.values()], "SQL queries br / si / go"], rows),
                       self.link("perf_raw.csv"))  # fmt: skip

    def overhead(self) -> str:
        per: dict[str, list[float]] = defaultdict(list)
        spark: dict[str, list[float]] = defaultdict(list)
        gate: dict[str, list[float]] = defaultdict(list)
        for r in self.spark:
            per[r["layer"]].append(100 * float(r["gate_s"]) / float(r["spark_s"]))
            spark[r["layer"]].append(float(r["spark_s"]))
            gate[r["layer"]].append(float(r["gate_s"]))
        vals = [p50(per[layer]) for layer in LAYERS]
        rows = [
            [layer, num(p50(spark[layer]), 1), num(p50(gate[layer])), f"{num(p50(per[layer]), 1)} %"]
            for layer in LAYERS
        ]
        take = (
            f"A gate adds {num(min(vals), 1)}–{num(max(vals), 1)} % to its Spark job (median of the repeats)."
        )
        chart = xychart(
            self.theme, "Gate time as % of the Spark job", list(LAYERS), "percent", [("%", vals)], "bar"
        )
        return section(
            "Gate overhead against the Spark pipeline",
            take,
            chart,
            table(["layer", "Spark job s", "gate s", "overhead"], rows),
            self.link("spark_reference.csv"),
        )

    def accuracy(self) -> str:
        agg: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
        rates: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        for r in self.acc:
            if r["defect"] == "none":
                continue
            tp, fp, fn = (int(r[k] or 0) for k in ("tp", "fp", "fn"))
            agg[r["defect"]] = [a + b for a, b in zip(agg[r["defect"]], (tp, fp, fn), strict=True)]
            key = (r["defect"], r["rate"])
            rates[key] = [rates[key][0] + (r["verdict_ok"] == "True"), rates[key][1] + 1]

        def ratio(a: int, b: int) -> float:
            return a / b if b else 1.0

        defects = sorted(agg)
        recall = [100 * ratio(agg[d][0], agg[d][0] + agg[d][2]) for d in defects]
        precision = [100 * ratio(agg[d][0], agg[d][0] + agg[d][1]) for d in defects]
        all_rates = sorted({r for _, r in rates}, key=float)
        rows = [[d, num(precision[i] / 100), num(recall[i] / 100),
                 *[f"{rates[(d, rt)][0]}/{rates[(d, rt)][1]}" for rt in all_rates]] for i, d in enumerate(defects)]  # fmt: skip
        ok = sum(r["verdict_ok"] == "True" for r in self.acc)
        take = (
            f"{ok} of {len(self.acc)} gate verdicts were right; precision "
            f"at least {num(min(precision) / 100)} and recall at least {num(min(recall) / 100)} for every defect type."
        )
        x = [SHORT.get(d, d[:10]) for d in defects]
        chart = xychart(
            self.theme, "Recall per defect type (%)", x, "percent", [("recall", recall)], "bar", 100
        )
        return section("Detection accuracy", take, chart,
                       table(["defect", "precision", "recall", *[f"verdicts at {float(r) * 100:g}%" for r in all_rates]], rows),
                       self.link("accuracy.csv"))  # fmt: skip

    def tools_sections(self) -> str:
        names = sorted({r["tool"] for r in self.tools})
        sizes = sorted({int(r["rows"]) for r in self.tools})

        def med(tool: str, rows: int, key: str) -> float:
            vals = [
                float(r[key])
                for r in self.tools
                if r["tool"] == tool and int(r["rows"]) == rows and r.get(key)
            ]
            return p50(vals)

        big = sizes[-1]
        order = sorted(names, key=lambda t: med(t, big, "run_s"))
        rows = [[t, *[num(med(t, s, "run_s")) for s in sizes], *[num(med(t, s, "max_rss_bytes") / 2**20, 0) for s in sizes],
                 "yes" if all(r["correct"] == "True" for r in self.tools if r["tool"] == t) else "no"] for t in order]  # fmt: skip
        take = (
            f"At {human(big)} rows {order[0]} is fastest ({num(med(order[0], big, 'run_s'))} s) and "
            f"{order[-1]} slowest ({num(med(order[-1], big, 'run_s'))} s) on the same six checks."
        )
        t_chart = xychart(
            self.theme,
            f"Run time at {human(big)} rows (s)",
            order,
            "seconds",
            [("s", [med(t, big, "run_s") for t in order])],
            "bar",
        )
        m_chart = xychart(
            self.theme,
            f"Peak memory at {human(big)} rows (MiB)",
            order,
            "MiB",
            [("MiB", [med(t, big, "max_rss_bytes") / 2**20 for t in order])],
            "bar",
        )
        hdr = [
            "tool",
            *[f"run s {human(s)}" for s in sizes],
            *[f"MiB {human(s)}" for s in sizes],
            "all checks right",
        ]
        return section(
            "Tool comparison",
            take,
            t_chart + "\n\n" + m_chart,
            table(hdr, rows),
            self.link("tools.csv") + " · " + self.link("tools_versions.json"),
        )

    def pipeline(self) -> str:
        stages: dict[int, dict[str, float]] = {}
        for r in self.pipe:
            s = int(r["rows"])
            stages[s] = {"bronze build": float(r["bronze_s"]), "silver build": float(r["silver_s"]),
                         "gold build": float(r["gold_s"])}  # fmt: skip
        for s in stages:
            for layer in LAYERS:
                stages[s][f"{layer} gate"] = p50(self.warm(s, "duckdb", layer))
        order = ["bronze build", "bronze gate", "silver build", "silver gate", "gold build", "gold gate"]
        big = max(stages)
        total = sum(stages[big].values())
        gates = sum(stages[big][f"{layer} gate"] for layer in LAYERS)
        rows = [
            [human(s), *[num(stages[s][k]) for k in order], num(sum(stages[s].values()))]
            for s in sorted(stages)
        ]
        take = f"At {human(big)} orders the three gates are {num(100 * gates / total, 0)} % of the {num(total, 1)} s seed-to-gold run."
        chart = pie(
            self.theme, f"Seed to gold at {human(big)} orders (s)", [(k, stages[big][k]) for k in order]
        )
        return section(
            "End-to-end pipeline, stage by stage",
            take,
            chart,
            table(["orders", *order, "total s"], rows),
            self.link("pipeline_raw.csv") + " · " + self.link("perf_raw.csv"),
        )

    def airflow(self) -> str:
        if not self.af_tasks:
            return ""
        durations: dict[tuple[str, str], list[float]] = defaultdict(list)
        states: dict[tuple[str, str], set[str]] = defaultdict(set)
        tasks: list[str] = []
        for r in self.af_tasks:
            key = (r["variant"], r["task"])
            states[key].add(r["state"])
            if r["task"] not in tasks:
                tasks.append(r["task"])
            if r["duration_s"]:
                durations[key].append(float(r["duration_s"]))
        starts: dict[str, str] = {}
        for r in self.af_tasks:
            if r["variant"] == "good" and r["start"]:
                starts[r["task"]] = min(starts.get(r["task"], r["start"]), r["start"])
        tasks.sort(key=lambda t: starts.get(t, "~"))
        good = [p50(durations[("good", t)]) for t in tasks]
        rows = [[t, num(p50(durations[("good", t)]), 1), "/".join(sorted(states[("good", t)])),
                 num(p50(durations[("bad", t)]), 1) if "upstream_failed" not in states[("bad", t)] else "–", "/".join(sorted(states[("bad", t)]))]
                for t in tasks]  # fmt: skip
        runs: dict[str, list[float]] = defaultdict(list)
        for r in self.af_runs:
            if r["duration_s"]:
                runs[r["variant"]].append(float(r["duration_s"]))
        bad = runs["bad"] or [0.0]
        dq = sum(p50(durations[("good", t)]) for t in tasks if t.startswith("dq_"))
        take = (
            f"A good DAG run takes {num(p50(runs['good']), 0)} s, of which the three gate tasks are {num(dq, 0)} s; "
            f"a bad run stops at dq_bronze after {num(p50(bad), 0)} s."
        )
        x = [t.replace("query_timetravel", "timetravel").replace("inject_bad_batch", "inject") for t in tasks]
        chart = xychart(self.theme, "Good DAG run, task duration (s)", x, "seconds", [("s", good)], "bar")
        return section(
            "Airflow DAG run, per task",
            take,
            chart,
            table(["task", "good s", "good state", "bad s", "bad state"], rows),
            self.link("airflow_tasks.csv") + " · " + self.link("airflow_runs.csv"),
        )

    def render(self) -> str:
        spec = self.specs
        pk = spec.get("packages", {})
        pkgs = ", ".join(f"{k} {v}" for k, v in pk.items()) if isinstance(pk, dict) else ""
        intro = (f"Generated by `make bench-charts` from {self.link('specs.json')}: {spec.get('cpu', '?')}, "
                 f"{spec.get('os', '?')}, Docker {spec.get('docker', '?')}, Trino {spec.get('trino', '?')} "
                 f"({spec.get('trino_heap', 'default heap')}), commit `{spec.get('commit', '?')}`, {pkgs}. "
                 "Do not edit by hand.")  # fmt: skip
        parts: list[Callable[[], str]] = [self.latency, self.per_layer, self.memory, self.pipeline, self.overhead,
                                          self.airflow, self.accuracy, self.tools_sections]  # fmt: skip
        return "\n".join([intro, "", *[p() for p in parts if p()]])


def rewrite(text: str, block: str, headline_chart: str) -> str:
    if BEGIN not in text or END not in text:
        raise SystemExit(f"{TARGET}: add {BEGIN} and {END} markers")
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    region = f"<!-- region:headline-chart -->\n{headline_chart}\n<!-- /region -->"
    tail = re.sub(r"<!-- region:headline-chart -->\n.*?<!-- /region -->", lambda _: region, tail, flags=re.S)
    return f"{head}{BEGIN}\n{block}\n{END}{tail}"


HEADLINE_PAGE = ROOT / "docs" / "benchmarks" / "README.md"


def rewrite_headline(text: str, tbl: str) -> str:
    return re.sub(r"<!-- region:headline -->\n.*?<!-- /region -->",
                  lambda _: f"<!-- region:headline -->\n{tbl}\n<!-- /region -->", text, flags=re.S)  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--extract", type=Path, help="write each Mermaid block of the page to DIR/chart-N.mmd"
    )
    args = parser.parse_args(argv)
    if args.extract:
        args.extract.mkdir(parents=True, exist_ok=True)
        blocks = re.findall(r"```mermaid\n(.*?)```", TARGET.read_text(encoding="utf-8"), re.S)
        for i, body in enumerate(blocks):
            (args.extract / f"chart-{i:02d}.mmd").write_text(body, encoding="utf-8")
        print(f"{len(blocks)} charts written to {args.extract}")
        return 0
    charts = Charts((args.data or latest_data()).resolve())
    old = TARGET.read_text(encoding="utf-8")
    new = rewrite(old, charts.render(), charts.headline_chart())
    old_head = HEADLINE_PAGE.read_text(encoding="utf-8")
    new_head = rewrite_headline(old_head, charts.headline_table())
    if args.check:
        if new != old or new_head != old_head:
            print(f"{TARGET.relative_to(ROOT)} is stale: run make bench-charts")
            return 1
        print("benchmark charts up to date")
        return 0
    TARGET.write_text(new, encoding="utf-8")
    HEADLINE_PAGE.write_text(new_head, encoding="utf-8")
    print(f"wrote {TARGET.relative_to(ROOT)} from {charts.data.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
