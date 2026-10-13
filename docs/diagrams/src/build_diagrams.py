"""Hand-authored reference diagrams (SVG) for the README.

Run: python docs/diagrams/src/build_diagrams.py && rsvg-convert -z 2 ... (see `render.sh`).
Pure stdlib; every coordinate is on an 8px grid so spacing stays consistent.
"""

from __future__ import annotations

import itertools
from pathlib import Path
from xml.sax.saxutils import escape

OUT = Path(__file__).resolve().parents[1]
FONT = "Inter, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
INK, MUTED, LINE = "#0f172a", "#475569", "#cbd5e1"


def text(x, y, s, size=14, weight=400, fill=INK, anchor="start", family=FONT, style=""):
    return (
        f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" font-weight="{weight}" '
        f'fill="{fill}" text-anchor="{anchor}"{style}>{escape(s)}</text>'
    )


def rect(x, y, w, h, fill, stroke="none", r=10, sw=1, extra=""):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{extra}/>'


def card(x, y, w, h, title, lines, accent, mono_title=False):
    out = [rect(x, y, w, h, "#ffffff", LINE, 8, 1, ' filter="url(#shadow)"'), rect(x, y, 4, h, accent, r=2)]
    fam = "'SF Mono', Menlo, Consolas, monospace" if mono_title else FONT
    total = 18 + 17 * len(lines)
    ty = y + (h - total) / 2 + 14
    out.append(text(x + w / 2 + 2, ty, title, 14.5, 650, INK, "middle", fam))
    for i, line in enumerate(lines):
        out.append(text(x + w / 2 + 2, ty + 20 + 17 * i, line, 12, 400, MUTED, "middle"))
    return "\n".join(out)


def arrow_down(x, y1, y2, color="#94a3b8"):
    return (
        f'<line x1="{x}" y1="{y1}" x2="{x}" y2="{y2 - 7}" stroke="{color}" stroke-width="2"/>'
        f'<path d="M{x - 6},{y2 - 9} L{x},{y2} L{x + 6},{y2 - 9} Z" fill="{color}"/>'
    )


DEFS = """<defs>
  <filter id="shadow" x="-5%" y="-5%" width="110%" height="120%">
    <feDropShadow dx="0" dy="1" stdDeviation="1.2" flood-color="#0f172a" flood-opacity="0.08"/>
  </filter>
</defs>"""


def svg(w, h, body):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">\n{DEFS}\n'
        + rect(0.5, 0.5, w - 1, h - 1, "#ffffff", "#e2e8f0", 16)
        + "\n"
        + body
        + "\n</svg>\n"
    )


# ----------------------------------------------------------------------------------------------
# 1. Layered reference architecture
# ----------------------------------------------------------------------------------------------
LAYERS = [
    (
        "1",
        "Entry points",
        "how a gate is started",
        "#2563eb",
        "#eff6ff",
        [
            ("soda-dq CLI", ["Typer · verify · run · seed", "validate · list"]),
            ("Airflow DAG", ["lakehouse_retail_medallion_dq", "dq_bronze · dq_silver · dq_gold"]),
            ("Docker runner ldl-soda", ["lakehouse-quality-gates:0.2.0", "docker exec from Airflow"]),
        ],
    ),
    (
        "2",
        "Application",
        "use cases · orchestration",
        "#7c3aed",
        "#f5f3ff",
        [
            ("RunMedallionUseCase", ["bronze → silver → gold", "stops at the first block"]),
            ("VerifyLayerUseCase", ["contracts → verify → decide", "→ sinks → notifiers"]),
            ("GatePolicy", ["GateDecision", "pass · warn · block"]),
        ],
    ),
    (
        "3",
        "Domain",
        "pure Python · no I/O",
        "#0d9488",
        "#f0fdfa",
        [
            ("Models", ["Layer · Outcome · GateStatus", "ContractSpec · CheckResult · GateReport"]),
            ("Ports (Protocols)", ["ContractRepository · ContractVerifier", "ResultSink · Notifier · Clock"]),
            ("Errors", ["DataQualityError", "+ 5 typed subclasses"]),
        ],
    ),
    (
        "4",
        "Adapters",
        "implement the ports",
        "#d97706",
        "#fffbeb",
        [
            ("SodaContractVerifier", ["Soda Core 4.26", "verify_contract_locally"]),
            ("FileContractRepository", ["YAML contracts", "{{ dataset_root }} token"]),
            ("DataSource providers", ["DuckDBProvider", "TrinoProvider"]),
            ("Result sinks", ["JSON · JUnit · Markdown", "Iceberg ops.dq_results"]),
            ("Notifiers", ["ConsoleNotifier", "SlackWebhookNotifier"]),
        ],
    ),
    (
        "5",
        "Data platform",
        "local-data-lakehouse",
        "#475569",
        "#f8fafc",
        [
            ("Query engines", ["DuckDB 1.5 · Trino 483", "Spark 4.1 (writes layers)"]),
            ("Lakekeeper", ["Iceberg REST catalog", "vended S3 credentials"]),
            ("Apache Iceberg tables", ["bronze · silver · gold", "snapshots · time travel"]),
            ("RustFS", ["S3 object storage", "s3://lake/warehouse"]),
            ("PostgreSQL", ["catalog metadata", "store"]),
        ],
    ),
]
CROSS = [
    ("Observability", ["structlog key/value events", "JSON logs (DQ_LOG_JSON)", "JSON · JUnit · MD reports"]),
    ("Configuration", ["pydantic-settings", "DQ_* env vars · .env", "validated at startup"]),
    ("Security", ["vended, scoped S3 creds", "non-root · read-only rootfs", "cap_drop ALL · SecretStr"]),
]


def layered() -> str:
    W, _H = 1600, 0  # height computed from content
    LX, BX, BW = 40, 248, 1032  # label column, band x, band width
    SX, SW = 1312, 248  # cross-cutting side band
    BH, GAP, TOP = 128, 32, 120
    body = [
        text(40, 56, "lakehouse-quality-gates · layered reference architecture", 26, 700),
        text(
            40,
            86,
            "Soda Core v4 data contracts as quality gates for an Apache Iceberg lakehouse. "
            "Dependencies point downward only.",
            14.5,
            400,
            MUTED,
        ),
    ]
    y = TOP
    centers = []
    for idx, (num, name, sub, accent, tint, comps) in enumerate(LAYERS):
        if idx == 1:  # composition root strip between entry points and application
            body.append(
                rect(BX, y - GAP + 6, BW, GAP + 12, "#f1f5f9", "#cbd5e1", 8, 1, ' stroke-dasharray="4 3"')
            )
            body.append(
                text(
                    BX + BW / 2,
                    y - GAP + 28,
                    "Composition root · container.py  —  the only place that "
                    "instantiates adapters and injects them into use cases",
                    12.5,
                    500,
                    MUTED,
                    "middle",
                )
            )
            y += 24
        body.append(rect(BX, y, BW, BH, tint, accent + "55", 12, 1))
        # label column
        body.append(rect(LX, y, 184, BH, "#ffffff", LINE, 12, 1))
        body.append(rect(LX, y, 6, BH, accent, r=3))
        body.append(f'<circle cx="{LX + 36}" cy="{y + BH / 2 - 14}" r="14" fill="{accent}"/>')
        body.append(text(LX + 36, y + BH / 2 - 9, num, 14, 700, "#ffffff", "middle"))
        body.append(text(LX + 60, y + BH / 2 - 9, name, 16, 700, INK))
        body.append(text(LX + 24, y + BH / 2 + 20, sub, 12, 400, MUTED))
        n = len(comps)
        pad, gap = 20, 16
        cw = (BW - 2 * pad - gap * (n - 1)) / n
        for i, (title, lines) in enumerate(comps):
            cx = BX + pad + i * (cw + gap)
            body.append(card(cx, y + 18, cw, BH - 36, title, lines, accent))
        centers.append((y, y + BH))
        y += BH + GAP
    # downward dependency arrows between bands (skip the composition strip area: arrow spans it)
    for (_t1, b1), (t2, _b2) in itertools.pairwise(centers):
        for fx in (0.1, 0.9):
            body.append(arrow_down(BX + BW * fx, b1 + 2, t2 - 2))
    bottom = centers[-1][1]
    # cross-cutting side band
    body.append(rect(SX, TOP, SW, bottom - TOP, "#f8fafc", "#94a3b855", 12, 1))
    body.append(text(SX + SW / 2, TOP + 30, "Cross-cutting concerns", 15, 700, INK, "middle"))
    body.append(text(SX + SW / 2, TOP + 50, "apply to every layer", 12, 400, MUTED, "middle"))
    ch = (bottom - TOP - 72 - 24 * 2 - 16) / 3
    for i, (title, lines) in enumerate(CROSS):
        cy = TOP + 72 + i * (ch + 24)
        body.append(card(SX + 16, cy, SW - 32, ch, title, lines, "#64748b"))
    # legend
    ly = bottom + 40
    body.append(f'<line x1="40" y1="{ly - 16}" x2="{W - 40}" y2="{ly - 16}" stroke="#e2e8f0"/>')
    body.append(arrow_down(56, ly - 2, ly + 22))
    body.append(text(72, ly + 15, "depends on (downward only)", 12.5, 400, MUTED))
    lx = 300
    for _num, name, _s, accent, _t, _c in LAYERS:
        body.append(rect(lx, ly + 2, 14, 14, accent, r=3))
        body.append(text(lx + 22, ly + 14, name, 12.5, 500, MUTED))
        lx += 34 + len(name) * 7.6
    body.append(
        text(
            W - 40,
            ly + 14,
            "Adapters implement domain ports (dependency inversion) · domain imports only the stdlib",
            12.5,
            400,
            MUTED,
            "end",
        )
    )
    return svg(W, int(ly + 48), "\n".join(body))


# ----------------------------------------------------------------------------------------------
# 2. Medallion data flow with quality gates
# ----------------------------------------------------------------------------------------------
GREEN, RED = "#16a34a", "#dc2626"


def arrow_right(x1, x2, y, color, label=""):
    out = [
        f'<line x1="{x1}" y1="{y}" x2="{x2 - 8}" y2="{y}" stroke="{color}" stroke-width="2.2"/>',
        f'<path d="M{x2 - 10},{y - 6} L{x2},{y} L{x2 - 10},{y + 6} Z" fill="{color}"/>',
    ]
    if label:
        out.append(text((x1 + x2) / 2, y - 10, label, 12, 600, color, "middle"))
    return "\n".join(out)


def gate(cx, cy, name, checks, s=54):
    pts = f"{cx},{cy - s} {cx + s},{cy} {cx},{cy + s} {cx - s},{cy}"
    return "\n".join(
        [
            f'<polygon points="{pts}" fill="#f0fdf4" stroke="{GREEN}" stroke-width="2" filter="url(#shadow)"/>',
            text(cx, cy - 4, name, 13.5, 700, INK, "middle"),
            text(cx, cy + 14, f"{checks} checks", 12, 500, MUTED, "middle"),
        ]
    )


def medallion() -> str:
    W, H = 1600, 600
    body = [
        text(40, 56, "Medallion flow with quality gates", 26, 700),
        text(
            40,
            86,
            "Each layer is built, then its Soda contract runs. Only a passing gate lets the next "
            "layer read it.",
            14.5,
            400,
            MUTED,
        ),
    ]
    MID = 250
    body.append(rect(40, 128, W - 80, 236, "#f8fafc", "#e2e8f0", 12))
    body.append(text(60, 154, "PASS LANE · data is promoted", 12, 700, GREEN, style=' letter-spacing="1"'))
    body.append(rect(40, 392, W - 80, 168, "#fef2f2", "#fecaca", 12))
    body.append(text(60, 418, "BLOCK LANE · promotion stops", 12, 700, RED, style=' letter-spacing="1"'))
    CW, GS = 168, 50  # card width, gate half-size
    seq = [
        ("card", "Sources", ["Retail CSV batches", "orders · customers"], "#64748b", None),
        ("card", "Bronze", ["orders_raw", "customers_raw"], "#b45309", "Spark · 02_ingest_bronze"),
        ("gate", "dq_bronze", 14),
        (
            "card",
            "Silver",
            ["orders · customers", "typed · deduplicated"],
            "#64748b",
            "Spark · 03_transform_silver",
        ),
        ("gate", "dq_silver", 21),
        ("card", "Gold", ["daily_order_metrics", "aggregates"], "#ca8a04", "Spark · 04_publish_gold"),
        ("gate", "dq_gold", 11),
        ("card", "Consumers", ["BI · ML · analysts", "trusted gold only"], "#2563eb", None),
    ]
    widths = [CW if e[0] == "card" else 2 * GS for e in seq]
    left, right = 72, W - 72
    gap = (right - left - sum(widths)) / (len(seq) - 1)
    x = left
    prev_right, prev_kind = None, None
    for e, w in zip(seq, widths, strict=False):
        if prev_right is not None:
            color, label = ("#94a3b8", "")
            if prev_kind == "gate":
                color, label = GREEN, "pass"
            elif e[0] == "card" and e[1] == "Bronze":
                label = "ingest"
            body.append(arrow_right(prev_right + 4, x - 4, MID, color, label))
        if e[0] == "card":
            _k, title, lines, accent, job = e
            body.append(card(x, MID - 48, w, 96, title, lines, accent))
            if job:
                body.append(text(x + w / 2, MID - 62, job, 11.5, 500, MUTED, "middle"))
        else:
            _k, name, checks = e
            cx = x + GS
            body.append(gate(cx, MID, name, checks, GS))
            body.append(
                f'<line x1="{cx}" y1="{MID + GS}" x2="{cx}" y2="{448 - 8}" stroke="{RED}" stroke-width="2.2" stroke-dasharray="6 4"/>'
            )
            body.append(f'<path d="M{cx - 6},{448 - 10} L{cx},{448} L{cx + 6},{448 - 10} Z" fill="{RED}"/>')
            body.append(text(cx + 10, 384, "block", 12, 600, RED))
            body.append(rect(cx - 112, 448, 224, 88, "#ffffff", "#fca5a5", 8, 1, ' filter="url(#shadow)"'))
            body.append(rect(cx - 112, 448, 4, 88, RED, r=2))
            body.append(text(cx, 474, "Gate blocked · exit 1", 13.5, 700, INK, "middle"))
            body.append(text(cx, 494, "Airflow task fails,", 12, 400, MUTED, "middle"))
            body.append(text(cx, 511, "downstream upstream_failed", 12, 400, MUTED, "middle"))
            body.append(text(cx, 528, "report + Slack alert", 12, 400, MUTED, "middle"))
        prev_right, prev_kind = x + w, e[0]
        x += w + gap
    body.append(
        text(
            W - 40,
            584,
            "Warnings never block (unless DQ_FAIL_ON_WARN=true) · failures, errors and "
            "unevaluated checks block",
            12.5,
            400,
            MUTED,
            "end",
        )
    )
    return svg(W, H, "\n".join(body))


if __name__ == "__main__":
    (OUT / "layered-architecture.svg").write_text(layered())
    (OUT / "medallion-flow.svg").write_text(medallion())
    print("wrote", OUT / "layered-architecture.svg", OUT / "medallion-flow.svg")
