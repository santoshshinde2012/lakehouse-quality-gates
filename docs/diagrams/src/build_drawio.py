# ruff: noqa: N806, PLR0917, E501  (layout constants in CAPS, coordinate-heavy helpers, long labels)
"""Generate the draw.io (diagrams.net) source files in docs/diagrams/drawio/.

The .drawio files are the editable source of truth; `make diagrams` regenerates them from this script
and exports .drawio.svg / .drawio.png (with the diagram embedded) through the draw.io desktop CLI.
Pure stdlib. Layout is on an 8 px grid; every band is a horizontal swimlane whose title is the layer.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

OUT = Path(__file__).resolve().parents[1] / "drawio"
FONT = "Helvetica"
INK, MUTED = "#0f172a", "#475569"


@dataclass
class Page:
    name: str
    width: int
    height: int
    cells: list[str] = field(default_factory=list)
    ids: itertools.count = field(default_factory=lambda: itertools.count(2))

    def _id(self) -> str:
        return f"c{next(self.ids)}"

    def vertex(
        self, value: str, style: str, x: float, y: float, w: float, h: float, parent: str = "1"
    ) -> str:
        cid = self._id()
        self.cells.append(
            f'<mxCell id="{cid}" value={quoteattr(value)} style={quoteattr(style)} vertex="1" parent="{parent}">'
            f'<mxGeometry x="{x:.0f}" y="{y:.0f}" width="{w:.0f}" height="{h:.0f}" as="geometry"/></mxCell>'
        )
        return cid

    def edge(self, src: str, dst: str, style: str, label: str = "", parent: str = "1", dy: int = 0) -> str:
        cid = self._id()
        offset = f'<mxPoint x="0" y="{dy}" as="offset"/>' if dy else ""
        self.cells.append(
            f'<mxCell id="{cid}" value={quoteattr(label)} style={quoteattr(style)} edge="1" parent="{parent}" '
            f'source="{src}" target="{dst}"><mxGeometry relative="1" as="geometry">{offset}</mxGeometry></mxCell>'
        )
        return cid

    def free_edge(self, x1: float, y1: float, x2: float, y2: float, style: str, label: str = "") -> str:
        cid = self._id()
        self.cells.append(
            f'<mxCell id="{cid}" value={quoteattr(label)} style={quoteattr(style)} edge="1" parent="1">'
            f'<mxGeometry relative="1" as="geometry"><mxPoint x="{x1:.0f}" y="{y1:.0f}" as="sourcePoint"/>'
            f'<mxPoint x="{x2:.0f}" y="{y2:.0f}" as="targetPoint"/></mxGeometry></mxCell>'
        )
        return cid

    def xml(self) -> str:
        body = "".join(self.cells)
        return (
            f'<diagram id="{self.name}" name="{self.name}"><mxGraphModel dx="{self.width}" dy="{self.height}" '
            f'grid="1" gridSize="8" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" '
            f'pageScale="1" pageWidth="{self.width}" pageHeight="{self.height}" background="#ffffff" math="0" '
            f'shadow="0"><root><mxCell id="0"/><mxCell id="1" parent="0"/>{body}</root></mxGraphModel></diagram>'
        )


def write(path: Path, page: Page) -> None:
    path.write_text(
        f'<mxfile host="build_drawio.py" agent="lakehouse-quality-gates" version="24.0.0">{page.xml()}</mxfile>\n'
    )


# --- styles ---------------------------------------------------------------------------------------
def html(title: str, lines: list[str], mono: bool = False) -> str:
    fam = "font-family:Menlo,Consolas,monospace;" if mono else ""
    sub = "<br>".join(escape(line) for line in lines)
    return (
        f'<div style="{fam}font-size:13px;font-weight:bold;color:{INK}">{escape(title)}</div>'
        f'<div style="font-size:11px;color:{MUTED};margin-top:4px">{sub}</div>'
    )


def card_style(accent: str, fill: str = "#ffffff") -> str:
    return (
        f"rounded=1;arcSize=8;whiteSpace=wrap;html=1;fillColor={fill};strokeColor={accent};strokeWidth=1.5;"
        f"fontFamily={FONT};align=center;verticalAlign=middle;spacing=6;shadow=0;"
    )


def band_style(accent: str, tint: str, label_w: int = 184) -> str:
    return (
        f"swimlane;horizontal=0;startSize={label_w};rounded=1;arcSize=3;html=1;whiteSpace=wrap;"
        f"fillColor={accent};swimlaneFillColor={tint};strokeColor={accent};fontColor=#ffffff;"
        f"fontFamily={FONT};fontSize=15;fontStyle=1;swimlaneLine=0;collapsible=0;"
    )


TITLE = (
    f"text;html=1;align=left;verticalAlign=middle;fontFamily={FONT};fontSize=24;fontStyle=1;fontColor={INK};"
)
SUBTITLE = f"text;html=1;align=left;verticalAlign=middle;fontFamily={FONT};fontSize=13;fontColor={MUTED};"
NOTE = f"text;html=1;align=center;verticalAlign=middle;fontFamily={FONT};fontSize=12;fontColor={MUTED};whiteSpace=wrap;"
DOWN = "endArrow=block;endFill=1;html=1;strokeColor=#94a3b8;strokeWidth=2;rounded=0;"
ORTHO = (
    f"edgeStyle=orthogonalEdgeStyle;rounded=1;orthogonalLoop=1;html=1;endArrow=block;endFill=1;strokeWidth=1.6;"
    f"fontFamily={FONT};fontSize=11;labelBackgroundColor=#ffffff;"
)


def cards_row(
    page: Page,
    parent: str,
    comps: list[tuple[str, list[str]]],
    accent: str,
    x0: float,
    width: float,
    y: float,
    h: float,
    gap: float = 16,
) -> list[str]:
    n = len(comps)
    cw = (width - gap * (n - 1)) / n
    out = []
    for i, (title, lines) in enumerate(comps):
        out.append(page.vertex(html(title, lines), card_style(accent), x0 + i * (cw + gap), y, cw, h, parent))
    return out


# --- 1. layered architecture ----------------------------------------------------------------------
LAYERS = [
    (
        "1 · Entry points",
        "#2563eb",
        "#eff6ff",
        [
            ("soda-dq CLI", ["Typer · verify · run · seed", "validate · list"]),
            ("Airflow DAG", ["lakehouse_retail_medallion_dq", "dq_bronze · dq_silver · dq_gold"]),
            ("Docker runner ldl-soda", ["lakehouse-quality-gates:0.2.0", "docker exec from Airflow"]),
        ],
    ),
    (
        "2 · Application",
        "#7c3aed",
        "#f5f3ff",
        [
            ("RunMedallionUseCase", ["bronze → silver → gold", "stops at the first block"]),
            ("VerifyLayerUseCase", ["contracts → verify → decide", "→ sinks → notifiers"]),
            ("GatePolicy", ["GateDecision", "pass · warn · block"]),
        ],
    ),
    (
        "3 · Domain",
        "#0d9488",
        "#f0fdfa",
        [
            ("Models", ["Layer · Outcome · GateStatus", "ContractSpec · CheckResult · GateReport"]),
            ("Ports (Protocols)", ["ContractRepository · ContractVerifier", "ResultSink · Notifier · Clock"]),
            ("Errors", ["DataQualityError", "+ 5 typed subclasses"]),
        ],
    ),
    (
        "4 · Adapters",
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
        "5 · Data platform",
        "#475569",
        "#f8fafc",
        [
            ("Query engines", ["DuckDB 1.5 · Trino 483", "Spark 4.1 (writes layers)"]),
            ("Lakekeeper", ["Iceberg REST catalog", "vended S3 credentials"]),
            ("Iceberg tables", ["bronze · silver · gold", "snapshots · time travel"]),
            ("RustFS", ["S3 object storage", "s3://lake/warehouse"]),
            ("PostgreSQL", ["catalog metadata", "store"]),
        ],
    ),
]
LAYER_NOTES = [
    "how a gate is started",
    "use cases · orchestration",
    "pure Python · no I/O",
    "implement the ports",
    "local-data-lakehouse",
]
CROSS = [
    ("Observability", ["structlog key/value events", "JSON logs (DQ_LOG_JSON)", "JSON · JUnit · MD reports"]),
    ("Configuration", ["pydantic-settings", "DQ_* env vars · .env", "validated at startup"]),
    ("Security", ["vended, scoped S3 creds", "non-root · read-only rootfs", "cap_drop ALL · SecretStr"]),
]


def layered() -> Page:
    X, W, LW = 40, 1240, 80  # band x, band width, label strip
    BH, GAP, TOP = 128, 40, 112
    SX, SW = 1312, 248
    page = Page("layered-architecture", 1600, 0)
    page.vertex("lakehouse-quality-gates · layered reference architecture", TITLE, 40, 24, 1000, 36)
    page.vertex(
        "Soda Core v4 data contracts as quality gates for an Apache Iceberg lakehouse. "
        "Dependencies point downward only.",
        SUBTITLE,
        40,
        60,
        1000,
        28,
    )
    y, bands = TOP, []
    for i, ((name, accent, tint, comps), note) in enumerate(zip(LAYERS, LAYER_NOTES, strict=True)):
        if i == 1:
            page.vertex(
                "<b>Composition root · container.py</b> — the only place that instantiates adapters "
                "and injects them into use cases",
                "rounded=1;arcSize=20;html=1;whiteSpace=wrap;dashed=1;dashPattern=4 3;fillColor=#f1f5f9;"
                f"strokeColor=#94a3b8;fontColor={MUTED};fontFamily={FONT};fontSize=12;",
                X + LW + 160,
                y - GAP + 8,
                W - LW - 320,
                GAP + 8,
            )
            y += 32
        label = f'{escape(name)}<br><span style="font-size:11px;font-weight:normal">{escape(note)}</span>'
        band = page.vertex(label, band_style(accent, tint, LW), X, y, W, BH)
        cards_row(page, band, comps, accent, LW + 20, W - LW - 40, 18, BH - 36)
        bands.append((y, y + BH))
        y += BH + GAP
    for (_t1, b1), (t2, _b2) in itertools.pairwise(bands):
        for fx in (X + LW + 80, X + W - 80):
            page.free_edge(fx, b1, fx, t2, DOWN)
    bottom = bands[-1][1]
    side = page.vertex(
        "Cross-cutting concerns",
        f"swimlane;startSize=40;rounded=1;arcSize=3;html=1;fillColor=#64748b;swimlaneFillColor=#f8fafc;"
        f"strokeColor=#64748b;fontColor=#ffffff;fontFamily={FONT};fontSize=15;fontStyle=1;"
        "swimlaneLine=0;collapsible=0;",
        SX,
        TOP,
        SW,
        bottom - TOP,
    )
    ch = (bottom - TOP - 40 - 16 - 24 * 3) / 3
    for i, (title, lines) in enumerate(CROSS):
        page.vertex(html(title, lines), card_style("#64748b"), 16, 40 + 24 + i * (ch + 24), SW - 32, ch, side)
    ly = bottom + 24
    legend = (
        "<b>Legend</b>  ↓ depends on (downward only) · adapters implement domain ports (dependency "
        "inversion) · the domain imports only the Python standard library"
    )
    page.vertex(legend, NOTE, 40, ly, 1520, 28)
    page.height = int(ly + 48)
    return page


# --- 2. medallion flow ----------------------------------------------------------------------------
GREEN, RED = "#16a34a", "#dc2626"


def medallion() -> Page:
    page = Page("medallion-flow", 1600, 640)
    page.vertex("Medallion flow with quality gates", TITLE, 40, 24, 1000, 36)
    page.vertex(
        "Each layer is built, then its Soda contract runs. Only a passing gate lets the next layer read it.",
        SUBTITLE,
        40,
        60,
        1200,
        28,
    )
    LW = 64
    pass_lane = page.vertex("PASS LANE", band_style(GREEN, "#f0fdf4", LW), 40, 112, 1520, 248)
    block_lane = page.vertex("BLOCK LANE", band_style(RED, "#fef2f2", LW), 40, 392, 1520, 176)
    seq = [
        ("card", "Sources", ["Retail CSV batches", "orders · customers"], "#64748b", ""),
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
        ("card", "Consumers", ["BI · ML · analysts", "trusted gold only"], "#2563eb", ""),
    ]
    CW, GW = 156, 108
    widths = [CW if s[0] == "card" else GW for s in seq]
    inner = 1520 - LW - 48
    gap = (inner - sum(widths)) / (len(seq) - 1)
    x, mid = LW + 24, 132  # relative to the pass lane
    prev, prev_kind = None, None
    for s, w in zip(seq, widths, strict=True):
        if s[0] == "card":
            _k, title, lines, accent, job = s
            cid = page.vertex(html(title, lines), card_style(accent), x, mid - 44, w, 88, pass_lane)
            if job:
                page.vertex(job, NOTE + "fontSize=11;", x - 20, mid - 76, w + 40, 24, pass_lane)
        else:
            _k, name, checks = s
            cid = page.vertex(
                f"<b>{name}</b><br><span style='font-size:11px;color:{MUTED}'>{checks} checks</span>",
                f"rhombus;whiteSpace=wrap;html=1;fillColor=#ffffff;strokeColor={GREEN};strokeWidth=2;"
                f"fontFamily={FONT};fontSize=12;fontColor={INK};",
                x,
                mid - 56,
                w,
                112,
                pass_lane,
            )
            blk = page.vertex(
                html(
                    "Gate blocked · exit 1",
                    ["Airflow task fails,", "downstream upstream_failed", "report + Slack alert"],
                ),
                card_style(RED),
                x + w / 2 - 112 + LW - LW,
                40,
                224,
                104,
                block_lane,
            )
            page.edge(
                cid,
                blk,
                ORTHO + f"strokeColor={RED};dashed=1;fontColor={RED};fontStyle=1;"
                "exitX=0.5;exitY=1;entryX=0.5;entryY=0;labelBackgroundColor=none;align=left;spacingLeft=8;",
                "block",
            )
        if prev is not None:
            color, label = ("#94a3b8", "")
            if prev_kind == "gate":
                color, label = GREEN, "pass"
            elif s[0] == "card" and s[1] == "Bronze":
                label = "ingest"
            page.edge(
                prev,
                cid,
                ORTHO + f"strokeColor={color};fontColor={color};fontStyle=1;"
                "exitX=1;exitY=0.5;entryX=0;entryY=0.5;labelBackgroundColor=none;",
                label,
                dy=-12,
            )
        prev, prev_kind = cid, s[0]
        x += w + gap
    page.vertex(
        "Warnings never block (unless DQ_FAIL_ON_WARN=true) · failures, execution errors and "
        "unevaluated checks block",
        NOTE,
        40,
        584,
        1520,
        28,
    )
    return page


# --- 3. deployment ---------------------------------------------------------------------------------
DEPLOY = [
    (
        "Host · macOS",
        "#2563eb",
        "#eff6ff",
        "Docker Desktop, published ports",
        [
            ("Browser", ["Airflow UI", "http://127.0.0.1:8090"]),
            ("soda-dq on the host", ["uv .venv · DQ_ENGINE=duckdb", "→ localhost:8181 · :9000"]),
            ("Trino client", ["localhost:8088", "(full profile, TRINO=1)"]),
        ],
    ),
    (
        "Orchestration",
        "#a21caf",
        "#fdf4ff",
        "ldl-net",
        [
            ("ldl-airflow-apiserver", ["Airflow 3 · :8080 →", "127.0.0.1:${AIRFLOW_API_PORT}"]),
            ("ldl-airflow-scheduler", ["+ ldl-airflow-dag-processor", "DAG bundles: dags-folder, soda-dq"]),
            ("ldl-airflow-postgres", ["Airflow metadata", "database"]),
            ("ldl-docker-proxy", ["socket proxy (allowlist)", "exec: ldl-spark · ldl-soda"]),
        ],
    ),
    (
        "Compute & query",
        "#d97706",
        "#fffbeb",
        "ldl-net",
        [
            ("ldl-spark", ["Spark 4.1.3 · Iceberg 1.12", "bronze · silver · gold jobs"]),
            ("ldl-soda", ["lakehouse-quality-gates:0.2.0", "non-root · read-only · cap_drop ALL"]),
            ("ldl-trino", ["Trino 483 · :8080 → :8088", "optional (TRINO=1)"]),
        ],
    ),
    (
        "Catalog",
        "#0d9488",
        "#f0fdfa",
        "ldl-net",
        [
            ("ldl-lakekeeper", ["Iceberg REST catalog · :8181", "vends scoped S3 credentials"]),
            ("ldl-lakehouse-init", ["bootstraps warehouse", '"lakehouse" + namespaces']),
        ],
    ),
    (
        "Storage",
        "#475569",
        "#f8fafc",
        "ldl-net",
        [
            ("ldl-postgres", ["Lakekeeper catalog", "metadata"]),
            ("ldl-objectstore", ["RustFS (S3) · :9000 / :9001", "s3://lake/warehouse"]),
        ],
    ),
]


def deployment() -> Page:
    X, W, LW = 40, 1520, 88
    BH, GAP, TOP = 120, 40, 112
    page = Page("deployment", 1600, 0)
    page.vertex("Deployment · Docker network ldl-net (verified on macOS)", TITLE, 40, 24, 1100, 36)
    page.vertex(
        "local-data-lakehouse containers plus the ldl-soda runner. typeglass-libsql held host port "
        "8080, so Airflow was published on 8090.",
        SUBTITLE,
        40,
        60,
        1400,
        28,
    )
    y, bands = TOP, []
    for name, accent, tint, note, comps in DEPLOY:
        label = f'{escape(name)}<br><span style="font-size:11px;font-weight:normal">{escape(note)}</span>'
        band = page.vertex(label, band_style(accent, tint, LW), X, y, W, BH)
        cards_row(page, band, comps, accent, LW + 20, W - LW - 40, 16, BH - 32)
        bands.append((y, y + BH))
        y += BH + GAP
    labels = ["HTTP / JDBC", "docker exec (via proxy)", "Iceberg REST", "metadata · S3 files"]
    for ((_t1, b1), (t2, _b2)), lab in zip(itertools.pairwise(bands), labels, strict=True):
        cx = X + LW + (W - LW) / 2
        page.free_edge(
            cx,
            b1,
            cx,
            t2,
            DOWN + f"fontFamily={FONT};fontSize=11;fontColor={MUTED};labelBackgroundColor=#ffffff;",
            lab,
        )
    page.height = int(y + 8)
    return page


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for page in (layered(), medallion(), deployment()):
        write(OUT / f"{page.name}.drawio", page)
        print("wrote", OUT / f"{page.name}.drawio")
