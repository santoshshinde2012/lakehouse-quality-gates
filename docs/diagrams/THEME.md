# Diagram theme

Every diagram in this repo is a native Mermaid block that GitHub renders inline. The sources live in
this folder as `.mmd` files. `make diagrams` copies them into `README.md` and every page under `docs/` that has a
`<!-- diagram:NAME -->` marker; `make diagrams-check` (and `pytest`) fails if a copy is stale. The
[catalogue](README.md) lists every diagram and where it is shown.

## Principles

- **Restrained.** White and very light grey zones with thin slate borders. One accent, indigo, marks
  this project's components. Red is used only for *blocked*, green only for *pass*.
- **System first.** The README hero shows systems and data flow, not classes. Code structure has its
  own diagram, and details go in tables below the diagrams, not inside boxes.
- **Short labels.** At most three words, plus an optional small second line. No class names in
  system views.
- **Readable on GitHub light and dark.** Every node and zone has an explicit light fill, dark text and a
  border; edge labels sit on white. The canvas itself is transparent.
- **Shapes carry meaning.** Cylinders are storage, stadiums are people, alerts and outcomes, and
  double-bordered boxes are jobs or containers.
- **Layout.** Group with subgraphs and link zone to zone (`SRC --> LAKE`) instead of node to node. This
  keeps each zone's internal `direction`, makes the zones line up, and avoids crossings. `linkStyle`
  indexes count every link, invisible `~~~` ones included.

## File layout

Every source starts with the same two parts, and the tests enforce both:

1. **The shared init line** (`%%{init: ...}%%`), identical in every file.
2. **A header comment**: `%% Title:`, `%% Purpose:`, `%% Last updated:` and a pointer back to this theme.

Sequence diagrams use `autonumber`, and a `Note` marks each key decision. Charts (`xychart-beta`) have no
legend, so the page names each series under the chart.

## Palette

| Role | classDef / style | Fill | Border | Text |
|---|---|---|---|---|
| Component | `node` | `#ffffff` | `#94a3b8` slate-400 | `#0f172a` |
| Storage | `store` | `#f1f5f9` slate-100 | `#64748b` slate-500 | `#0f172a` |
| Person, alert, outcome | `actor` | `#ffffff` | `#64748b` slate-500 | `#0f172a` |
| This project | `ours` | `#eef2ff` indigo-50 | `#4f46e5` indigo-600 | `#1e1b4b` |
| Pass | `pass` | `#f0fdf4` green-50 | `#16a34a` green-600 | `#14532d` |
| Blocked | `blocked` | `#fef2f2` red-50 | `#dc2626` red-600 | `#7f1d1d` |
| Zone | `style X …` | `#f8fafc` slate-50 | `#cbd5e1` slate-300 | `#334155` |
| Our zone | `style X …` | `#f5f7ff` | `#a5b4fc` indigo-300 | `#3730a3` |

Edges use `#64748b`. Pass edges are 2px green `#16a34a`; fail edges are dotted, 1.5px red `#dc2626`.
The font is Inter, falling back to `system-ui`, at 14px.

## Reusable snippet

````text
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#ffffff','primaryTextColor':'#0f172a','primaryBorderColor':'#94a3b8','secondaryColor':'#f8fafc','tertiaryColor':'#f8fafc','lineColor':'#64748b','textColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#cbd5e1','titleColor':'#334155','edgeLabelBackground':'#ffffff','noteBkgColor':'#f8fafc','noteTextColor':'#0f172a','noteBorderColor':'#cbd5e1','actorBkg':'#ffffff','actorBorder':'#94a3b8','actorTextColor':'#0f172a','actorLineColor':'#94a3b8','signalColor':'#475569','signalTextColor':'#0f172a','labelBoxBkgColor':'#f8fafc','labelBoxBorderColor':'#94a3b8','labelTextColor':'#0f172a','loopTextColor':'#0f172a','activationBkgColor':'#eef2ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a','xyChart':{'backgroundColor':'#ffffff','plotColorPalette':'#4f46e5, #d97706, #0d9488','titleColor':'#0f172a','xAxisLabelColor':'#334155','yAxisLabelColor':'#334155','xAxisLineColor':'#94a3b8','yAxisLineColor':'#94a3b8'}}}}%%
flowchart LR
  subgraph A["Zone A"]
    direction TB
    N1["Component"]:::node
    N2[("Storage")]:::store
    N1 --> N2
  end
  subgraph B["Our zone"]
    direction TB
    N3("Our component"):::ours
    N4(["Blocked"]):::blocked
    N3 -. fail .-> N4
  end
  A -->|read| B
  classDef node fill:#ffffff,stroke:#94a3b8,stroke-width:1px,color:#0f172a
  classDef store fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef ours fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef actor fill:#ffffff,stroke:#64748b,stroke-width:1px,color:#0f172a
  classDef blocked fill:#fef2f2,stroke:#dc2626,stroke-width:1.5px,color:#7f1d1d
  classDef pass fill:#f0fdf4,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  style A fill:#f8fafc,stroke:#cbd5e1,stroke-width:1px,color:#334155
  style B fill:#f5f7ff,stroke:#a5b4fc,stroke-width:1.5px,color:#3730a3
  linkStyle 1 stroke:#dc2626,stroke-width:1.5px
````

## Rendering locally

```bash
make diagrams        # sync the Markdown copies, render docs/diagrams/svg/*.svg with npx @mermaid-js/mermaid-cli
make diagrams-check  # stale-copy check + parse every source with mermaid-cli (no files written)
```

## Charts (xychart-beta)

The shared init also sets `xyChart` variables (white background, slate axes and a three-colour plot palette:
indigo `#4f46e5`, amber `#d97706`, teal `#0d9488`), so benchmark charts such as `10-scaling.mmd` use the
same look as the flowcharts. Mermaid charts have no legend; name the series in the text under the chart.
