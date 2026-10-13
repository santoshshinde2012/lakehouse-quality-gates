# Diagram theme

Every diagram in this repo is a native Mermaid block that GitHub renders inline. The sources live in
this folder as `.mmd` files and are copied into `README.md` and `docs/architecture.md` by
`make diagrams`; `make diagrams-check` (also run by `pytest`) fails if a copy is stale.

## Design rules

- **One shared init directive** on the first line of every block: the `base` theme with explicit colours,
  so GitHub doesn't swap in its own dark palette.
- **Explicit light fills with dark text and a darker border on every node and band.** The diagram
  background is transparent, so this is what keeps it readable on GitHub light *and* dark.
- Edge labels sit on a white `edgeLabelBackground`; edges use slate `#64748b`, which shows up on both
  backgrounds. Sequence diagrams wrap their messages in a light `rect` panel for the same reason.
- **One accent per layer** (below), reused across every diagram, so a colour always means the same thing.
- Pass edges are thick green; fail edges are dashed red and point to a red **Blocked** node.
- Font: Inter, falling back to `system-ui`; 14px.

## Palette

| Role | classDef | Fill | Border | Text |
|---|---|---|---|---|
| Entry points | `entry` | `#e0f2fe` sky-100 | `#0284c7` sky-600 | `#0c4a6e` |
| Composition root | `root` | `#e2e8f0` slate-200 | `#475569` slate-600 | `#0f172a` |
| Application | `app` | `#e0e7ff` indigo-100 | `#4f46e5` indigo-600 | `#1e1b4b` |
| Domain | `domain` | `#ede9fe` violet-100 | `#7c3aed` violet-600 | `#2e1065` |
| Adapters | `adapter` | `#ccfbf1` teal-100 | `#0d9488` teal-600 | `#134e4a` |
| Data platform | `platform` | `#fef3c7` amber-100 | `#d97706` amber-600 | `#451a03` |
| Cross-cutting | `xcut` | `#f1f5f9` slate-100, dashed | `#64748b` slate-500 | `#1e293b` |
| Pass / published | `pass` | `#dcfce7` green-100 | `#16a34a` green-600 | `#14532d` |
| Fail / blocked | `fail` | `#fee2e2` red-100 | `#dc2626` red-600 | `#7f1d1d` |

Bands (subgraphs) use the 50-shade of their layer colour as the fill and the 600-shade as the border.

## Reusable snippet

Paste the first line unchanged, then pick the classDefs you need:

````text
%%{init: {'theme':'base','themeVariables':{'fontFamily':'Inter, system-ui, -apple-system, Segoe UI, Helvetica, Arial, sans-serif','fontSize':'14px','primaryColor':'#eef2ff','primaryTextColor':'#0f172a','primaryBorderColor':'#6366f1','secondaryColor':'#f1f5f9','secondaryTextColor':'#0f172a','secondaryBorderColor':'#94a3b8','tertiaryColor':'#f8fafc','tertiaryTextColor':'#0f172a','tertiaryBorderColor':'#cbd5e1','lineColor':'#64748b','textColor':'#0f172a','nodeTextColor':'#0f172a','titleColor':'#0f172a','clusterBkg':'#f8fafc','clusterBorder':'#94a3b8','edgeLabelBackground':'#ffffff','noteBkgColor':'#fef9c3','noteTextColor':'#422006','noteBorderColor':'#ca8a04','actorBkg':'#eef2ff','actorBorder':'#6366f1','actorTextColor':'#1e1b4b','actorLineColor':'#94a3b8','signalColor':'#334155','signalTextColor':'#0f172a','labelBoxBkgColor':'#eef2ff','labelBoxBorderColor':'#6366f1','labelTextColor':'#1e1b4b','loopTextColor':'#1e1b4b','activationBkgColor':'#e0e7ff','activationBorderColor':'#4f46e5','sequenceNumberColor':'#ffffff','classText':'#0f172a'}}}%%
flowchart TB
  subgraph APP["Application"]
    direction LR
    A1["VerifyLayerUseCase"]:::app
    A2["GatePolicy"]:::app
    A1 ~~~ A2
  end
  classDef entry fill:#e0f2fe,stroke:#0284c7,stroke-width:1.5px,color:#0c4a6e
  classDef root fill:#e2e8f0,stroke:#475569,stroke-width:1.5px,color:#0f172a
  classDef app fill:#e0e7ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  classDef domain fill:#ede9fe,stroke:#7c3aed,stroke-width:1.5px,color:#2e1065
  classDef adapter fill:#ccfbf1,stroke:#0d9488,stroke-width:1.5px,color:#134e4a
  classDef platform fill:#fef3c7,stroke:#d97706,stroke-width:1.5px,color:#451a03
  classDef xcut fill:#f1f5f9,stroke:#64748b,stroke-width:1.5px,stroke-dasharray:4 3,color:#1e293b
  classDef pass fill:#dcfce7,stroke:#16a34a,stroke-width:1.5px,color:#14532d
  classDef fail fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#7f1d1d
  style APP fill:#eef2ff,stroke:#4f46e5,stroke-width:1.5px,color:#1e1b4b
  linkStyle 0 stroke:#16a34a,stroke-width:2.5px
````

Layout tips used here: put `direction LR` inside each band and chain its nodes with invisible links
(`A ~~~ B`) so they stay in one row, link band to band (`ENTRY ==> ROOT`) instead of node to node to
avoid crossings, and remember that `linkStyle` indexes count invisible links too.

## Rendering locally

```bash
make diagrams        # sync the Markdown copies, render docs/diagrams/svg/*.svg with npx @mermaid-js/mermaid-cli
make diagrams-check  # stale-copy check + parse every source with mermaid-cli (no files written)
```
