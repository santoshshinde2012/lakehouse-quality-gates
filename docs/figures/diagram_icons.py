"""Original outline icons for article figures, authored on a 24-unit SVG grid.

ICON_PATHS maps a name to (element tag, attribute dictionary) tuples. The drawing
elements are unbranded, use no external assets, and can be rendered independently.
"""

from __future__ import annotations

from html import escape

type SVGElement = tuple[str, dict[str, str]]

ICON_PATHS: dict[str, tuple[SVGElement, ...]] = {
    "database": (
        ("ellipse", {"cx": "12", "cy": "5", "rx": "8", "ry": "3"}),
        ("path", {"d": "M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5"}),
        ("path", {"d": "M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"}),
    ),
    "database_check": (
        ("ellipse", {"cx": "10", "cy": "5", "rx": "7", "ry": "3"}),
        ("path", {"d": "M3 5v13c0 1.7 3.1 3 7 3h1M17 5v5"}),
        ("path", {"d": "M3 11c0 1.7 3.1 3 7 3h1"}),
        ("polyline", {"points": "13,17 16,20 22,13"}),
    ),
    "branch": (
        ("circle", {"cx": "6", "cy": "5", "r": "2.5"}),
        ("circle", {"cx": "6", "cy": "19", "r": "2.5"}),
        ("circle", {"cx": "18", "cy": "5", "r": "2.5"}),
        ("path", {"d": "M6 7.5v9M6 14c0-4 12-1 12-6.5"}),
    ),
    "shield_check": (
        ("path", {"d": "M12 2.5 20 6v5c0 5-3.4 8.5-8 10.5C7.4 19.5 4 16 4 11V6Z"}),
        ("polyline", {"points": "8,12 11,15 16,9.5"}),
    ),
    "file": (
        ("path", {"d": "M6 2.5h8L20 8v13.5H6Z"}),
        ("polyline", {"points": "14,2.5 14,8 20,8"}),
        ("path", {"d": "M9 12h8M9 16h8"}),
    ),
    "layers": (
        ("polygon", {"points": "12,2.5 22,8 12,13.5 2,8"}),
        ("polyline", {"points": "2,12 12,17.5 22,12"}),
        ("polyline", {"points": "2,16 12,21.5 22,16"}),
    ),
    "lock": (
        ("rect", {"x": "4", "y": "10", "width": "16", "height": "12", "rx": "2"}),
        ("path", {"d": "M7 10V7a5 5 0 0 1 10 0v3"}),
        ("path", {"d": "M12 15v3"}),
    ),
    "upload": (
        ("polyline", {"points": "6,8 12,2.5 18,8"}),
        ("path", {"d": "M12 3v12M3 15v6h18v-6"}),
    ),
    "users": (
        ("circle", {"cx": "9", "cy": "7", "r": "3.5"}),
        (
            "path",
            {
                "d": "M2.5 21v-2a6.5 6.5 0 0 1 13 0v2"
                "M17 3.8a3.5 3.5 0 0 1 0 6.4M18 13.5a5.5 5.5 0 0 1 3.5 5V21"
            },
        ),
    ),
    "stop": (
        ("polygon", {"points": "8,2.5 16,2.5 21.5,8 21.5,16 16,21.5 8,21.5 2.5,16 2.5,8"}),
        ("path", {"d": "M8 8l8 8M16 8l-8 8"}),
    ),
    "trash": (("path", {"d": "M3 6h18M9 6V2.5h6V6M5 6l1 15.5h12L19 6M10 10v7M14 10v7"}),),
    "clock": (
        ("circle", {"cx": "12", "cy": "12", "r": "9"}),
        ("polyline", {"points": "12,6 12,12 16,14"}),
    ),
    "memory": (
        ("rect", {"x": "5", "y": "5", "width": "14", "height": "14", "rx": "2"}),
        ("rect", {"x": "9", "y": "9", "width": "6", "height": "6", "rx": "1"}),
        ("path", {"d": "M8 2v3M16 2v3M8 19v3M16 19v3M2 8h3M2 16h3M19 8h3M19 16h3"}),
    ),
    "check": (("polyline", {"points": "4,12 9,17 20,6"}),),
}


def icon_svg(name: str, x: float, y: float, size: float = 48, color: str = "#4658c9") -> str:
    """Return a scalable decorative SVG group, positioned at its top-left corner."""
    elements = ICON_PATHS[name]
    if size <= 0:
        raise ValueError("Icon size must be positive")
    parts = [
        f'<g transform="translate({x} {y}) scale({size / 24:g})" '
        f'fill="none" stroke="{escape(color, quote=True)}" stroke-width="2" '
        'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
    ]
    for tag, attributes in elements:
        serialized = " ".join(f'{key}="{escape(value, quote=True)}"' for key, value in attributes.items())
        parts.append(f"<{tag} {serialized}/>")
    return "\n".join([*parts, "</g>"])
