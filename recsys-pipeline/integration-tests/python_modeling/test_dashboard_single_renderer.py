"""The dashboard has one renderer: the React app reading frontend/data/dashboard.json.

analysis_dashboard_report.py used to carry a second one -- 249 lines building a
self-contained index.html for six of the dashboard's thirteen sections -- and
run-movie-category-sim.sh ran both on every run, then named the smaller artifact
in its closing banner. The snapshot publishes every field that renderer read, at
equal or greater row counts, so the HTML was a strict subset of the React app.

_esc and _ci are deliberately not guarded below. Both are generic enough that a
future compute function could legitimately want them, and a guard that forbids a
two-line string escaper is one that gets deleted rather than obeyed.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MODELING = REPO / "services" / "python-modeling"
SIM = REPO / "scripts" / "run-movie-category-sim.sh"

sys.path.insert(0, str(MODELING))

RENDERING_SYMBOLS = (
    "render_html", "svg_bar", "svg_line", "html_table", "na_card", "section", "main",
)


def test_compute_module_exposes_no_html_rendering():
    import analysis_dashboard_report as dash

    present = [name for name in RENDERING_SYMBOLS if hasattr(dash, name)]
    assert not present, (
        "analysis_dashboard_report is the dashboard's compute layer; these rendering "
        f"symbols are back: {', '.join(present)}"
    )
