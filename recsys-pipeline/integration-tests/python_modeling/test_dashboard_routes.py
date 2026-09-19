"""Every dashboard section is mounted on exactly one page, on the page its route names.

The dashboard was one page with thirteen sections. Splitting it across /online and
/offline meant moving components between files, which is how a section gets silently
dropped: nothing compiles a JSX import that no page renders, and the page still builds.

components/groups.js is the single source of truth. These tests hold the pages to it.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FRONTEND = REPO / "frontend"
GROUPS = FRONTEND / "components" / "groups.js"

# Section key -> the component that renders it. The key is what groups.js maps to a route
# and what dashboard.json calls the section; the component is what a page mounts.
COMPONENTS = {
    "query": "QuerySection", "keyword": "KeywordSection", "engagement": "EngagementSection",
    "satisfaction": "SatisfactionSection", "freshness": "FreshnessSection",
    "diversity": "DiversitySection", "fairness": "FairnessSection", "safety": "SafetySection",
    "latency": "LatencySection", "recall": "RecallSection", "ranking": "RankingSection",
    "ope": "OpeSection", "relevance": "RelevanceSection",
}


def _section_route():
    """SECTION_ROUTE parsed out of groups.js, so the test reads the same map the app does."""
    source = GROUPS.read_text(encoding="utf-8")
    body = re.search(r"export const SECTION_ROUTE = \{(.*?)\};", source, re.S)
    assert body, "groups.js must export a SECTION_ROUTE map"
    return dict(re.findall(r"(\w+):\s*\"([^\"]+)\"", body.group(1)))


def _pages():
    """Each route path mapped to its page source."""
    pages = {}
    for path in sorted(FRONTEND.glob("app/**/page.jsx")):
        relative = path.parent.relative_to(FRONTEND / "app")
        route = "/" if str(relative) == "." else f"/{relative}"
        pages[route] = path.read_text(encoding="utf-8")
    return pages


def test_every_section_is_mounted_on_the_page_its_route_names():
    routes, pages = _section_route(), _pages()
    missing = []
    for key, route in sorted(routes.items()):
        component = COMPONENTS[key]
        if f"<{component}" not in pages.get(route, ""):
            missing.append(f"{key}: groups.js says {route}, but {route} does not mount <{component}")
    assert not missing, "sections not mounted where the map says:\n" + "\n".join(missing)


def test_no_section_is_mounted_on_two_pages():
    pages = _pages()
    duplicated = []
    for key, component in sorted(COMPONENTS.items()):
        mounted = [route for route, source in pages.items() if f"<{component}" in source]
        if len(mounted) > 1:
            duplicated.append(f"{component} on {', '.join(sorted(mounted))}")
    assert not duplicated, "sections mounted more than once:\n" + "\n".join(duplicated)


def test_every_exported_section_component_has_a_route():
    """A new section that nobody placed would otherwise render nowhere and fail nothing."""
    exported = set()
    for path in sorted((FRONTEND / "components").glob("*.jsx")):
        exported.update(re.findall(r"export function (\w+Section)\b", path.read_text(encoding="utf-8")))
    placed = set(COMPONENTS.values())
    unplaced = sorted(exported - placed)
    assert not unplaced, (
        "these section components are exported but appear in no route mapping: "
        + ", ".join(unplaced)
    )


def test_scorecard_tiles_link_through_the_route_map():
    """A bare `#key` href was correct on one page and reaches nothing across three."""
    source = (FRONTEND / "components" / "scorecard.jsx").read_text(encoding="utf-8")
    assert "SECTION_ROUTE[key]" in source, (
        "Scorecard must build tile hrefs from SECTION_ROUTE; a bare fragment only "
        "resolves when every section shares one page"
    )


def test_every_css_variable_used_is_defined():
    """An undefined custom property is silently dropped, and nothing catches it.

    `border-bottom: 1px solid var(--border)` with no --border declared computes to no
    border at all: the stylesheet still parses, `npm run build` still succeeds, and the
    rule just does not apply. #255 shipped exactly that twice in the nav.
    """
    css = (FRONTEND / "app" / "globals.css").read_text(encoding="utf-8")
    declared = set(re.findall(r"^\s*(--[\w-]+)\s*:", css, re.M))
    # A property can also be declared inline from JSX -- keyword-report.jsx sets
    # {"--token-score": t} per token -- so the stylesheet alone is not the full picture.
    for path in sorted((FRONTEND / "components").glob("*.jsx")):
        declared.update(re.findall(r"\"(--[\w-]+)\"\s*:", path.read_text(encoding="utf-8")))
    used = set(re.findall(r"var\((--[\w-]+)", css))
    undefined = sorted(used - declared)
    assert not undefined, (
        "these custom properties are used but never declared, so every rule using them "
        f"is silently dropped: {', '.join(undefined)}"
    )
