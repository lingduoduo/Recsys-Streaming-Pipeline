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


def test_sections_are_collapsible_disclosures():
    """Thirteen full-height sections stacked in one column is what this replaced.

    Every card renders through Section, so one component decides it for all of them.
    """
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    section = re.search(r"export function Section\(\{(.*?)\n\}", ui, re.S)
    assert section, "ui.jsx must export a Section component"
    body = section.group(1)
    assert "<details" in body and "<summary" in body, (
        "Section must render a <details>/<summary> disclosure; a plain <section> is the "
        "stacked layout this replaced"
    )


def test_a_collapsed_section_still_shows_its_headline():
    """Collapsing without the headline turns the page into a contents list.

    Every section computes a headline -- "CTR 4.2%", "p95 42 ms" -- and the collapsed
    row is where it has to appear for the closed page to still read as a report.
    """
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    summary = re.search(r"<summary[^>]*>(.*?)</summary>", ui, re.S)
    assert summary, "Section's disclosure must have a summary"
    assert "headline" in summary.group(1), (
        "the summary must render {headline}; without it a collapsed section shows only "
        "its title"
    )


def test_na_cards_are_not_disclosures():
    """A section with no measurement has nothing to expand into."""
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    na = re.search(r"export function NaCard\(\{(.*?)\n\}", ui, re.S)
    assert na, "ui.jsx must export a NaCard component"
    assert "<details" not in na.group(1), (
        "NaCard must stay a flat card: opening it would reveal nothing"
    )


def test_the_hash_target_is_opened():
    """A scorecard tile links to /online#satisfaction.

    With sections collapsed, that anchor would otherwise land a reader on a closed row.
    Nav already runs on every route and is already a client component, so it is where
    the eight lines of DOM work belong rather than in a second client boundary.
    """
    nav = (FRONTEND / "components" / "nav.jsx").read_text(encoding="utf-8")
    assert "hashchange" in nav, "Nav must react to hashchange, not only to the first load"
    assert re.search(r"\.open\s*=\s*true", nav), (
        "Nav must set open on the <details> the hash names, or a tile click lands on a "
        "collapsed section"
    )


def test_a_collapsed_measurement_row_shows_a_number():
    """The seven measurement sections carry prose headlines, not figures.

    build_measurement_dashboard sets satisfaction's headline to "Observed user
    satisfaction", so collapsing them would have shown a label and no number -- while the
    six diagnostics, whose headlines are computed strings like "CTR 22% · CVR 5%", would.
    MeasurementSection reuses the HEADLINES spec the scorecard tiles use, so both halves
    of a closed page report something.
    """
    measurements = (FRONTEND / "components" / "measurements.jsx").read_text(encoding="utf-8")
    assert "headlineValue" in measurements, (
        "MeasurementSection must compute the same figure the scorecard shows"
    )
    assert re.search(r"metric=\{metric\}", measurements), (
        "MeasurementSection must pass that figure to Section as `metric`"
    )
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    summary = re.search(r"<summary[^>]*>(.*?)</summary>", ui, re.S)
    assert summary and "metric" in summary.group(1), (
        "Section's summary must render {metric}, or the figure never reaches the row"
    )
