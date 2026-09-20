"""The dashboard's catalogue, its routes and its chrome agree with each other.

Thirteen sections, one per page, listed in a sidebar. components/groups.js is the
catalogue and the single source of truth; these tests hold everything else to it.

Four assertions from earlier iterations are gone because the designs they guarded are:
sections are no longer mounted in page files (one dynamic route renders them all), and
they are no longer collapsible (#256 made them disclosures when nine shared a route,
which is the wrong shape for one section per page).
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FRONTEND = REPO / "frontend"
GROUPS = FRONTEND / "components" / "groups.js"
REGISTRY = FRONTEND / "components" / "section-registry.jsx"
SIDEBAR = FRONTEND / "components" / "sidebar.jsx"
PAGE = FRONTEND / "app" / "[group]" / "[section]" / "page.jsx"

SECTION_KEYS = {
    "query", "keyword", "engagement", "satisfaction", "freshness", "diversity",
    "fairness", "safety", "latency", "recall", "ranking", "relevance", "ope",
}


def _catalogue():
    """The SECTIONS map parsed out of groups.js: key -> group."""
    source = GROUPS.read_text(encoding="utf-8")
    body = re.search(r"export const SECTIONS = \{(.*?)\n\};", source, re.S)
    assert body, "groups.js must export a SECTIONS map"
    return dict(re.findall(r"(\w+):\s*\{\s*\n?\s*group:\s*\"(\w+)\"", body.group(1)))


def test_the_catalogue_lists_every_section():
    assert set(_catalogue()) == SECTION_KEYS


def test_every_section_has_a_component_and_every_component_a_section():
    """No page file names a component now, so the registry is where a section goes missing."""
    registry = REGISTRY.read_text(encoding="utf-8")
    mapped = set(re.findall(r"^\s+(\w+):\s*\w+Section,", registry, re.M))
    catalogue = set(_catalogue())
    assert mapped == catalogue, (
        f"registry and catalogue disagree: only in registry {sorted(mapped - catalogue)}, "
        f"only in catalogue {sorted(catalogue - mapped)}"
    )


def test_every_section_prerenders():
    """generateStaticParams enumerates the catalogue, so all thirteen are built."""
    page = PAGE.read_text(encoding="utf-8")
    assert "generateStaticParams" in page, "every sidebar link must prerender"
    assert "Object.entries(SECTIONS)" in page, (
        "the params must come from the catalogue, not a second hand-written list"
    )


def test_an_unknown_section_is_a_404():
    page = PAGE.read_text(encoding="utf-8")
    assert "notFound()" in page, "an unknown group/section pair must 404, not render empty"


def test_the_sidebar_lists_every_section_once():
    sidebar = SIDEBAR.read_text(encoding="utf-8")
    assert "SECTION_ROUTE[section]" in sidebar, (
        "sidebar links must come from SECTION_ROUTE so they cannot drift from the catalogue"
    )
    assert "GROUPS.map" in sidebar, "the sidebar must render one labelled block per group"


def test_the_sidebar_does_not_import_the_component_registry():
    """It is a client component: importing the registry would pull every chart into the bundle."""
    sidebar = SIDEBAR.read_text(encoding="utf-8")
    assert "section-registry" not in sidebar
    groups = GROUPS.read_text(encoding="utf-8")
    # An import statement, not the word: groups.js's own comment says "imported only by
    # the server page", and a substring check would flag its own documentation.
    statements = re.findall(r"^import\b.*", groups, re.M)
    assert not statements, (
        "groups.js must stay pure data so the client sidebar can import it cheaply; found: "
        + "; ".join(statements)
    )


def test_the_page_header_names_the_section():
    page = PAGE.read_text(encoding="utf-8")
    for fragment in ("{groupLabel}", "{spec.label}", "{spec.description}"):
        assert fragment in page, f"the page header must render {fragment}"


def test_sections_are_flat_cards():
    """#256 made these disclosures for a nine-section route. One per page has nothing to collapse."""
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    section = re.search(r"export function Section\(\{(.*?)\n\}", ui, re.S)
    assert section, "ui.jsx must export a Section component"
    assert "<details" not in section.group(1), (
        "Section must be a flat card: with one section per page a disclosure opens as a "
        "single collapsed row"
    )


def test_a_section_card_does_not_repeat_the_page_title():
    ui = (FRONTEND / "components" / "ui.jsx").read_text(encoding="utf-8")
    section = re.search(r"export function Section\(\{(.*?)\n\}", ui, re.S)
    assert "{title}" not in section.group(1), (
        "the page header renders the title; rendering it here too prints it twice"
    )


def test_measurement_sections_still_carry_their_figure():
    """The seven measurement sections have prose headlines, so the number comes from HEADLINES."""
    measurements = (FRONTEND / "components" / "measurements.jsx").read_text(encoding="utf-8")
    assert "headlineValue" in measurements and re.search(r"metric=\{metric\}", measurements), (
        "MeasurementSection must pass the scorecard's figure to Section as `metric`"
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


def test_every_section_has_a_tile_on_the_overview():
    """A section added to the catalogue must not skip the summary.

    The overview is the only page showing every section at once, so a section with no
    tile is invisible there while looking complete -- nothing else would fail.
    """
    scorecard = (FRONTEND / "components" / "scorecard.jsx").read_text(encoding="utf-8")
    specced = set()
    for name in ("HEADLINES", "DIAGNOSTICS"):
        body = re.search(rf"const {name} = \{{(.*?)\n\}};", scorecard, re.S)
        assert body, f"scorecard.jsx must declare a {name} map"
        specced.update(re.findall(r"^  (\w+):", body.group(1), re.M))
    catalogue = set(_catalogue())
    missing = sorted(catalogue - specced)
    assert not missing, (
        "these sections are in the catalogue but have no overview tile: " + ", ".join(missing)
    )


def test_row_based_tiles_declare_which_extremum_they_take():
    """A default of "max" is the flattering choice, and it should be deliberate.

    Three of the four row-based tiles were wrong in #260: ranking reported its best
    AUC while a signal sat near random, keyword took the maximum of a signed field and
    so reported the smaller mismatch, and recall took an extremum over rows that vary
    by k, which reported k rather than quality. Each now says which extremum it means.
    """
    scorecard = (FRONTEND / "components" / "scorecard.jsx").read_text(encoding="utf-8")
    body = re.search(r"const DIAGNOSTICS = \{(.*?)\n\};", scorecard, re.S)
    assert body, "scorecard.jsx must declare a DIAGNOSTICS map"
    entries = re.split(r"\n  (?=\w+: \{)", body.group(1))
    undeclared = []
    for entry in entries:
        name = re.match(r"\s*(\w+):", entry)
        if not name or "rows:" not in entry:
            continue
        if "select:" not in entry and "where:" not in entry:
            undeclared.append(name.group(1))
    assert not undeclared, (
        "these row-based tiles take an extremum without saying which, so they default to "
        f"the flattering maximum: {', '.join(undeclared)}"
    )


def test_no_spread_object_carries_a_react_key():
    """React requires `key` passed directly to an element, never through a spread.

    #260 built `const common = { key, href, title }` and spread it into <MetricTile>,
    which React rejects at render time. Nothing here caught it: the Python guards read
    source text, and `npm run build` prerendered the broken page without complaint. It
    surfaced only when someone opened the dashboard.

    This checks the shape rather than the behaviour -- an object literal that is spread
    into JSX must not define `key`.
    """
    offenders = []
    for path in sorted((FRONTEND / "components").glob("*.jsx")):
        source = path.read_text(encoding="utf-8")
        for name in sorted(set(re.findall(r"\{\.\.\.(\w+)\}", source))):
            body = re.search(rf"const {name} = \{{(.*?)\}};", source, re.S)
            if body and re.search(r"(^|[{,\s])key\s*[,:]", body.group(1)):
                offenders.append(f"{path.name}: `{name}` is spread into JSX and defines `key`")
    assert not offenders, (
        "React keys must be passed directly to the element:\n" + "\n".join(offenders)
    )


def test_the_keyword_heatmap_degrades_when_the_grid_is_absent():
    """Every snapshot exported before the grid existed lacks it.

    An empty table would read as "no genres were served"; the section has to say the
    snapshot predates the grid instead.
    """
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    assert "grid" in report, "keyword-report.jsx must render data.keyword.grid"
    heatmap = re.search(r"function RelevanceHeatmap\((.*?)\n\}", report, re.S)
    assert heatmap, "the heatmap must be its own component"
    assert re.search(r"re-?export|fresh export|older snapshot", heatmap.group(1), re.I), (
        "an absent grid must explain itself rather than render an empty table"
    )


def test_the_topic_heatmap_marks_its_structural_diagonal():
    """An item whose primary genre is Action always carries Action, so every (X, X) cell
    is forced. Unmarked, the bright diagonal reads as a finding."""
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    assert "topic_grid" in report, "the section must render data.topic_grid"
    assert "heat-forced" in report, "the diagonal needs its own class to be distinguishable"
    css = (FRONTEND / "app" / "globals.css").read_text(encoding="utf-8")
    assert "heat-forced" in css, "heat-forced must be styled, or the marking is invisible"
    # The reader has to be told what the outline means.
    assert re.search(r"forced|always carries|by construction", report, re.I), (
        "the legend must say why those cells are outlined"
    )


def test_the_heatmap_fallback_does_not_hardcode_one_axis_name():
    """RelevanceHeatmap draws both the category and the topic grid. A message naming one of
    them would be wrong on the other -- the hazard of generalising a component but not its copy.
    """
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    heatmap = re.search(r"function RelevanceHeatmap\((.*?)\n\}", report, re.S)
    assert heatmap, "the heatmap must be its own component"
    fallback = re.search(r"if \(!rows\?\.length\) \{(.*?)\n  \}", heatmap.group(1), re.S)
    assert fallback, "the empty-rows fallback must be present"
    assert "category" not in fallback.group(1), (
        "the fallback names a specific axis; it renders for the topic grid too"
    )
    assert "rowLabel" in fallback.group(1), "it should name the axis it was given"


def test_heatmap_column_headers_are_scoped():
    """Both axes carry meaning, so a cell needs its column header associated as well as its row."""
    report = (FRONTEND / "components" / "keyword-report.jsx").read_text(encoding="utf-8")
    heatmap = re.search(r"function RelevanceHeatmap\((.*?)\n\}", report, re.S).group(1)
    assert 'scope="row"' in heatmap, "row headers must stay scoped"
    assert 'scope="col"' in heatmap, "column headers must be scoped too"
