"""Every relative link in live documentation resolves, every documented retrieval
endpoint carries the prefix the backend serves it at, and every flow document says
which repository its stage runs in.

PR #244 rewrote the ten headings in docs/recommendation_architecture/API.md to carry
/api/v1/retrieval and left behind the two anchors that pointed at them. The same
commit's message claimed it had "corrected every stale endpoint path" while twenty
bare paths survived it. Both regressions are silent: nothing compiles a markdown
link, and nothing calls a path written in prose. These tests are the check that was
missing.

Two categories are deliberately not scanned. Dated design records under
.superpowers/docs and .planning are history -- they describe the repository as it was
and must not be rewritten. And http/mailto targets are not fetched: that would make
the suite depend on the network and on another repository's default branch, so a link
to a renamed file in Recsys-Backend-Service still breaks here silently.
"""

import re
from pathlib import Path

GIT_ROOT = Path(__file__).resolve().parents[2]

HISTORICAL_PREFIXES = (".superpowers/", ".planning/")
SKIP_DIRS = {
    ".git", "target", "node_modules", ".next", "__pycache__",
    ".worktrees", ".pytest_cache", ".venv", "venv",
}

LINK = re.compile(r"\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#+\s+(.*)")


def live_markdown():
    """Every markdown file describing the repository as it is now."""
    for path in sorted(GIT_ROOT.rglob("*.md")):
        relative = path.relative_to(GIT_ROOT)
        if relative.as_posix().startswith(HISTORICAL_PREFIXES):
            continue
        if set(relative.parts) & SKIP_DIRS:
            continue
        yield relative, path.read_text(encoding="utf-8")


def slugs(path):
    """The anchors GitHub generates for a markdown file's headings.

    Lowercase, drop backticks/asterisks/underscores/brackets/parentheses, drop every
    remaining character outside [a-z0-9 -], then spaces to hyphens. A `#` comment
    inside a fenced block reads as a heading here, which can only add an anchor that
    nothing links to -- it cannot mask a broken link.
    """
    found = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = HEADING.match(line)
        if not match:
            continue
        text = match.group(1).strip().lower()
        text = re.sub(r"[`*_\[\]()]", "", text)
        text = re.sub(r"[^a-z0-9 -]", "", text)
        found.add(text.replace(" ", "-"))
    return found


def test_every_relative_documentation_link_resolves():
    broken = []
    for relative, text in live_markdown():
        for match in LINK.finditer(text):
            target = match.group(1)
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path_part, _, fragment = target.partition("#")
            source = GIT_ROOT / relative
            resolved = (source.parent / path_part).resolve() if path_part else source
            line = text[: match.start()].count("\n") + 1
            if not resolved.exists():
                broken.append(f"{relative}:{line} -> {target} (no such file)")
                continue
            if fragment and resolved.suffix == ".md" and fragment.lower() not in slugs(resolved):
                broken.append(f"{relative}:{line} -> {target} (no such heading)")
    assert not broken, "broken documentation links:\n" + "\n".join(broken)


PREFIX = "/api/v1/retrieval"

# The two aligned ASCII blocks in recsys-pipeline/README.md carry this note above the
# fence instead of the prefix inline: expanding the paths inside them would shift
# every arrow on the line out of alignment.
PREFIX_NOTE = "Paths are relative to the service prefix"

ENDPOINTS = "recommend|feedback|metrics|predict|embedding|users"
BARE_ENDPOINT = re.compile(rf"(?:(?:GET|POST)\s+|localhost:8080)/(?:{ENDPOINTS})\b")


def noted_fence_lines(text):
    """Line numbers inside a fenced block whose note above the fence declares the prefix."""
    lines = text.splitlines()
    noted, inside, declared = set(), False, False
    for index, line in enumerate(lines):
        if line.startswith("```"):
            if inside:
                inside = False
            else:
                inside = True
                declared = any(PREFIX_NOTE in before for before in lines[max(0, index - 3): index])
            continue
        if inside and declared:
            noted.add(index + 1)
    return noted


def test_documented_retrieval_endpoints_carry_the_service_prefix():
    bare = []
    for relative, text in live_markdown():
        noted = noted_fence_lines(text)
        for match in BARE_ENDPOINT.finditer(text):
            line = text[: match.start()].count("\n") + 1
            if line in noted:
                continue
            bare.append(f"{relative}:{line} -> {match.group(0)}")
    assert not bare, (
        f"retrieval endpoints documented without the {PREFIX} prefix:\n" + "\n".join(bare)
    )


BOUNDARY_SERVICE = "Recsys-Backend-Service"
BOUNDARY_ANCHOR = "README.md#repository-boundary"

FLOWS = GIT_ROOT / "recsys-pipeline" / "docs" / "recommendation_flows"


def test_every_flow_document_marks_the_repository_boundary():
    """Eight of the nine stages run entirely in the other repository.

    A reader who opens one of these mid-narrative must be told that before they
    read a class name, because this checkout has no Java to check it against.
    """
    unmarked = []
    for path in sorted(FLOWS.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        if BOUNDARY_SERVICE not in text or BOUNDARY_ANCHOR not in text:
            unmarked.append(path.name)
    assert not unmarked, "flow documents that do not mark the boundary: " + ", ".join(unmarked)


def test_no_document_claims_an_in_repo_serving_side_effect_writer():
    """served_history and impressions left with the service.

    Neither key has a writer, a reader or a test in this checkout; they appear
    only in documentation. Saying otherwise claims a capability this repository
    does not have.
    """
    offenders = [
        f"{relative}" for relative, text in live_markdown() if "in-repo serving side-effect" in text
    ]
    assert not offenders, "documents claiming an in-repo serving writer: " + ", ".join(offenders)
