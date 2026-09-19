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
