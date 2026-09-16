"""The retrieval service moved to lingduoduo/Recsys-Backend-Service.

These guard against a reference creeping back into a file meant to describe this
repository as it is now.

Two categories are deliberately not scanned. Dated design records under
.superpowers/docs, docs/superpowers and .planning are history: they describe the
repository as it was and must not be rewritten. And .py and .scala files carry
provenance comments that name the Java classes which produced a fixture or a feature
layout -- GrpoFeatures in grpo_offline_eval.py, RecsysEventAvroCodec in
ServingImpressionFixtureSpec.scala -- which remain true statements about where the
data came from. Scanning .py would also flag this file, which contains the search
string itself, and the `not in` assertion in test_service_scripts.py.
"""

from pathlib import Path

GIT_ROOT = Path(__file__).resolve().parents[2]
NEEDLE = "java-retrieval-service"

HISTORICAL_PREFIXES = (".superpowers/", "docs/superpowers/", ".planning/")
SKIP_DIRS = {
    ".git", "target", "node_modules", ".next", "__pycache__",
    ".worktrees", ".pytest_cache", ".venv", "venv",
}
BUILD_SUFFIXES = {".xml", ".yml", ".yaml", ".sh"}
DOC_SUFFIXES = {".md", ".html"}


def references(suffixes):
    """Every live file with one of `suffixes` that still names the service."""
    hits = []
    for path in GIT_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in suffixes:
            continue
        relative = path.relative_to(GIT_ROOT)
        if relative.as_posix().startswith(HISTORICAL_PREFIXES):
            continue
        if set(relative.parts) & SKIP_DIRS:
            continue
        if NEEDLE in path.read_text(encoding="utf-8", errors="ignore"):
            hits.append(relative.as_posix())
    return sorted(hits)


def test_no_build_file_workflow_or_script_references_the_extracted_service():
    hits = references(BUILD_SUFFIXES)
    assert not hits, f"build/CI/script files still reference the service: {hits}"


def test_no_live_document_references_the_extracted_service():
    hits = references(DOC_SUFFIXES)
    assert not hits, f"live documents still reference the service: {hits}"
