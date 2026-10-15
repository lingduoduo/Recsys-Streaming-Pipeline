"""The retrieval service moved to lingduoduo/Recsys-Backend-Service.

These guard against a reference creeping back into a file meant to describe this
repository as it is now.

Two categories are deliberately not scanned. Dated design records under
.superpowers/docs and .planning are history: they describe the repository as it
was and must not be rewritten. And .py and .scala files carry
provenance comments that name the Java classes which produced a fixture or a feature
layout -- GrpoFeatures in grpo_offline_eval.py, RecsysEventAvroCodec in
ServingImpressionFixtureSpec.scala -- which remain true statements about where the
data came from. Scanning .py would also flag this file, which contains the search
string itself, and the `not in` assertion in test_service_scripts.py.
"""

from pathlib import Path

import pytest

GIT_ROOT = Path(__file__).resolve().parents[2]
NEEDLE = "java-retrieval-service"

HISTORICAL_PREFIXES = (".superpowers/", ".planning/")
SKIP_DIRS = {
    ".git", "target", "node_modules", ".next", "__pycache__",
    ".worktrees", ".pytest_cache", ".venv", "venv",
}
BUILD_SUFFIXES = {".xml", ".yml", ".yaml", ".sh"}
DOC_SUFFIXES = {".md", ".html"}


@pytest.fixture(scope="module")
def live_texts():
    """Every live build or document file's text, keyed by its repo-relative path."""
    texts = {}
    for path in GIT_ROOT.rglob("*"):
        if not path.is_file() or path.suffix not in BUILD_SUFFIXES | DOC_SUFFIXES:
            continue
        relative = path.relative_to(GIT_ROOT)
        if relative.as_posix().startswith(HISTORICAL_PREFIXES):
            continue
        if set(relative.parts) & SKIP_DIRS:
            continue
        texts[relative] = path.read_text(encoding="utf-8", errors="ignore")
    return texts


def references(live_texts, suffixes):
    """Every live file with one of `suffixes` that still names the service."""
    return sorted(relative.as_posix() for relative, text in live_texts.items()
                  if relative.suffix in suffixes and NEEDLE in text)


def test_no_build_file_workflow_or_script_references_the_extracted_service(live_texts):
    hits = references(live_texts, BUILD_SUFFIXES)
    assert not hits, f"build/CI/script files still reference the service: {hits}"


def test_no_live_document_references_the_extracted_service(live_texts):
    hits = references(live_texts, DOC_SUFFIXES)
    assert not hits, f"live documents still reference the service: {hits}"
