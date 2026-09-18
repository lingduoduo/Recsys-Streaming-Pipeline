"""The four contract files shared with lingduoduo/Recsys-Backend-Service.

This is the "pipeline-owned contract comparison" that RecsysEventSchemaDriftTest in that
repository documents. It reads schemas/CONTRACTS.md rather than repeating its table, so the
document cannot drift from what is enforced.
"""

import hashlib
import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = REPO_ROOT / "schemas" / "CONTRACTS.md"

ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*`([0-9a-f]{64})`\s*\|\s*`([^`]+)`\s*\|$", re.M)


def _manifest_rows():
    """(pipeline path, sha256, service path) for every row of the manifest table."""
    return ROW.findall(MANIFEST.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_every_contract_file_matches_its_recorded_hash():
    for pipeline_path, expected, service_path in _manifest_rows():
        target = REPO_ROOT / pipeline_path
        assert target.exists(), f"{pipeline_path} is listed in CONTRACTS.md but missing"
        assert _sha256(target) == expected, (
            f"{pipeline_path} changed. Recsys-Backend-Service holds a copy at {service_path} "
            f"which must be updated in that repository too; then refresh the hash in "
            f"schemas/CONTRACTS.md."
        )


def test_the_manifest_lists_exactly_the_known_shared_contracts():
    """A new shared contract must be paired here, not added to one repository alone."""
    assert {row[0] for row in _manifest_rows()} == {
        "schemas/recsys-event-v3.avsc",
        "schemas/fixtures/serving-impression-v3.avro",
        "integration-tests/fixtures/user_profile_v1.json",
        "services/spark-streaming-job/src/test/resources/sequence-schema.json",
    }


def test_contract_copies_match_a_real_backend_checkout():
    """Opt-in: the only check here that can catch drift originating in the service."""
    backend = os.environ.get("RECSYS_BACKEND_REPO")
    if not backend or not Path(backend).is_dir():
        pytest.skip("set RECSYS_BACKEND_REPO to a Recsys-Backend-Service checkout to compare copies")

    for pipeline_path, _, service_path in _manifest_rows():
        theirs = Path(backend) / service_path
        assert theirs.exists(), f"{service_path} is missing from {backend}"
        assert theirs.read_bytes() == (REPO_ROOT / pipeline_path).read_bytes(), (
            f"{pipeline_path} and {service_path} have diverged"
        )
