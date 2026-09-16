"""Keep the retrieval-service's frozen contract snapshots aligned with pipeline producers."""

import json
import os
from pathlib import Path
import unittest


PIPELINE_ROOT = Path(__file__).resolve().parents[1]
RETRIEVAL_SERVICE_DIR = Path(
    os.environ.get(
        "RETRIEVAL_SERVICE_DIR",
        PIPELINE_ROOT / "services" / "java-retrieval-service",
    )
).resolve()


class RetrievalContractTest(unittest.TestCase):
    def require_file(self, path: Path) -> Path:
        if not path.is_file():
            self.fail(f"Required retrieval contract artifact is missing: {path}")
        return path

    def load_json(self, path: Path):
        with self.require_file(path).open(encoding="utf-8") as artifact:
            return json.load(artifact)

    def test_event_schemas_match_the_canonical_producer_schema(self):
        canonical = self.load_json(PIPELINE_ROOT / "schemas" / "recsys-event-v3.avsc")
        production = self.load_json(
            RETRIEVAL_SERVICE_DIR
            / "src"
            / "main"
            / "resources"
            / "schemas"
            / "recsys-event-v3.avsc"
        )
        snapshot = self.load_json(
            RETRIEVAL_SERVICE_DIR
            / "src"
            / "test"
            / "resources"
            / "contracts"
            / "recsys-event-v3.avsc"
        )

        self.assertEqual(canonical, production, "retrieval production schema drifted from the producer")
        self.assertEqual(canonical, snapshot, "retrieval test schema snapshot drifted from the producer")

    def test_impression_fixture_matches_the_canonical_producer_fixture(self):
        canonical = self.require_file(
            PIPELINE_ROOT / "schemas" / "fixtures" / "serving-impression-v3.avro"
        ).read_bytes()
        snapshot = self.require_file(
            RETRIEVAL_SERVICE_DIR
            / "src"
            / "test"
            / "resources"
            / "contracts"
            / "serving-impression-v3.avro"
        ).read_bytes()

        self.assertEqual(canonical, snapshot, "retrieval impression fixture drifted from the producer")

    def test_user_profile_snapshot_matches_the_producer_fixture(self):
        canonical = self.load_json(
            PIPELINE_ROOT / "integration-tests" / "fixtures" / "user_profile_v1.json"
        )
        snapshot = self.load_json(
            RETRIEVAL_SERVICE_DIR
            / "src"
            / "test"
            / "resources"
            / "contracts"
            / "user_profile_v1.json"
        )

        self.assertEqual(canonical, snapshot, "retrieval profile snapshot drifted from the producer")


if __name__ == "__main__":
    unittest.main()
