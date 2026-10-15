import sys
from pathlib import Path

import pytest

# Every test here imports the modeling services, post-training arms, or dashboard
# exporter as top-level modules; put their directories on the path once.
_PIPELINE = Path(__file__).resolve().parents[2]
for _path in (_PIPELINE / "frontend",
              _PIPELINE / "services" / "python-modeling" / "post-training",
              _PIPELINE / "services" / "python-modeling"):
    sys.path.insert(0, str(_path))

from archive_fakes import FakeProducer


@pytest.fixture
def make_producer():
    """Factory for FakeProducer; call it with the failure knobs a test needs."""
    return FakeProducer
