"""LineChart's y-scale is arithmetic, so it is tested by running it in node, not by reading JSX."""
import json
import shutil
import subprocess
from pathlib import Path

import pytest

MODULE = (Path(__file__).parents[2] / "frontend" / "components" / "line-scale.mjs").resolve()
pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")


def run_js(body: str):
    script = f'import {{ lineScale }} from "{MODULE.as_uri()}";\n{body}'
    try:
        out = subprocess.run(["node", "--input-type=module", "-e", script],
                             capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        pytest.fail("node did not return within 30s evaluating the line-scale module")
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_line_scale_maps_the_range_onto_the_plot():
    got = run_js('const s = lineScale([0.1, 0.3], 120, 6);'
                 'console.log(JSON.stringify([s.lo, s.hi, s.flat, s.y(0.1), s.y(0.3), s.y(0.2)]));')
    assert got[:3] == [0.1, 0.3, False]
    assert got[3] == 114 and got[4] == 6 and abs(got[5] - 60) < 1e-9


def test_line_scale_centres_a_flat_series():
    """A constant series used to sit on the floor with the same value labelled top and bottom."""
    got = run_js('const s = lineScale([0.5, 0.5, 0.5], 120, 6);'
                 'console.log(JSON.stringify([s.flat, s.y(0.5)]));')
    assert got == [True, 60]
