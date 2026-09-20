"""The heat mapping is arithmetic, so it is tested by running it, not by scanning JSX.

CI's python job has no setup-node step; ubuntu-latest ships node anyway, so these skip
rather than fail if it is ever absent.
"""
import json
import shutil
import subprocess
from pathlib import Path

import pytest

MODULE = (Path(__file__).parents[2] / "frontend" / "components" / "heat-domain.mjs").resolve()
pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")


def run_js(body: str):
    """Import the real module in node and return whatever the snippet prints as JSON."""
    script = f'import {{ percentile, heatDomain, heatScore }} from "{MODULE.as_uri()}";\n{body}'
    out = subprocess.run(["node", "--input-type=module", "-e", script],
                         capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_percentile_interpolates_linearly():
    # p50 of 1..5 is the middle element; p25 falls between the first and second.
    assert run_js('console.log(JSON.stringify(percentile([1,2,3,4,5], 50)))') == 3
    assert run_js('console.log(JSON.stringify(percentile([1,2,3,4,5], 25)))') == 2
    assert run_js('console.log(JSON.stringify(percentile([0,10], 50)))') == 5


def test_heat_domain_pools_every_group():
    """The two grids share one domain, so a value in either must be able to set an end."""
    got = run_js(
        'const a = [{ctr: 0.10}, {ctr: 0.20}];'
        'const b = [{ctr: 0.30}, {ctr: 0.40}];'
        # 0..100 percentiles so the ends are the pooled extremes, not interpolated ones.
        'console.log(JSON.stringify(heatDomain([a, b], 0, 100)));'
    )
    assert got == [0.10, 0.40], "a group-local domain would have returned [0.10, 0.20]"


def test_heat_score_clamps_outside_the_domain():
    """Clipping is the cost of the robust domain, so it must be exact at the ends."""
    got = run_js(
        'const d = [0.10, 0.20];'
        'console.log(JSON.stringify([heatScore(0.05, d), heatScore(0.10, d),'
        ' heatScore(0.15, d), heatScore(0.20, d), heatScore(0.25, d)]));'
    )
    # The ends must be EXACT: saturation is the whole contract of a clipped domain.
    assert got[0] == 0 and got[1] == 0, "below the low end must pin to 0"
    assert got[3] == 1 and got[4] == 1, "at or above the high end must pin to 1"
    # The interior only has to be right to within float error -- (0.15-0.10)/(0.20-0.10)
    # is 0.4999999999999999 in IEEE754, which is the arithmetic being correct, not wrong.
    assert got[2] == pytest.approx(0.5)


def test_heat_score_is_zero_for_a_degenerate_domain():
    """One distinct value everywhere must not divide by zero or render NaN."""
    assert run_js('console.log(JSON.stringify(heatScore(0.1, [0.1, 0.1])))') == 0
    assert run_js('console.log(JSON.stringify(heatScore(null, [0.1, 0.2])))') == 0
