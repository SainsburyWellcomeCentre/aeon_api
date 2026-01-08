"""Tests for the `swc.aeon.analysis.utils` module."""

import pytest

from swc.aeon.analysis import utils
from swc.aeon.io.api import load
from tests.schema import exp02


@pytest.mark.parametrize(
    ("radius", "expected_sum"),
    [(0, 0), (4, -170), (-4, 170)],
    ids=["zero radius", "positive radius", "negative radius"],
)
def test_distancetravelled(monotonic_dir, radius, expected_sum):
    """Test `distancetravelled` correctly computes the expected sum (down to the closest integer)
    for the specified test file.
    """
    data = load(monotonic_dir, exp02.Patch2.Encoder)
    result = utils.distancetravelled(data.angle, radius)
    assert int(result.sum()) == expected_sum
