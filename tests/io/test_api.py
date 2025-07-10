"""Tests for the aeon API."""

import pandas as pd
import pytest

from swc import aeon
from tests.schema import exp02, social03

pytestmark = pytest.mark.api


def test_load_start_only(nonmonotonic_dir):
    data = aeon.load(nonmonotonic_dir, exp02.Patch2.Encoder, start=pd.Timestamp("2022-06-06T13:00:49"))
    assert len(data) > 0


def test_load_end_only(nonmonotonic_dir):
    data = aeon.load(nonmonotonic_dir, exp02.Patch2.Encoder, end=pd.Timestamp("2022-06-06T13:00:49"))
    assert len(data) > 0


def test_load_filter_nonchunked(nonmonotonic_dir):
    data = aeon.load(nonmonotonic_dir, exp02.Metadata, start=pd.Timestamp("2022-06-06T09:00:00"))
    assert len(data) > 0


def test_load_monotonic(monotonic_dir):
    data = aeon.load(monotonic_dir, exp02.Patch2.Encoder)
    assert len(data) > 0
    assert data.index.is_monotonic_increasing


def test_load_nonmonotonic(nonmonotonic_dir):
    data = aeon.load(nonmonotonic_dir, exp02.Patch2.Encoder)
    assert not data.index.is_monotonic_increasing


def test_pose_load_nonmonotonic_file(nonmonotonic_dir):
    data = aeon.load(nonmonotonic_dir, social03.CameraTop.Pose)
    assert not data.index.is_monotonic_increasing


def test_pose_load_nonmonotonic_file_time_start_only_sort_fallback(nonmonotonic_dir):
    with pytest.warns(UserWarning, match="data index for .* contains out-of-order timestamps!"):
        data = aeon.load(
            nonmonotonic_dir, social03.CameraTop.Pose, start=pd.Timestamp("2024-07-03T10:00:00")
        )
    assert data.index.is_monotonic_increasing


if __name__ == "__main__":
    pytest.main()
