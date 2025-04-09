"""Tests for the aeon API."""

from pathlib import Path
from typing import cast

import pandas as pd
import pytest
from pandas import testing as tm

from swc import aeon
from swc.aeon.io.api import chunk, to_datetime, to_seconds
from tests.schema import exp02, social03

monotonic_path = Path(__file__).parent.parent / "data" / "monotonic"
nonmonotonic_path = Path(__file__).parent.parent / "data" / "nonmonotonic"


@pytest.mark.api
def test_load_start_only():
    data = aeon.load(nonmonotonic_path, exp02.Patch2.Encoder, start=pd.Timestamp("2022-06-06T13:00:49"))
    assert len(data) > 0


@pytest.mark.api
def test_load_end_only():
    data = aeon.load(nonmonotonic_path, exp02.Patch2.Encoder, end=pd.Timestamp("2022-06-06T13:00:49"))
    assert len(data) > 0


@pytest.mark.api
def test_load_filter_nonchunked():
    data = aeon.load(nonmonotonic_path, exp02.Metadata, start=pd.Timestamp("2022-06-06T09:00:00"))
    assert len(data) > 0


@pytest.mark.api
def test_load_monotonic():
    data = aeon.load(monotonic_path, exp02.Patch2.Encoder)
    assert len(data) > 0
    assert data.index.is_monotonic_increasing


@pytest.mark.api
def test_load_nonmonotonic():
    data = aeon.load(nonmonotonic_path, exp02.Patch2.Encoder)
    assert not data.index.is_monotonic_increasing


@pytest.mark.api
def test_pose_load_nonmonotonic_data():
    data = aeon.load(nonmonotonic_path, social03.CameraTop.Pose)
    assert not data.index.is_monotonic_increasing


@pytest.mark.api
def test_pose_load_nonmonotonic_data_time_start_only_sort_fallback():
    with pytest.warns(UserWarning):
        data = aeon.load(
            nonmonotonic_path, social03.CameraTop.Pose, start=pd.Timestamp("2024-07-03T10:00:00")
        )
        assert data.index.is_monotonic_increasing


@pytest.mark.parametrize(
    "seconds",
    [
        0,  # Edge case: REFERENCE_EPOCH
        123456789,  # Arbitrary value
        pd.Series([0.0, 123456789.0]),  # Series value
    ],
)
def test_datetime_seconds_conversion(seconds):
    # test round-trip conversion
    converted_datetime = to_datetime(seconds)
    converted_seconds = to_seconds(converted_datetime)
    if isinstance(seconds, pd.Series):
        tm.assert_series_equal(converted_seconds, seconds)
    else:
        assert converted_seconds == seconds


@pytest.mark.parametrize(
    "time",
    [
        pd.Timestamp(0),  # Datetime value
        pd.Series([pd.to_datetime(0)]),  # Series value
        pd.DatetimeIndex([pd.to_datetime(0)]),  # Datetime index value
    ],
)
def test_chunk_identity_conversion(time):
    if isinstance(time, pd.Series):
        time_chunk = cast(pd.Series, chunk(time))
        tm.assert_series_equal(time_chunk, time)
    elif isinstance(time, pd.DatetimeIndex):
        time_chunk = cast(pd.DatetimeIndex, chunk(time))
        tm.assert_index_equal(time_chunk, time)
    else:
        time_chunk = chunk(time)
        assert time_chunk == time


if __name__ == "__main__":
    pytest.main()
