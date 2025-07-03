"""Tests for the aeon API."""

from contextlib import nullcontext
from typing import cast

import pandas as pd
import pytest
from pandas import testing as tm

from swc import aeon
from swc.aeon.io.api import chunk, to_datetime, to_seconds
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


@pytest.mark.api
@pytest.mark.parametrize(
    ("time", "expected"),
    [
        (pd.Timestamp("2022-06-13 12:14:54"), nullcontext(1)),
        ([pd.Timestamp("2022-06-13 12:14:54"), pd.Timestamp("2022-06-13 12:14:55")], nullcontext(2)),
        (pd.date_range("2022-06-13 12:14:54", periods=2, freq="s"), nullcontext(2)),
        (
            pd.DataFrame(
                data={"value": [0, 0]}, index=pd.date_range("2022-06-13 12:14:54", periods=2, freq="s")
            ),
            nullcontext(2),
        ),
        (
            pd.Timestamp("2022-06-13 13:00:00"),
            nullcontext(1),  # Single row df filled with NaNs
        ),
        (
            pd.Timestamp("2022-06-13 11:00:00"),
            nullcontext(1),  # Single row df filled with NaNs
        ),
        ([], nullcontext(0)),  # Empty df
    ],
    ids=[
        "Single Timestamp",
        "List of Timestamps",
        "DatetimeIndex",
        "DataFrame with DatetimeIndex",
        "Timestamp before available data",
        "Timestamp after available data",
        "Empty list",
    ],
)
def test_load_time_arg(time, expected, monotonic_dir):
    """Test that `load` handles different kinds of `time` input."""
    with expected as expected_df_length:
        result = aeon.load(monotonic_dir, exp02.Patch2.Encoder, time=time)
        assert len(result) == expected_df_length


if __name__ == "__main__":
    pytest.main()
