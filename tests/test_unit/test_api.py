"""Tests for the `swc.aeon.io.api` module."""

from typing import cast

import pandas as pd
import pytest
from pandas import testing as tm

from swc.aeon.io.api import chunk, chunk_key, to_datetime, to_seconds


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ("monotonic_file", ("2022-06-13T13_14_25", pd.Timestamp("2022-06-13 12:00:00"))),
        ("nonmonotonic_file", ("2022-06-06T09-24-28", pd.Timestamp("2022-06-06 13:00:00"))),
    ],
    ids=["Monotonic data file", "Nonmonotonic data file"],
)
def test_chunk_key(data, expected, request):
    """Test `chunk_key` correctly extracts the epoch and chunk time for a given data file."""
    result = chunk_key(request.getfixturevalue(data))
    assert result == expected


@pytest.mark.parametrize(
    "seconds",
    [
        0,  # Edge case: REFERENCE_EPOCH
        123456789,  # Arbitrary value
        pd.Series([0.0, 123456789.0]),  # Series value
    ],
)
def test_datetime_seconds_conversion(seconds):
    """Test round-trip conversion between seconds and datetime."""
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
