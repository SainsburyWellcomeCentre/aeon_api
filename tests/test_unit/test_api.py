"""Tests for the `swc.aeon.io.api` module."""

import datetime

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
        123456789.999999,  # Arbitrary value
        pd.Series([0.0, 123456789.999999]),  # Series value
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
    [datetime.datetime(1907, 11, 29, 21, 33, 9, 999999), pd.Timestamp("1907-11-29 21:33:09.999999")],
    ids=["datetime.datetime", "pandas.Timestamp"],
)
@pytest.mark.parametrize(
    "input_format",
    ["scalar", pd.Series, pd.DatetimeIndex],
    ids=["Scalar", "pandas.Series", "pandas.DatetimeIndex"],
)
def test_chunk(time, input_format):
    """Test that `chunk` can handle different time formats as input and correctly returns the
    acquisition chunk hour.
    """
    expected = pd.Timestamp("1907-11-29 21:00:00")
    time = time if input_format == "scalar" else input_format([time])
    result = chunk(time)
    if isinstance(result, pd.Series):
        tm.assert_series_equal(result, pd.Series([expected]))
    elif isinstance(result, pd.DatetimeIndex):
        tm.assert_index_equal(result, pd.DatetimeIndex([expected]))
    else:
        assert isinstance(result, pd.Timestamp)
        assert isinstance(result, datetime.datetime)
        assert result == expected
