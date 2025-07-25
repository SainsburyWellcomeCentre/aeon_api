"""Tests for the `swc.aeon.io.api` module."""

import datetime
from contextlib import nullcontext

import pandas as pd
import pytest
from pandas import testing as tm

from swc.aeon.io.api import CHUNK_DURATION, chunk, chunk_key, chunk_range, load, to_datetime, to_seconds
from swc.aeon.io.reader import Encoder


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


@pytest.mark.parametrize(
    "start",
    [datetime.datetime(1907, 11, 29, 21, 33, 9, 999999), pd.Timestamp("1907-11-29 21:33:09.999999")],
    ids=["datetime.datetime", "pandas.Timestamp"],
)
def test_chunk_range(start):
    """Test that `chunk_range` returns the correct range of acquisition chunks for different input types."""
    end = start + pd.Timedelta(hours=24)
    result = chunk_range(start, end)
    expected = pd.date_range("1907-11-29 21:00", "1907-11-30 21:00", freq=f"{CHUNK_DURATION}h")
    tm.assert_index_equal(result, expected)


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ("monotonic_file", ("2022-06-13T13_14_25", pd.Timestamp("2022-06-13 12:00:00"))),
        ("nonmonotonic_file", ("2022-06-06T09-24-28", pd.Timestamp("2022-06-06 13:00:00"))),
        ("metadata_file", ("2022-06-06T09-24-28", pd.Timestamp("2022-06-06 09:24:28"))),
    ],
    ids=[
        "Monotonic data file",
        "Nonmonotonic data file",
        "Metadata file (not chunked)",
    ],
)
def test_chunk_key(data, expected, request):
    """Test `chunk_key` correctly extracts the epoch and chunk time for a given data file."""
    result = chunk_key(request.getfixturevalue(data))
    assert result == expected


@pytest.mark.parametrize(
    ("root", "to_str", "expect_monotonic"),
    [
        ("monotonic_dir", False, True),
        ("nonmonotonic_dir", True, False),
        (["nonmonotonic_dir", "monotonic_dir"], False, True),
        (["monotonic_dir", "nonmonotonic_dir"], True, False),
    ],
    ids=[
        "PathLike",
        "str",
        "List of PathLike: monotonic dir has priority",
        "List of str: nonmonotonic dir has priority",
    ],
)
def test_load_root_arg_types_and_priority(root, to_str, expect_monotonic, request):
    """Test that `load` handles different `root` types and
    when a list is provided, the last dir in `root` takes precedence.
    """
    if isinstance(root, list):
        root = [request.getfixturevalue(r) for r in root]
    else:
        root = request.getfixturevalue(root)
    root = [str(r) for r in root] if to_str and isinstance(root, list) else str(root)
    result = load(root, Encoder("Patch2_90_*"))
    assert expect_monotonic == result.index.is_monotonic_increasing


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
            pytest.raises(TypeError, match="Cannot compare dtypes float64 and datetime64"),
        ),
        (
            pd.Timestamp("2022-06-13 12:00:00"),
            nullcontext(1),  # df filled with NaN values
        ),
        ([], nullcontext(0)),  # Empty list
    ],
    ids=[
        "Single Timestamp",
        "List of Timestamps",
        "DatetimeIndex",
        "DataFrame with DatetimeIndex",
        "Timestamp after available data",
        "Timestamp before available data",
        "Empty list",
    ],
)
def test_load_time_arg(monotonic_dir, time, expected):
    """Test that `load` handles different `time` types."""
    with expected as expected_df_length:
        result = load(monotonic_dir, Encoder("Patch2_90_*"), time=time)
        assert len(result) == expected_df_length
