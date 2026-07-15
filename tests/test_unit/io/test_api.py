"""Tests for the `swc.aeon.io.api` module."""

import datetime
from contextlib import nullcontext

import pandas as pd
import pytest
from pandas import testing as tm

from swc.aeon.io.api import (
    CHUNK_DURATION,
    _filter_time_range,  # pyright: ignore[reportPrivateUsage]
    chunk,
    chunk_key,
    chunk_range,
    load,
    to_datetime,
    to_seconds,
)
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
    expected = pd.Timestamp("1907-11-29 21:00:00", tzinfo=datetime.UTC)
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
    expected = pd.date_range(
        "1907-11-29 21:00", "1907-11-30 21:00", freq=f"{CHUNK_DURATION}h", tz=datetime.UTC
    )
    tm.assert_index_equal(result, expected)


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (
            "monotonic_file",
            ("2022-06-13T13_14_25", pd.Timestamp("2022-06-13 12:00:00", tzinfo=datetime.UTC)),
        ),
        (
            "nonmonotonic_file",
            ("2022-06-06T09-24-28", pd.Timestamp("2022-06-06 13:00:00", tzinfo=datetime.UTC)),
        ),
        (
            "metadata_file",
            ("2022-06-06T09-24-28", pd.Timestamp("2022-06-06 09:24:28", tzinfo=datetime.UTC)),
        ),
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


@pytest.fixture(params=["monotonic", "nonmonotonic"], ids=["monotonic", "nonmonotonic"])
def filter_df(request) -> pd.DataFrame:
    """DataFrame with 10 one-minute timestamps.

    The nonmonotonic variant swaps positions 3 and 4:
    00:00, 00:01, 00:02, 00:04, 00:03, 00:05, ...
    """
    idx_list = pd.date_range("2022-01-01 00:00:00", periods=10, freq="min").tolist()
    if request.param == "nonmonotonic":
        idx_list[3], idx_list[4] = idx_list[4], idx_list[3]
    return pd.DataFrame({"value": range(10)}, index=idx_list)


@pytest.mark.parametrize(
    "inclusive",
    ["both", "left", "right", "neither"],
    ids=["both", "left", "right", "neither"],
)
@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (
            pd.Timestamp("2022-01-01 00:02:00"),
            None,
            {"both": 8, "left": 8, "right": 7, "neither": 7},
        ),
        (
            None,
            pd.Timestamp("2022-01-01 00:04:00"),
            {"both": 5, "left": 4, "right": 5, "neither": 4},
        ),
        (
            pd.Timestamp("2022-01-01 00:02:00"),
            pd.Timestamp("2022-01-01 00:04:00"),
            {"both": 3, "left": 2, "right": 2, "neither": 1},
        ),
        (
            pd.Timestamp("2022-01-01 00:02:30"),
            None,
            {"both": 7, "left": 7, "right": 7, "neither": 7},
        ),
        (
            None,
            pd.Timestamp("2022-01-01 00:04:30"),
            {"both": 5, "left": 5, "right": 5, "neither": 5},
        ),
    ],
    ids=[
        "start only",
        "end only",
        "both provided",
        "start not in index",
        "end not in index",
    ],
)
def test_filter_time_range(filter_df, start, end, inclusive, expected):
    """Test `_filter_time_range` with both monotonic and non-monotonic DatetimeIndex."""
    result = _filter_time_range(filter_df, start, end, inclusive=inclusive)
    assert len(result) == expected[inclusive]


@pytest.mark.filterwarnings("ignore:.*out-of-order timestamps")
@pytest.mark.parametrize(
    ("root", "to_str", "expect_monotonic"),
    [
        ("monotonic_dir", False, True),
        ("nonmonotonic_dir", True, True),
        (["nonmonotonic_dir", "monotonic_dir"], False, True),
        (["monotonic_dir", "nonmonotonic_dir"], True, True),
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
def test_load_time_arg(monotonic_dir, time, expected):
    """Test that `load` handles different `time` types."""
    with expected as expected_df_length:
        result = load(monotonic_dir, Encoder("Patch2_90_*"), time=time)
        assert len(result) == expected_df_length


@pytest.mark.parametrize(
    ("data_dir", "start", "end"),
    [
        # Monotonic cases
        ("monotonic_dir", pd.Timestamp("2022-06-13 12:14:54"), None),
        ("monotonic_dir", None, pd.Timestamp("2022-06-13 12:14:55")),
        (
            "monotonic_dir",
            pd.Timestamp("2022-06-13 12:14:54"),
            pd.Timestamp("2022-06-13 12:14:55"),
        ),
        # Non-monotonic cases
        ("nonmonotonic_dir", pd.Timestamp("2022-06-06 13:00:48"), None),
        ("nonmonotonic_dir", None, pd.Timestamp("2022-06-06 13:00:49.99")),
        (
            "nonmonotonic_dir",
            pd.Timestamp("2022-06-06 13:00:48"),
            pd.Timestamp("2022-06-06 13:00:48.99"),
        ),
    ],
    ids=[
        "Monotonic: End is None",
        "Monotonic: Start is None",
        "Monotonic: Both provided",
        "Nonmonotonic: End is None",
        "Nonmonotonic: Start is None",
        "Nonmonotonic: Both provided",
    ],
)
def test_load_start_end_args(data_dir, start, end, request):
    """Test `load` handling of `start` and `end` with both monotonic and non-monotonic data.

    Ensures:
    - Filtering respects `start` and `end` bounds
    - Non-monotonic data additionally triggers warning
    """
    root_dir = request.getfixturevalue(data_dir)
    is_monotonic = data_dir == "monotonic_dir"
    context = (
        pytest.warns(UserWarning, match="out-of-order timestamps") if not is_monotonic else nullcontext()
    )
    with context:
        result = load(root_dir, Encoder("Patch2_90_*"), start=start, end=end)
    assert result.index.is_monotonic_increasing
    if start is not None:
        assert (result.index >= pd.to_datetime(start, utc=True)).all(), (
            f"Start filter failed: min index {result.index.min()} < {start}"
        )
    if end is not None:
        assert (result.index <= pd.to_datetime(end, utc=True)).all(), (
            f"End filter failed: max index {result.index.max()} > {end}"
        )


@pytest.mark.parametrize(
    ("sort", "expect_monotonic"),
    [(True, True), (False, False)],
    ids=["sort=True", "sort=False"],
)
def test_load_sort_arg(nonmonotonic_dir, sort, expect_monotonic):
    """Test that `sort` controls output order for non-monotonic data, and warning is always emitted."""
    with pytest.warns(UserWarning, match="out-of-order timestamps"):
        result = load(nonmonotonic_dir, Encoder("Patch2_90_*"), sort=sort)
    assert result.index.is_monotonic_increasing == expect_monotonic
