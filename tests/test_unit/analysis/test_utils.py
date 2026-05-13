"""Tests for the `swc.aeon.analysis.utils` module."""

import pandas as pd
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
    """Test distancetravelled correctly computes the expected sum (down to the closest integer)
    for the specified test file.
    """
    data = load(monotonic_dir, exp02.Patch2.Encoder)
    result = utils.distancetravelled(data.angle, radius)
    assert int(result.sum()) == expected_sum


def _make_visits_data(ids, events, times):
    """Build a DataFrame with named time index as input to visits()."""
    return pd.DataFrame(
        {"id": ids, "event": events},
        index=pd.DatetimeIndex(pd.to_datetime(times), name="time"),
    )


@pytest.mark.parametrize(
    ("ids", "events", "times", "expected"),
    [
        (
            [1, 1],
            ["Enter", "Exit"],
            ["2022-01-01 10:00:00", "2022-01-01 10:05:00"],
            pd.DataFrame(
                {
                    "id": [1],
                    "enter": pd.to_datetime(["2022-01-01 10:00:00"]),
                    "exit": pd.to_datetime(["2022-01-01 10:05:00"]),
                    "duration": [pd.Timedelta("5min")],
                }
            ),
        ),
        (
            [1, 2, 1, 2],
            ["Enter", "Enter", "Exit", "Exit"],
            ["2022-01-01 10:00:00", "2022-01-01 10:01:00", "2022-01-01 10:05:00", "2022-01-01 10:06:00"],
            pd.DataFrame(
                {
                    "id": [1, 2],
                    "enter": pd.to_datetime(["2022-01-01 10:00:00", "2022-01-01 10:01:00"]),
                    "exit": pd.to_datetime(["2022-01-01 10:05:00", "2022-01-01 10:06:00"]),
                    "duration": [pd.Timedelta("5min"), pd.Timedelta("5min")],
                }
            ),
        ),
        (
            [1, 2, 2],
            ["Enter", "Enter", "Exit"],
            ["2022-01-01 10:00:00", "2022-01-01 10:01:00", "2022-01-01 10:06:00"],
            pd.DataFrame(
                {
                    "id": [2],
                    "enter": pd.to_datetime(["2022-01-01 10:01:00"]),
                    "exit": pd.to_datetime(["2022-01-01 10:06:00"]),
                    "duration": [pd.Timedelta("5min")],
                }
            ),
        ),
        (
            [1, 1, 1],
            ["Enter", "Enter", "Exit"],
            ["2022-01-01 10:00:00", "2022-01-01 10:03:00", "2022-01-01 10:06:00"],
            pd.DataFrame(
                {
                    "id": [1, 1],
                    "enter": pd.to_datetime(["2022-01-01 10:00:00", "2022-01-01 10:03:00"]),
                    "exit": pd.to_datetime(["NaT", "2022-01-01 10:06:00"]),
                    "duration": pd.to_timedelta(["NaT", "3min"]),
                }
            ),
        ),
    ],
    ids=[
        "single pair",
        "multiple pairs",
        "unmatched onset dropped",
        "multiple onsets share same offset, set earlier visit to NA",
    ],
)
def test_visits(ids, events, times, expected):
    """Test visits computes visit durations and metadata for various event sequences."""
    data = _make_visits_data(ids=ids, events=events, times=times)
    result = utils.visits(data)
    pd.testing.assert_frame_equal(result, expected)


def test_visits_custom_labels():
    """Test visits with custom onset/offset labels."""
    data = _make_visits_data(
        ids=[1, 1],
        events=["Start", "Stop"],
        times=["2022-01-01 10:00:00", "2022-01-01 10:10:00"],
    )
    result = utils.visits(data, onset="Start", offset="Stop")
    expected = pd.DataFrame(
        {
            "id": [1],
            "start": pd.to_datetime(["2022-01-01 10:00:00"]),
            "stop": pd.to_datetime(["2022-01-01 10:10:00"]),
            "duration": [pd.Timedelta("10min")],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    ("index", "start", "expected"),
    [
        (
            pd.to_datetime(["2022-01-01 10:00:00", "2022-01-01 10:00:30", "2022-01-01 10:01:00"]),
            None,
            [0.0, 0.5, 1.0],
        ),
        (
            pd.to_datetime(["2022-01-01 10:01:00", "2022-01-01 10:02:00"]),
            pd.Timestamp("2022-01-01 10:00:00"),
            [1.0, 2.0],
        ),
    ],
    ids=["no start, use first element", "start provided"],
)
def test_sessiontime(index, start, expected):
    """Test sessiontime computes elapsed time in minutes."""
    result = utils.sessiontime(index, start)
    assert list(result) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("position", "target", "expected"),
    [
        (pd.DataFrame({"x": [3.0], "y": [4.0]}), [0.0, 0.0], [5.0]),
        (pd.DataFrame({"x": [2.0], "y": [3.0]}), [2.0, 3.0], [0.0]),
        (pd.DataFrame({"x": [0.0, 3.0], "y": [0.0, 4.0]}), [0.0, 0.0], [0.0, 5.0]),
    ],
    ids=["pythagorean triple", "distance to self", "multiple points"],
)
def test_distance(position, target, expected):
    """Test distance computes the Euclidean distance from position to target."""
    result = utils.distance(position, target)
    assert list(result) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("kwargs", "expected_values"),
    [
        (
            {"window": "3s", "frequency": 1},
            [1.0, 1.5, 2.0, 8 / 3, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
        ),
        (
            {"window": "3s", "frequency": 1, "weight": 2},
            [2.0, 3.0, 4.0, 16 / 3, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0],
        ),
        (
            {"window": "3s", "frequency": 1, "smooth": "5s"},
            [1.0, 1.5, 2.0, 9 / 4, 12 / 5, 14 / 5, 3.0, 3.0, 3.0, 3.0],
        ),
        (
            {"window": "3s", "frequency": 1, "center": True},
            [2.5, 8 / 3, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 8 / 3, 2.5],
        ),
    ],
    ids=[
        "baseline: required parameters only",
        "optional weight=2; doubles the rate values",
        "optional smooth=5s; applies smoothing",
        "optional center=True; centers the window",
    ],
)
def test_rate(kwargs, expected_values):
    """Test rate varying one parameter at a time for a simple event sequence."""
    times = pd.date_range("2022-01-01 10:00:00", periods=10, freq="1s")
    events = pd.Series(1.0, index=times)
    result = utils.rate(events, **kwargs)
    expected = pd.Series(expected_values, index=times)
    pd.testing.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (pd.Timestamp("2022-01-01 10:00:00"), pd.Timestamp("2022-01-01 10:00:15")),
        (pd.Timestamp("2022-01-01 10:00:00"), None),
        (None, pd.Timestamp("2022-01-01 10:00:15")),
    ],
    ids=["start and end", "start only", "end only"],
)
def test_rate_index_bounds(start, end):
    """Test rate extends index to start/end bounds when provided."""
    times = pd.date_range("2022-01-01 10:00:05", periods=5, freq="1s")
    events = pd.Series(dtype=float, index=times)
    result = utils.rate(events, window="3s", frequency=1, start=start, end=end)
    if start is not None:
        assert result.index[0] == start
    if end is not None:
        assert result.index[-1] == end


@pytest.mark.parametrize(
    ("kwargs", "expected_values"),
    [
        (
            {"window_len_sec": 5, "frequency": "1s"},
            [12.0, 18.0, 24.0, 30.0, 36.0, 228 / 5, 264 / 5, 288 / 5, 60.0, 60.0],
        ),
        (
            {"window_len_sec": 5, "frequency": "1s", "unit_len_sec": 1},
            [1 / 5, 3 / 10, 2 / 5, 1 / 2, 3 / 5, 19 / 25, 22 / 25, 24 / 25, 1.0, 1.0],
        ),
        (
            {"window_len_sec": 5, "frequency": "1s", "smooth": "3s"},
            [12.0, 18.0, 24.0, 36.0, 48.0, 56.0, 60.0, 60.0, 60.0, 60.0],
        ),
        (
            {"window_len_sec": 5, "frequency": "1s", "center": True},
            [48.0, 51.0, 264 / 5, 288 / 5, 60.0, 60.0, 288 / 5, 264 / 5, 51.0, 48.0],
        ),
    ],
    ids=[
        "baseline: required parameters only",
        "unit_len_sec=1; scales values down by 60 (default is 60s)",
        "smooth=3s; applies smoothing",
        "center=True; centers the window",
    ],
)
def test_get_events_rates(kwargs, expected_values):
    """Test get_events_rates varying one parameter at a time for a simple event sequence."""
    times = pd.date_range("2022-01-01 10:00:00", periods=10, freq="1s")
    events = pd.Series(1.0, index=times)
    result = utils.get_events_rates(events, **kwargs)
    expected = pd.Series(expected_values, index=times)
    pd.testing.assert_series_equal(result, expected)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (pd.Timestamp("2022-01-01 10:00:00"), pd.Timestamp("2022-01-01 10:00:15")),
        (pd.Timestamp("2022-01-01 10:00:00"), None),
        (None, pd.Timestamp("2022-01-01 10:00:15")),
    ],
    ids=["start and end", "start only", "end only"],
)
def test_get_events_rates_index_bounds(start, end):
    """Test get_events_rates extends index to start/end bounds when provided."""
    times = pd.date_range("2022-01-01 10:00:05", periods=5, freq="1s")
    events = pd.Series(1.0, index=times)
    result = utils.get_events_rates(events, window_len_sec=5, frequency="1s", start=start, end=end)
    if start is not None:
        assert result.index[0] == start
    if end is not None:
        assert result.index[-1] == end


@pytest.mark.parametrize(
    ("wheel", "in_patch", "expected"),
    [
        (
            [0.0, 2.0, 2.0, 2.0, 2.0],
            [True, True, True, True, True],
            [False, True, True, True, True],
        ),
        (
            [0.0, 0.0, 0.0, 0.0, 0.0],
            [True, True, True, True, True],
            [False, False, False, False, False],
        ),
        (
            [0.0, 2.0, 0.0, 0.0, 0.0],
            [True, True, False, True, True],
            [False, True, False, False, False],
        ),
    ],
    ids=["wheel active", "wheel inactive", "epoch reset: wheel active only in first epoch"],
)
def test_activepatch(wheel, in_patch, expected):
    """Test activepatch identifies active patch periods based on wheel movement and in_patch status."""
    times = pd.date_range("2022-01-01", periods=5, freq="200ms")
    wheel = pd.Series(wheel, index=times)
    in_patch = pd.Series(in_patch, index=times)
    result = utils.activepatch(wheel, in_patch)
    assert list(result.values) == expected
