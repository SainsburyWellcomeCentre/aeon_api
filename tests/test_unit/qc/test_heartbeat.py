"""Tests for heartbeat_gaps and heartbeat_duplicates."""

import datetime
from unittest.mock import patch

import pandas as pd

from swc.aeon.qc.heartbeat import heartbeat_duplicates, heartbeat_gaps

_PATCH = "swc.aeon.qc.heartbeat.load"
_ROOT = "/fake/root"
_START = pd.Timestamp("2024-01-15T09:00:00", tz="UTC")


def make_heartbeat_seconds(seconds: list[int], base: str = "2024-01-15T09:00:00") -> pd.DataFrame:
    """Build a heartbeat frame whose ``second`` column is given and whose index follows it."""
    t0 = pd.Timestamp(base, tz="UTC")
    idx = pd.DatetimeIndex([t0 + pd.Timedelta(seconds=int(s)) for s in seconds], name="time")
    return pd.DataFrame({"second": seconds}, index=idx)


def test_heartbeat_gaps_empty_data(heartbeat_reader):
    """No data gives an empty frame with the documented schema and data_found False."""
    with patch(_PATCH, return_value=pd.DataFrame()):
        result = heartbeat_gaps(_ROOT, heartbeat_reader, _START)
    assert result.empty
    assert list(result.columns) == ["duration", "n_missed", "second_before", "second_after", "device"]
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC
    assert result.attrs["data_found"] is False


def test_heartbeat_gaps_consecutive_seconds(heartbeat_reader):
    """Consecutive second counters produce no gaps."""
    with patch(_PATCH, return_value=make_heartbeat_seconds([10, 11, 12, 13])):
        result = heartbeat_gaps(_ROOT, heartbeat_reader, _START)
    assert result.empty
    assert result.attrs["data_found"] is True
    assert result.attrs["n_heartbeats"] == 4


def test_heartbeat_gaps_single_gap(heartbeat_reader):
    """A jump in the second counter is one gap with the right size and endpoints."""
    with patch(_PATCH, return_value=make_heartbeat_seconds([10, 11, 16, 17])):
        result = heartbeat_gaps(_ROOT, heartbeat_reader, _START)
    assert len(result) == 1
    row = result.iloc[0]
    assert row["n_missed"] == 4
    assert row["second_before"] == 11
    assert row["second_after"] == 16
    assert row["duration"] == pd.Timedelta(seconds=5)
    assert row["device"] == heartbeat_reader.pattern
    # Index is the gap start (time of the last heartbeat before the gap).
    assert result.index[0] == _START + pd.Timedelta(seconds=11)


def test_heartbeat_gaps_multiple_gaps(heartbeat_reader):
    """Several jumps are reported in order."""
    with patch(_PATCH, return_value=make_heartbeat_seconds([0, 1, 4, 5, 20])):
        result = heartbeat_gaps(_ROOT, heartbeat_reader, _START)
    assert list(result["n_missed"]) == [2, 14]
    assert list(result["second_after"]) == [4, 20]


def test_heartbeat_gaps_single_row(heartbeat_reader):
    """A single heartbeat cannot form a gap."""
    with patch(_PATCH, return_value=make_heartbeat_seconds([7])):
        result = heartbeat_gaps(_ROOT, heartbeat_reader, _START)
    assert result.empty


def test_heartbeat_duplicates_empty_data(heartbeat_reader):
    """No data gives an empty frame with the documented schema."""
    with patch(_PATCH, return_value=pd.DataFrame()):
        result = heartbeat_duplicates(_ROOT, heartbeat_reader, _START)
    assert result.empty
    assert list(result.columns) == ["second", "count", "device"]
    assert result.attrs["data_found"] is False
    assert result.attrs["n_heartbeats"] == 0


def test_heartbeat_duplicates_none(heartbeat_reader):
    """Unique second counters produce no rows."""
    with patch(_PATCH, return_value=make_heartbeat_seconds([1, 2, 3])):
        result = heartbeat_duplicates(_ROOT, heartbeat_reader, _START)
    assert result.empty
    assert result.attrs["n_heartbeats"] == 3


def test_heartbeat_duplicates_reported_once_per_second(heartbeat_reader):
    """A repeated second counter is reported once with its count and first timestamp."""
    with patch(_PATCH, return_value=make_heartbeat_seconds([1, 2, 2, 2, 3])):
        result = heartbeat_duplicates(_ROOT, heartbeat_reader, _START)
    assert len(result) == 1
    assert result.iloc[0]["second"] == 2
    assert result.iloc[0]["count"] == 3
    assert result.index[0] == _START + pd.Timedelta(seconds=2)
