"""Tests for the dropped_frames function."""

import datetime
from unittest.mock import patch

import pandas as pd

from qc.helpers import make_video
from swc.aeon.qc.video import dropped_frames

_PATCH = "swc.aeon.qc.video.load"
_ROOT = "/fake/root"
_START = pd.Timestamp("2024-01-15T09:00:00", tz="UTC")

_TS = [
    "2024-01-15T09:00:00.000",
    "2024-01-15T09:00:00.020",
    "2024-01-15T09:00:00.040",
    "2024-01-15T09:00:00.060",
    "2024-01-15T09:00:00.080",
]


def test_dropped_frames_empty_data(video_reader):
    """Empty data gives an empty DataFrame with the documented schema."""
    empty = pd.DataFrame(columns=["hw_counter", "hw_timestamp", "_frame", "_path", "_epoch"])
    with patch(_PATCH, return_value=empty):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert result.empty
    assert list(result.columns) == [
        "duration",
        "n_dropped",
        "hw_counter_before",
        "hw_counter_after",
        "device",
    ]
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC


def test_dropped_frames_no_drops(video_reader):
    """Test that dropped_frames returns empty when hw_counter increments by exactly 1 throughout."""
    data = make_video([0, 1, 2, 3, 4], _TS)
    with patch(_PATCH, return_value=data):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert result.empty


def test_dropped_frames_single_drop(video_reader):
    """Test that dropped_frames detects a single drop event with correct n_dropped and counter values."""
    # Frames 2, 3, 4 are missing between hw_counter 1 and 5
    data = make_video([0, 1, 5, 6, 7], _TS)
    with patch(_PATCH, return_value=data):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert len(result) == 1
    assert result.iloc[0]["n_dropped"] == 3
    assert result.iloc[0]["hw_counter_before"] == 1
    assert result.iloc[0]["hw_counter_after"] == 5
    # Index is drop start (time of last frame before the gap)
    assert result.index[0] == pd.Timestamp(_TS[1], tz=datetime.UTC)
    assert result.iloc[0]["duration"] == pd.Timedelta("20ms")


def test_dropped_frames_multiple_drops(video_reader):
    """Test that dropped_frames detects multiple drop events and reports correct counts."""
    data = make_video([0, 3, 4, 10, 11], _TS)
    with patch(_PATCH, return_value=data):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert len(result) == 2
    assert result.iloc[0]["n_dropped"] == 2  # 0 = 3
    assert result.iloc[1]["n_dropped"] == 5  # 4 = 10


def test_dropped_frames_n_dropped_is_integer(video_reader):
    """Test that the n_dropped column has an integer dtype."""
    data = make_video([0, 5], _TS[:2])
    with patch(_PATCH, return_value=data):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert pd.api.types.is_integer_dtype(result["n_dropped"])


def test_dropped_frames_device_column(video_reader):
    """Test that the device column is populated with the reader pattern."""
    data = make_video([0, 5], _TS[:2])
    with patch(_PATCH, return_value=data):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert result.iloc[0]["device"] == video_reader.pattern


def test_dropped_frames_single_row(video_reader):
    """Test that a single-row DataFrame produces no drops (no diffs possible)."""
    data = make_video([42], _TS[:1])
    with patch(_PATCH, return_value=data):
        result = dropped_frames(_ROOT, video_reader, _START)
    assert result.empty
