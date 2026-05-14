"""Tests for the `swc.aeon.analysis.movies` module."""

from unittest.mock import ANY, patch

import numpy as np
import pandas as pd
import pytest

from swc.aeon.analysis import movies


def _frame(height, width, value=0):
    """Returns a frame of the specified dimensions and pixel value."""
    return np.full((height, width, 3), value, dtype=np.uint8)


def _clip_data(n_clips, frames_per_clip):
    """Returns a DataFrame that mimics triggerclip output,
    with n_clips * frames_per_clip rows and
    "clip_sequence", "frame_sequence", "_path", and "_frame" columns.
    """
    rows = []
    for clip_i in range(n_clips):
        for frame_i in range(frames_per_clip):
            rows.append(
                {
                    "clip_sequence": clip_i,
                    "frame_sequence": frame_i,
                    "_path": f"v{clip_i}.avi",
                    "_frame": frame_i,
                }
            )
    return pd.DataFrame(rows)


def _data_with_times(n, freq="1s", start="2022-01-01 10:00:00"):
    """Returns a DataFrame with n rows, indexed by a DatetimeIndex starting at start and spaced by freq."""
    times = pd.date_range(start, periods=n, freq=freq)
    return pd.DataFrame({"value": range(n)}, index=pd.DatetimeIndex(times, name="time"))


@pytest.mark.parametrize(
    ("width", "height", "shape", "n_frames", "checks"),
    [
        (
            20,
            20,
            None,
            1,
            [(10, 10, 60)],
        ),
        (
            20,
            20,
            4,
            4,
            [(5, 5, 60), (5, 15, 120), (15, 5, 180), (15, 15, 240)],
        ),
        (
            40,
            20,
            (2, 2),
            4,
            [(5, 10, 60), (5, 30, 120), (15, 10, 180), (15, 30, 240)],
        ),
        (
            20,
            20,
            (2, 2),
            1,
            [(5, 5, 60), (5, 15, 0), (15, 5, 0), (15, 15, 0)],
        ),
        (
            20,
            20,
            (1, 1),
            1,
            [(10, 10, 60)],
        ),
    ],
    ids=[
        "shape=None (default) uses all frames",
        "shape as number of frames, auto-calculates square grid",
        "shape as grid (rows, cols) tuple",
        "fewer frames than cells, empty cells are zero",
        "single frame with explicit 1x1 shape fills the entire grid",
    ],
)
def test_gridframes(width, height, shape, n_frames, checks):
    """Test gridframes places frames in correct grid cells using varying parameters."""
    # Create frames with different pixel values to identify them in the grid
    frames = [_frame(5, 5, (i + 1) * 60) for i in range(n_frames)]
    result = movies.gridframes(frames, width=width, height=height, shape=shape)
    assert result.shape == (height, width, 3)
    for row, col, expected in checks:
        assert result[row, col, 0] == expected


@pytest.mark.parametrize(
    ("frames", "expected"),
    [
        ([_frame(4, 4, 100)], _frame(4, 4, 100)),
        ([_frame(4, 4, 50), _frame(4, 4, 50)], _frame(4, 4, 50)),
        ([_frame(4, 4, 0), _frame(4, 4, 100)], _frame(4, 4, 50)),
    ],
    ids=[
        "average of single frame is the same frame",
        "average of identical frames is the same frame",
        "average of black and white frames is grey frame at mid-value",
    ],
)
def test_averageframes(frames, expected):
    """Test averageframes with varying frame inputs."""
    result = movies.averageframes(frames)
    assert result.shape == expected.shape
    np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize(
    ("frames", "n", "fun", "expected"),
    [
        (list(range(6)), 2, list, [[0, 1], [2, 3], [4, 5]]),
        (list(range(5)), 2, list, [[0, 1], [2, 3]]),
        ([], 2, list, []),
        ([1, 2, 3], 1, list, [[1], [2], [3]]),
        ([np.array([1, 2]), np.array([3, 4])], 1, np.sum, [3, 7]),
    ],
    ids=[
        "group 6 frames into groups of 2",
        "incomplete final group is dropped",
        "empty input produces no groups",
        "n=1 yields each element as its own group",
        "apply sum to each group",
    ],
)
def test_groupframes(frames, n, fun, expected):
    """Test groupframes with varying inputs and functions."""
    result = list(movies.groupframes(frames, n, fun))
    assert result == expected


@pytest.mark.parametrize(
    ("events", "before", "after", "expected_values", "expected_frame_seq", "expected_clip_seq"),
    [
        (pd.DatetimeIndex(["2022-01-01 10:00:05"]), None, None, [5], [0], [0]),
        (
            pd.DatetimeIndex(["2022-01-01 10:00:05"]),
            pd.Timedelta("2s"),
            None,
            [3, 4, 5],
            [0, 1, 2],
            [0, 0, 0],
        ),
        (
            pd.DatetimeIndex(["2022-01-01 10:00:05"]),
            None,
            pd.Timedelta("2s"),
            [5, 6, 7],
            [0, 1, 2],
            [0, 0, 0],
        ),
        (
            pd.DatetimeIndex(["2022-01-01 10:00:05"]),
            pd.Timedelta("2s"),
            pd.Timedelta("2s"),
            [3, 4, 5, 6, 7],
            [0, 1, 2, 3, 4],
            [0, 0, 0, 0, 0],
        ),
        (
            pd.Series(1, index=pd.DatetimeIndex(["2022-01-01 10:00:03", "2022-01-01 10:00:07"])),
            None,
            None,
            [3, 7],
            [0, 0],
            [0, 1],
        ),
    ],
    ids=["default", "before only", "after only", "before and after", "multiple events as Series"],
)
def test_triggerclip(events, before, after, expected_values, expected_frame_seq, expected_clip_seq):
    """Test triggerclip selects correct rows for each before/after and event combination."""
    data = _data_with_times(10)
    result = movies.triggerclip(data, events, before=before, after=after)
    assert list(result["value"]) == expected_values
    assert list(result["frame_sequence"]) == expected_frame_seq
    assert list(result["clip_sequence"]) == expected_clip_seq


def test_collatemovie():
    """Test collatemovie yields one mean-aggregated frame per distinct frame_sequence value."""
    clipdata = _clip_data(n_clips=2, frames_per_clip=2)
    # sorted order: (frame_seq=0,clip0)=50, (frame_seq=0,clip1)=100,
    #               (frame_seq=1,clip0)=100, (frame_seq=1,clip1)=150
    frames = [_frame(4, 4, 50), _frame(4, 4, 100), _frame(4, 4, 100), _frame(4, 4, 150)]
    fun = lambda g: np.mean(g, axis=0)
    with patch("swc.aeon.analysis.movies.video.frames", return_value=iter(frames)):
        result = list(movies.collatemovie(clipdata, fun))
    assert len(result) == 2
    np.testing.assert_array_equal(result[0], _frame(4, 4, 75))
    np.testing.assert_array_equal(result[1], _frame(4, 4, 125))


def test_gridmovie():
    """Test gridmovie forwards width, height, and shape to gridframes."""
    clipdata = _clip_data(n_clips=2, frames_per_clip=1)
    frames = [_frame(10, 10), _frame(10, 10)]
    with (
        patch("swc.aeon.analysis.movies.gridframes") as mock_gf,
        patch("swc.aeon.analysis.movies.video.frames", return_value=iter(frames)),
    ):
        mock_gf.return_value = _frame(20, 30)
        # use list to consume generator or mock never fires
        list(movies.gridmovie(clipdata, width=20, height=30, shape=(2, 1)))
        mock_gf.assert_called_with(ANY, 20, 30, (2, 1))
