"""Tests for the `swc.aeon.analysis.plotting` module."""

import matplotlib as mpl

mpl.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest
from matplotlib.collections import LineCollection

from swc.aeon.analysis import plotting


@pytest.fixture(autouse=True)
def close_figures():
    """Automatically close all figures after each test."""
    yield
    plt.close("all")


def _position(n=100, seed=42):
    """Returns a DataFrame mimicking position data with x, y coordinates."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"x": rng.uniform(0, 100, n), "y": rng.uniform(0, 100, n)})


def _events(n=20, freq="1s", start="2022-01-01 10:00:00"):
    """Returns a Series mimicking event data, with DatetimeIndex."""
    times = pd.date_range(start, periods=n, freq=freq)
    return pd.Series(1, index=pd.DatetimeIndex(times, name="time"))


@pytest.mark.parametrize(
    ("frequency", "ax", "kwargs", "expected_bins"),
    [
        (10, None, {"bins": 10}, 10),
        (10, plt.subplots()[1], {"bins": 10}, 10),
        (20, None, {"bins": 5}, 5),
    ],
    ids=["default parameters", "axes provided", "kwargs provided"],
)
def test_heatmap(frequency, ax, kwargs, expected_bins):
    """Test heatmap y-axis, colorbar label, and kwargs forwarding across parameter combinations."""
    mesh, cbar = plotting.heatmap(_position(), frequency=frequency, ax=ax, **kwargs)
    used_ax = ax if ax is not None else plt.gca()
    assert used_ax.yaxis_inverted()
    assert cbar.ax.get_ylabel() == "time (s)"
    arr = mesh.get_array()
    assert arr is not None
    assert arr.shape == (expected_bins, expected_bins)


def test_heatmap_frequency_scales_weights():
    """Test heatmap values are halved when frequency is doubled (weights = 1/frequency)."""
    pos = _position()
    mesh10, _ = plotting.heatmap(pos, frequency=10, bins=10)
    mesh20, _ = plotting.heatmap(pos, frequency=20, bins=10)
    arr10, arr20 = mesh10.get_array(), mesh20.get_array()
    assert arr10 is not None
    assert arr20 is not None
    # each sample weighted 1/frequency, so double freq = half values
    np.testing.assert_allclose(arr10, arr20 * 2)


@pytest.mark.parametrize(
    ("cx", "cy", "r", "ax"),
    [(0, 0, 1, None), (5, 3, 10, None), (-2, -7, 0.5, plt.subplots()[1])],
    ids=[
        "unit circle at origin",
        "positive center, large radius",
        "negative center, small radius, axes provided",
    ],
)
def test_circle(cx, cy, r, ax):
    """Test circle draws 360 points lying on the circle of radius r around (cx, cy)."""
    ax = ax if ax is not None else plt.gca()
    plotting.circle(cx, cy, r, ax=ax)
    line = ax.lines[0]
    xdata = np.asarray(line.get_xdata())
    ydata = np.asarray(line.get_ydata())
    assert len(xdata) == 360
    distances = np.sqrt((xdata - cx) ** 2 + (ydata - cy) ** 2)
    np.testing.assert_allclose(distances, r)


def test_circle_uses_current_axes_when_none():
    """Test circle falls back to plt.gca() when no axes is provided."""
    _, ax = plt.subplots()
    plt.sca(ax)
    plotting.circle(0, 0, 1)
    assert len(ax.lines) == 1


@pytest.mark.parametrize(
    ("window", "frequency", "ax", "kwargs"),
    [
        ("5s", 10, None, {}),
        ("10s", 20, plt.subplots()[1], {"label": "pellets rate"}),
    ],
    ids=["default axes", "axes provided"],
)
def test_rateplot(window, frequency, ax, kwargs):
    """Test rateplot varying relevant parameters.

    The parameters being left out are those passed to `analysis.utils.rate`
    and thus tested separately in `test_utils.py`.
    """
    events = _events()
    used_ax = ax if ax is not None else plt.gca()
    n_lines_before = len(used_ax.lines)
    n_collections_before = len(used_ax.collections)
    plotting.rateplot(events, window=window, frequency=frequency, ax=ax, **kwargs)
    line = used_ax.lines[n_lines_before]
    assert np.asarray(line.get_xdata())[0] == pytest.approx(0.0)
    assert np.all(np.asarray(line.get_ydata()) >= 0)
    assert len(used_ax.collections) == n_collections_before + 1
    lc = used_ax.collections[n_collections_before]
    assert isinstance(lc, LineCollection)
    segments = lc.get_segments()
    assert len(segments) == len(events)
    vline_x = np.array([seg[0, 0] for seg in segments])
    np.testing.assert_allclose(vline_x, np.arange(len(events)) / 60.0)
    if "label" in kwargs:
        assert line.get_label() == kwargs["label"]


@pytest.mark.parametrize(
    ("bottom", "top", "expected_bottom", "expected_top"),
    [
        (0.1, 0.2, -0.1, 1.2),
        (0, 0, 0.0, 1.0),
        (0.1, 0.1, -0.1, 1.1),
    ],
    ids=["asymmetric margins", "zero margins", "equal margins"],
)
def test_set_ymargin(bottom, top, expected_bottom, expected_top):
    """Test set_ymargin extends ylim relative to data range by specified proportions."""
    _, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])
    plotting.set_ymargin(ax, bottom=bottom, top=top)
    assert ax.get_ylim() == (expected_bottom, expected_top)


@pytest.mark.parametrize(
    ("x", "y", "ax", "kwargs"),
    [
        (np.linspace(0, 1, 5), np.zeros(5), None, {}),
        (np.array([0.0, 1.0, 0.0, -1.0, 0.0]), np.array([1.0, 0.0, -1.0, 0.0, 1.0]), plt.subplots()[1], {}),
        (np.linspace(0, 1, 5), np.linspace(0, 1, 5), None, {"linewidth": 3}),
    ],
    ids=["default axes", "axes provided", "kwargs forwarded"],
)
def test_colorline(x, y, ax, kwargs):
    """Test colorline builds n-1 segments with correct coordinates and applies kwargs."""
    used_ax = ax if ax is not None else plt.gca()
    result = plotting.colorline(x, y, ax=ax, **kwargs)
    assert isinstance(result, LineCollection)
    assert result in used_ax.collections
    segs = result.get_segments()
    assert len(segs) == len(x) - 1
    for i, seg in enumerate(segs):
        np.testing.assert_array_equal(seg, [[x[i], y[i]], [x[i + 1], y[i + 1]]])
    if "linewidth" in kwargs:
        assert np.asarray(result.get_linewidth())[0] == kwargs["linewidth"]
