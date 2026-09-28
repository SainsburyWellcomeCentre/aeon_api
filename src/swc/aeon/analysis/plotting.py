"""Helper functions for plotting data."""

import math
from typing import cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import colors
from matplotlib.axes import Axes
from matplotlib.collections import LineCollection, QuadMesh
from matplotlib.colorbar import Colorbar

from swc.aeon.analysis.utils import rate, sessiontime


def heatmap(
    position: pd.Series, frequency: float, ax: Axes | None = None, **kwargs
) -> tuple[QuadMesh, Colorbar]:
    """Plot a log-scale 2D heatmap of dwell time in seconds from position data.

    Args:
        position: A series of position data containing x and y coordinates.
        frequency: The sampling frequency for the position data.
        ax: The Axes on which to draw the heatmap.
        **kwargs: Additional keyword arguments passed to `hist2d`.

    Returns:
        A tuple containing the QuadMesh object representing the heatmap and the Colorbar object representing
        the color scale.
    """
    if ax is None:
        ax = plt.gca()
    _, _, _, mesh = ax.hist2d(
        position.x, position.y, weights=np.ones(len(position)) / frequency, norm=colors.LogNorm(), **kwargs
    )
    ax.invert_yaxis()
    cbar = plt.colorbar(mesh, ax=ax)
    cbar.set_label("time (s)")
    return mesh, cbar


def circle(x: float, y: float, radius: float, *args, ax: Axes | None = None, **kwargs):
    """Plot a circle centered at the given x, y position with the specified radius.

    Args:
        x: The x-component of the circle center.
        y: The y-component of the circle center.
        radius: The radius of the circle.
        ax: The Axes on which to draw the circle.
        *args: Additional positional arguments passed to `plot` after `x` and `y` coordinates.
        **kwargs: Additional keyword arguments passed to `plot`.
    """
    if ax is None:
        ax = plt.gca()
    points = pd.DataFrame({"angle": np.linspace(0, 2 * math.pi, 360)})
    points["x"] = radius * np.cos(points.angle) + x
    points["y"] = radius * np.sin(points.angle) + y
    ax.plot(points.x, points.y, *args, **kwargs)


def rateplot(
    events: pd.Series,
    window: pd.DateOffset | pd.Timedelta | str,
    frequency: float,
    weight: float = 1.0,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
    smooth: pd.DateOffset | pd.Timedelta | str | None = None,
    center: bool = True,
    ax: Axes | None = None,
    **kwargs,
):
    """Plot the continuous event rate and raster of a discrete event sequence.

    The window size and sampling frequency can be specified.

    Args:
        events: The discrete sequence of events, indexed by datetime.
        window: The time period of each window used to compute the rate.
        frequency: The sampling frequency for the continuous rate.
        weight: A weight used to scale the continuous rate of each window.
        start: The left bound of the time range for the continuous rate.
        end: The right bound of the time range for the continuous rate.
        smooth: The size of the smoothing kernel applied to the continuous rate output.
        center: Specifies whether to center the convolution kernels.
        ax: The Axes on which to draw the rate plot and raster.
        **kwargs: Additional keyword arguments passed to :meth:`matplotlib.axes.Axes.plot`
            and :meth:`matplotlib.axes.Axes.vlines` for the continuous rate and raster, respectively.
    """
    label = kwargs.pop("label", None)
    eventrate = rate(events, window, frequency, weight, start, end, smooth=smooth, center=center)
    if ax is None:
        ax = plt.gca()
    ax.plot(
        (eventrate.index - eventrate.index[0]).total_seconds() / 60,
        eventrate,
        label=label,
        **kwargs,
    )
    index = cast(pd.DatetimeIndex, events.index)
    ax.vlines(sessiontime(index, eventrate.index[0]), -0.2, -0.1, linewidth=1, **kwargs)


def set_ymargin(ax: Axes, bottom: float, top: float):
    """Set the vertical margins of the specified Axes.

    Args:
        ax: The Axes for which to specify the vertical margin.
        bottom: The size of the bottom margin.
        top: The size of the top margin.
    """
    ax.set_ymargin(0)
    ax.autoscale_view()
    ylim = ax.get_ylim()
    delta = ylim[1] - ylim[0]
    bottom = ylim[0] - delta * bottom
    top = ylim[1] + delta * top
    ax.set_ylim(bottom, top)


def colorline(
    x: np.ndarray,
    y: np.ndarray,
    z: np.ndarray | None = None,
    cmap: str | colors.Colormap | None = None,
    norm: colors.Normalize | None = None,
    ax: Axes | None = None,
    **kwargs,
) -> LineCollection:
    """Plot a dynamically colored line on the specified Axes.

    Args:
        x: The horizontal coordinates of the data points.
        y: The vertical coordinates of the data points.
        z: The dynamic variable used to color each data point by indexing the color map.
        cmap: The colormap used to map normalized data values to RGBA colors.
        norm: The normalizing object used to scale data to the range [0, 1] for indexing the color map.
        ax: The Axes on which to draw the colored line.
        **kwargs: Additional keyword arguments passed to :class:`matplotlib.collections.LineCollection`.

    Returns:
        The LineCollection object representing the colored line.
    """
    if ax is None:
        ax = plt.gca()
    if z is None:
        z = np.linspace(0.0, 1.0, len(x))
    if cmap is None:
        cmap = plt.get_cmap("copper")
    if norm is None:
        norm = colors.Normalize(0.0, 1.0)
    z = np.asarray(z)
    points = np.array([x, y]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)
    lines = LineCollection(segments, array=z, cmap=cmap, norm=norm, **kwargs)  # type: ignore
    ax.add_collection(lines)
    return lines
