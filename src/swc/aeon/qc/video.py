"""Dropped frame detection and frame rate stability for video acquisition streams."""

import datetime
from os import PathLike

import numpy as np
import pandas as pd

from swc.aeon.io.api import load
from swc.aeon.io.reader import Video

EMPTY_COLS = ("duration", "n_dropped", "hw_counter_before", "hw_counter_after", "device")

FRAME_RATE_COLS = (
    "n_frames",
    "fps_inferred",
    "interval_median_ms",
    "interval_std_ms",
    "interval_p99_ms",
    "interval_max_ms",
)

HISTOGRAM_BINS = 200


def dropped_frames(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Video,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Detect dropped video frames by inspecting hardware frame counter jumps.

    Args:
        root: Dataset root path or paths.
        reader: The ``Video`` reader.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        data: The stream, already loaded and sorted; loaded here when not given.

    Returns:
        A DataFrame with one row per drop event, indexed by the time of the last frame
        before it.

        - duration (Timedelta), n_dropped (int): Length of the drop and frames lost.
        - hw_counter_before (int), hw_counter_after (int): The counter either side.
        - device (str): The reader pattern.

        ``attrs`` hold ``data_found`` and ``n_frames``, the frames counted including dropped
        ones.
    """
    if data is None:
        data = load(root, reader, start=start, end=end)
    data = data.dropna(subset=["hw_counter"])

    if data.empty:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        return result

    counter = data["hw_counter"].astype(np.int64)
    delta = counter.diff()
    time_deltas = data.index.to_series().diff()

    drop_mask = delta > 1

    drop_ends = data.index[drop_mask]
    drop_starts = drop_ends - time_deltas[drop_mask]

    result = pd.DataFrame(
        {
            "duration": time_deltas[drop_mask].values,
            "n_dropped": (delta[drop_mask] - 1).astype(int).values,
            "hw_counter_before": counter.shift(1)[drop_mask].astype(int).values,
            "hw_counter_after": counter[drop_mask].astype(int).values,
            "device": reader.pattern,
        },
        index=pd.DatetimeIndex(drop_starts, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    result.attrs["n_frames"] = len(data) + int((delta[drop_mask] - 1).sum())
    return result


def frame_rate_stability(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Video,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Measure frame-to-frame timing stability using the camera's internal hardware clock.

    Uses ``hw_timestamp`` (FLIR Spinnaker ChunkData.Timestamp, in nanoseconds) on
    consecutive frames only, where ``hw_counter`` advances by one. Dropped-frame
    intervals are excluded here and counted by ``dropped_frames``. The frame rate is inferred
    from the median interval, not read from metadata.

    Args:
        root: Dataset root path or paths.
        reader: The ``Video`` reader.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        data: The stream, already loaded and sorted; loaded here when not given.

    Returns:
        A single-row DataFrame indexed by ``start``.

        - n_frames (int), fps_inferred (float).
        - interval_median_ms, interval_std_ms, interval_p99_ms, interval_max_ms (float).

        ``attrs`` hold ``data_found``, ``fps_source``, ``clock`` and, when intervals exist, a
        histogram of them over ``[0, 5 * median]`` in ``histogram_bin_edges_ms`` and
        ``histogram_counts``, with ``histogram_n_above`` counting intervals past the edge.
    """
    if data is None:
        data = load(root, reader, start=start, end=end)
    data = data.dropna(subset=["hw_counter"])

    nan_row = pd.DataFrame(
        {col: [float("nan")] for col in FRAME_RATE_COLS},
        index=pd.DatetimeIndex([start], name="time", tz=datetime.UTC),
    )
    nan_row["n_frames"] = 0

    def _set_attrs(df: pd.DataFrame, data_found: bool) -> pd.DataFrame:
        df.attrs["data_found"] = data_found
        df.attrs["fps_source"] = "inferred_from_median"
        df.attrs["clock"] = "hw_timestamp_ns"
        return df

    if data.empty:
        return _set_attrs(nan_row, False)

    counter = data["hw_counter"].astype(np.int64)
    consecutive = counter.diff() == 1
    intervals_ms = (data["hw_timestamp"].diff()[consecutive] / 1e6).dropna()

    if intervals_ms.empty:
        nan_row["n_frames"] = len(data)
        return _set_attrs(nan_row, True)

    median_ms = float(intervals_ms.median())
    result = pd.DataFrame(
        {
            "n_frames": [len(data)],
            "fps_inferred": [1000.0 / median_ms if median_ms > 0 else float("nan")],
            "interval_median_ms": [median_ms],
            "interval_std_ms": [float(intervals_ms.std())],
            "interval_p99_ms": [float(intervals_ms.quantile(0.99))],
            "interval_max_ms": [float(intervals_ms.max())],
        },
        index=pd.DatetimeIndex([start], name="time", tz=datetime.UTC),
    )
    upper = 5.0 * median_ms if median_ms > 0 else float(intervals_ms.max())
    bin_edges = np.linspace(0.0, upper, HISTOGRAM_BINS + 1)
    counts, _ = np.histogram(intervals_ms.to_numpy(), bins=bin_edges)
    result.attrs["histogram_bin_edges_ms"] = bin_edges
    result.attrs["histogram_counts"] = counts
    result.attrs["histogram_n_above"] = int((intervals_ms > upper).sum())
    return _set_attrs(result, True)
