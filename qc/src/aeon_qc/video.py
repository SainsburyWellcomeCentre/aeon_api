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
    root: str | PathLike,
    reader: Video,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Detect dropped video frames by inspecting hardware frame counter jumps.

    Returns one row per drop event (UTC DatetimeIndex ``"time"`` = last frame before drop)
    with columns ``duration``, ``n_dropped``, ``hw_counter_before``, ``hw_counter_after``,
    ``device``. Sets ``attrs["data_found"]`` and ``attrs["n_frames"]``.
    """
    data = load(root, reader, start=start, end=end)

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
    root: str | PathLike,
    reader: Video,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Measure frame-to-frame timing stability using the camera's internal hardware clock.

    Uses hw_timestamp (FLIR Spinnaker ChunkData.Timestamp, nanoseconds) on consecutive
    frames only (hw_counter.diff() == 1), so dropped-frame intervals are excluded. (these
    are counted separately by dropped_frames). Framerate is inferred from the median interval;
    not read from metadata.
    Returns a single-row DataFrame with interval distribution statistics.
    Sets attrs["data_found"], attrs["fps_source"], attrs["clock"], and (when
    intervals exist) a histogram of inter-frame intervals over [0, 5*median]
    in attrs["histogram_bin_edges_ms"], attrs["histogram_counts"], and
    attrs["histogram_n_above"] for intervals above the upper edge.
    """
    data = load(root, reader, start=start, end=end)

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
    counts, _ = np.histogram(intervals_ms.values, bins=bin_edges)
    result.attrs["histogram_bin_edges_ms"] = bin_edges
    result.attrs["histogram_counts"] = counts
    result.attrs["histogram_n_above"] = int((intervals_ms > upper).sum())
    return _set_attrs(result, True)
