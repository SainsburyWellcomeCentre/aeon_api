"""Dropped frame detection for video acquisition streams."""

import datetime
from os import PathLike

import numpy as np
import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Video

EMPTY_COLS = ("duration", "n_dropped", "hw_counter_before", "hw_counter_after", "device")


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
