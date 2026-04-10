"""Heartbeat gap detection for Harp devices."""

import datetime
from os import PathLike

import numpy as np
import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Heartbeat

EMPTY_COLS = ("duration", "n_missed", "second_before", "second_after", "device")
DUPLICATE_COLS = ("second", "count", "device")


def heartbeat_gaps(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Heartbeat,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
) -> pd.DataFrame:
    """Detect gaps where a Harp device stops sending heartbeats.

    A gap is any row where ``second`` increments by more than 1.

    Returns a DataFrame (UTC DatetimeIndex ``"time"``) with ``data_found``
    attr and columns: ``duration``, ``n_missed``, ``second_before``,
    ``second_after``, ``device``.
    """
    data = load(root, reader, start=start, end=end)

    if data.empty:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        return result

    second = data["second"].astype(np.int64)
    delta = second.diff()
    time_deltas = data.index.to_series().diff()

    gap_mask = delta > 1

    gap_ends = data.index[gap_mask]
    gap_starts = gap_ends - time_deltas[gap_mask]

    result = pd.DataFrame(
        {
            "duration": time_deltas[gap_mask].values,
            "n_missed": (delta[gap_mask] - 1).astype(int).values,
            "second_before": second.shift(1)[gap_mask].astype(int).values,
            "second_after": second[gap_mask].astype(int).values,
            "device": reader.pattern,
        },
        index=pd.DatetimeIndex(gap_starts, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    result.attrs["n_heartbeats"] = len(data)
    return result


def heartbeat_duplicates(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Heartbeat,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
) -> pd.DataFrame:
    """Detect seconds where a Harp device emits more than one heartbeat.

    Duplicates inflate ``device_count`` vs ``expected_device_count`` and trigger a
    HarpSynch alert. Returns columns ``second``, ``count``, ``device``; index is the
    timestamp of the first duplicate occurrence.
    """
    data = load(root, reader, start=start, end=end)

    empty = pd.DataFrame(
        columns=list(DUPLICATE_COLS),
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    )

    if data.empty:
        empty.attrs["data_found"] = False
        empty.attrs["n_heartbeats"] = 0
        return empty

    second = data["second"].astype(np.int64)
    counts = second.value_counts()
    dup_counts = counts[counts > 1]

    if dup_counts.empty:
        empty.attrs["data_found"] = True
        empty.attrs["n_heartbeats"] = len(data)
        return empty

    # For each duplicate second, find the timestamp of its first occurrence.
    first_ts = data.groupby(second).apply(lambda g: g.index[0])
    dup_first_ts = first_ts.loc[dup_counts.index].sort_values()

    result = pd.DataFrame(
        {
            "second": dup_first_ts.index.values,
            "count": dup_counts.loc[dup_first_ts.index].values,
            "device": reader.pattern,
        },
        index=pd.DatetimeIndex(dup_first_ts.values, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    result.attrs["n_heartbeats"] = len(data)
    return result
