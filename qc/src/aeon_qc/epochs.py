"""Epoch gap detection — identifies periods where data collection was interrupted."""

import datetime
from os import PathLike

import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Metadata as MetadataReader

EMPTY_COLS = ("gap_duration",)


def epoch_gaps(
    root: str | PathLike,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Return one row per Bonsai session start (epoch) within the time range.

    Each epoch is a ``Metadata.yml`` write event. Multiple rows indicate restarts;
    ``gap_duration`` shows the outage between consecutive starts (``NaT`` for the last epoch).
    A short gap suggests a crash; a long gap suggests a planned stop.
    """
    meta = load(root, MetadataReader(), start=start, end=end)

    if meta.empty:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        return result

    epoch_starts = meta.index.sort_values()
    durations = pd.Series(epoch_starts, index=epoch_starts).diff().shift(-1)

    result = pd.DataFrame(
        {"gap_duration": durations.values},
        index=pd.DatetimeIndex(epoch_starts, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    return result
