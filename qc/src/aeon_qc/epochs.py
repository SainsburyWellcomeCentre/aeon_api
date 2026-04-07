"""Epoch gap detection — identifies periods where data collection was interrupted."""

import datetime
from os import PathLike

import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Metadata as MetadataReader

EMPTY_COLS = ("next_epoch_start", "gap_duration")
MIN_EPOCHS = 2


def epoch_gaps(
    root: str | PathLike | list[str] | list[PathLike],
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Detect gaps between consecutive recording epochs within a time range."""
    meta = load(root, MetadataReader(), start=start, end=end)

    if len(meta) < MIN_EPOCHS:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = not meta.empty
        return result

    epoch_starts = meta.index.sort_values()
    next_starts = epoch_starts[1:]
    durations = next_starts - epoch_starts[:-1]

    result = pd.DataFrame(
        {
            "next_epoch_start": next_starts.values,
            "gap_duration": durations.values,
        },
        index=pd.DatetimeIndex(epoch_starts[:-1], name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    return result
