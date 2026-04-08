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
    """Return one row per recording epoch within a time range.

    Each epoch is marked by a ``Metadata.yml`` file written when the Bonsai
    workflow starts. If ``start`` and ``end`` span multiple Bonsai sessions,
    ``load()`` assembles all ``Metadata.yml`` timestamps across those sessions.

    In normal operation there will be a single epoch (one row). Multiple rows
    indicate restarts; ``gap_duration`` then shows the outage between
    consecutive starts. A short gap (seconds to minutes) typically indicates a
    crash; a long gap indicates a planned stop or prolonged outage.

    Args:
        root: The dataset root path(s), forwarded to ``load()``.
        start: Optional left bound of the time range to examine.
        end: Optional right bound of the time range to examine.

    Returns:
        A tidy ``pd.DataFrame`` with a UTC ``DatetimeIndex`` (``name="time"``)
        giving the start of each epoch, and columns:

        - ``gap_duration`` (pd.Timedelta): Time elapsed from this epoch start
          to the next. ``NaT`` for the final epoch.

        Returns an empty DataFrame if no epochs are found.
    """
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
