"""Pellet delivery failure detection for Harp feeder devices."""

import datetime
from os import PathLike

import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Reader

EMPTY_COLS = ("outcome", "device")


def pellet_failures(
    root: str | PathLike | list[str] | list[PathLike],
    deliver_reader: Reader,
    missed_reader: Reader | None = None,
    retried_reader: Reader | None = None,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Detect pellet delivery failures for a single feeder device."""
    deliver = load(root, deliver_reader, start=start, end=end)

    failure_rows: list[tuple[pd.Timestamp, str]] = []

    n_missed = 0
    if missed_reader is not None:
        missed = load(root, missed_reader, start=start, end=end)
        n_missed = len(missed)
        for ts in missed.index:
            failure_rows.append((ts, "missed"))

    n_retried = 0
    if retried_reader is not None:
        retried = load(root, retried_reader, start=start, end=end)
        n_retried = len(retried)
        for ts in retried.index:
            failure_rows.append((ts, "retried"))

    device = deliver_reader.pattern

    if not failure_rows:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
    else:
        timestamps, outcomes = zip(*sorted(failure_rows), strict=True)
        result = pd.DataFrame(
            {"outcome": list(outcomes), "device": device},
            index=pd.DatetimeIndex(list(timestamps), name="time", tz=datetime.UTC),
        )

    result.attrs["n_deliveries"] = len(deliver)
    result.attrs["n_retried"] = n_retried
    result.attrs["n_missed"] = n_missed
    result.attrs["data_found"] = not deliver.empty
    return result
