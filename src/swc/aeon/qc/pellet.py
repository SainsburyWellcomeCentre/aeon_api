"""Pellet delivery failure detection for Harp feeder devices."""

import datetime
from os import PathLike

import pandas as pd

from swc.aeon.io.api import Reader, load

EMPTY_COLS = ("outcome", "device")


def pellet_failures(
    root: str | PathLike | list[str] | list[PathLike],
    deliver_reader: Reader,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    missed_reader: Reader | None = None,
    retried_reader: Reader | None = None,
    data: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Detect pellet delivery failures for a single feeder device.

    Args:
        root: Dataset root path or paths.
        deliver_reader: The ``DeliverPellet`` reader.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        missed_reader: The ``MissedPellet`` reader, when the schema has one.
        retried_reader: The ``RetriedDelivery`` reader, when the schema has one.
        data: Preloaded frames under the keys ``deliver``, ``missed`` and ``retried``; any
            reader without a frame here is loaded from ``root``.

    Returns:
        A DataFrame with one row per failure, indexed by time.

        - outcome (str): ``missed`` or ``retried``.
        - device (str): The reader pattern.

        ``attrs`` hold ``data_found``, ``n_deliveries``, ``n_missed`` and ``n_retried``.
    """
    data = data or {}
    deliver = data["deliver"] if "deliver" in data else load(root, deliver_reader, start=start, end=end)

    failure_rows: list[tuple[pd.Timestamp, str]] = []

    n_missed = 0
    if missed_reader is not None:
        missed = data["missed"] if "missed" in data else load(root, missed_reader, start=start, end=end)
        n_missed = len(missed)
        for ts in missed.index:
            failure_rows.append((ts, "missed"))

    n_retried = 0
    if retried_reader is not None:
        retried = data["retried"] if "retried" in data else load(root, retried_reader, start=start, end=end)
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
