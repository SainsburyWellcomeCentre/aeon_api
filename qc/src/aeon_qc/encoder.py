"""Wheel encoder gap detection — identifies dropped samples within activity bouts."""

import datetime
from os import PathLike

import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Encoder

EMPTY_COLS = ("duration", "n_missed", "device")

EXPECTED_INTERVAL = pd.Timedelta(milliseconds=2)


def encoder_gaps(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Encoder,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Detect dropped samples in the wheel encoder stream.

    The wheel encoder emits samples at a constant ~500 Hz (2 ms interval)
    whenever the wheel is moving. Any inter-sample gap within an activity bout
    that exceeds 2 ms indicates dropped samples. ``n_missed`` is estimated as
    ``round(gap_duration / 2 ms)``.

    Args:
        root: The dataset root path(s), forwarded to ``load()``.
        reader: An ``Encoder`` reader instance specifying the device stream to
            inspect.
        start: Optional left bound of the time range to examine.
        end: Optional right bound of the time range to examine.

    Returns:
        A tidy ``pd.DataFrame`` with a UTC ``DatetimeIndex`` (``name="time"``)
        giving the last timestamp before each gap, and columns:

        - ``duration`` (pd.Timedelta): Length of the gap.
        - ``n_missed`` (int): Estimated number of missed samples.
        - ``device`` (str): The reader pattern used as a device identifier.

        Returns an empty DataFrame if no data is found or no gaps are detected.
    """
    data = load(root, reader, start=start, end=end)

    if data.empty:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        return result

    deltas = data.index.to_series().diff()
    gap_mask = deltas > EXPECTED_INTERVAL

    gap_ends = data.index[gap_mask]
    gap_starts = gap_ends - deltas[gap_mask]
    durations = deltas[gap_mask]
    n_missed = (durations / EXPECTED_INTERVAL).round().astype(int)

    result = pd.DataFrame(
        {
            "duration": durations.values,
            "n_missed": n_missed.values,
            "device": reader.pattern,
        },
        index=pd.DatetimeIndex(gap_starts, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    result.attrs["n_samples"] = len(data) + int(n_missed.sum())
    return result
