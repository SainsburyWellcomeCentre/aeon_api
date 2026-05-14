"""Gap detection for continuous-rate Harp streams (e.g. photodiode at 1 kHz).

A reader is treated as continuous-rate when it has an ``expected_hz`` attribute
set on the instance. Stream classes tag the reader at construction time (see
``swc.aeon.qc.octagon.Photodiode`` / ``VideoController``); ``run_qc`` dispatches
``harp_gaps`` on any ``Harp`` reader that has that attribute.
"""

import datetime
from os import PathLike

import pandas as pd

from swc.aeon.io.api import load
from swc.aeon.io.reader import Harp

EMPTY_COLS = ("duration", "n_missed", "device")
DEFAULT_THRESHOLD = pd.Timedelta(seconds=1)


def harp_gaps(
    root: str | PathLike,
    reader: Harp,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    threshold: pd.Timedelta = DEFAULT_THRESHOLD,
) -> pd.DataFrame:
    """Detect dropped samples in a continuous-rate Harp stream.

    The reader must have an ``expected_hz`` attribute set; gaps longer than
    ``threshold`` (default 1 s) are flagged. Returns columns ``duration``,
    ``n_missed``, ``device``.
    """
    expected_hz: float = reader.expected_hz
    expected_interval = pd.Timedelta(seconds=1.0 / expected_hz)

    data = load(root, reader, start=start, end=end)

    if data.empty:
        result = pd.DataFrame(
            columns=list(EMPTY_COLS),
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        result.attrs["metric"] = "harp_gaps"
        result.attrs["expected_hz"] = expected_hz
        return result

    deltas = data.index.to_series().diff()
    gap_mask = deltas > threshold

    gap_ends = data.index[gap_mask]
    gap_starts = gap_ends - deltas[gap_mask]
    durations = deltas[gap_mask]
    n_missed = (durations / expected_interval).round().astype(int)

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
    result.attrs["metric"] = "harp_gaps"
    result.attrs["expected_hz"] = expected_hz
    return result
