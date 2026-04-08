"""Environment QC metrics — message log errors and state duration analysis."""

import datetime
from os import PathLike

import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Reader


def message_log_errors(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Reader,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Extract non-Info entries from a MessageLog stream."""
    data = load(root, reader, start=start, end=end)

    if data.empty:
        result = pd.DataFrame(
            columns=["priority", "type", "message"],
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        result.attrs["n_total"] = 0
        return result

    errors = data[data["priority"].str.lower() != "info"].copy()
    result = errors[["priority", "type", "message"]]
    result.index.name = "time"
    result.attrs["data_found"] = True
    result.attrs["n_total"] = len(data)
    return result


def environment_state_durations(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Reader,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Compute time spent in each environment state from state-transition events."""
    data = load(root, reader, start=start, end=end)

    if data.empty:
        result = pd.DataFrame(
            columns=["state", "duration"],
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = False
        return result

    times = data.index.sort_values()
    states = data.loc[times, "state"]

    if len(times) == 1 and end is None:
        # Single transition with no known end — cannot compute any duration
        result = pd.DataFrame(
            columns=["state", "duration"],
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = True
        return result

    # Build end-times for each period
    if end is not None:
        end_times = list(times[1:]) + [pd.Timestamp(end, tz=datetime.UTC)]
    else:
        end_times = list(times[1:])
        times = times[:-1]
        states = states.iloc[:-1]

    durations = [pd.Timestamp(e) - s for s, e in zip(times, end_times)]

    result = pd.DataFrame(
        {"state": states.values, "duration": durations},
        index=pd.DatetimeIndex(times, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    return result
