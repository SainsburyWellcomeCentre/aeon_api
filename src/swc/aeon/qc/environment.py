"""Message log and environment state metrics for the Environment device."""

import datetime
from os import PathLike

import pandas as pd

from swc.aeon.io.api import Reader, load


def message_log_errors(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Reader,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Extract non-Info entries from a MessageLog stream.

    Args:
        root: Dataset root path or paths.
        reader: The ``MessageLog`` reader.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        data: The stream, already loaded and sorted; loaded here when not given.

    Returns:
        A DataFrame with one row per non-Info entry, indexed by time.

        - priority (str), type (str), message (str): The entry as logged.

        ``attrs`` hold ``data_found`` and ``n_total``, the entries of any priority.
    """
    if data is None:
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


def harp_sync_alerts(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Reader,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Extract and parse HarpSynch alert entries from a MessageLog stream.

    The Bonsai SynchronizerMonitor raises the alert when the device count differs from
    the expected count, when the maximum timestamp difference is above zero, or when the
    mean UTC timestamp is more than 30 minutes from the current time.

    Args:
        root: Dataset root path or paths.
        reader: The ``MessageLog`` reader.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        data: The stream, already loaded and sorted; loaded here when not given.

    Returns:
        A DataFrame with one row per alert, indexed by time.

        - mean_timestamp (float), mean_utc_timestamp (float): Parsed from the message body.
        - expected_device_count (int), device_count (int), max_difference (float): Likewise.

        ``attrs`` hold ``data_found`` and ``n_total_messages``.
    """
    cols = [
        "mean_timestamp",
        "mean_utc_timestamp",
        "expected_device_count",
        "device_count",
        "max_difference",
    ]
    empty_result = pd.DataFrame(
        columns=cols,
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    ).astype({"expected_device_count": "Int64", "device_count": "Int64", "max_difference": float})

    if data is None:
        data = load(root, reader, start=start, end=end)
    if data.empty:
        empty_result.attrs["data_found"] = False
        empty_result.attrs["n_total_messages"] = 0
        return empty_result

    alerts = data[data["type"].str.lower() == "harpsynch"].copy()
    if alerts.empty:
        empty_result.attrs["data_found"] = True
        empty_result.attrs["n_total_messages"] = len(data)
        return empty_result

    parsed = alerts["message"].str.split("\t", expand=True)
    parsed.columns = cols
    parsed["expected_device_count"] = parsed["expected_device_count"].astype("Int64")
    parsed["device_count"] = parsed["device_count"].astype("Int64")
    parsed["max_difference"] = parsed["max_difference"].astype(float)
    parsed.index = alerts.index
    parsed.index.name = "time"

    parsed.attrs["data_found"] = True
    parsed.attrs["n_total_messages"] = len(data)
    return parsed


def environment_state_durations(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Reader,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute time spent in each environment state from state-transition events.

    Args:
        root: Dataset root path or paths.
        reader: The ``EnvironmentState`` reader.
        start: Left bound of the time range.
        end: Optional right bound of the time range; closes the final state.
        data: The stream, already loaded and sorted; loaded here when not given.

    Returns:
        A DataFrame with one row per state period, indexed by its start.

        - state (str): The environment state.
        - duration (Timedelta): Time until the next transition, or until ``end``.

        Without ``end`` the final period has no known end and is omitted. ``attrs`` hold
        ``data_found``.
    """
    if data is None:
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
        # A single transition with no known end has no duration to compute
        result = pd.DataFrame(
            columns=["state", "duration"],
            index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
        )
        result.attrs["data_found"] = True
        return result

    # Build end-times for each period
    if end is not None:
        end_ts = pd.Timestamp(end)
        end_ts = (
            end_ts.tz_localize(datetime.UTC) if end_ts.tzinfo is None else end_ts.tz_convert(datetime.UTC)
        )
        end_times = list(times[1:]) + [end_ts]
    else:
        end_times = list(times[1:])
        times = times[:-1]
        states = states.iloc[:-1]

    durations = [pd.Timestamp(e) - s for s, e in zip(times, end_times, strict=False)]

    result = pd.DataFrame(
        {"state": states.values, "duration": durations},
        index=pd.DatetimeIndex(times, name="time", tz=datetime.UTC),
    )
    result.attrs["data_found"] = True
    return result
