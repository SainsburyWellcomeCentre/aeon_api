"""Environment QC metrics — message log errors and state duration analysis."""

import datetime
from os import PathLike

import pandas as pd

from swc.aeon.io.api import load
from swc.aeon.io.reader import Reader


def message_log_errors(
    root: str | PathLike,
    reader: Reader,
    start: datetime.datetime,
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


def harp_sync_alerts(
    root: str | PathLike,
    reader: Reader,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Extract and parse HarpSynch alert entries from a MessageLog stream.

    Returns one row per alert with structured columns parsed from the
    Bonsai SynchronizerMonitor log message body. The Bonsai alert fires
    when any of: DeviceCount != ExpectedDeviceCount, MaxDifference > 0,
    or Abs(MeanUtcTimestamp - UtcNow) > 30 minutes.
    """
    cols = [
        "mean_timestamp", "mean_utc_timestamp",
        "expected_device_count", "device_count", "max_difference",
    ]
    empty_result = pd.DataFrame(
        columns=cols,
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    ).astype({"expected_device_count": "Int64", "device_count": "Int64", "max_difference": float})

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
    root: str | PathLike,
    reader: Reader,
    start: datetime.datetime,
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
        end_ts = pd.Timestamp(end)
        end_ts = end_ts.tz_localize(datetime.UTC) if end_ts.tzinfo is None \
            else end_ts.tz_convert(datetime.UTC)
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
