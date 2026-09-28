"""Helper functions for data analysis and visualization."""

import datetime

import numpy as np
import pandas as pd


def distancetravelled(angle: pd.Series, radius: float = 4.0) -> pd.Series:
    """Calculate the total distance travelled on the wheel.

    Takes into account the wheel radius and the total number of turns in both directions across time.

    Args:
        angle: A series of magnetic encoder measurements.
        radius: The radius of the wheel, in metric units.

    Returns:
        The total distance travelled on the wheel, in metric units.
    """
    maxvalue = int(np.iinfo(np.uint16).max >> 2)
    jumpthreshold = maxvalue // 2
    turns = angle.astype(int).diff()
    clickup = (turns < -jumpthreshold).astype(int)
    clickdown = (turns > jumpthreshold).astype(int) * -1
    turns = (clickup + clickdown).cumsum()
    distance = 2 * np.pi * radius * (turns + angle / maxvalue)
    distance = distance - distance.iloc[0]
    return distance


def visits(data: pd.DataFrame, onset: str = "Enter", offset: str = "Exit") -> pd.DataFrame:
    """Compute duration, onset and offset times from paired events.

    Allows for missing data by trying to match event onset times with subsequent offset times.
    If the match fails, event offset metadata is filled with NaN. Any additional metadata columns
    in the DataFrame will be paired and included in the output.

    Args:
        data: A DataFrame containing visit onset and offset events.
        onset: The label used to identify event onsets.
        offset: The label used to identify event offsets.

    Returns:
        A DataFrame containing duration and metadata for each visit.
    """
    lonset = onset.lower()
    loffset = offset.lower()
    lsuffix = f"_{lonset}"
    rsuffix = f"_{loffset}"
    id_onset = "id" + lsuffix
    event_onset = "event" + lsuffix
    event_offset = "event" + rsuffix
    time_onset = "time" + lsuffix
    time_offset = "time" + rsuffix

    # find all possible onset / offset pairs
    data = data.reset_index()
    data_onset = data[data.event == onset]
    data_offset = data[data.event == offset]
    data = pd.merge(data_onset, data_offset, on="id", how="left", suffixes=(lsuffix, rsuffix))

    # valid pairings have the smallest positive duration
    data["duration"] = data[time_offset] - data[time_onset]
    valid_visits = data[data.duration >= pd.Timedelta(0)]
    data = data.iloc[valid_visits.groupby([time_onset, "id"]).duration.idxmin()]
    data = data[data.duration > pd.Timedelta(0)]

    # duplicate offsets indicate missing data from previous pairing
    missing_data = data.duplicated(subset=time_offset, keep="last")
    if missing_data.any():
        data.loc[missing_data, ["duration"] + [name for name in data.columns if rsuffix in name]] = pd.NA
    # rename columns and sort data
    data.rename({time_onset: lonset, id_onset: "id", time_offset: loffset}, axis=1, inplace=True)
    data = data[["id"] + [name for name in data.columns if "_" in name] + [lonset, loffset, "duration"]]
    data.drop([event_onset, event_offset], axis=1, inplace=True)
    data.sort_index(inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data


def rate(
    events: pd.Series,
    window: pd.DateOffset | pd.Timedelta | str,
    frequency: float,
    weight: float = 1.0,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    smooth: pd.DateOffset | pd.Timedelta | str | None = None,
    center: bool = False,
) -> pd.Series:
    """Compute the continuous event rate from a discrete event sequence.

    The window size and sampling frequency can be specified.

    Args:
        events: The discrete sequence of events.
        window: The time period of each window used to compute the rate.
        frequency: The sampling frequency for the continuous rate.
        weight: A weight used to scale the continuous rate of each window.
        start: The left bound of the time range for the continuous rate.
        end: The right bound of the time range for the continuous rate.
        smooth: The size of the smoothing kernel applied to the continuous rate output.
        center: Specifies whether to center the convolution kernels.

    Returns:
        A Series containing the continuous event rate over time.
    """
    counts = pd.Series(weight, events.index)
    if start is not None and start < events.index[0]:
        counts.loc[start] = 0  # type: ignore[index]
    if end is not None and end > events.index[-1]:
        counts.loc[end] = 0  # type: ignore[index]
    counts.sort_index(inplace=True)
    counts = counts.resample(pd.Timedelta(1 / frequency, "s")).sum()
    rate = counts.rolling(window, center=center).sum()
    return rate.rolling(window if smooth is None else smooth, center=center).mean()


def get_events_rates(
    events: pd.Series,
    window_len_sec: int,
    frequency: str | pd.DateOffset | pd.Timedelta,
    unit_len_sec: int = 60,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    smooth: str | pd.Timedelta | None = None,
    center: bool = False,
) -> pd.Series:
    """Compute the event rate from a sequence of events over a specified window.

    Args:
        events: The discrete sequence of events, with timestamps in seconds as index.
        window_len_sec: The length of the window over which the event rate is estimated.
        frequency: The sampling frequency for the continuous rate.
        unit_len_sec: The length of one sample point. Default is 60 seconds.
        start: The left bound of the time range for the continuous rate.
        end: The right bound of the time range for the continuous rate.
        smooth: The size of the smoothing kernel applied to the continuous rate output.
        center: Specifies whether to center the convolution kernels.

    Returns:
        A Series containing the continuous event rate over time.
    """
    # events is an array with the time (in seconds) of event occurence
    # window_len_sec is the size of the window over which the event rate is estimated
    # unit_len_sec is the length of one sample point
    window_len_sec_str = f"{window_len_sec:d}S"
    counts = pd.Series(1.0, events.index)
    if start is not None and start < events.index[0]:
        counts.loc[start] = 0  # type: ignore[index]
    if end is not None and end > events.index[-1]:
        counts.loc[end] = 0  # type: ignore[index]
    counts.sort_index(inplace=True)
    counts_resampled = counts.resample(frequency).sum()
    counts_rolled = (
        counts_resampled.rolling(window_len_sec_str, center=center).sum() * unit_len_sec / window_len_sec
    )
    counts_rolled_smoothed = counts_rolled.rolling(
        window_len_sec_str if smooth is None else smooth, center=center
    ).mean()
    return counts_rolled_smoothed


def sessiontime(index: pd.DatetimeIndex, start: datetime.datetime | None = None) -> pd.Index:
    """Convert absolute timestamps to elapsed minutes from a reference start time.

    Args:
        index: Absolute timestamps to convert.
        start: Reference timestamp. If omitted, the first value in ``index`` is used.

    Returns:
        Relative time in minutes from ``start`` for each timestamp in ``index``.
    """
    if start is None:
        start = index[0]
    return (index - start).total_seconds() / 60


def distance(position: pd.DataFrame, target) -> pd.Series:
    """Compute the euclidean distance to a specified target.

    Args:
        position: A DataFrame with 'x' and 'y' columns for position coordinates.
        target: The target coordinates to compute distance from.

    Returns:
        Euclidean distance from each position to the target.
    """
    return np.sqrt(np.square(position[["x", "y"]] - target).sum(axis=1))


def activepatch(wheel: pd.Series, in_patch: pd.Series) -> pd.Series:
    """Infer patch activity from wheel movement within candidate patch intervals.

    Args:
        wheel: Cumulative wheel distance over time.
        in_patch: Boolean mask indicating periods when the patch may be active
            (e.g. when the subject is in the patch area).

    Returns:
        Boolean Series aligned to ``in_patch`` indicating whether the patch is active
        at each timestamp. True once wheel displacement exceeds 1 unit within a 1-second
        rolling window, remaining true until the next patch exit.
    """
    exit_patch = in_patch.astype(np.int8).diff() < 0
    in_wheel = (wheel.diff().rolling("1s").sum() > 1).reindex(in_patch.index, method="pad")
    epochs = exit_patch.cumsum()
    return in_wheel.groupby(epochs).cummax()
