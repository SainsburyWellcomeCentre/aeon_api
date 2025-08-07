"""API for reading Aeon data from disk."""

import bisect
import datetime
import warnings
from os import PathLike
from pathlib import Path

import pandas as pd
from typing_extensions import deprecated

CHUNK_DURATION = 1
"""The duration of each acquisition chunk, in whole hours."""

REFERENCE_EPOCH = datetime.datetime(1904, 1, 1)
"""The reference epoch for UTC harp time."""


@deprecated("Please use the to_datetime function instead.")
def aeon(seconds):
    """Converts a Harp timestamp, in seconds, to a datetime object.

    .. deprecated:: 0.2.0
       This function is deprecated and will be removed in a future release.
       Use :func:`to_datetime` instead.
    """
    return to_datetime(seconds)  # pragma: no cover


def to_datetime(seconds):
    """Converts a Harp timestamp, in seconds, to a datetime object.

    Args:
        seconds (float, int, pandas.Series): The Harp timestamp in seconds.

    Returns:
        datetime.datetime, pandas.Series: The Harp timestamp(s) in datetime format.
    """
    return REFERENCE_EPOCH + pd.to_timedelta(seconds, "s")


def to_seconds(time):
    """Converts a datetime object to a Harp timestamp, in seconds.

    Args:
        time (datetime.datetime, pandas.DatetimeIndex or pandas.Series):
            A datetime object, index or series specifying the measurement timestamp(s).

    Returns:
        float, pandas.Series: The Harp timestamp in seconds.
    """
    if isinstance(time, pd.Series):
        return (time - REFERENCE_EPOCH).dt.total_seconds()
    else:
        return (time - REFERENCE_EPOCH).total_seconds()


def chunk(time):
    """Returns the whole hour acquisition chunk for a measurement timestamp.

    Args:
        time (datetime.datetime, pandas.DatetimeIndex or pandas.Series):
            A datetime object, index or series specifying the measurement timestamp(s).

    Returns:
        pandas.Timestamp, pandas.Series or pandas.DatetimeIndex:
            If `time` is a scalar, a Timestamp representing the acquisition chunk for
            the measurement timestamp. If `time` is a Series or DatetimeIndex, an object of
            the same type is returned, with each element specifying the acquisition chunk for
            the corresponding measurement timestamp.

    """
    if isinstance(time, pd.Series):
        hour = CHUNK_DURATION * (time.dt.hour // CHUNK_DURATION)
        return pd.to_datetime(time.dt.date) + pd.to_timedelta(hour, "h")
    elif isinstance(time, pd.DatetimeIndex):
        hour = CHUNK_DURATION * (time.hour // CHUNK_DURATION)
        return pd.to_datetime(time.date) + pd.to_timedelta(hour, "h")
    else:
        hour = CHUNK_DURATION * (time.hour // CHUNK_DURATION)
        return pd.to_datetime(datetime.datetime.combine(time.date(), datetime.time(hour=hour)))


def chunk_range(start, end):
    """Returns a range of whole hour acquisition chunks.

    Args:
        start (datetime.datetime): The left bound of the time range.
        end (datetime.datetime): The right bound of the time range.

    Returns:
        pandas.DatetimeIndex: A DatetimeIndex representing the acquisition chunk range.
    """
    return pd.date_range(chunk(start), chunk(end), freq=pd.DateOffset(hours=CHUNK_DURATION))


def chunk_key(file):
    """Returns the acquisition chunk key for the specified file.

    Args:
        file (pathlib.Path): The path to the file.

    Returns:
        tuple[str, datetime.datetime]: A tuple containing the epoch string
            and the acquisition chunk datetime.
    """
    epoch = file.parts[-3]
    chunk_str = file.stem.split("_")[-1]
    try:
        date_str, time_str = chunk_str.split("T")
    except ValueError:
        epoch = file.parts[-2]
        date_str, time_str = epoch.split("T")
    return epoch, datetime.datetime.fromisoformat(date_str + "T" + time_str.replace("-", ":"))


def _set_index(data):
    if not isinstance(data.index, pd.DatetimeIndex):
        data.index = to_datetime(data.index)
    data.index.name = "time"


def _empty(columns):
    return pd.DataFrame(columns=columns, index=pd.DatetimeIndex([], name="time"))


def _filter_time_range(df, start, end, inclusive="both"):
    """Filters a DataFrame by a given time range.

    Args:
        df (pandas.DataFrame): The DataFrame to filter.
        start (datetime.datetime): The left bound of the time range.
        end (datetime.datetime): The right bound of the time range.
        inclusive (str): Specifies whether the start and end bounds are inclusive or exclusive.
            Options are "both", "left", "right", or "neither".

    Returns:
        pandas.DataFrame: The filtered DataFrame.
    """
    result = df.loc[start:end]
    if inclusive == "both" or len(result) == 0:
        return result
    first_idx_equals_start = result.index[0] == start
    last_idx_equals_end = result.index[-1] == end
    if inclusive == "left":  # drop final row if the index is equal to end
        return result.iloc[:-1] if last_idx_equals_end else result
    elif inclusive == "right":  # drop first row if the index is equal to start
        return result.iloc[1:] if first_idx_equals_start else result
    elif inclusive == "neither":
        result = result.iloc[1:] if first_idx_equals_start else result
        result = result.iloc[:-1] if last_idx_equals_end else result
        return result
    else:
        raise ValueError(f"Invalid inclusive: {inclusive!r}")


def load(
    root, reader, start=None, end=None, inclusive="both", time=None, tolerance=None, epoch=None, **kwargs
):
    """Extracts chunk data from the root path of an Aeon dataset.

    Reads all chunk data using the specified data stream reader. A subset of the data can be loaded
    by specifying an optional time range, or a list of timestamps used to index the data on file.
    Returned data will be sorted chronologically.

    Args:
        root (str, PathLike, list[str], list[PathLike]):
            The root path, or prioritised sequence of paths, where data is stored.
        reader (swc.aeon.io.reader.Reader):
            A data stream reader object used to read chunk data from the dataset.
        start (datetime.datetime, optional): The left bound of the time range to extract.
        end (datetime.datetime, optional): The right bound of the time range to extract.
        inclusive (str, optional): Specifies whether the start and end bounds are inclusive or exclusive.
            Options are "both", "left", "right", or "neither".
        time (datetime.datetime, list[datetime.datetime], pandas.DatetimeIndex, pandas.DataFrame, optional):
            A single timestamp, list of timestamps, DatetimeIndex, or a DataFrame with
            DatetimeIndex specifying the timestamps to extract.
        tolerance (datetime.timedelta, optional):
            The maximum distance between original and new timestamps for inexact matches.
        epoch (str, optional):
            A wildcard pattern to use when searching epoch data.
        **kwargs: Optional keyword arguments to forward to ``reader`` when reading chunk data.

    Returns:
        pandas.DataFrame: A DataFrame containing epoch event metadata, sorted by time.

    """
    if isinstance(root, str):
        root = Path(root)
    if isinstance(root, PathLike):
        root = [root]

    epoch_pattern = "**" if epoch is None else epoch
    fileset = {
        chunk_key(fname): fname
        for path in root
        for fname in Path(path).glob(f"{epoch_pattern}/**/{reader.pattern}.{reader.extension}")
    }
    files = sorted(fileset.items())

    if time is not None:
        # ensure input is converted to timestamp series
        if isinstance(time, pd.DataFrame):
            time = time.index
        if not isinstance(time, pd.Series):
            time = pd.Series(time)
            time.index = time

        dataframes = []
        filetimes = [chunk for (_, chunk), _ in files]
        files = [file for _, file in files]
        for key, values in time.groupby(by=chunk):
            i = bisect.bisect_left(filetimes, key)  # type: ignore
            if i < len(filetimes):
                frame = reader.read(files[i], **kwargs)
                _set_index(frame)
            else:
                frame = _empty(reader.columns)
            data = frame.reset_index()
            data.set_index("time", drop=False, inplace=True)
            data = data.reindex(values, method="pad", tolerance=tolerance)
            missing = len(data.time) - data.time.count()
            if missing > 0 and i > 0:
                # expand reindex to allow adjacent chunks
                # to fill missing values
                previous = reader.read(files[i - 1], **kwargs)
                _set_index(previous)
                data = pd.concat([previous, frame])
                data = data.reindex(values, method="pad", tolerance=tolerance)
            else:
                data.drop(columns="time", inplace=True)
            dataframes.append(data)

        if len(dataframes) == 0:
            return _empty(reader.columns)

        return pd.concat(dataframes)

    if start is not None or end is not None:
        chunk_start = chunk(start) if start is not None else pd.Timestamp.min
        chunk_end = chunk(end) if end is not None else pd.Timestamp.max
        files = list(filter(lambda item: chunk_start <= chunk(item[0][1]) <= chunk_end, files))

    if len(files) == 0:
        return _empty(reader.columns)

    data = pd.concat([reader.read(file, **kwargs) for _, file in files])
    _set_index(data)
    if start is not None or end is not None:
        try:
            return _filter_time_range(data, start, end, inclusive)
        except KeyError:
            if not data.index.is_monotonic_increasing:
                warnings.warn(
                    f"data index for {reader.pattern} contains out-of-order timestamps!", stacklevel=2
                )
                data = data.sort_index()
            else:  # pragma: no cover
                raise
    return data
