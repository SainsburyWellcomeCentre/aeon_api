"""Sequential timestamp check for any timestamped Aeon stream."""

import datetime
import warnings
from os import PathLike

import numpy as np
import pandas as pd

from swc.aeon.io.api import Reader, load

EMPTY_COLS = ("kind", "step_seconds", "index_in_stream", "device")
BACKWARDS = "backwards"
DUPLICATE = "duplicate"


def timestamp_order(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Reader,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Detect timestamps that go backwards or repeat within a stream.

    Flags every sample whose timestamp is earlier than or equal to the previous
    sample's timestamp, in the order the rows were written.

    Args:
        root: The root path, or sequence of root paths, of the dataset.
        reader: The stream reader to check.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        data: The stream as loaded with ``sort=False``; loaded here when not given.

    Returns:
        One row per violation, indexed by the violating sample's UTC ``time``, with
        columns ``kind``, ``step_seconds``, ``index_in_stream`` (position within
        the loaded window) and ``device``. ``attrs`` carry ``data_found``, ``n_samples``, ``n_backwards``,
        ``n_duplicates``, ``max_backwards_seconds`` and ``metric``.

    """
    if data is None:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*out-of-order timestamps.*")
            data = load(root, reader, start=start, end=end, sort=False)

    result = pd.DataFrame(
        columns=list(EMPTY_COLS),
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    )
    result.attrs["metric"] = "timestamp_order"
    result.attrs["data_found"] = not data.empty
    result.attrs["n_samples"] = len(data)
    result.attrs["n_backwards"] = 0
    result.attrs["n_duplicates"] = 0
    result.attrs["max_backwards_seconds"] = 0.0
    if data.empty:
        return result

    # pandas-stubs before 3.0 type the diffed index as a float series.
    steps = data.index.to_series().diff().dt.total_seconds().to_numpy()  # pyright: ignore[reportAttributeAccessIssue]
    backwards = steps < 0
    duplicate = steps == 0
    mask = backwards | duplicate
    if not mask.any():
        return result

    kind = np.where(backwards[mask], BACKWARDS, DUPLICATE)
    result = pd.DataFrame(
        {
            "kind": kind,
            "step_seconds": steps[mask],
            "index_in_stream": np.flatnonzero(mask),
            "device": reader.pattern,
        },
        index=pd.DatetimeIndex(data.index[mask], name="time", tz=datetime.UTC),
    )
    result.attrs["metric"] = "timestamp_order"
    result.attrs["data_found"] = True
    result.attrs["n_samples"] = len(data)
    result.attrs["n_backwards"] = int(backwards.sum())
    result.attrs["n_duplicates"] = int(duplicate.sum())
    result.attrs["max_backwards_seconds"] = float(-steps[backwards].min()) if backwards.any() else 0.0
    return result
