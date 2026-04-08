"""Cross-device heartbeat synchronisation delta metric."""

import datetime
from os import PathLike

import pandas as pd
from swc.aeon.io.api import load
from swc.aeon.io.reader import Heartbeat

EMPTY_COLS = ("second", "device", "delta_seconds")
MIN_DEVICES = 2


def make_empty() -> pd.DataFrame:
    """Return an empty sync_delta DataFrame with the correct schema."""
    idx = pd.DatetimeIndex([], tz=datetime.UTC, name="time")
    return pd.DataFrame(columns=list(EMPTY_COLS), index=idx)


def sync_delta(
    root: str | PathLike | list[str] | list[PathLike],
    readers: dict[str, Heartbeat],
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
    reference: str | None = None,
) -> pd.DataFrame:
    """Compare heartbeat timestamps across Harp devices to detect sync drift.

    All Harp devices emit a heartbeat once per second via the ClockSynchronizer.
    Their timestamps should be identical. This function loads all provided
    Heartbeat streams, aligns them on the ``second`` counter (the shared logical
    clock), and returns per-second timestamp deltas relative to a reference device.

    Args:
        root: Dataset root path(s), forwarded to ``load()``.
        readers: Mapping of device name to ``Heartbeat`` reader instance.
        start: Optional left bound of the time range.
        end: Optional right bound of the time range.
        reference: Name of the device to use as reference. Defaults to
            ``"ClockSynchronizer"`` (or a key starting with that prefix) if
            present, otherwise the first key in ``readers``.

    Returns:
        Tidy DataFrame indexed by UTC reference timestamp (``name="time"``) with
        columns:

        - ``second`` (int): shared logical clock value used for alignment.
        - ``device`` (str): device name.
        - ``delta_seconds`` (float): signed offset vs. reference in seconds
          (positive = device timestamp is ahead of reference).

        Returns an empty DataFrame with the correct schema if fewer than two
        devices have data.
    """
    data: dict[str, pd.DataFrame] = {
        name: load(root, reader, start=start, end=end) for name, reader in readers.items()
    }
    data = {name: df for name, df in data.items() if not df.empty}

    if len(data) < MIN_DEVICES:
        return make_empty()

    # Resolve reference device: prefer ClockSynchronizer (exact or dotted prefix).
    if reference is None:
        cs_key = next(
            (k for k in data if k == "ClockSynchronizer" or k.startswith("ClockSynchronizer.")),
            None,
        )
        reference = cs_key if cs_key is not None else next(iter(data))
    elif reference not in data:
        reference = next(iter(data))

    ref_df = data[reference]

    # Build second (int) - reference timestamp lookup.
    ref_sec = ref_df["second"].astype(int)
    ref_lookup = pd.Series(ref_df.index, index=ref_sec)
    ref_lookup = ref_lookup[~ref_lookup.index.duplicated(keep="first")]

    rows: list[pd.DataFrame] = []
    for name, df in data.items():
        if name == reference:
            continue
        dev_sec = df["second"].astype(int)
        common_mask = dev_sec.isin(ref_lookup.index)
        df_common = df[common_mask]
        if df_common.empty:
            continue
        matched_sec = dev_sec[common_mask]
        ref_ts = ref_lookup.loc[matched_sec.values]
        ref_ts_idx = pd.DatetimeIndex(ref_ts.array, name="time")
        delta = (df_common.index - ref_ts_idx).total_seconds()
        rows.append(
            pd.DataFrame(
                {
                    "second": matched_sec.values,
                    "device": name,
                    "delta_seconds": delta,
                },
                index=ref_ts_idx,
            )
        )

    if not rows:
        return make_empty()

    return pd.concat(rows).sort_index()
