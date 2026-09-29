"""Synthetic frames shared by the swc.aeon.qc unit tests."""

import datetime

import pandas as pd


def make_heartbeat(timestamps: list[str]) -> pd.DataFrame:
    """Build a synthetic heartbeat DataFrame from ISO timestamp strings."""
    idx = pd.DatetimeIndex(timestamps, tz=datetime.UTC, name="time")
    return pd.DataFrame({"second": range(len(idx))}, index=idx)


def make_heartbeat_offset(timestamps: list[str], offset_ms: float = 0.0) -> pd.DataFrame:
    """Build a synthetic heartbeat DataFrame with a fixed timestamp offset in milliseconds."""
    df = make_heartbeat(timestamps)
    if offset_ms != 0.0:
        df.index = df.index + pd.Timedelta(milliseconds=offset_ms)
    return df


def make_video(hw_counters: list[int], timestamps: list[str]) -> pd.DataFrame:
    """Build a synthetic video metadata DataFrame."""
    idx = pd.DatetimeIndex(timestamps, tz=datetime.UTC, name="time")
    return pd.DataFrame(
        {
            "hw_counter": hw_counters,
            "hw_timestamp": 0,
            "_frame": range(len(idx)),
            "_path": "",
            "_epoch": "",
        },
        index=idx,
    )
