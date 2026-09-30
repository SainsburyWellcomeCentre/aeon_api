"""Tests for the timestamp_order metric."""

import datetime
import warnings
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from qc.helpers import make_heartbeat
from swc.aeon.io.api import to_seconds
from swc.aeon.io.reader import Csv, Heartbeat
from swc.aeon.qc.sequence import timestamp_order

_PATCH = "swc.aeon.qc.sequence.load"
_ROOT = "/fake/root"
_START = pd.Timestamp("2024-01-15T09:00:00", tz="UTC")


def test_timestamp_order_empty_data(heartbeat_reader):
    """Empty input gives an empty result with the documented schema and attrs."""
    with patch(_PATCH, return_value=pd.DataFrame()):
        result = timestamp_order(_ROOT, heartbeat_reader, _START)
    assert result.empty
    assert list(result.columns) == ["kind", "step_seconds", "index_in_stream", "device"]
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC
    assert result.attrs["data_found"] is False
    assert result.attrs["n_samples"] == 0


def test_timestamp_order_loads_unsorted(heartbeat_reader):
    """Without preloaded data the stream is loaded with sort=False."""
    with patch(_PATCH, return_value=pd.DataFrame()) as load:
        timestamp_order(_ROOT, heartbeat_reader, _START, end=_START + pd.Timedelta(hours=1))
    _, kwargs = load.call_args
    assert kwargs["sort"] is False
    assert kwargs["start"] == _START


def test_timestamp_order_monotonic(heartbeat_reader):
    """Strictly increasing timestamps produce no rows."""
    data = make_heartbeat(["2024-01-15T09:00:00", "2024-01-15T09:00:01", "2024-01-15T09:00:02"])
    result = timestamp_order(_ROOT, heartbeat_reader, _START, data=data)
    assert result.empty
    assert result.attrs["data_found"] is True
    assert result.attrs["n_samples"] == 3
    assert result.attrs["n_backwards"] == 0
    assert result.attrs["n_duplicates"] == 0


def test_timestamp_order_backwards_and_duplicate(heartbeat_reader):
    """A backwards step and a repeated timestamp are each reported once."""
    data = make_heartbeat(
        [
            "2024-01-15T09:00:00.000",
            "2024-01-15T09:00:01.000",
            "2024-01-15T09:00:00.950",  # backwards by 50 ms
            "2024-01-15T09:00:02.000",
            "2024-01-15T09:00:02.000",  # duplicate
        ]
    )
    result = timestamp_order(_ROOT, heartbeat_reader, _START, data=data)
    assert len(result) == 2
    assert list(result["kind"]) == ["backwards", "duplicate"]
    assert result.iloc[0]["step_seconds"] == pytest.approx(-0.05)
    assert result.iloc[1]["step_seconds"] == pytest.approx(0.0)
    assert list(result["index_in_stream"]) == [2, 4]
    assert result.index[0] == pd.Timestamp("2024-01-15T09:00:00.950", tz="UTC")
    assert result.attrs["n_backwards"] == 1
    assert result.attrs["n_duplicates"] == 1
    assert result.attrs["max_backwards_seconds"] == pytest.approx(0.05)
    assert (result["device"] == heartbeat_reader.pattern).all()


def write_csv_chunk(epoch_dir: Path, device: str, chunk_name: str, seconds: list[float]) -> None:
    """Write a two-column CSV chunk file in the Aeon layout."""
    device_dir = epoch_dir / device
    device_dir.mkdir(parents=True, exist_ok=True)
    path = device_dir / f"{device}_State_{chunk_name}.csv"
    lines = ["Seconds,value"] + [f"{s:.6f},{i}" for i, s in enumerate(seconds)]
    path.write_text("\n".join(lines) + "\n")


def test_violations_found_on_disk_in_file_order(tmp_path):
    """Out-of-order rows on disk are read in file order and reported, without a warning."""
    epoch = tmp_path / "2024-01-15T09-00-00"
    t0 = to_seconds(pd.Timestamp("2024-01-15T09:00:00", tz="UTC"))
    t1 = to_seconds(pd.Timestamp("2024-01-15T10:00:00", tz="UTC"))
    # Second chunk written first on purpose; discovery must sort by chunk time.
    write_csv_chunk(epoch, "Dev", "2024-01-15T10-00-00", [t1, t1 + 1])
    write_csv_chunk(epoch, "Dev", "2024-01-15T09-00-00", [t0, t0 + 2, t0 + 1, t0 + 1])
    reader = Csv("Dev_State_*", columns=["value"])

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = timestamp_order(tmp_path, reader, start=_START)
    assert list(result["kind"]) == ["backwards", "duplicate"]
    assert list(result["index_in_stream"]) == [2, 3]
    assert result.attrs["n_samples"] == 6


def test_no_files(tmp_path):
    """No matching files gives an empty result with data_found False."""
    result = timestamp_order(tmp_path, Heartbeat("Nothing_8_*"), start=_START)
    assert result.empty
    assert result.attrs["data_found"] is False
    assert result.attrs["n_samples"] == 0
