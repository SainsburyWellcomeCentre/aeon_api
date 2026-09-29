"""Unit tests for harp_gaps on a continuous-rate Harp stream (wheel encoder at 500 Hz)."""

import datetime

import pandas as pd
import pytest

from swc.aeon.io.reader import Encoder
from swc.aeon.qc.harp import harp_gaps

_PATCH = "swc.aeon.qc.harp.load"
_START = pd.Timestamp("2024-01-01T10:00:00", tz="UTC")
_INTERVAL = pd.Timedelta(milliseconds=2)


@pytest.fixture
def encoder_reader():
    """Return an Encoder reader tagged with the 500 Hz expected rate, as run_qc does."""
    reader = Encoder("Patch1_90_*")
    reader.expected_hz = 500.0
    return reader


def make_encoder(timestamps: list[str]) -> pd.DataFrame:
    """Build a synthetic encoder DataFrame from ISO timestamp strings."""
    idx = pd.DatetimeIndex(timestamps, tz=datetime.UTC, name="time")
    return pd.DataFrame(
        {"angle": [0.0] * len(idx), "intensity": [2850.0] * len(idx)},
        index=idx,
    )


def test_harp_gaps_flags_gap_above_threshold(monkeypatch, encoder_reader):
    """A 20 ms gap is flagged when the threshold is the expected 2 ms interval."""
    timestamps = [
        "2024-01-01T10:00:00.000",
        "2024-01-01T10:00:00.002",
        "2024-01-01T10:00:00.004",
        "2024-01-01T10:00:00.006",
        "2024-01-01T10:00:00.026",  # 20 ms gap here
        "2024-01-01T10:00:00.028",
    ]
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder(timestamps))

    result = harp_gaps("/fake/root", encoder_reader, _START, threshold=_INTERVAL)

    assert len(result) == 1
    assert result.index.name == "time"
    assert result.index.tz == datetime.UTC
    assert list(result.columns) == ["duration", "n_missed", "device"]
    assert result["duration"].iloc[0] == pd.Timedelta(milliseconds=20)
    assert result["n_missed"].iloc[0] == 10
    assert result.attrs["expected_hz"] == 500.0
    assert result.attrs["n_samples"] == 16


def test_harp_gaps_default_threshold_ignores_short_gaps(monkeypatch, encoder_reader):
    """With the default 1 s threshold a 20 ms gap is not reported."""
    timestamps = ["2024-01-01T10:00:00.000", "2024-01-01T10:00:00.020"]
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder(timestamps))

    result = harp_gaps("/fake/root", encoder_reader, _START)

    assert result.empty
    assert result.attrs["data_found"] is True


def test_harp_gaps_empty_data(monkeypatch, encoder_reader):
    """No data gives an empty frame with the documented columns and data_found False."""
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder([]))

    result = harp_gaps("/fake/root", encoder_reader, _START)

    assert result.empty
    assert list(result.columns) == ["duration", "n_missed", "device"]
    assert result.index.name == "time"
    assert result.index.tz == datetime.UTC
    assert result.attrs["data_found"] is False


def test_harp_gaps_start_end_forwarded(monkeypatch, encoder_reader):
    """Forward start and end arguments to load()."""
    captured = {}

    def fake_load(root, reader, start=None, end=None):
        captured["start"] = start
        captured["end"] = end
        return make_encoder([])

    monkeypatch.setattr(_PATCH, fake_load)

    t1 = pd.Timestamp("2024-01-01T18:00:00", tz="UTC")
    harp_gaps("/fake/root", encoder_reader, start=_START, end=t1)

    assert captured["start"] == _START
    assert captured["end"] == t1


def test_harp_gaps_rounds_missed_samples(monkeypatch, encoder_reader):
    """A 5 ms gap at 2 ms spacing rounds to 2 missed samples."""
    timestamps = [
        "2024-01-01T10:00:00.000",
        "2024-01-01T10:00:00.002",
        "2024-01-01T10:00:00.007",
        "2024-01-01T10:00:00.009",
    ]
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder(timestamps))

    result = harp_gaps("/fake/root", encoder_reader, _START, threshold=_INTERVAL)

    assert len(result) == 1
    assert result["duration"].iloc[0] == pd.Timedelta(milliseconds=5)
    assert result["n_missed"].iloc[0] == 2
