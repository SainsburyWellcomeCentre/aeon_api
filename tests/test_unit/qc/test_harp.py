"""Unit tests for harp_gaps on a continuous-rate Harp stream (wheel encoder at 500 Hz)."""

import datetime

import pandas as pd
import pytest

from swc.aeon.io.api import to_datetime
from swc.aeon.io.reader import Encoder
from swc.aeon.qc.harp import harp_gaps

_PATCH = "swc.aeon.qc.harp.load"
_START = pd.Timestamp("2024-01-01T10:00:00", tz="UTC")


@pytest.fixture
def encoder_reader():
    """Return an Encoder reader tagged with the 500 Hz expected rate, as run_qc does."""
    reader = Encoder("Patch1_90_*")
    reader.expected_hz = 500.0  # pyright: ignore[reportAttributeAccessIssue]
    return reader


def make_encoder(timestamps: list[str]) -> pd.DataFrame:
    """Build a synthetic encoder DataFrame from ISO timestamp strings (microsecond index)."""
    idx = pd.DatetimeIndex(timestamps, tz=datetime.UTC, name="time")
    return pd.DataFrame(
        {"angle": [0.0] * len(idx), "intensity": [2850.0] * len(idx)},
        index=idx,
    )


def make_encoder_from_seconds(seconds: list[float]) -> pd.DataFrame:
    """Build an encoder DataFrame indexed as load() indexes Harp data (nanoseconds)."""
    idx = pd.DatetimeIndex(to_datetime(pd.Index(seconds)), name="time")
    return pd.DataFrame({"angle": 0.0, "intensity": 2850.0}, index=idx)


_GAP_20MS = [
    "2024-01-01T10:00:00.000",
    "2024-01-01T10:00:00.002",
    "2024-01-01T10:00:00.004",
    "2024-01-01T10:00:00.006",
    "2024-01-01T10:00:00.026",  # 20 ms interval: 9 samples missing
    "2024-01-01T10:00:00.028",
]


def test_harp_gaps_counts_every_missing_sample(monkeypatch, encoder_reader):
    """Any interval longer than 1.5 expected intervals is a gap; n_missed is exact."""
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder(_GAP_20MS))

    result = harp_gaps("/fake/root", encoder_reader, _START)

    assert len(result) == 1
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC
    assert list(result.columns) == ["kind", "duration", "n_intervals", "n_missed", "device"]
    assert result["kind"].iloc[0] == "gap"
    assert result["duration"].iloc[0] == pd.Timedelta(milliseconds=20)
    assert result["n_missed"].iloc[0] == 9
    assert result.index[0] == pd.Timestamp("2024-01-01T10:00:00.006", tz="UTC")
    assert result.attrs["expected_hz"] == 500.0
    assert result.attrs["n_samples"] == 6
    assert result.attrs["n_missed_total"] == 9
    assert result.attrs["interval_ratio_max"] == pytest.approx(10.0)
    assert result.attrs["longest"][0]["interval_ms"] == pytest.approx(20.0)


@pytest.mark.parametrize(
    ("interval_ms", "expected_missed"),
    [(2.9, 0), (3.1, 1), (4.8, 1), (5.2, 2), (6.0, 2)],
    ids=["jitter-below-1.5x", "just-over-1.5x", "2.4x", "2.6x", "3x"],
)
def test_harp_gaps_rounds_to_whole_samples(monkeypatch, encoder_reader, interval_ms, expected_missed):
    """An interval counts as round(interval / expected) - 1 missing samples."""
    t0 = 3_786_861_600.0
    seconds = [t0, t0 + 0.002, t0 + 0.002 + interval_ms / 1000]
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder_from_seconds(seconds))

    result = harp_gaps("/fake/root", encoder_reader, _START)

    assert result.attrs["n_missed_total"] == expected_missed
    assert result.attrs["n_gap_events"] == (1 if expected_missed else 0)


def test_harp_gaps_measures_intervals_without_gaps(monkeypatch, encoder_reader):
    """A clean stream reports zero gaps together with its interval distribution."""
    t0 = 3_786_861_600.0
    seconds = [t0 + i * 0.002 + (0.00003 if i % 2 else 0.0) for i in range(100)]
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder_from_seconds(seconds))

    result = harp_gaps("/fake/root", encoder_reader, _START)

    assert result.empty
    assert result.attrs["data_found"] is True
    assert result.attrs["n_samples"] == 100
    assert result.attrs["interval_ratio_min"] == pytest.approx(0.985, abs=1e-3)
    assert result.attrs["interval_ratio_max"] == pytest.approx(1.015, abs=1e-3)
    assert len(result.attrs["longest"]) == 10


def test_harp_gaps_optional_threshold_reports_only_longer_gaps(monkeypatch, encoder_reader):
    """With a threshold, only gaps longer than it are rows; the measures are unchanged."""
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder(_GAP_20MS))

    result = harp_gaps("/fake/root", encoder_reader, _START, threshold=pd.Timedelta(seconds=1))

    assert result.empty
    assert result.attrs["interval_ratio_max"] == pytest.approx(10.0)


def test_harp_gaps_empty_data(monkeypatch, encoder_reader):
    """No data gives an empty frame with the documented columns and data_found False."""
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder([]))

    result = harp_gaps("/fake/root", encoder_reader, _START)

    assert result.empty
    assert list(result.columns) == ["kind", "duration", "n_intervals", "n_missed", "device"]
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC
    assert result.attrs["data_found"] is False
    assert result.attrs["interval_ratio_max"] is None


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


@pytest.mark.parametrize(
    ("intervals_ms", "kind", "missed"),
    [
        ([1.504, 1.504], "gap", 1),
        ([1.504, 0.496], "irregular", 0),
        ([0.5, 0.5], "extra", 0),
        ([2.0], "gap", 1),
    ],
    ids=["two-late-intervals-one-lost", "late-sample", "extra-sample", "one-lost"],
)
def test_harp_gaps_counts_missing_samples_per_run(monkeypatch, intervals_ms, kind, missed):
    """Missing samples are counted over a run of irregular intervals, not per interval."""
    reader = Encoder("Photodiode_44_*")
    reader.expected_hz = 1000.0  # pyright: ignore[reportAttributeAccessIssue]
    t0 = 3_786_861_600.0
    seconds = [t0 + i * 0.001 for i in range(5)]
    for step in intervals_ms:
        seconds.append(seconds[-1] + step / 1000)
    seconds += [seconds[-1] + (i + 1) * 0.001 for i in range(5)]
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_encoder_from_seconds(seconds))

    result = harp_gaps("/fake/root", reader, _START)

    assert list(result["kind"]) == [kind]
    assert result["n_intervals"].iloc[0] == len(intervals_ms)
    assert result.attrs["n_missed_total"] == missed
