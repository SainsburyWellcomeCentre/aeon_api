"""Unit tests for pellet_failures."""

import datetime

import pandas as pd

from swc.aeon.io.reader import BitmaskEvent, Harp
from swc.aeon.qc.pellet import pellet_failures

_PATCH = "swc.aeon.qc.pellet.load"
_DELIVER_READER = BitmaskEvent("Patch1_35_*", 0x80, "TriggerPellet")
_MISSED_READER = Harp("Patch1_202_*", columns=["missed_pellet"])
_RETRIED_READER = Harp("Patch1_203_*", columns=["retried_delivery"])
_START = pd.Timestamp("2024-01-01T09:00:00", tz="UTC")


def make_events(timestamps: list[str], col: str = "value") -> pd.DataFrame:
    """Build a synthetic event DataFrame from ISO timestamp strings."""
    idx = pd.DatetimeIndex(timestamps, tz=datetime.UTC, name="time")
    return pd.DataFrame({col: [True] * len(idx)}, index=idx)


def test_no_failures_without_failure_readers(monkeypatch):
    """Deliveries alone, with no missed or retried readers, give an empty result."""
    deliver = make_events(["2024-01-01T10:00:00", "2024-01-01T10:00:05"], "TriggerPellet")
    monkeypatch.setattr(_PATCH, lambda *a, **kw: deliver)

    result = pellet_failures("/fake/root", _DELIVER_READER, _START)

    assert result.empty
    assert list(result.columns) == ["outcome", "device"]
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC
    assert result.attrs["n_deliveries"] == 2
    assert result.attrs["data_found"] is True


def test_with_missed_reader(monkeypatch):
    """MissedPellet events appear as 'missed' rows."""
    deliver = make_events(["2024-01-01T10:00:00"], "TriggerPellet")
    missed = make_events(["2024-01-01T10:00:10"], "missed_pellet")
    loads = iter([deliver, missed])
    monkeypatch.setattr(_PATCH, lambda *a, **kw: next(loads))

    result = pellet_failures("/fake/root", _DELIVER_READER, _START, missed_reader=_MISSED_READER)

    assert len(result) == 1
    assert result["outcome"].iloc[0] == "missed"
    assert result.attrs["n_missed"] == 1
    assert result.attrs["n_retried"] == 0


def test_with_retried_reader(monkeypatch):
    """RetriedDelivery events appear as 'retried' rows."""
    deliver = make_events(["2024-01-01T10:00:00"], "TriggerPellet")
    retried = make_events(["2024-01-01T10:00:15"], "retried_delivery")
    loads = iter([deliver, retried])
    monkeypatch.setattr(_PATCH, lambda *a, **kw: next(loads))

    result = pellet_failures("/fake/root", _DELIVER_READER, _START, retried_reader=_RETRIED_READER)

    assert len(result) == 1
    assert result["outcome"].iloc[0] == "retried"
    assert result.attrs["n_retried"] == 1


def test_missed_and_retried_sorted_by_time(monkeypatch):
    """Rows from both readers are merged and ordered by timestamp."""
    deliver = make_events(["2024-01-01T10:00:00"], "TriggerPellet")
    missed = make_events(["2024-01-01T10:00:20"], "missed_pellet")
    retried = make_events(["2024-01-01T10:00:10"], "retried_delivery")
    loads = iter([deliver, missed, retried])
    monkeypatch.setattr(_PATCH, lambda *a, **kw: next(loads))

    result = pellet_failures(
        "/fake/root",
        _DELIVER_READER,
        _START,
        missed_reader=_MISSED_READER,
        retried_reader=_RETRIED_READER,
    )

    assert list(result["outcome"]) == ["retried", "missed"]
    assert (result["device"] == _DELIVER_READER.pattern).all()


def test_empty_deliver_data(monkeypatch):
    """No delivery events give an empty result, zero n_deliveries and data_found False."""
    monkeypatch.setattr(_PATCH, lambda *a, **kw: make_events([], "TriggerPellet"))

    result = pellet_failures("/fake/root", _DELIVER_READER, _START)

    assert result.empty
    assert list(result.columns) == ["outcome", "device"]
    assert result.attrs["n_deliveries"] == 0
    assert result.attrs["data_found"] is False


def test_start_end_forwarded(monkeypatch):
    """Forward start and end arguments to every load() call."""
    captured = []

    def fake_load(root, reader, start=None, end=None):
        captured.append((start, end))
        return make_events([], "value")

    monkeypatch.setattr(_PATCH, fake_load)

    t1 = pd.Timestamp("2024-01-01T18:00:00", tz="UTC")
    pellet_failures(
        "/fake/root",
        _DELIVER_READER,
        start=_START,
        end=t1,
        missed_reader=_MISSED_READER,
        retried_reader=_RETRIED_READER,
    )

    assert len(captured) == 3  # deliver, missed, retried
    for start_arg, end_arg in captured:
        assert start_arg == _START
        assert end_arg == t1
