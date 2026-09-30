"""Tests for the sync_delta function."""

import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from qc.helpers import make_heartbeat, make_heartbeat_offset
from swc.aeon.io.reader import Heartbeat
from swc.aeon.qc.sync import sync_delta

_PATCH = "swc.aeon.qc.sync.load"
_ROOT = "/fake/root"
_START = pd.Timestamp("2024-01-15T09:00:00", tz="UTC")

_TS3 = [
    "2024-01-15T09:00:00",
    "2024-01-15T09:00:01",
    "2024-01-15T09:00:02",
]

_READERS2 = {
    "ClockSynchronizer": Heartbeat("CS_8_*"),
    "Patch1": Heartbeat("P1_8_*"),
}


def test_sync_delta_empty_data():
    """All readers empty gives an empty DataFrame with the documented schema."""
    empty = pd.DataFrame()
    with patch(_PATCH, return_value=empty):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert result.empty
    assert list(result.columns) == ["second", "device", "delta_seconds"]
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC


def test_sync_delta_single_reader_returns_empty():
    """Test that sync_delta returns empty when only one device has data (no comparison possible)."""
    ref_df = make_heartbeat(_TS3)
    empty = pd.DataFrame()
    with patch(_PATCH, side_effect=[ref_df, empty]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert result.empty


def test_sync_delta_perfect_sync():
    """Test that identical timestamps across devices produce delta_seconds == 0.0 for all rows."""
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat(_TS3)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert not result.empty
    assert result["delta_seconds"].values == pytest.approx([0.0, 0.0, 0.0])


def test_sync_delta_constant_offset():
    """Test that a device with timestamps +50 ms ahead reports delta_seconds ≈ +0.050 for all rows."""
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat_offset(_TS3, offset_ms=50.0)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert result["delta_seconds"].values == pytest.approx([0.050, 0.050, 0.050])


def test_sync_delta_negative_offset():
    """Test that a device with timestamps -30 ms behind reports delta_seconds ≈ -0.030 for all rows."""
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat_offset(_TS3, offset_ms=-30.0)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert result["delta_seconds"].values == pytest.approx([-0.030, -0.030, -0.030])


def test_sync_delta_reference_auto_selects_clock_synchronizer():
    """Test that ClockSynchronizer is automatically chosen as reference when present."""
    cs_df = make_heartbeat(_TS3)
    patch1_df = make_heartbeat_offset(_TS3, offset_ms=20.0)
    readers = {"Patch1": Heartbeat("P1_8_*"), "ClockSynchronizer": Heartbeat("CS_8_*")}
    with patch(_PATCH, side_effect=[patch1_df, cs_df]):
        result = sync_delta(_ROOT, readers, _START)
    # Patch1 is the non-reference device; Patch1 is +20ms ahead of CS = delta = +0.020
    assert list(result["device"].unique()) == ["Patch1"]
    assert result["delta_seconds"].values == pytest.approx([0.020, 0.020, 0.020])


def test_sync_delta_reference_falls_back_to_first_key():
    """Test that when ClockSynchronizer is absent the first dict key becomes the reference."""
    readers = {"Patch1": Heartbeat("P1_8_*"), "Patch2": Heartbeat("P2_8_*")}
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat_offset(_TS3, offset_ms=10.0)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, readers, _START)
    # Patch1 is reference (first key), Patch2 is non-reference
    assert list(result["device"].unique()) == ["Patch2"]
    assert result["delta_seconds"].values == pytest.approx([0.010, 0.010, 0.010])


def test_sync_delta_explicit_reference():
    """Test that passing reference='Patch1' overrides automatic ClockSynchronizer selection."""
    readers = {"ClockSynchronizer": Heartbeat("CS_8_*"), "Patch1": Heartbeat("P1_8_*")}
    cs_df = make_heartbeat(_TS3)
    patch1_df = make_heartbeat_offset(_TS3, offset_ms=5.0)
    with patch(_PATCH, side_effect=[cs_df, patch1_df]):
        result = sync_delta(_ROOT, readers, _START, reference="Patch1")
    # Patch1 is reference, so only ClockSynchronizer appears in output
    assert list(result["device"].unique()) == ["ClockSynchronizer"]
    # CS timestamps are 5 ms behind Patch1 = delta should be negative
    assert result["delta_seconds"].values == pytest.approx([-0.005, -0.005, -0.005])


def test_sync_delta_reference_not_in_output():
    """Test that the reference device is absent from the device column in the output."""
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat(_TS3)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert "ClockSynchronizer" not in result["device"].values


def test_sync_delta_output_schema():
    """Test that the output DataFrame has the correct columns, index name, and UTC timezone."""
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat(_TS3)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert list(result.columns) == ["second", "device", "delta_seconds"]
    assert result.index.name == "time"
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz == datetime.UTC


def test_sync_delta_partial_overlap():
    """Test that only seconds present in both devices appear in the output (inner join)."""
    ts_ref = [f"2024-01-15T09:{i // 60:02d}:{i % 60:02d}" for i in range(100)]
    ts_dev = [f"2024-01-15T09:{(50 + i) // 60:02d}:{(50 + i) % 60:02d}" for i in range(100)]
    ref_df = make_heartbeat(ts_ref)  # seconds 0 to 99
    dev_df = make_heartbeat(ts_dev)  # seconds 100 to 199 (but same second values 0 to 99 after range())
    # Manually set second values so ref has 0-99 and dev has 50-149
    ref_df["second"] = range(100)
    dev_df["second"] = range(50, 150)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    # Only seconds 50 to 99 (50 matches) should appear
    assert len(result) == 50
    assert int(result["second"].min()) == 50
    assert int(result["second"].max()) == 99


def test_sync_delta_three_devices():
    """Test that with 3 devices the output contains exactly 2 non-reference device names."""
    readers = {
        "ClockSynchronizer": Heartbeat("CS_8_*"),
        "Patch1": Heartbeat("P1_8_*"),
        "Patch2": Heartbeat("P2_8_*"),
    }
    cs_df = make_heartbeat(_TS3)
    p1_df = make_heartbeat_offset(_TS3, offset_ms=10.0)
    p2_df = make_heartbeat_offset(_TS3, offset_ms=20.0)
    with patch(_PATCH, side_effect=[cs_df, p1_df, p2_df]):
        result = sync_delta(_ROOT, readers, _START)
    assert set(result["device"].unique()) == {"Patch1", "Patch2"}
    assert len(result) == 6  # 3 seconds × 2 devices


def test_sync_delta_varies_over_time():
    """Test that non-constant drift is captured correctly on a per-second basis."""
    ref_df = make_heartbeat(_TS3)
    # Device has increasing offsets: +10ms, +20ms, +30ms
    dev_df = make_heartbeat(_TS3)
    dev_df.index = pd.DatetimeIndex(
        [
            pd.Timestamp("2024-01-15T09:00:00.010", tz=datetime.UTC),
            pd.Timestamp("2024-01-15T09:00:01.020", tz=datetime.UTC),
            pd.Timestamp("2024-01-15T09:00:02.030", tz=datetime.UTC),
        ],
        name="time",
    )
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert result["delta_seconds"].values == pytest.approx([0.010, 0.020, 0.030])


def test_sync_delta_second_cast_to_int():
    """Test that float second values are correctly cast to int for alignment."""
    ref_df = make_heartbeat(_TS3)
    dev_df = make_heartbeat_offset(_TS3, offset_ms=5.0)
    # Convert second column to float (as it may appear in real data)
    ref_df["second"] = ref_df["second"].astype(float)
    dev_df["second"] = dev_df["second"].astype(float)
    with patch(_PATCH, side_effect=[ref_df, dev_df]):
        result = sync_delta(_ROOT, _READERS2, _START)
    assert len(result) == 3
    assert result["delta_seconds"].values == pytest.approx([0.005, 0.005, 0.005])
