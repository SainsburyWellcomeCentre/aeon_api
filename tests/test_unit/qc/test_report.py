"""Tests for run_qc(), generate_report(), and iter_readers()."""

import datetime
from unittest.mock import patch

import pandas as pd
import pytest
import yaml
from dotmap import DotMap

import swc.aeon.schema.core as stream
from qc.helpers import make_heartbeat, make_video
from swc.aeon.io.reader import Heartbeat
from swc.aeon.qc.report import DETAIL_ROW_CAP, generate_report, iter_readers, run_qc
from swc.aeon.schema.streams import Device

_ROOT = "/fake/root"
_START = pd.Timestamp("2024-01-15T09:00:00", tz="UTC")
_END = pd.Timestamp("2024-01-15T10:00:00", tz="UTC")
_LOAD = "swc.aeon.qc.report.load"
_EMPTY_VIDEO = pd.DataFrame(columns=["hw_counter", "hw_timestamp", "_frame", "_path", "_epoch"])


def _empty_order() -> pd.DataFrame:
    df = pd.DataFrame(
        columns=["kind", "step_seconds", "index_in_stream", "device"],
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    )
    df.attrs["metric"] = "timestamp_order"
    return df


# --- iter_readers ---


def test_iter_readers_singleton():
    """A singleton device yields one (name, reader) pair."""
    schema = DotMap([Device("Patch1", stream.Heartbeat)])
    pairs = list(iter_readers(schema))
    assert len(pairs) == 1
    _name, reader = pairs[0]
    assert isinstance(reader, Heartbeat)


def test_iter_readers_multi_stream():
    """A multi-stream device yields qualified names."""
    schema = DotMap([Device("CameraTop", stream.Video, stream.Position)])
    pairs = dict(iter_readers(schema))
    assert any("CameraTop" in k for k in pairs)


def test_iter_readers_skips_non_readers():
    """Non-Reader values in the schema are skipped silently."""
    schema = DotMap({"not_a_reader": "just_a_string"})
    assert list(iter_readers(schema)) == []


# --- run_qc ---


def test_run_qc_loads_each_stream_once_and_checks_order():
    """Every Harp and CSV stream is loaded once, unsorted, and gets an .order result."""
    schema = DotMap([Device("Patch1", stream.Heartbeat), Device("CameraTop", stream.Video)])
    with patch(_LOAD, side_effect=[make_heartbeat([]), _EMPTY_VIDEO]) as load:
        results = run_qc(_ROOT, schema, _START, end=_END)
    assert load.call_count == 2
    _, kwargs = load.call_args
    assert kwargs["start"] == _START
    assert kwargs["end"] == _END
    assert {"Patch1.Heartbeat.order", "CameraTop.Video.order"} <= set(results)


def test_run_qc_discovers_heartbeat():
    """run_qc runs heartbeat_gaps and heartbeat_duplicates for a Heartbeat reader."""
    schema = DotMap([Device("Patch1", stream.Heartbeat)])
    with patch(_LOAD, return_value=make_heartbeat([])):
        results = run_qc(_ROOT, schema, _START)
    assert "Patch1.Heartbeat" in results
    assert "Patch1.Heartbeat.duplicates" in results


def test_run_qc_discovers_video():
    """run_qc runs dropped_frames and frame_rate_stability for a Video reader."""
    schema = DotMap([Device("CameraTop", stream.Video)])
    with patch(_LOAD, return_value=_EMPTY_VIDEO):
        results = run_qc(_ROOT, schema, _START)
    assert "CameraTop.Video" in results
    assert "CameraTop.frame_rate" in results


def test_run_qc_discovers_encoder():
    """run_qc runs harp_gaps for an Encoder reader, tagging it with 500 Hz."""
    schema = DotMap([Device("Patch1", stream.Encoder)])
    with patch(_LOAD, return_value=pd.DataFrame(columns=["angle", "intensity"])):
        results = run_qc(_ROOT, schema, _START)
    assert results["Patch1.Encoder"].attrs["expected_hz"] == 500.0


def test_run_qc_sorts_after_order_check():
    """An out-of-order stream is reported by the order check and sorted for the other metrics."""
    schema = DotMap([Device("CameraTop", stream.Video)])
    data = make_video(
        [0, 2, 1],
        ["2024-01-15T09:00:00.000", "2024-01-15T09:00:00.040", "2024-01-15T09:00:00.020"],
    )
    with patch(_LOAD, return_value=data):
        results = run_qc(_ROOT, schema, _START)
    order = results["CameraTop.Video.order"]
    assert list(order["kind"]) == ["backwards"]
    # After sorting the counters run 0, 1, 2 so dropped_frames sees no gap.
    assert results["CameraTop.Video"].empty
    assert results["CameraTop.Video"].attrs["n_frames"] == 3


def test_run_qc_produces_sync_delta_key():
    """Two or more Heartbeat readers produce a sync_delta result from the loaded frames."""
    schema = DotMap([Device("Patch1", stream.Heartbeat), Device("Patch2", stream.Heartbeat)])
    with (
        patch(_LOAD, return_value=make_heartbeat([])),
        patch("swc.aeon.qc.sync.load") as sync_load,
    ):
        results = run_qc(_ROOT, schema, _START)
    assert "sync_delta" in results
    sync_load.assert_not_called()


def test_run_qc_no_sync_delta_for_single_heartbeat():
    """A single Heartbeat reader produces no sync_delta result."""
    schema = DotMap([Device("Patch1", stream.Heartbeat)])
    with patch(_LOAD, return_value=make_heartbeat([])):
        results = run_qc(_ROOT, schema, _START)
    assert "sync_delta" not in results


# --- generate_report ---


def _make_hb_gaps() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2024-01-15T09:00:01+00:00"], name="time")
    return pd.DataFrame(
        {
            "duration": [pd.Timedelta(seconds=10)],
            "n_missed": [9],
            "second_before": [1],
            "second_after": [11],
            "device": ["Patch1_8_*"],
        },
        index=idx,
    )


def _make_vid_drops() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2024-01-15T09:00:00.020+00:00"], name="time")
    return pd.DataFrame(
        {
            "duration": [pd.Timedelta("20ms")],
            "n_dropped": [3],
            "hw_counter_before": [1],
            "hw_counter_after": [5],
            "device": ["CameraTop_*"],
        },
        index=idx,
    )


def _make_order(n: int) -> pd.DataFrame:
    idx = pd.date_range("2024-01-15T09:00:00", periods=n, freq="1s", tz="UTC", name="time")
    df = pd.DataFrame(
        {
            "kind": ["backwards"] * n,
            "step_seconds": [-0.001] * n,
            "index_in_stream": range(n),
            "device": ["Patch1_8_*"] * n,
        },
        index=idx,
    )
    df.attrs.update(
        {
            "metric": "timestamp_order",
            "data_found": True,
            "n_samples": 1000,
            "n_backwards": n,
            "n_duplicates": 0,
            "max_backwards_seconds": 0.001,
        }
    )
    return df


def _load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


def test_generate_report_creates_file(tmp_path):
    """generate_report writes a file at the requested path."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {}, output, _START)
    assert output.exists()


def test_generate_report_yaml_structure(tmp_path):
    """The report has the expected top-level keys and an open-ended time range."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {}, output, _START, end=None)
    data = _load_yaml(output)
    assert set(data) >= {"generated_at", "dataset_root", "time_range", "devices"}
    assert data["time_range"]["start"] == _START.isoformat()
    assert data["time_range"]["end"] is None


def test_generate_report_heartbeat_summary(tmp_path):
    """A heartbeat gaps result produces summary and detail sections."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {"Patch1.Heartbeat": _make_hb_gaps()}, output, _START)
    section = _load_yaml(output)["devices"]["Patch1.Heartbeat"]
    assert section["metric"] == "heartbeat_gaps"
    assert section["summary"]["n_gaps"] == 1
    assert section["summary"]["total_dropout_seconds"] == pytest.approx(10.0)
    assert len(section["detail"]) == 1


def test_generate_report_video_summary(tmp_path):
    """A dropped frames result produces summary and detail sections."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {"CameraTop.Video": _make_vid_drops()}, output, _START)
    section = _load_yaml(output)["devices"]["CameraTop.Video"]
    assert section["metric"] == "dropped_frames"
    assert section["summary"]["n_drop_events"] == 1
    assert section["summary"]["total_frames_dropped"] == 3
    assert len(section["detail"]) == 1


def test_generate_report_empty_heartbeat_zero_counts(tmp_path):
    """An empty heartbeat gaps frame produces zero counts."""
    output = tmp_path / "report.yaml"
    empty_hb = pd.DataFrame(
        columns=["duration", "n_missed", "second_before", "second_after", "device"],
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    )
    generate_report(_ROOT, {"Patch1.Heartbeat": empty_hb}, output, _START)
    assert _load_yaml(output)["devices"]["Patch1.Heartbeat"]["summary"]["n_gaps"] == 0


def test_generate_report_timestamp_order_section(tmp_path):
    """A timestamp_order result is keyed by its metric attr and its detail is capped."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {"Patch1.order": _make_order(DETAIL_ROW_CAP + 5)}, output, _START)
    section = _load_yaml(output)["devices"]["Patch1.order"]
    assert section["metric"] == "timestamp_order"
    assert section["summary"]["n_backwards"] == DETAIL_ROW_CAP + 5
    assert section["summary"]["n_samples"] == 1000
    assert section["summary"]["detail_truncated_to"] == DETAIL_ROW_CAP
    assert len(section["detail"]) == DETAIL_ROW_CAP
    assert section["detail"][0]["kind"] == "backwards"


def test_generate_report_timestamp_order_empty(tmp_path):
    """An empty timestamp_order result still gets a section with zero counts."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {"Patch1.order": _empty_order()}, output, _START)
    section = _load_yaml(output)["devices"]["Patch1.order"]
    assert section["summary"]["n_backwards"] == 0
    assert section["detail"] == []
    assert "detail_truncated_to" not in section["summary"]


# --- sync_delta ---


def _make_sync_delta_df() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2024-01-15T09:00:00+00:00", "2024-01-15T09:00:01+00:00"], name="time")
    return pd.DataFrame(
        {"second": [0, 1], "device": ["Patch1", "Patch1"], "delta_seconds": [0.010, 0.020]},
        index=idx,
    )


def test_generate_report_sync_delta_section(tmp_path):
    """A non-empty sync_delta frame produces the expected section."""
    output = tmp_path / "report.yaml"
    generate_report(_ROOT, {"sync_delta": _make_sync_delta_df()}, output, _START)
    section = _load_yaml(output)["devices"]["sync_delta"]
    assert section["metric"] == "sync_delta"
    assert section["summary"]["n_devices"] == 1
    assert section["summary"]["worst_device"] == "Patch1"
    assert section["detail"][0]["device"] == "Patch1"


def test_generate_report_sync_delta_empty(tmp_path):
    """An empty sync_delta frame produces n_devices == 0 and null statistics."""
    output = tmp_path / "report.yaml"
    empty_sync = pd.DataFrame(
        columns=["second", "device", "delta_seconds"],
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    )
    generate_report(_ROOT, {"sync_delta": empty_sync}, output, _START)
    summary = _load_yaml(output)["devices"]["sync_delta"]["summary"]
    assert summary["n_devices"] == 0
    assert summary["max_abs_delta_seconds"] is None
    assert summary["worst_device"] is None
