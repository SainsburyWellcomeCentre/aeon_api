"""High-level run_qc orchestration and YAML report generation."""

import datetime
import pickle
from collections.abc import Iterator
from os import PathLike
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from swc.aeon.io.api import Reader
from swc.aeon.io.reader import Encoder, Heartbeat, Video

from aeon_qc.encoder import encoder_gaps
from aeon_qc.environment import environment_state_durations, harp_sync_alerts, message_log_errors
from aeon_qc.epochs import epoch_gaps
from aeon_qc.heartbeat import heartbeat_gaps
from aeon_qc.pellet import pellet_failures
from aeon_qc.sync import MIN_DEVICES, sync_delta
from aeon_qc.video import dropped_frames



def iter_readers(schema: Any) -> Iterator[tuple[str, Reader]]:
    """Yield (qualified_name, reader) pairs from a schema DotMap."""
    for device_name, streams in schema.items():
        if isinstance(streams, Reader):
            yield (device_name, streams)
        elif isinstance(streams, dict):
            for stream_name, reader in streams.items():
                if isinstance(reader, Reader):
                    yield (f"{device_name}.{stream_name}", reader)


def run_qc(
    root: str | PathLike | list[str] | list[PathLike],
    schema: Any,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> dict[str, pd.DataFrame]:
    """Run all applicable QC checks against every stream in a schema DotMap."""
    results: dict[str, pd.DataFrame] = {"epoch_gaps": epoch_gaps(root, start=start, end=end)}
    heartbeat_readers: dict[str, Heartbeat] = {}
    for qualified_name, reader in iter_readers(schema):
        if isinstance(reader, Heartbeat):
            results[qualified_name] = heartbeat_gaps(
                root, reader, start=start, end=end
            )
            heartbeat_readers[qualified_name] = reader
        elif isinstance(reader, Video):
            results[qualified_name] = dropped_frames(root, reader, start=start, end=end)
        elif isinstance(reader, Encoder):
            results[qualified_name] = encoder_gaps(root, reader, start=start, end=end)
    if len(heartbeat_readers) >= MIN_DEVICES:
        results["sync_delta"] = sync_delta(root, heartbeat_readers, start=start, end=end)

    # Pellet QC: group readers by device name, run pellet_failures for feeder devices
    device_streams: dict[str, dict[str, Reader]] = {}
    for qualified_name, reader in iter_readers(schema):
        device, _, stream = qualified_name.partition(".")
        device_streams.setdefault(device, {})[stream or device] = reader

    for device_name, streams in device_streams.items():
        if "DeliverPellet" in streams:
            results[f"{device_name}.pellet_stats"] = pellet_failures(
                root,
                deliver_reader=streams["DeliverPellet"],
                missed_reader=streams.get("MissedPellet"),
                retried_reader=streams.get("RetriedDelivery"),
                start=start,
                end=end,
            )
        if "MessageLog" in streams:
            results[f"{device_name}.message_log"] = message_log_errors(
                root, streams["MessageLog"], start=start, end=end
            )
            results[f"{device_name}.harp_sync_alerts"] = harp_sync_alerts(
                root, streams["MessageLog"], start=start, end=end
            )
        if "EnvironmentState" in streams:
            results[f"{device_name}.environment_state"] = environment_state_durations(
                root, streams["EnvironmentState"], start=start, end=end
            )

    return results


def generate_report(
    root: str | PathLike | list[str] | list[PathLike],
    results: dict[str, pd.DataFrame],
    output_path: str | PathLike,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> Path:
    """Write a human-readable YAML QC summary from QC metric DataFrames."""
    output_path = Path(output_path)
    root_list = root if isinstance(root, list) else [root]

    report: dict[str, Any] = {
        "generated_at": datetime.datetime.now(tz=datetime.UTC).isoformat(),
        "dataset_root": str(root_list[0]) if len(root_list) == 1 else [str(p) for p in root_list],
        "time_range": {
            "start": start.isoformat() if start is not None else None,
            "end": end.isoformat() if end is not None else None,
        },
        "devices": {},
    }

    for device_name, df in results.items():
        if "gap_duration" in df.columns:
            report["devices"][device_name] = epoch_gaps_section(df)
        elif "n_dropped" in df.columns:
            report["devices"][device_name] = video_section(df)
        elif "n_missed" in df.columns:
            report["devices"][device_name] = encoder_section(df)
        elif "duration" in df.columns and "state" in df.columns:
            report["devices"][device_name] = environment_state_section(df)
        elif "duration" in df.columns:
            report["devices"][device_name] = heartbeat_section(df)
        elif "outcome" in df.columns:
            report["devices"][device_name] = pellet_section(df)
        elif "max_difference" in df.columns:
            report["devices"][device_name] = harp_sync_alerts_section(df)
        elif "priority" in df.columns:
            report["devices"][device_name] = message_log_section(df)
        elif "delta_seconds" in df.columns:
            report["devices"][device_name] = sync_delta_section(df)

    missing = [name for name, df in results.items() if not df.attrs.get("data_found", True)]
    if missing:
        report["missing_devices"] = sorted(missing)

    with open(output_path, "w") as f:
        yaml.dump(report, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    return output_path


def save_results(results: dict[str, pd.DataFrame], output_path: str | PathLike) -> Path:
    """Pickle a run_qc results dict to disk for later analysis."""
    output_path = Path(output_path)
    with open(output_path, "wb") as f:
        pickle.dump(results, f)
    return output_path


def heartbeat_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for a heartbeat_gaps result."""
    data_found = df.attrs.get("data_found", True)
    n_heartbeats = df.attrs.get("n_heartbeats", 0)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_heartbeats": n_heartbeats,
            "n_gaps": 0,
            "total_dropout_seconds": 0.0,
            "mean_duration_seconds": None,
        }
    else:
        summary = {
            "data_found": data_found,
            "n_heartbeats": n_heartbeats,
            "n_gaps": len(df),
            "total_dropout_seconds": float(df["duration"].dt.total_seconds().sum()),
            "mean_duration_seconds": float(df["duration"].dt.total_seconds().mean()),
        }
    return {"metric": "heartbeat_gaps", "summary": summary}


def video_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for a dropped_frames result."""
    data_found = df.attrs.get("data_found", True)
    n_frames = df.attrs.get("n_frames", 0)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_frames": n_frames,
            "n_drop_events": 0,
            "total_frames_dropped": 0,
            "mean_duration_seconds": None,
        }
        detail: list[dict[str, Any]] = []
    else:
        summary = {
            "data_found": data_found,
            "n_frames": n_frames,
            "n_drop_events": len(df),
            "total_frames_dropped": int(df["n_dropped"].sum()),
            "mean_duration_seconds": float(df["duration"].dt.total_seconds().mean()),
        }
        detail = [
            {
                "time": row.Index.isoformat(),
                "n_dropped": int(row.n_dropped),
                "hw_counter_before": int(row.hw_counter_before),
                "hw_counter_after": int(row.hw_counter_after),
            }
            for row in df.itertuples()
        ]
    return {"metric": "dropped_frames", "summary": summary, "detail": detail}


def sync_delta_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for a sync_delta result."""
    if df.empty:
        summary: dict[str, Any] = {
            "n_devices": 0,
            "max_abs_delta_seconds": None,
            "mean_abs_delta_seconds": None,
            "std_delta_seconds": None,
            "worst_device": None,
        }
        detail: list[dict[str, Any]] = []
    else:
        abs_delta = df["delta_seconds"].abs()
        by_device = df.groupby("device")["delta_seconds"]
        worst_device = by_device.apply(lambda s: s.abs().max()).idxmax()
        summary = {
            "n_devices": int(df["device"].nunique()),
            "max_abs_delta_seconds": float(abs_delta.max()),
            "mean_abs_delta_seconds": float(abs_delta.mean()),
            "std_delta_seconds": float(df["delta_seconds"].std()),
            "worst_device": worst_device,
        }
        detail = [
            {
                "device": device,
                "max_delta_seconds": float(grp["delta_seconds"].abs().max()),
                "mean_delta_seconds": float(grp["delta_seconds"].abs().mean()),
                "std_delta_seconds": float(grp["delta_seconds"].std()),
            }
            for device, grp in df.groupby("device")
        ]
    return {"metric": "sync_delta", "summary": summary, "detail": detail}


def epoch_gaps_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for an epoch_gaps result."""
    data_found = df.attrs.get("data_found", True)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_epochs": 0,
            "total_gap_seconds": 0.0,
            "max_gap_seconds": None,
            "min_gap_seconds": None,
        }
        detail: list[dict[str, Any]] = []
    else:
        durations = df["gap_duration"].dropna().dt.total_seconds()
        summary = {
            "data_found": data_found,
            "n_epochs": len(df),
            "total_gap_seconds": float(durations.sum()),
            "max_gap_seconds": float(durations.max()) if len(durations) else None,
            "min_gap_seconds": float(durations.min()) if len(durations) else None,
        }
        detail = [
            {
                "epoch_start": row.Index.isoformat(),
                "gap_duration_seconds": float(row.gap_duration.total_seconds())
                if pd.notna(row.gap_duration)
                else None,
            }
            for row in df.itertuples()
        ]
    return {"metric": "epoch_gaps", "summary": summary, "detail": detail}


def encoder_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for an encoder_gaps result."""
    data_found = df.attrs.get("data_found", True)
    n_samples = df.attrs.get("n_samples", 0)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_samples": n_samples,
            "n_gap_events": 0,
            "total_missed_samples": 0,
            "mean_duration_ms": None,
        }
        detail: list[dict[str, Any]] = []
    else:
        summary = {
            "data_found": data_found,
            "n_samples": n_samples,
            "n_gap_events": len(df),
            "total_missed_samples": int(df["n_missed"].sum()),
            "mean_duration_ms": float(df["duration"].dt.total_seconds().mean() * 1000),
        }
        detail = [
            {
                "duration_ms": float(row.duration.total_seconds() * 1000),
                "n_missed": int(row.n_missed),
            }
            for row in df.itertuples()
        ]
    return {"metric": "encoder_gaps", "summary": summary, "detail": detail}


def pellet_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for a pellet_failures result."""
    data_found = df.attrs.get("data_found", True)
    n_deliveries = df.attrs.get("n_deliveries", 0)
    n_retried = df.attrs.get("n_retried", 0)
    n_missed = df.attrs.get("n_missed", 0)
    summary: dict[str, Any] = {
        "data_found": data_found,
        "n_deliveries": n_deliveries,
        "n_retried": n_retried,
        "n_missed": n_missed,
    }
    detail: list[dict[str, Any]] = [
        {"time": row.Index.isoformat(), "outcome": row.outcome}
        for row in df.itertuples()
    ]
    return {"metric": "pellet_failures", "summary": summary, "detail": detail}


def harp_sync_alerts_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for a harp_sync_alerts result."""
    data_found = df.attrs.get("data_found", True)
    n_total = df.attrs.get("n_total_messages", 0)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_total_messages": n_total,
            "n_alerts": 0,
            "max_max_difference": None,
            "min_device_count": None,
        }
        detail: list[dict[str, Any]] = []
    else:
        summary = {
            "data_found": data_found,
            "n_total_messages": n_total,
            "n_alerts": len(df),
            "max_max_difference": float(df["max_difference"].max()),
            "min_device_count": int(df["device_count"].min()),
        }
        detail = [
            {
                "time": row.Index.isoformat(),
                "device_count": int(row.device_count),
                "expected_device_count": int(row.expected_device_count),
                "max_difference": float(row.max_difference),
            }
            for row in df.itertuples()
        ]
    return {"metric": "harp_sync_alerts", "summary": summary, "detail": detail}


def message_log_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for a message_log_errors result."""
    data_found = df.attrs.get("data_found", True)
    n_total = df.attrs.get("n_total", 0)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_total": n_total,
            "n_warnings": 0,
            "n_errors": 0,
        }
        detail: list[dict[str, Any]] = []
    else:
        counts = df["priority"].str.lower().value_counts()
        summary = {
            "data_found": data_found,
            "n_total": n_total,
            "n_warnings": int(counts.get("warning", 0)),
            "n_errors": int(counts.get("error", 0)),
        }
        detail = [
            {
                "time": row.Index.isoformat(),
                "priority": row.priority,
                "type": row.type,
                "message": row.message,
            }
            for row in df.itertuples()
        ]
    return {"metric": "message_log_errors", "summary": summary, "detail": detail}


def environment_state_section(df: pd.DataFrame) -> dict[str, Any]:
    """Build the YAML section for an environment_state_durations result."""
    data_found = df.attrs.get("data_found", True)
    if df.empty:
        summary: dict[str, Any] = {
            "data_found": data_found,
            "n_transitions": 0,
            "state_totals_seconds": {},
        }
        detail: list[dict[str, Any]] = []
    else:
        totals = (
            df.groupby("state")["duration"]
            .sum()
            .dt.total_seconds()
            .round(1)
            .to_dict()
        )
        summary = {
            "data_found": data_found,
            "n_transitions": len(df),
            "state_totals_seconds": {k: float(v) for k, v in totals.items()},
        }
        detail = [
            {
                "time": row.Index.isoformat(),
                "state": row.state,
                "duration_seconds": float(row.duration.total_seconds()),
            }
            for row in df.itertuples()
        ]
    return {"metric": "environment_state", "summary": summary, "detail": detail}
