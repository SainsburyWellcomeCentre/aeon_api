"""High-level run_qc orchestration and YAML report generation."""

import datetime
from collections.abc import Iterator
from os import PathLike
from pathlib import Path
from typing import Any

import pandas as pd
import yaml
from swc.aeon.io.api import Reader
from swc.aeon.io.reader import Heartbeat, Video

from aeon_qc.heartbeat import heartbeat_gaps
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
    results: dict[str, pd.DataFrame] = {}
    heartbeat_readers: dict[str, Heartbeat] = {}
    for qualified_name, reader in iter_readers(schema):
        if isinstance(reader, Heartbeat):
            results[qualified_name] = heartbeat_gaps(
                root, reader, start=start, end=end
            )
            heartbeat_readers[qualified_name] = reader
        elif isinstance(reader, Video):
            results[qualified_name] = dropped_frames(root, reader, start=start, end=end)
    if len(heartbeat_readers) >= MIN_DEVICES:
        results["sync_delta"] = sync_delta(root, heartbeat_readers, start=start, end=end)
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
        if "n_dropped" in df.columns:
            report["devices"][device_name] = video_section(df)
        elif "duration" in df.columns:
            report["devices"][device_name] = heartbeat_section(df)
        elif "delta_seconds" in df.columns:
            report["devices"][device_name] = sync_delta_section(df)

    missing = [name for name, df in results.items() if not df.attrs.get("data_found", True)]
    if missing:
        report["missing_devices"] = sorted(missing)

    with open(output_path, "w") as f:
        yaml.dump(report, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

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
