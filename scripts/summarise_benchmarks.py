#!/usr/bin/env python3
"""Summarise every YAML report written by run_benchmarks.py into one table.

Walks ``<output>/<dataset>/<label>_<start>.yaml`` and writes ``summary.csv`` and
``summary.md`` with one row per epoch. A whole benchmark run can then be read at a glance
without opening individual reports.

Epochs are then judged against the thresholds in ``thresholds.yaml``: a value above its
threshold is listed in the ``flags`` column. Thresholds only flag. Every measured value
is kept, and changing a threshold needs only this script to run again, not QC.

Usage:
    uv run python scripts/summarise_benchmarks.py [options]

Options:
    --input DIR         Benchmark output root (default: benchmarks_output)
    --output DIR        Where to write summary.csv and summary.md (default: same as --input)
    --thresholds PATH   Thresholds file (default: thresholds.yaml, if present)
    --no-thresholds     Summarise without judging
    --strict            Exit with status 1 when any epoch is flagged
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

COLUMNS = [
    "dataset",
    "label",
    "start",
    "end",
    "hours",
    "epochs",
    "heartbeat_gaps",
    "heartbeat_dropout_s",
    "max_sync_delta_ms",
    "harp_sync_alerts",
    "dropped_frame_events",
    "frames_dropped",
    "harp_gap_events",
    "harp_missed_samples",
    "harp_irregular_runs",
    "harp_samples",
    "harp_max_interval_ratio",
    "order_backwards",
    "order_duplicates",
    "pellet_missed",
    "pellet_retried",
    "log_errors",
    "harpsync_offset_s",
    "harpsync_faults",
    "harpsync_worst_step_us",
    "fit_worst_chunk_ms",
    "onix_clock_ppm",
    "onix_clock_events",
    "hub_offset_max_dev_ticks",
    "missing_devices",
]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", default="benchmarks_output", help="Benchmark output root")
    parser.add_argument("--output", default=None, help="Directory for summary.csv and summary.md")
    parser.add_argument("--thresholds", default="thresholds.yaml", help="Thresholds file")
    parser.add_argument("--no-thresholds", action="store_true", help="Summarise without judging")
    parser.add_argument(
        "--strict", action="store_true", help="Exit with status 1 when any epoch is flagged"
    )
    return parser.parse_args()


def load_thresholds(path: Path) -> dict[str, float]:
    """Read ``{column: largest acceptable value}`` from a thresholds file.

    Raises:
        ValueError: If a key is not a summary column or a value is not a number.

    """
    with open(path) as f:
        thresholds = (yaml.safe_load(f) or {}).get("thresholds") or {}
    unknown = sorted(set(thresholds) - set(COLUMNS))
    if unknown:
        raise ValueError(f"{path}: unknown columns {unknown}; valid columns are {COLUMNS}")
    for key, value in thresholds.items():
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"{path}: threshold for {key!r} must be a number, got {value!r}")
    return {key: float(value) for key, value in thresholds.items()}


def judge(frame: pd.DataFrame, thresholds: dict[str, float]) -> pd.DataFrame:
    """Add ``n_flags`` and ``flags`` (``column=value>threshold`` entries) to the summary.

    Blank values (a measurement that does not apply to the epoch) are not judged.
    """
    frame = frame.copy()
    flags: list[str] = []
    counts: list[int] = []
    for _, row in frame.iterrows():
        hits = []
        for column, limit in thresholds.items():
            value = pd.to_numeric(row[column], errors="coerce")
            if pd.notna(value) and value > limit:
                hits.append(f"{column}={value:g}>{limit:g}")
        flags.append("; ".join(hits))
        counts.append(len(hits))
    frame.insert(3, "n_flags", counts)
    frame["flags"] = flags
    return frame


def summarise_report(path: Path) -> dict[str, Any]:
    """Reduce one epoch report to a flat row of counts."""
    with open(path) as f:
        report = yaml.safe_load(f)
    label, _, start = path.stem.partition("_")
    row: dict[str, Any] = dict.fromkeys(COLUMNS, 0)
    row.update({"dataset": path.parent.name, "label": label, "start": start})
    time_range = report.get("time_range") or {}
    row["end"] = time_range.get("end") or ""
    if time_range.get("start") and time_range.get("end"):
        span = pd.Timestamp(time_range["end"]) - pd.Timestamp(time_range["start"])
        row["hours"] = round(span.total_seconds() / 3600, 1)
    else:
        row["hours"] = ""
    row["missing_devices"] = len(report.get("missing_devices") or [])

    max_sync = 0.0
    offsets: list[float] = []
    ppms: list[float] = []
    step_us: list[float] = []
    interval_ratios: list[float] = []
    chunk_ms: list[float] = []
    hub_ticks: list[int] = []
    for section in (report.get("devices") or {}).values():
        metric = section.get("metric")
        summary = section.get("summary") or {}
        if metric == "epoch_gaps":
            row["epochs"] = summary.get("n_epochs", 0)
        elif metric == "heartbeat_gaps":
            row["heartbeat_gaps"] += summary.get("n_gaps", 0)
            row["heartbeat_dropout_s"] += summary.get("total_dropout_seconds", 0.0)
        elif metric == "sync_delta":
            value = summary.get("max_abs_delta_seconds")
            if value is not None:
                max_sync = max(max_sync, float(value) * 1000)
        elif metric == "harp_sync_alerts":
            row["harp_sync_alerts"] += summary.get("n_alerts", 0)
        elif metric == "dropped_frames":
            row["dropped_frame_events"] += summary.get("n_drop_events", 0)
            row["frames_dropped"] += summary.get("total_frames_dropped", 0)
        elif metric == "harp_gaps":
            row["harp_gap_events"] += summary.get("n_gap_events", 0)
            row["harp_missed_samples"] += summary.get("total_missed_samples", 0)
            row["harp_irregular_runs"] += summary.get("n_irregular_runs", 0)
            row["harp_samples"] += summary.get("n_samples", 0)
            if summary.get("interval_ratio_max") is not None:
                interval_ratios.append(float(summary["interval_ratio_max"]))
        elif metric == "timestamp_order":
            row["order_backwards"] += summary.get("n_backwards", 0)
            row["order_duplicates"] += summary.get("n_duplicates", 0)
        elif metric == "pellet_failures":
            row["pellet_missed"] += summary.get("n_missed", 0)
            row["pellet_retried"] += summary.get("n_retried", 0)
        elif metric == "message_log_errors":
            row["log_errors"] += summary.get("n_errors", 0) + summary.get("n_warnings", 0)
        elif metric == "harp_sync_integrity":
            if summary.get("seconds_offset") is not None:
                offsets.append(float(summary["seconds_offset"]))
            row["harpsync_faults"] += summary.get("n_faults", 0)
            worst, per_second = (
                summary.get("deviation_max_abs_ticks"),
                summary.get("clock_step_median_ticks"),
            )
            if worst is not None and per_second:
                step_us.append(float(worst) / float(per_second) * 1e6)
        elif metric == "harp_sync_drift":
            if summary.get("clock_rate_ppm") is not None:
                ppms.append(float(summary["clock_rate_ppm"]))
            if summary.get("worst_chunk_max_abs_residual_ms") is not None:
                chunk_ms.append(float(summary["worst_chunk_max_abs_residual_ms"]))
        elif metric == "onix_clock_sequence":
            row["onix_clock_events"] += summary.get("n_events", 0)
        elif metric == "onix_hub_offset" and summary.get("deviation_max_abs_ticks") is not None:
            hub_ticks.append(int(summary["deviation_max_abs_ticks"]))

    row["max_sync_delta_ms"] = round(max_sync, 3)
    row["heartbeat_dropout_s"] = round(row["heartbeat_dropout_s"], 1)
    row["harpsync_offset_s"] = round(max(offsets), 2) if offsets else ""
    row["onix_clock_ppm"] = round(sum(ppms) / len(ppms), 1) if ppms else ""
    row["harpsync_worst_step_us"] = round(max(step_us), 3) if step_us else ""
    row["fit_worst_chunk_ms"] = round(max(chunk_ms), 4) if chunk_ms else ""
    row["hub_offset_max_dev_ticks"] = max(hub_ticks) if hub_ticks else ""
    row["harp_max_interval_ratio"] = round(max(interval_ratios), 4) if interval_ratios else ""
    return row


def summarise(input_dir: Path) -> pd.DataFrame:
    """Build the summary table from every report under ``input_dir``."""
    reports = sorted(input_dir.glob("*/*.yaml"))
    rows = [summarise_report(path) for path in reports]
    frame = pd.DataFrame(rows, columns=COLUMNS)
    return frame.sort_values(["dataset", "start"]).reset_index(drop=True)


def to_markdown(frame: pd.DataFrame) -> str:
    """Render the summary as a GitHub-flavoured Markdown table without extra dependencies."""
    header = "| " + " | ".join(frame.columns) + " |"
    rule = "|" + "|".join(" --- " for _ in frame.columns) + "|"
    body = ["| " + " | ".join(str(v) for v in row) + " |" for row in frame.itertuples(index=False)]
    return "\n".join([header, rule, *body]) + "\n"


def main() -> None:
    """Write summary.csv and summary.md for a benchmark output directory."""
    args = parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output) if args.output else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    frame = summarise(input_dir)
    thresholds_path = Path(args.thresholds)
    judged = not args.no_thresholds and thresholds_path.is_file()
    if judged:
        frame = judge(frame, load_thresholds(thresholds_path))
    csv_path = output_dir / "summary.csv"
    md_path = output_dir / "summary.md"
    frame.to_csv(csv_path, index=False)
    md_path.write_text(to_markdown(frame), encoding="utf-8")
    print(f"{len(frame)} epoch reports summarised")
    print(f"  {csv_path}")
    print(f"  {md_path}")
    if not judged:
        print("  not judged (no thresholds file)" if not args.no_thresholds else "  not judged")
        return
    flagged = frame[frame["n_flags"] > 0]
    print(f"  judged against {thresholds_path}: {len(flagged)} of {len(frame)} epoch(s) flagged")
    for row in flagged.itertuples():
        print(f"    {row.dataset} {row.label} {row.start}: {row.flags}")
    if args.strict and len(flagged):
        sys.exit(1)


if __name__ == "__main__":
    main()
