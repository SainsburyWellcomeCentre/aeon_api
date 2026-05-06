#!/usr/bin/env python3
"""Pre-flight check for aeon_qc benchmarks.

Walks every (dataset, epoch) pair in the benchmarks YAML and verifies
the root directory exists, the schema resolves, and the epoch directory
is present on disk with at least one device of data. Never calls
``load()`` so the entire pass runs in seconds.

Usage:
    uv run python scripts/dry_run_benchmarks.py [options]

Options:
    --benchmarks PATH   Path to benchmarks YAML (default: real_benchmarks.yaml)
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

from aeon_qc.schemas import (
    REGISTRY,
    diagnose_devices,
    normalise_timestamp,
    parse_epoch_timestamp,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--benchmarks",
        default="real_benchmarks.yaml",
        help="Path to benchmarks YAML",
    )
    return parser.parse_args()


def list_epoch_dirs(root: str | Path) -> list[tuple[pd.Timestamp, Path]]:
    """Return [(start_ts, dir_path), ...] sorted by start, for filesystem-driven epoch discovery."""
    return sorted(
        (parse_epoch_timestamp(d), d)
        for d in Path(root).iterdir()
        if d.is_dir() and "T" in d.name
    )


def check_epoch(
    root: str,
    start: pd.Timestamp,
    end: pd.Timestamp | None,
    expected_devices: set[str],
) -> list[str]:
    """Return a list of issues for this epoch, or [] if clean."""
    epoch_dir = Path(root) / start.strftime("%Y-%m-%dT%H-%M-%S")
    if not epoch_dir.is_dir():
        return [f"MISSING epoch dir: {epoch_dir.name}"]
    if end is None:
        end = start + pd.Timedelta(hours=1)
    diag = diagnose_devices(root, start=start, end=end)
    fs = diag["filesystem"]
    issues: list[str] = []
    if not fs:
        issues.append("no devices found in filesystem scan")
    if expected_devices:
        missing = expected_devices - fs - {"Metadata"}
        if missing:
            issues.append(f"expected devices missing on disk: {sorted(missing)}")
    return issues


def main() -> int:
    """Walk the benchmarks YAML and report any (dataset, epoch) pairs that look broken."""
    args = parse_args()
    with open(args.benchmarks) as f:
        benchmarks = yaml.safe_load(f)

    total_issues = 0

    for dataset in benchmarks["datasets"]:
        name = dataset["name"]
        root = dataset["root"]
        schema_key = dataset.get("schema")
        epochs = dataset.get("epochs") or []
        print(f"\n=== {name} ===")

        if not Path(root).is_dir():
            print(f"  FAIL: root does not exist: {root}")
            total_issues += 1
            continue

        if schema_key and schema_key in REGISTRY:
            expected = {n for n in REGISTRY[schema_key] if n != "Metadata"}
            schema_label = f"schema: {schema_key} ({len(expected)} devices)"
        elif schema_key:
            print(f"  FAIL: schema {schema_key!r} not in REGISTRY")
            total_issues += 1
            continue
        else:
            expected = set()
            schema_label = "schema: <auto: build_schema fallback>"
        print(f"  root: OK  {schema_label}")

        if not epochs:
            disk_epochs = list_epoch_dirs(root)
            if not disk_epochs:
                print("  WARN: no epoch directories found under root")
                continue
            print(f"  (no epochs listed - discovered {len(disk_epochs)} on disk)")
            epoch_iter = [
                {"phase": "auto", "start": ts.strftime("%Y-%m-%dT%H-%M-%S")}
                for ts, _ in disk_epochs
            ]
        else:
            epoch_iter = epochs

        dataset_end = normalise_timestamp(dataset["end"]) if dataset.get("end") else None

        for i, epoch in enumerate(epoch_iter):
            start = normalise_timestamp(epoch["start"])
            if i + 1 < len(epoch_iter):
                end: pd.Timestamp | None = normalise_timestamp(epoch_iter[i + 1]["start"])
            elif dataset_end is not None:
                end = dataset_end
            else:
                later = [ts for ts, _ in list_epoch_dirs(root) if ts > start]
                end = later[0] if later else None

            issues = check_epoch(root, start, end, expected)
            label = epoch.get("phase") or epoch.get("ssid") or "epoch"
            tag = "OK" if not issues else "WARN"
            print(f"  [{i + 1}/{len(epoch_iter)}] {tag} {label} {epoch['start']}")
            for msg in issues:
                print(f"      - {msg}")
            total_issues += len(issues)

    print(f"\nTotal issues: {total_issues}")
    return 0 if total_issues == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
