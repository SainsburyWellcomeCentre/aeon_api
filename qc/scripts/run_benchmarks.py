#!/usr/bin/env python3
"""Run aeon_qc over every epoch defined in benchmarks.yaml.

Usage:
    uv run python scripts/run_benchmarks.py [options]

Options:
    --benchmarks PATH   Path to benchmarks.yaml (default: benchmarks.yaml)
    --output DIR        Output root directory (default: benchmarks_output)
"""

import argparse
from pathlib import Path

import pandas as pd
import yaml

from aeon_qc import generate_report, run_qc, save_results
from aeon_qc.schemas import REGISTRY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--benchmarks", default="benchmarks.yaml", help="Path to benchmarks.yaml")
    parser.add_argument("--output", default="benchmarks_output", help="Output root directory")
    return parser.parse_args()


def run_epoch(
    root: str,
    schema: object,
    start: pd.Timestamp,
    end: pd.Timestamp | None,
    output_dir: Path,
    stem: str,
) -> None:
    end_str = end.isoformat() if end is not None else "open"
    print(f"    start={start.isoformat()}  end={end_str}")

    results = run_qc(root, schema, start=start, end=end)

    yaml_path = output_dir / f"{stem}.yaml"
    pkl_path = output_dir / f"{stem}.pkl"
    generate_report(root, results, yaml_path, start=start, end=end)
    save_results(results, pkl_path)

    for key, df in results.items():
        if not df.attrs.get("data_found", True):
            print(f"      {key}: NO DATA")
        elif df.empty:
            print(f"      {key}: 0 events")
        else:
            print(f"      {key}: {len(df)} event(s)")


def main() -> None:
    args = parse_args()

    with open(args.benchmarks) as f:
        benchmarks = yaml.safe_load(f)

    for dataset in benchmarks["datasets"]:
        if not dataset.get("schema") or not dataset.get("epochs"):
            continue

        schema = REGISTRY[dataset["schema"]]
        root = dataset["root"]
        epochs = dataset["epochs"]
        output_dir = Path(args.output) / dataset["name"]
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n=== {dataset['name']} ({len(epochs)} epochs) ===")

        for i, epoch in enumerate(epochs):
            start = pd.Timestamp(epoch["start"])
            end = pd.Timestamp(epochs[i + 1]["start"]) if i + 1 < len(epochs) else None
            label = epoch.get("phase") or epoch.get("ssid") or "epoch"
            start_safe = start.strftime("%Y-%m-%dT%H-%M-%S")
            stem = f"{label}_{start_safe}"

            print(f"  [{i + 1}/{len(epochs)}] {stem}")
            run_epoch(root, schema, start, end, output_dir, stem)


if __name__ == "__main__":
    main()
