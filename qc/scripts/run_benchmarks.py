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
from aeon_qc.schemas import (
    REGISTRY,
    build_schema,
    derive_epoch_window,
    is_epoch_dir,
    normalise_timestamp,
    parse_epoch_timestamp,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--benchmarks", default="benchmarks.yaml", help="Path to benchmarks.yaml")
    parser.add_argument("--output", default="benchmarks_output", help="Output root directory")
    return parser.parse_args()


def next_epoch_on_disk(root: str, start: pd.Timestamp) -> pd.Timestamp | None:
    """Return the start of the first epoch directory after `start`, or None."""
    epoch_dirs = sorted(d for d in Path(root).iterdir() if is_epoch_dir(d))
    later = [parse_epoch_timestamp(d) for d in epoch_dirs if parse_epoch_timestamp(d) > start]
    return later[0] if later else None


def run_epoch(
    root: str,
    schema: object,
    start: pd.Timestamp,
    end: pd.Timestamp | None,
    output_dir: Path,
    stem: str,
) -> None:
    """Run QC for one epoch and write the YAML report and pickled results."""
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
    """Iterate every (dataset, epoch) in the benchmarks YAML and run QC."""
    args = parse_args()

    with open(args.benchmarks) as f:
        benchmarks = yaml.safe_load(f)

    for dataset in benchmarks["datasets"]:
        root = dataset["root"]
        if not Path(root).is_dir():
            print(f"\n=== {dataset['name']} ===  SKIP: root does not exist: {root}")
            continue

        schema_key = dataset.get("schema")
        epochs = dataset.get("epochs") or []
        epochs_auto = False
        if not epochs:
            disk_epochs = sorted(
                d for d in Path(root).iterdir() if is_epoch_dir(d)
            )
            if not disk_epochs:
                print(f"\n=== {dataset['name']} ===  SKIP: no epochs listed and none on disk")
                continue
            epochs = [
                {"phase": "auto", "start": parse_epoch_timestamp(d).strftime("%Y-%m-%dT%H-%M-%S")}
                for d in disk_epochs
            ]
            epochs_auto = True
            print(f"\n=== {dataset['name']} ({len(epochs)} epochs auto-discovered) ===")
        else:
            print(f"\n=== {dataset['name']} ({len(epochs)} epochs) ===")

        registry_schema = REGISTRY[schema_key] if schema_key and schema_key in REGISTRY else None
        if schema_key and registry_schema is None:
            print(f"  WARN: schema {schema_key!r} not in REGISTRY, falling back to build_schema")

        output_dir = Path(args.output) / dataset["name"]
        output_dir.mkdir(parents=True, exist_ok=True)

        dataset_end = normalise_timestamp(dataset["end"]) if dataset.get("end") else None
        if dataset_end is not None:
            epochs = [e for e in epochs if normalise_timestamp(e["start"]) < dataset_end]

        for i, epoch in enumerate(epochs):
            start = normalise_timestamp(epoch["start"])
            if i + 1 < len(epochs):
                end: pd.Timestamp | None = normalise_timestamp(epochs[i + 1]["start"])
            elif dataset_end is not None:
                end = dataset_end
            else:
                end = next_epoch_on_disk(root, start)
            label = epoch.get("phase") or epoch.get("ssid") or "epoch"
            start_safe = start.strftime("%Y-%m-%dT%H-%M-%S")
            stem = f"{label}_{start_safe}"

            schema = registry_schema if registry_schema is not None else build_schema(
                root, start=start, end=end if end is not None else start + pd.Timedelta(hours=1)
            )

            load_start, load_end = start, end
            if epochs_auto:
                derived = derive_epoch_window(Path(root) / start_safe)
                if derived is not None and derived[0] != start:
                    load_start, load_end = derived
                    print(
                        "    filename-derived window: "
                        f"{load_start.isoformat()} -> {load_end.isoformat()}"
                    )

            print(f"  [{i + 1}/{len(epochs)}] {stem}")
            run_epoch(root, schema, load_start, load_end, output_dir, stem)


if __name__ == "__main__":
    main()
