#!/usr/bin/env python3
"""Pre-flight check for swc.aeon.qc benchmarks.

Replicates the discovery logic of scripts/run_benchmarks.py without calling
``swc.aeon.io.api.load()``: resolves the schema the same way (REGISTRY or
build_schema), enumerates epochs the same way (YAML or filesystem),
derives the load window the same way (filenames for auto-discovered
epochs), and for each reader in the schema globs the epoch directory to
confirm at least one matching file exists. Reports readers that would
return ``data_found=False`` in the real run.

Usage:
    uv run python scripts/dry_run_benchmarks.py [options]

Options:
    --benchmarks PATH   Path to benchmarks YAML (default: scripts/benchmarks.yaml)
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

from swc.aeon.qc.report import iter_readers
from swc.aeon.qc.schemas import (
    REGISTRY,
    SELF_CONTAINED_SCHEMAS,
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
    parser.add_argument(
        "--benchmarks",
        default="scripts/benchmarks.yaml",
        help="Path to benchmarks YAML",
    )
    return parser.parse_args()


def list_epoch_dirs(root: str | Path) -> list[tuple[pd.Timestamp, Path]]:
    """Return [(start_ts, dir_path), ...] sorted by start, for filesystem-driven epoch discovery."""
    return sorted((parse_epoch_timestamp(d), d) for d in Path(root).iterdir() if is_epoch_dir(d))


def check_readers(epoch_dir: Path, schema: object, extra_roots: list[str] | None = None) -> list[str]:
    """For each reader in schema, confirm at least one file matches its pattern under epoch_dir.

    Mirrors what ``swc.aeon.io.api.load`` does internally (glob
    ``<epoch>/**/<reader.pattern>.<reader.extension>``) but without
    reading any files or applying time-window filtering. Readers with no
    file under ``epoch_dir`` are also looked up under ``extra_roots`` (any
    epoch). Returns one "no files" message per reader that finds nothing.
    """
    issues: list[str] = []
    for qualified_name, reader in iter_readers(schema):
        pattern = f"**/{reader.pattern}.{reader.extension}"
        if next(epoch_dir.glob(pattern), None):
            continue
        if any(next(Path(r).glob(f"*/{pattern}"), None) for r in extra_roots or []):
            continue
        issues.append(f"no files for {qualified_name}: {reader.pattern}.{reader.extension}")
    return issues


def main() -> int:
    """Walk the benchmarks YAML and report any (dataset, epoch) pairs that would produce no data."""
    args = parse_args()
    with open(args.benchmarks) as f:
        benchmarks = yaml.safe_load(f)

    total_issues = 0

    for dataset in benchmarks["datasets"]:
        name = dataset["name"]
        roots = dataset.get("roots") or dataset.get("root") or []
        roots = [roots] if isinstance(roots, str) else list(roots)
        root = roots[0] if roots else ""
        schema_key = dataset.get("schema")
        epochs = dataset.get("epochs") or []
        epochs_auto = False
        print(f"\n=== {name} ===")

        missing_roots = [r for r in roots if not Path(r).is_dir()]
        if not roots or missing_roots:
            print(f"  FAIL: root does not exist: {missing_roots[0] if missing_roots else '<none>'}")
            total_issues += 1
            continue

        registry_schema = REGISTRY[schema_key] if schema_key and schema_key in REGISTRY else None
        if schema_key and registry_schema is None:
            print(f"  WARN: schema {schema_key!r} not in REGISTRY, will use build_schema")
        self_contained = schema_key in SELF_CONTAINED_SCHEMAS
        suffix = " [self-contained]" if self_contained else ""
        if registry_schema is not None:
            n_devices = sum(1 for n in registry_schema if n != "Metadata")
            print(f"  root: OK  schema: {schema_key} ({n_devices} devices){suffix}")
        else:
            print(f"  root: OK  schema: <auto: build_schema fallback>{suffix}")

        if not epochs:
            disk_epochs = list_epoch_dirs(root)
            if not disk_epochs:
                print("  WARN: no epoch directories found under root")
                continue
            print(f"  (no epochs listed - discovered {len(disk_epochs)} on disk)")
            epoch_iter = [
                {"phase": "auto", "start": ts.strftime("%Y-%m-%dT%H-%M-%S")} for ts, _ in disk_epochs
            ]
            epochs_auto = True
        else:
            epoch_iter = epochs

        dataset_end = normalise_timestamp(dataset["end"]) if dataset.get("end") else None

        for i, epoch in enumerate(epoch_iter):
            start = normalise_timestamp(epoch["start"])
            label = epoch.get("phase") or epoch.get("ssid") or "epoch"
            start_safe = start.strftime("%Y-%m-%dT%H-%M-%S")
            epoch_dir = Path(root) / start_safe

            issues: list[str] = []
            if not epoch_dir.is_dir():
                issues.append(f"MISSING epoch dir: {start_safe}")
            elif self_contained:
                derived = derive_epoch_window(epoch_dir)
                if derived is None:
                    issues.append("filename-window derivation failed (no parseable filenames)")
                schema = (
                    registry_schema
                    if registry_schema is not None
                    else build_schema(
                        str(epoch_dir),
                        start=start,
                        end=derived[1] if derived is not None else start + pd.Timedelta(hours=1),
                    )
                )
                issues.extend(check_readers(epoch_dir, schema))
            else:
                if i + 1 < len(epoch_iter):
                    end: pd.Timestamp | None = normalise_timestamp(epoch_iter[i + 1]["start"])
                elif dataset_end is not None:
                    end = dataset_end
                else:
                    later = [ts for ts, _ in list_epoch_dirs(root) if ts > start]
                    end = later[0] if later else None
                if registry_schema is not None and len(roots) == 1:
                    schema = registry_schema
                else:
                    schema = build_schema(
                        roots,
                        start=start,
                        end=end if end is not None else start + pd.Timedelta(hours=1),
                    )
                if epochs_auto:
                    derived = derive_epoch_window(epoch_dir)
                    if derived is None:
                        issues.append("filename-window derivation failed (no parseable filenames)")
                issues.extend(check_readers(epoch_dir, schema, roots[1:]))

            tag = "OK" if not issues else "WARN"
            print(f"  [{i + 1}/{len(epoch_iter)}] {tag} {label} {epoch['start']}")
            for msg in issues:
                print(f"      - {msg}")
            total_issues += len(issues)

    print(f"\nTotal issues: {total_issues}")
    return 0 if total_issues == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
