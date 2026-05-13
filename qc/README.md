# aeon-qc

Data quality control (QC) metrics for [Project Aeon](https://github.com/SainsburyWellcomeCentre/aeon_roadmap) datasets.

Developed as a prototype against [aeon_roadmap#40](https://github.com/SainsburyWellcomeCentre/aeon_roadmap/issues/40).
Consider folding back into [`aeon_api`](https://github.com/SainsburyWellcomeCentre/aeon_api).

## QC metrics

| Metric | Result key | What it detects |
|---|---|---|
| Epoch gaps | `epoch_gaps` | Gaps between consecutive Bonsai session starts (crashes, restarts) |
| Heartbeat gaps | `<device>.Heartbeat` | Periods where a Harp device stops emitting heartbeat events (~1 Hz) |
| Heartbeat duplicates | `<device>.Heartbeat.duplicates` | Seconds where a Harp device emits more than one heartbeat (resync events) |
| Sync delta | `sync_delta` | Per-second timestamp drift between Harp devices relative to the ClockSynchronizer |
| Harp sync alerts | `<device>.harp_sync_alerts` | HarpSynch alerts from the Bonsai SynchronizerMonitor (device count or clock misalignment) |
| Dropped frames | `<device>.Video` | Jumps in the camera hardware frame counter indicating lost frames |
| Encoder gaps | `<device>.Encoder` | Dropped samples in the wheel encoder stream (~500 Hz) |
| Pellet failures | `<device>.pellet_stats` | Hardware-reported missed and retried pellet deliveries |
| Message log errors | `<device>.message_log` | Warning and Error entries from the Bonsai message log |
| Environment state durations | `<device>.environment_state` | Time spent in each environment state (Running, Maintenance) |

## Installation

1. Clone the repo.

2. From a terminal in the root of this repo:

```cmd
./deploy.cmd
```

This installs `uv` if needed, creates `.venv`, and installs all dependencies including dev tools.

## Quick start

```python
import pandas as pd
from aeon_qc import run_qc, generate_report
from aeon_qc.schemas import REGISTRY

root = "Z:/aeon/data/raw/AEON4/social0.4"
schema = REGISTRY["social04"]

results = run_qc(root, schema, start=pd.Timestamp("2024-09-01T00:00:00+00:00"))
generate_report(root, results, "qc_report.yaml", start=pd.Timestamp("2024-09-01T00:00:00+00:00"))
```

See the [run-qc tutorial](docs/tutorials/run-qc.md) for a full walkthrough.

## Batch runs

To run QC across all datasets defined in `benchmarks.yaml`:

```bash
uv run python scripts/run_benchmarks.py --benchmarks benchmarks.yaml --output benchmarks_output/
```

See the [batch-qc tutorial](docs/tutorials/batch-qc.md) for manifest format and options.

## Development

```bash
uv run ruff check src/         # lint
uv run pyright src/            # type check
```

## Citation

Sainsbury Wellcome Centre Foraging Behaviour Working Group. (2023). Aeon: An open-source platform to study the neural basis of ethological behaviours over naturalistic timescales. https://doi.org/10.5281/zenodo.8411157

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8411157.svg)](https://zenodo.org/doi/10.5281/zenodo.8411157)
