---
uid: tutorials-run-qc
title: Interactive QC on an Aeon dataset
---

# Interactive QC on an Aeon dataset

`aeon-qc` inspects Project Aeon raw datasets and reports on data quality across all acquisition devices. It reads directly from the dataset directory structure using the `swc-aeon` API. Please note that successful use of this and other tools depend on data being logge din the AEON standard format. 

This tutorial covers interactive use — running QC from a Python script or notebook on a single dataset window. For running QC across many datasets and epochs automatically, see [Batch QC with benchmarks.yaml](batch-qc.md).

## What it checks

| Metric | What it finds |
|---|---|
| Epoch gaps | Gaps between Bonsai session starts (crashes, restarts) |
| Heartbeat gaps | Periods where a Harp device stopped sending heartbeat events |
| Heartbeat duplicates | Seconds where a Harp device emits more than one heartbeat |
| Sync delta | Timestamp drift between Harp devices relative to the clock synchroniser |
| Harp sync alerts | HarpSync alert entries parsed from the Bonsai message log |
| Dropped video frames | Jumps in the camera hardware frame counter |
| Encoder gaps | Dropped samples in the wheel encoder stream |
| Pellet failures | Hardware-reported missed and retried pellet deliveries |
| Message log errors | Warning and Error entries from the Bonsai message log |
| Environment state durations | Time spent in Running vs Maintenance states |

---

## Prerequisites

- Python ≥ 3.11
- [`uv`](https://docs.astral.sh/uv/) (installed automatically by `deploy.cmd` if missing)

---

## Installation

From the repository root:

```cmd
./deploy.cmd
```

This creates a `.venv`, installs all dependencies, and makes the `aeon_qc` package importable within that environment.

---

## Quick start

The fastest way to run all checks on a dataset is to auto-discover the device schema from `Metadata.yml` and pass it to `run_qc`:

```python
import pandas as pd
from aeon_qc import run_qc, generate_report
from aeon_qc.schemas import schema_from_metadata

root = "/ceph/aeon/aeon/data/raw/AEON3/social0.2"
start = pd.Timestamp("2024-02-01T22:00:00", tz="UTC")
end   = pd.Timestamp("2024-02-02T10:00:00", tz="UTC")

schema  = schema_from_metadata(root)
results = run_qc(root, schema, start=start, end=end)

for name, df in results.items():
    found = df.attrs.get("data_found", True)
    print(f"{name}: {'NO DATA' if not found else f'{len(df)} event(s)'}")

generate_report(root, results, "qc_report.yaml", start=start, end=end)
```

> [!TIP]
> `end` is optional. Omitting it runs QC across all epochs that begin after `start`, which may take a long time for long experiments.

---

## Understanding the schema

Every Aeon epoch directory contains a `Metadata.yml` that lists the devices active during that session. `schema_from_metadata` reads this file and builds a schema automatically.

For the AEON3 social0.2 experiment, the device list includes:

| Device | Type in Metadata.yml | QC applied |
|---|---|---|
| `ClockSynchronizer` | `TimestampGenerator` (has `PortName`) | Heartbeat gaps, sync delta reference |
| `VideoController` | `CameraController` (has `PortName`) | Heartbeat gaps |
| `CameraTop`, `CameraWest`, … | `SpinnakerVideoSource` | Dropped frames |
| `Patch1`, `Patch2`, `Patch3` | `UndergroundFeeder` (has `PortName`) | Heartbeat gaps, encoder gaps, pellet failures |
| `Nest` | `WeightScale` (has `PortName`) | Heartbeat gaps (data usually absent) |
| `NestRfid1`, `NestRfid2`, `GateRfid`, … | `RfidReader` (has `PortName`) | Heartbeat gaps |
| `AudioAmbient` | `AudioSource` (no `PortName`) | Not QC'd |
| `LightCycle` | `EnvironmentCondition` (no `PortName`) | Not QC'd |

The discovery rules are:
- Devices with a `PortName` field → Harp device → `Heartbeat` reader
- Devices with `"Type": "SpinnakerVideoSource"` → camera → `Video` reader

### Using a static schema
Predefined schemas for legacy Aeon experiments is recapitulated from aeon_mecha here. These have been phased out with [aeon-api](https://github.com/SainsburyWellcomeCentre/aeon_api), where data schemas are expected to live with individual experiment schema sets.
To use a pre-defined schema from the registry:

```python
from aeon_qc.schemas import REGISTRY

schema = REGISTRY["social02"]   # covers all streams for the social 0.2 experiment
```

Available registry keys:

| Key | Experiment |
|---|---|
| `exp02` | Foraging (two patches, AEON1/2) |
| `social02` | Social 0.2 (AEON3/4) |
| `social03` | Social 0.3 (AEON3/4) |
| `social04` | Social 0.4 (AEON3/4) |
| `octagon01` | Octagon 0.1 (OCTAGON01) |

`schema_from_metadata` automatically selects the matching registry schema if the root path contains a recognisable experiment name (e.g. `social0.2` → `social02`). The auto-discovery fallback (Heartbeat + Video only) is used for unknown experiment types.

---

## Reading the results

`run_qc` returns a `dict[str, pd.DataFrame]`. Each key identifies a device stream or metric; each value is a tidy DataFrame with a UTC `DatetimeIndex`.

```python
results["epoch_gaps"]                              # one row per Bonsai session start
results["sync_delta"]                              # timestamp drift per device per second
results["ClockSynchronizer.Heartbeat"]             # heartbeat gaps for the clock synchroniser
results["ClockSynchronizer.Heartbeat.duplicates"]  # duplicate heartbeat seconds
results["VideoController.Heartbeat"]               # heartbeat gaps for the video controller
results["CameraTop.Video"]                         # dropped frame events for CameraTop
results["Patch1.Heartbeat"]                        # heartbeat gaps for Patch1
results["Patch1.Encoder"]                          # encoder sample drops for Patch1
results["Patch1.pellet_stats"]                     # pellet delivery failures for Patch1
results["Environment.harp_sync_alerts"]            # HarpSync alert log entries
results["Environment.message_log"]                 # non-Info Bonsai log entries
results["Environment.environment_state"]           # time in Running / Maintenance states
```

Each DataFrame carries metadata in `.attrs`:

```python
df = results["CameraTop.Video"]
df.attrs["data_found"]   # False if no files were found on disk for this device
df.attrs["n_frames"]     # total frames counted (including dropped)
```

An empty DataFrame with `data_found=False` means the device was in the schema but produced no data files — this is expected for `WeightScale` devices which do not emit heartbeats.

---

## Inspecting specific metrics

### Epoch gaps

```python
df = results["epoch_gaps"]
# columns: gap_duration (Timedelta, NaT for the final epoch)
# index:   UTC timestamp of each Bonsai session start
```

### Heartbeat gaps

```python
df = results["ClockSynchronizer.Heartbeat"]
# columns: duration (Timedelta), n_missed (int), second_before (int), second_after (int), device (str)
# index:   UTC timestamp of gap start

print(f"{len(df)} gap(s), total dropout: {df['duration'].sum()}")
```

### Sync delta

```python
df = results["sync_delta"]
# columns: second (int), device (str), delta_seconds (float)
# index:   UTC reference timestamp (from ClockSynchronizer)
```

### Harp sync alerts

```python
df = results["Environment.harp_sync_alerts"]
# columns: device_count, expected_device_count, max_difference
# index:   UTC timestamp of each alert

print(f"{len(df)} HarpSynch alert(s)")
```

### Dropped video frames

```python
df = results["CameraTop.Video"]
# columns: duration, n_dropped, hw_counter_before, hw_counter_after, device

if not df.empty:
    print(f"{df['n_dropped'].sum()} frames dropped across {len(df)} event(s)")
```

### Pellet failures

```python
df = results["Patch1.pellet_stats"]
# columns: outcome ('missed' or 'retried'), device

n_deliveries = df.attrs["n_deliveries"]
n_retried    = df.attrs["n_retried"]
n_missed     = df.attrs["n_missed"]

print(f"{n_deliveries} deliveries: {n_retried} retried, {n_missed} missed")
```

### Message log errors

```python
df = results["Environment.message_log"]
# columns: priority, type, message
```

---

## Generating a YAML report

`generate_report` writes a structured summary to disk:

```python
from aeon_qc import generate_report

generate_report(root, results, "reports/social02_aeon3.yaml", start=start, end=end)
```

The output format:

```yaml
generated_at: "2026-04-07T10:05:00+00:00"
dataset_root: /ceph/aeon/aeon/data/raw/AEON3/social0.2
time_range:
  start: "2024-02-01T22:00:00+00:00"
  end: "2024-02-02T10:00:00+00:00"
devices:
  ClockSynchronizer.Heartbeat:
    metric: heartbeat_gaps
    summary:
      data_found: true
      n_heartbeats: 43200
      n_gaps: 0
      total_dropout_seconds: 0.0
      mean_duration_seconds: null
    detail: []
  CameraTop.Video:
    metric: dropped_frames
    summary:
      data_found: true
      n_frames: 2160000
      n_drop_events: 3
      total_frames_dropped: 11
      mean_duration_seconds: 0.042
    detail:
      - time: "2024-02-02T01:14:22.400000+00:00"
        n_dropped: 4
        hw_counter_before: 864210
        hw_counter_after: 864215
```

---

## Saving results for later analysis

To avoid re-running QC for downstream analysis, pickle the results dict:

```python
import pickle
from aeon_qc import save_results

save_results(results, "reports/social02_aeon3.pkl")

# Later:
with open("reports/social02_aeon3.pkl", "rb") as f:
    results = pickle.load(f)
```

---

## Next steps

- To run QC systematically across many datasets and epochs, see [Batch QC with benchmarks.yaml](batch-qc.md).
- See the [API reference](xref:aeon_qc) for full function signatures and return value descriptions.
- See [aeon_roadmap#40](https://github.com/SainsburyWellcomeCentre/aeon_roadmap/issues/40) for the full list of requested metrics and their implementation status.
