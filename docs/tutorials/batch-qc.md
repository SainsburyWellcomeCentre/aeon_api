---
uid: tutorials-batch-qc
title: Batch QC with benchmarks.yaml
---

# Batch QC with benchmarks.yaml

For systematic QC across multiple datasets and epochs, a script that reads a `benchmarks.yaml` manifest and produces YAML reports and pickled results for every epoch.

For interactive use on a single dataset window, see [Interactive QC on an Aeon dataset](run-qc.md).

---

## How it works

`benchmarks.yaml` lists datasets and the epochs that you want to QC. The script `scripts/run_benchmarks.py` iterates every epoch, runs `run_qc` over the epoch's time window, and saves a YAML report and a pickled results dict for each one.

**Time window per epoch:** `start` comes from the manifest; `end` is the next epoch's `start`. For the last epoch in a dataset, `end` is determined by scanning epoch directories on disk to find the next one after `start`. If no subsequent epoch exists on disk, `end` is `None` and the window is open-ended. If an `end` is given in the manifest, it is used instead: only epochs whose `start` precedes it are included. Epoch gaps are reported as part of the QC, indicating Bonsai had crashed and restarted, automatically or manually.

---

## The benchmarks.yaml format

```yaml
datasets:

  - name: social02-aeon3                              # unique identifier, used in output paths
    root: /ceph/aeon/aeon/data/raw/AEON3/social0.2   # path to the dataset root on the cluster
    schema: social02                                  # REGISTRY key (see table below)
    epochs:
      - {phase: presocial,  start: "2024-01-31T11-28-39"}
      - {phase: presocial,  start: "2024-02-01T22-36-47"}
      - {phase: social,     start: "2024-02-09T16-07-32"}
      - {phase: postsocial, start: "2024-02-25T17-22-33"}

  - name: octagon-conf1
    root: /ceph/aeon/aeon/data/raw/OCTAGON01/conf1
    schema: octagon01
    epochs:
      - {ssid: 24997, start: "2024-03-25T12-16-27"}
      - {ssid: 25010, start: "2024-03-25T12-41-07"}

  - name: harris-ses-043                              # schema: null skips this dataset, only placeholder
    root: /ceph/aeon/aeon/data/raw/aeon/test2/harris_benchmark_rawdata/ses-043_date-20251223
    schema: null
    epochs: []
```

### Field reference

| Field | Required | Description |
|---|---|---|
| `name` | yes | Unique identifier; used as the output subdirectory name |
| `root` | yes | Absolute path to the dataset root on the SWC cluster |
| `schema` | yes | REGISTRY key, or `null` to skip this dataset |
| `end` | no | UTC ISO 8601 timestamp capping the dataset; only epochs whose `start` precedes this are processed, and the final epoch's window closes here. Without it, the final epoch's `end` is found by scanning epoch directories on disk; if no subsequent epoch exists, the window is open-ended. |
| `epochs` | yes | List of epoch entries; empty list `[]` skips the dataset |
| `epochs[].start` | yes | Epoch start timestamp — filesystem format (`2024-01-31T11-28-39`) or ISO 8601 (`2024-01-31T11:28:39+00:00`); naive strings are assumed UTC |
| `epochs[].phase` | no | Label used in output filenames (e.g. `presocial`, `social`) |
| `epochs[].ssid` | no | Session ID label used in output filenames (alternative to `phase`) |

### Available schema keys

| Key | Experiment |
|---|---|
| `exp02` | Foraging (two patches, AEON1/2) |
| `social02` | Social 0.2 (AEON3/4) |
| `social03` | Social 0.3 (AEON3/4) |
| `social04` | Social 0.4 (AEON3/4) |
| `octagon01` | Octagon 0.1 (OCTAGON01) |

### Finding epoch start timestamps

Epoch start timestamps correspond to Bonsai session starts. Each session creates a new epoch directory under the dataset root named by its UTC timestamp. You can list them on the cluster:

```bash
ls /ceph/aeon/aeon/data/raw/AEON3/social0.2/
# 2024-01-31T11-28-39  2024-02-01T22-36-47  2024-02-02T00-15-00  ...
```

Paste the directory name directly as the `start` value — no conversion needed:

```yaml
- {phase: presocial, start: "2024-01-31T11-28-39"}
```

---

## Running the script

From the repository root:

```bash
uv run python scripts/run_benchmarks.py [options]
```

### Options

| Option | Default | Description |
|---|---|---|
| `--benchmarks PATH` | `benchmarks.yaml` | Path to the benchmarks manifest |
| `--output DIR` | `benchmarks_output/` | Root directory for output files |

### Run

```bash
uv run python scripts/run_benchmarks.py

# Use a different manifest or output directory
uv run python scripts/run_benchmarks.py --benchmarks /path/to/my_benchmarks.yaml --output /path/to/results/
```

---

## Output layout

```
benchmarks_output/
  social02-aeon3/
    presocial_2024-01-31T11-28-39.yaml   # human-readable QC summary
    presocial_2024-01-31T11-28-39.pkl    # pickled results dict
    presocial_2024-02-01T22-36-47.yaml
    presocial_2024-02-01T22-36-47.pkl
    social_2024-02-09T16-07-32.yaml
    ...
  octagon-conf1/
    24997_2024-03-25T12-16-27.yaml
    ...
```

The filename stem is `{label}_{start}` where `label` is the `phase` or `ssid` field from the epoch entry. The YAML report format is described in [Interactive QC — Generating a YAML report](run-qc.md#generating-a-yaml-report).

---

## Loading saved results

```python
import pickle
from pathlib import Path

pkl_path = Path("benchmarks_output/social02-aeon3/presocial_2024-01-31T11-28-39.pkl")
with open(pkl_path, "rb") as f:
    results = pickle.load(f)

# results is the same dict[str, pd.DataFrame] returned by run_qc
df = results["ClockSynchronizer.Heartbeat"]
print(f"{len(df)} heartbeat gap(s)")
```

---

## Adding a new dataset

1. Find the dataset root on the cluster and identify which `schema` key applies.
2. List the epoch directories to get start timestamps.
3. Add an entry to `benchmarks.yaml` — use `schema: null` and `epochs: []` as a placeholder if the schema is not yet ported.
4. Run the script to produce results.

---

## Next steps

- See [Interactive QC on an Aeon dataset](run-qc.md) for exploring results in a notebook.
- See the [API reference](xref:swc.aeon.qc) for full function signatures.
- See [aeon_roadmap#40](https://github.com/SainsburyWellcomeCentre/aeon_roadmap/issues/40) for the full list of requested metrics.
