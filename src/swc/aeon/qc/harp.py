"""Gap detection for continuous-rate Harp streams."""

import datetime
from os import PathLike

import numpy as np
import pandas as pd

from swc.aeon.io.api import load
from swc.aeon.io.reader import Harp

EMPTY_COLS = ("kind", "duration", "n_intervals", "n_missed", "device")
GAP = "gap"
IRREGULAR = "irregular"
EXTRA = "extra"

IRREGULAR_FRACTION = 0.25
"""An interval more than a quarter of an expected interval from it is irregular. This only
groups intervals into runs; normal jitter in Aeon Harp streams stays within 0.1 of an
interval."""

WORST_N = 10
"""Number of longest intervals kept, with their times."""


def harp_gaps(
    root: str | PathLike | list[str] | list[PathLike],
    reader: Harp,
    start: datetime.datetime,
    end: datetime.datetime | None = None,
    threshold: pd.Timedelta | None = None,
    data: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Count missing samples in a continuous-rate Harp stream and measure its intervals.

    The reader must carry an ``expected_hz`` attribute. Stream classes set it at
    construction (see ``swc.aeon.qc.octagon.Photodiode``). ``run_qc`` dispatches this check on
    any ``Harp`` reader that has it.

    Consecutive irregular intervals, more than a quarter of an expected interval away from
    it, form a run. The samples missing from a run are its span in expected intervals,
    rounded, minus the number of intervals in it. Counting over the run rather than per
    interval tells a lost sample from a late one. Two intervals of 1.5 expected are one
    missing sample. An interval of 1.5 followed by one of 0.5 is a late sample with nothing
    missing. Every run is a row. The interval distribution is kept in ``attrs`` whether or
    not there are gaps.

    Args:
        root: Dataset root path or paths.
        reader: The continuous-rate ``Harp`` reader, tagged with ``expected_hz``.
        start: Left bound of the time range.
        end: Optional right bound of the time range.
        threshold: Keep only gap rows longer than this; other runs and the measures are
            unchanged.
        data: The stream, already loaded and sorted; loaded here when not given.

    Returns:
        A DataFrame with one row per run of irregular intervals, indexed by the time of
        the sample before it.

        - kind (str): ``gap`` when samples are missing, ``irregular`` when the run spans a
          whole number of expected intervals, ``extra`` when it holds more samples than its
          span allows.
        - duration (Timedelta): Span of the run.
        - n_intervals (int), n_missed (int): Intervals in the run and samples missing from it.
        - device (str): The reader pattern.

        ``attrs`` hold ``expected_hz``, ``n_samples`` (samples recorded), ``n_missed_total``,
        ``n_gap_events``, ``n_irregular_runs``, ``n_extra_runs``, the interval ratio (interval
        divided by the expected interval) at its minimum, 0.01th percentile, median, 99.99th
        percentile and maximum, and ``longest`` (the longest intervals with their times).
    """
    expected_hz: float = reader.expected_hz  # pyright: ignore[reportAttributeAccessIssue]
    expected_interval = pd.Timedelta(seconds=1.0 / expected_hz)

    if data is None:
        data = load(root, reader, start=start, end=end)

    result = pd.DataFrame(
        columns=list(EMPTY_COLS),
        index=pd.DatetimeIndex([], name="time", tz=datetime.UTC),
    )
    result.attrs.update(
        {
            "metric": "harp_gaps",
            "data_found": not data.empty,
            "expected_hz": expected_hz,
            "n_samples": len(data),
            "n_missed_total": 0,
            "n_gap_events": 0,
            "n_irregular_runs": 0,
            "n_extra_runs": 0,
            "interval_ratio_min": None,
            "interval_ratio_p0_01": None,
            "interval_ratio_median": None,
            "interval_ratio_p99_99": None,
            "interval_ratio_max": None,
            "longest": [],
        }
    )
    if len(data) < 2:  # noqa: PLR2004 (an interval needs two samples)
        return result

    # Timedelta arithmetic keeps this independent of the index resolution (ns or us).
    times = pd.DatetimeIndex(data.index)
    deltas = pd.TimedeltaIndex(times[1:] - times[:-1])
    ratio = np.asarray(deltas / expected_interval, dtype=np.float64)

    irregular = np.abs(ratio - 1.0) > IRREGULAR_FRACTION
    edges = np.flatnonzero(np.diff(np.concatenate(([0], irregular.astype(np.int8), [0]))))
    run_start, run_stop = edges[::2], edges[1::2]
    cumulative = np.concatenate(([0.0], np.cumsum(ratio)))
    n_intervals = run_stop - run_start
    missed = np.rint(cumulative[run_stop] - cumulative[run_start]).astype(np.int64) - n_intervals
    kind = np.where(missed > 0, GAP, np.where(missed < 0, EXTRA, IRREGULAR))
    span = (times[run_stop] - times[run_start]) if len(run_start) else pd.TimedeltaIndex([])

    keep = np.ones(len(run_start), dtype=bool)
    if threshold is not None:
        keep = (kind != GAP) | np.asarray(span > threshold)

    order = np.argsort(ratio)[::-1][:WORST_N]
    longest = [
        {
            "time": times[k].isoformat(),
            "interval_ms": float(deltas[k].total_seconds() * 1000),
            "interval_ratio": float(ratio[k]),
        }
        for k in order
    ]

    if keep.any():
        result = pd.DataFrame(
            {
                "kind": kind[keep],
                "duration": span[keep],
                "n_intervals": n_intervals[keep],
                "n_missed": np.maximum(missed[keep], 0),
                "device": reader.pattern,
            },
            index=pd.DatetimeIndex(times[run_start[keep]], name="time", tz=datetime.UTC),
        )
    gaps = kind == GAP
    result.attrs.update(
        {
            "metric": "harp_gaps",
            "data_found": True,
            "expected_hz": expected_hz,
            "n_samples": len(data),
            "n_missed_total": int(missed[gaps].sum()),
            "n_gap_events": int(gaps.sum()),
            "n_irregular_runs": int((kind == IRREGULAR).sum()),
            "n_extra_runs": int((kind == EXTRA).sum()),
            "interval_ratio_min": float(ratio.min()),
            "interval_ratio_p0_01": float(np.percentile(ratio, 0.01)),
            "interval_ratio_median": float(np.median(ratio)),
            "interval_ratio_p99_99": float(np.percentile(ratio, 99.99)),
            "interval_ratio_max": float(ratio.max()),
            "longest": longest,
        }
    )
    return result
