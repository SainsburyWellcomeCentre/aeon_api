"""Benchmarks for `_filter_time_range` to guard against performance regressions.

Run with::

    uv run pytest --benchmark-only

To compare against a saved baseline::

    uv run pytest --benchmark-only --benchmark-save=<name>
    uv run pytest --benchmark-only --benchmark-compare=<name>
"""

import datetime

import numpy as np
import pandas as pd
import pytest

from swc.aeon.io.api import (
    _filter_time_range,  # pyright: ignore[reportPrivateUsage]
)

_FREQ = "2ms"  # 500 Hz
_ROWS_PER_HOUR = 500 * 3600  # 1_800_000
_START = pd.Timestamp("2022-06-13 12:30:00", tz=datetime.UTC)


@pytest.fixture(params=[1, 4, 8], ids=["1h", "4h", "8h"])
def monotonic_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Monotonic DataFrame simulating `n_hours` of concatenated 500 Hz chunk files."""
    n_hours = request.param
    idx = pd.date_range(
        "2022-06-13 12:00:00",
        periods=n_hours * _ROWS_PER_HOUR,
        freq=_FREQ,
        tz=datetime.UTC,
    )
    return pd.DataFrame({"value": np.random.default_rng(0).standard_normal(len(idx))}, index=idx)


def test_filter_time_range(benchmark, monotonic_df):
    """Test _filter_time_range with monotonic data sampled at 500 Hz ranging from 1 to 8 hours."""
    benchmark(_filter_time_range, monotonic_df, _START, None)
