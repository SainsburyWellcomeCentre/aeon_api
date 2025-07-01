"""Tests for the `swc.aeon.io.api` module."""
import pandas as pd
import pytest

from swc.aeon.io import api


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ("monotonic_data", ("2022-06-13T13_14_25", pd.Timestamp("2022-06-13 12:00:00"))),
        ("nonmonotonic_data", ("2022-06-06T09-24-28", pd.Timestamp("2022-06-06 13:00:00"))),
    ],
    ids=["monotonic", "nonmonotonic"],
)
def test_chunk_key(data, expected, request):
    """Test `chunk_key` returns the correct epoch and chunk time."""
    result = api.chunk_key(request.getfixturevalue(data))
    assert result == expected
