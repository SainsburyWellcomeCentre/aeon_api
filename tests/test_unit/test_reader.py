"""Tests for the Reader classes."""

from pathlib import Path

import pandas as pd
import pytest

from swc.aeon.io.reader import Chunk, Harp, Reader


@pytest.mark.parametrize("columns", [pd.Index([1, 2, 3]), ["col1", "col2"]], ids=["string", "array-like"])
def test_base_reader_read(columns):
    """Test that the base Reader `read` returns an empty DataFrame with the correct structure."""
    reader = Reader("pattern", columns, "ext")
    df = reader.read(Path("dummy.ext"))
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert isinstance(df.index, pd.DatetimeIndex)
    assert set(df.columns) == set(columns)


def test_harp_read(monotonic_data):
    """Test that the Harp reader reads data correctly."""
    reader = Harp("pattern", ["col1", "col2"])
    df = reader.read(monotonic_data)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(df.columns) == {"col1", "col2"}
    assert pd.api.types.is_float_dtype(df.index)


@pytest.mark.parametrize(
    ("reader_arg", "expected_pattern", "expected_extension"),
    [
        (Harp("pattern", ["col1", "col2"]), "pattern", "bin"),
        (None, None, None),  # When reader is None, all defaults are None
    ],
    ids=["Harp", "Default"],
)
def test_chunk_init(reader_arg, expected_pattern, expected_extension):
    """Test that the Chunk class initialises with the correct pattern and extension."""
    reader = Chunk(reader=reader_arg)
    assert reader.pattern == expected_pattern
    assert reader.extension == expected_extension


@pytest.mark.parametrize("reader_arg", [Harp("pattern", ["col1", "col2"]), None], ids=["Harp", "Default"])
def test_chunk_read(reader_arg, monotonic_data):
    """Test that Chunk `read` returns a DataFrame with the correct structure."""
    reader = Chunk(reader=reader_arg, pattern="pattern", extension="bin")
    df = reader.read(monotonic_data)
    expected = pd.DataFrame(
        data={"path": [monotonic_data], "epoch": ["2022-06-13T13_14_25"]},
        index=[pd.Timestamp("2022-06-13 12:00:00")],
        columns=["path", "epoch"],
    )
    assert df.equals(expected)
