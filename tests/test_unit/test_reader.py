"""Tests for the Reader classes."""

from pathlib import Path

import pandas as pd
import pytest
from dotmap import DotMap

from swc.aeon.io.reader import (
    BitmaskEvent,
    Chunk,
    Csv,
    DigitalBitmask,
    Encoder,
    Harp,
    Heartbeat,
    JsonList,
    Log,
    Metadata,
    Pose,
    Position,
    Reader,
    Subject,
    Video,
)


@pytest.mark.parametrize("columns", [pd.Index([1, 2, 3]), ["col1", "col2"]], ids=["Index", "Array-like"])
def test_base_reader_read(columns):
    """Test that the base Reader `read` returns an empty DataFrame with the expected index and columns."""
    reader = Reader("pattern", columns, "ext")
    df = reader.read(Path("dummy.ext"))
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert isinstance(df.index, pd.DatetimeIndex)
    assert set(df.columns) == set(columns)


@pytest.mark.parametrize(
    ("reader_class", "init_args", "expected_columns"),
    [
        (Subject, None, ["id", "weight", "event"]),
        (Log, None, ["priority", "type", "message"]),
        (Heartbeat, None, ["second"]),
        (Encoder, None, ["angle", "intensity"]),
        (Position, None, ["x", "y", "angle", "major", "minor", "area", "id"]),
        (BitmaskEvent, {"value": 0x22, "tag": "PelletDetected"}, ["event"]),  # remove
        (DigitalBitmask, {"mask": 0x1, "columns": ["state"]}, ["state"]),  # remove
        (Pose, {"model_root": "test_root"}, None),  # remove
    ],
)
def test_init_only_readers(reader_class, init_args, expected_columns):
    """Test readers with only an `__init__` method."""
    reader = reader_class("pattern", **(init_args or {}))
    assert reader.pattern == "pattern"
    if expected_columns is not None:
        assert set(reader.columns) == set(expected_columns)
    else:
        assert reader.columns is None
    # Also assert the reader attributes are correctly set
    if init_args and "columns" in init_args:
        init_args.pop("columns")
        for key, value in init_args.items():
            assert getattr(reader, key) == value


def test_harp_read(monotonic_file):
    """Test that Harp `read` returns a DataFrame with the expected columns and data from a given file."""
    reader = Harp("pattern", ["col1", "col2"])
    df = reader.read(monotonic_file)
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
    """Test that `Chunk` initialises with the expected pattern and extension."""
    reader = Chunk(reader=reader_arg)
    assert reader.pattern == expected_pattern
    assert reader.extension == expected_extension


@pytest.mark.parametrize("reader_arg", [Harp("pattern", ["col1", "col2"]), None], ids=["Harp", "Default"])
def test_chunk_read(reader_arg, monotonic_file):
    """Test that Chunk `read` returns a DataFrame with path and epoch for a given file."""
    reader = Chunk(reader=reader_arg, pattern="pattern", extension="bin")
    df = reader.read(monotonic_file)
    expected = pd.DataFrame(
        data={"path": [monotonic_file], "epoch": ["2022-06-13T13_14_25"]},
        index=[pd.Timestamp("2022-06-13 12:00:00")],
        columns=["path", "epoch"],
    )
    assert df.equals(expected)


def test_metadata_read(metadata_file):
    """Test that Metadata `read` returns an empty DataFrame with the correct structure."""
    reader = Metadata()
    df = reader.read(metadata_file)
    expected = pd.DataFrame(
        data={
            "workflow": ["Experiment0.2.bonsai"],
            "commit": ["249cdc654af63e6959e64f7ff2c21f219cc912ea"],
            "metadata": [
                DotMap(
                    {
                        "Devices": {
                            "VideoController": {
                                "PortName": "COM3",
                                "GlobalTriggerFrequency": "50",
                                "LocalTriggerFrequency": "125",
                            }
                        },
                    }
                )
            ],
        },
        index=pd.DatetimeIndex([pd.Timestamp("2022-06-06 09:24:28")]),
        columns=["workflow", "commit", "metadata"],
    )
    assert df.equals(expected)


@pytest.mark.parametrize("file", ["empty_csv_file", "video_csv_file"], ids=["Empty", "Non-empty"])
def test_csv_read(file, request):
    """Test that CSV reader returns a DataFrame with the expected structure."""
    reader = Csv("pattern", ["col1", "col2", "col3"])
    df = reader.read(request.getfixturevalue(file))
    if file == "empty_csv_file":
        assert df.empty
        assert set(df.columns) == {"col1", "col2", "col3"}
        assert isinstance(df.index, pd.RangeIndex)
    else:
        assert not df.empty
        assert set(df.columns) == {"col2", "col3"}  # col1 becomes index
        assert pd.api.types.is_float_dtype(df.index)


@pytest.mark.parametrize(
    ("init_columns", "expected_columns"),
    [([], {"value"}), (["name"], {"value", "name"})],
    ids=["Default", "Extract `name` column"],
)
def test_jsonl_read(jsonl_file, init_columns, expected_columns):
    """Test that JSONL reader returns a DataFrame with the expected structure."""
    reader = JsonList("pattern", init_columns)
    df = reader.read(jsonl_file)
    assert not df.empty
    assert set(df.columns) == expected_columns
    assert pd.api.types.is_float_dtype(df.index)


@pytest.mark.parametrize(
    ("reader", "expected_columns"),
    [
        (BitmaskEvent("pattern", value=0x22, tag="PelletDetected"), {"event"}),
        (DigitalBitmask("pattern", mask=0x22, columns=["state"]), {"state"}),
    ],
    ids=["BitmaskEvent", "DigitalBitmask"],
)
def test_bitmask_read(reader, expected_columns, bitmaskevent_file):
    """Test that BitmaskEvent reader returns a DataFrame with the expected structure."""
    df = reader.read(bitmaskevent_file)
    assert set(df.columns) == expected_columns
    assert pd.api.types.is_float_dtype(df.index)
    if isinstance(reader, BitmaskEvent):
        assert df["event"].unique() == "PelletDetected"
    else:  # expect all "state" as True for DigitalBitmask read
        assert df["state"].all()


def test_video_read(video_csv_file, monotonic_epoch):
    """Test that Video reader returns a DataFrame with the expected structure."""
    reader = Video("pattern")
    df = reader.read(video_csv_file)
    assert set(df.columns) == {"hw_counter", "hw_timestamp", "_frame", "_path", "_epoch"}
    assert pd.api.types.is_float_dtype(df.index)
    assert Path(df["_path"].iloc[0]) == video_csv_file.with_suffix(".avi")
    assert df["_epoch"].iloc[0] == monotonic_epoch


class TestPose:
    """Tests for the Pose reader."""

    def test_init(self):
        """Test that Pose reader initialises with the correct model root."""
        reader = Pose("pattern_202", model_root="test_root")
        assert reader._model_root == "test_root"  # type: ignore
        assert reader._pattern_offset == 8  # type: ignore

    def test_get_class_names(self, sleap_topdown_config_file):
        """Test that the correct class names are extracted from a given config file."""
        class_names = Pose.get_class_names(sleap_topdown_config_file)
        assert class_names == ["BAA-1104045", "BAA-1104047"]
