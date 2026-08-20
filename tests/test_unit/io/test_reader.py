"""Tests for the Reader classes."""

import datetime
from contextlib import nullcontext
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from dotmap import DotMap
from pandas import testing as tm

from swc.aeon.io.reader import (
    Binary,
    BitmaskEvent,
    Chunk,
    Csv,
    DigitalBitmask,
    Encoder,
    Harp,
    HarpSyncAlignment,
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
        (None, "pattern", "bin"),  # When reader is None, pattern and extension must be specified
    ],
    ids=["Harp", "Default"],
)
def test_chunk_init(reader_arg, expected_pattern, expected_extension):
    """Test that `Chunk` initialises with the expected pattern and extension."""
    reader = (
        Chunk(reader=reader_arg)
        if reader_arg is not None
        else Chunk(pattern=expected_pattern, extension=expected_extension)
    )
    assert reader.pattern == expected_pattern
    assert reader.extension == expected_extension


def test_chunk_init_defaults():
    with pytest.raises(ValueError, match="reader must be specified if pattern or extension are None."):
        Chunk()


@pytest.mark.parametrize("reader_arg", [Harp("pattern", ["col1", "col2"]), None], ids=["Harp", "Default"])
def test_chunk_read(reader_arg, monotonic_file):
    """Test that Chunk `read` returns a DataFrame with path and epoch for a given file."""
    reader = Chunk(reader=reader_arg, pattern="pattern", extension="bin")
    df = reader.read(monotonic_file)
    expected = pd.DataFrame(
        data={"path": [monotonic_file], "epoch": ["2022-06-13T13_14_25"]},
        index=[pd.Timestamp("2022-06-13 12:00:00", tzinfo=datetime.UTC)],
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
    """Test that CSV `read` returns a DataFrame with the expected structure."""
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


def test_binary_read(tmp_path):
    """Test that Binary `read` returns a DataFrame of the flat binary records with a default index."""
    data = np.arange(12, dtype=np.float32).reshape(-1, 3)
    file = tmp_path / "records.bin"
    file.write_bytes(data.tobytes())
    reader = Binary("pattern", columns=["x", "y", "z"], dtype=np.float32)
    df = reader.read(file)
    assert set(df.columns) == {"x", "y", "z"}
    assert isinstance(df.index, pd.RangeIndex)
    assert np.array_equal(df.to_numpy(), data)


@pytest.mark.parametrize(
    ("init_columns", "expected_columns"),
    [([], {"value"}), (["name"], {"value", "name"})],
    ids=["Default", "Extract `name` column"],
)
def test_jsonl_read(jsonl_file, init_columns, expected_columns):
    """Test that JsonList `read` returns a DataFrame with the expected structure."""
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
    """Test that BitmaskEvent `read` returns a DataFrame with the expected structure."""
    df = reader.read(bitmaskevent_file)
    assert set(df.columns) == expected_columns
    assert pd.api.types.is_float_dtype(df.index)
    if isinstance(reader, BitmaskEvent):
        assert df["event"].unique() == "PelletDetected"
    else:  # expect all "state" as True for DigitalBitmask read
        assert df["state"].all()


def test_video_read(video_csv_file, monotonic_epoch):
    """Test that Video `read` returns a DataFrame with the expected structure."""
    reader = Video("pattern")
    df = reader.read(video_csv_file)
    assert set(df.columns) == {"hw_counter", "hw_timestamp", "_frame", "_path", "_epoch"}
    assert pd.api.types.is_float_dtype(df.index)
    assert Path(df["_path"].iloc[0]) == video_csv_file.with_suffix(".avi")
    assert df["_epoch"].iloc[0] == monotonic_epoch


class TestPose:
    """Tests for the Pose reader."""

    EXPECTED_POSE_COLUMNS = {
        "identity",
        "identity_likelihood",
        "part",
        "x",
        "y",
        "part_likelihood",
        "model",
    }

    @pytest.mark.parametrize(
        ("init_args", "file", "expected"),
        [
            (
                {"pattern": "CameraTop_test-node1*"},
                "pose_topdown_legacy_data_file",
                nullcontext(EXPECTED_POSE_COLUMNS),
            ),
            (
                {"pattern": "CameraTop_202_*"},
                "pose_topdown_data_file",
                nullcontext(EXPECTED_POSE_COLUMNS),
            ),
            (
                {"pattern": "CameraTop_202_*"},
                "pose_missing_config_topdown_data_file",
                pytest.raises(FileNotFoundError, match="Cannot find model dir"),
            ),
            (
                {"pattern": "CameraTop_202_*", "model_root": "pose_shared_sleap_topdown_config_dir"},
                "pose_missing_config_topdown_data_file",
                nullcontext(EXPECTED_POSE_COLUMNS),
            ),
            (
                {"pattern": "CameraTop_202_*", "model_root": "pose_missing_config_file_dir"},
                "pose_missing_config_topdown_data_file",
                pytest.raises(FileNotFoundError, match="Cannot find model dir"),
            ),
        ],
        ids=[
            "Config dir in same dir as pose data: Bonsai.SLEAP0.2 legacy data",
            "Config dir in same dir as pose data: Bonsai.SLEAP0.3 data",
            "Config dir not found in local pose data dir",
            "Config dir in shared `model_root`",
            "Config dir not found in shared `model_root`",
        ],
    )
    def test_pose_read(self, init_args, file, expected, request):
        """Test that Pose `read` correctly locates the model config file associated with
        the pose data file and returns a DataFrame with the correct structure.
        If the config file is not found, an error is raised.
        """
        if "model_root" in init_args:
            init_args["model_root"] = request.getfixturevalue(init_args["model_root"])
        reader = Pose(**init_args)
        with expected as expected_columns:
            df = reader.read(request.getfixturevalue(file), include_model=True)
            assert set(df.columns) == expected_columns

    @pytest.mark.parametrize(
        ("file", "expected"),
        [
            ("pose_topdown_config_file", nullcontext(["BAA-1104045", "BAA-1104047"])),
            ("pose_centered_instance_config_file", nullcontext([])),
            (
                "pose_unsupported_config_file",
                pytest.raises(ValueError, match="model config file .* not supported"),
            ),
            (
                "pose_supported_config_file_missing_required_key",
                pytest.raises(KeyError, match="Cannot find class_vectors in .*"),
            ),
        ],
        ids=[
            "Supported config: topdown model (with class labels)",
            "Supported config: centered-instance model (no class labels)",
            "Unsupported config",
            "Config missing required 'head' key",
        ],
    )
    def test_get_class_names(self, file, expected, request):
        """Test that the correct class names are extracted from a valid config file
        or that an error is raised for unsupported config files.
        """
        config_file = request.getfixturevalue(file)
        with expected as expected_values:
            class_names = Pose.get_class_names(config_file)
            assert class_names == expected_values

    @pytest.mark.parametrize(
        ("file", "expected"),
        [
            ("pose_topdown_config_file", nullcontext(["anchor_centroid", "centroid"])),
            (
                "pose_topdown_config_file_missing_part_names",
                pytest.raises(KeyError, match="Cannot find anchor or bodyparts"),
            ),
        ],
        ids=["Valid config", "Config missing part_names"],
    )
    def test_get_bodyparts(self, file, expected, request):
        """Test that the correct body parts are extracted from a valid config file
        or that an error is raised for missing part names.
        """
        config_file = request.getfixturevalue(file)
        with expected as expected_values:
            bodyparts = Pose.get_bodyparts(config_file)
            assert bodyparts == expected_values

    def test_class_int2str(self):
        """Test that integer class (subject) IDs are converted to string class names."""
        result = Pose.class_int2str(pd.DataFrame({"identity": [0, 1, 0, 1]}), ["A", "B"])
        expected = pd.DataFrame({"identity": ["A", "B", "A", "B"]})
        tm.assert_frame_equal(result, expected)

    @pytest.mark.parametrize(
        ("config_dir", "config_name", "expected"),
        [
            (
                "pose_sleap_topdown_config_dir",
                None,  # Use default config file name
                nullcontext("confmap_config.json"),
            ),
            (
                "pose_missing_config_file_dir",
                None,  # Use default config file name
                pytest.raises(FileNotFoundError, match="Cannot find config file"),
            ),
            (
                "pose_sleap_topdown_config_dir",
                ["custom_config.json"],
                pytest.raises(FileNotFoundError, match="Cannot find config file"),
            ),
        ],
        ids=[
            "Config file exists in config dir",
            "Default config file not found",
            "Specified config file not found",
        ],
    )
    def test_get_config_file(self, config_dir, config_name, expected, test_data_dir, request):
        """Test that the correct config file is returned based on the model root."""
        config_dir = request.getfixturevalue(config_dir)
        with expected as expected_file_name:
            config_file = Pose.get_config_file(config_dir, config_name)
            assert config_file == config_dir / expected_file_name


def test_harpsyncalignment_read(harpsync_file):
    """Test that HarpSyncAlignment fits the source-to-Harp correspondence into a single summary row."""
    file, expected = harpsync_file
    df = HarpSyncAlignment("NeuropixelsV2Beta_HarpSync_*").read(file)
    pd.testing.assert_frame_equal(df, expected, rtol=1e-6)


def test_harpsyncalignment_parameters_reconstruct_harp_time(harpsync_file):
    """Test that the fitted slope and intercept recover Harp time from the source clock."""
    file, _ = harpsync_file
    row = HarpSyncAlignment("NeuropixelsV2Beta_HarpSync_*").read(file).iloc[0]
    predicted = HarpSyncAlignment.estimate_harp_seconds(
        np.array([row["clock_start"], row["clock_end"]]), row["slope"], row["intercept"]
    )
    np.testing.assert_allclose(predicted, [row["harp_start"], row["harp_end"]])


def test_harpsyncalignment_real_data_accuracy(harpsync_real_file):
    """Test that the fit reproduces Harp time from recorded ONIX data to sub-millisecond accuracy.

    The mean-centered numpy fit was verified to match the original scikit-learn LinearRegression on
    this dataset to within floating-point precision; this guards that numerical result without a
    scikit-learn dependency. The ISO8601 chunk time in the file name also confirms the UTC index.
    """
    df = HarpSyncAlignment("NeuropixelsV2_HarpSync_*").read(harpsync_real_file)
    assert df.index[0] == pd.Timestamp("2026-06-28 10:00:00", tz="UTC")
    row = df.iloc[0]
    assert row["r2"] == pytest.approx(1.0)
    raw = pd.read_csv(harpsync_real_file)
    predicted = HarpSyncAlignment.estimate_harp_seconds(
        raw["Value.Clock"].to_numpy(dtype=float), row["slope"], row["intercept"]
    )
    np.testing.assert_allclose(predicted, raw["Seconds"].to_numpy(dtype=float), rtol=0, atol=1e-4)


def test_harpsyncalignment_fits_seconds_index_not_harptime(harpsync_real_file):
    """Test that the fit uses the Seconds index for Harp time, not the Value.HarpTime column.

    In the recorded data the Value.HarpTime column reports the second about to elapse and so lags the
    true Harp time, held in the Seconds index, by one second. Fitting against it would bias the model.
    """
    row = HarpSyncAlignment("NeuropixelsV2_HarpSync_*").read(harpsync_real_file).iloc[0]
    raw = pd.read_csv(harpsync_real_file).iloc[0]
    assert row["harp_start"] == raw["Seconds"]
    assert row["harp_start"] - raw["Value.HarpTime"] == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("clock", "expected"),
    [
        (20.0, 45.0),
        (np.array([0.0, 10.0, 20.0]), np.array([5.0, 25.0, 45.0])),
        (pd.Series([0.0, 10.0, 20.0]), pd.Series([5.0, 25.0, 45.0])),
    ],
)
def test_estimate_harp_seconds_applies_linear_model(clock, expected):
    """Test that estimate_harp_seconds applies the slope and intercept and preserves the input type."""
    result = HarpSyncAlignment.estimate_harp_seconds(clock, slope=2.0, intercept=5.0)
    assert type(result) is type(expected)
    assert np.array_equal(np.asarray(result), np.asarray(expected))
