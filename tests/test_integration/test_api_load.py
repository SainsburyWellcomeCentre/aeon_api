"""Integration tests for end-to-end data loading via aeon.load()."""

from contextlib import nullcontext

import pandas as pd
import pytest

from swc import aeon
from tests.schema import exp02, social02, social03


@pytest.mark.parametrize(
    ("fixture_name", "device", "start", "end", "expected_sorted"),
    [
        ("nonmonotonic_dir", exp02.Patch2.Encoder, pd.Timestamp("2022-06-06T13:00:49"), None, False),
        ("nonmonotonic_dir", exp02.Patch2.Encoder, None, pd.Timestamp("2022-06-06T13:00:49"), True),
        ("nonmonotonic_dir", exp02.Metadata, pd.Timestamp("2022-06-06T09:00:00"), None, True),
        ("monotonic_dir", exp02.Patch2.Encoder, None, None, True),
        ("nonmonotonic_dir", exp02.Patch2.Encoder, None, None, False),
    ],
    ids=[
        "non-monotonic data: start only",
        "non-monotonic data: end only",
        "non-chunked data: start only",
        "monotonic data: both start and end unspecified",
        "nonmonotonic data: both start and end unspecified",
    ],
)
def test_load_with_start_and_end_filters(fixture_name, device, start, end, expected_sorted, request):
    """Test `load` with `start` and `end` filters on monotonic, non-monotonic, and non-chunked data."""
    data_dir = request.getfixturevalue(fixture_name)
    data = aeon.load(data_dir, device, start=start, end=end)
    assert len(data) > 0
    assert data.index.is_monotonic_increasing == expected_sorted


@pytest.mark.parametrize(
    ("inclusive", "expect_start_included", "expect_end_included"),
    [
        ("both", True, True),
        ("left", True, False),
        ("right", False, True),
        ("neither", False, False),
    ],
)
def test_load_start_end(nonmonotonic_dir, inclusive, expect_start_included, expect_end_included):
    start = pd.Timestamp("2022-06-06T13:00:49")
    end = pd.Timestamp("2022-06-06T13:00:49.004000186")
    data = aeon.load(
        nonmonotonic_dir,
        exp02.Patch2.Encoder,
        start=start,
        end=end,
        inclusive=inclusive,
    )
    assert start in data.index if expect_start_included else start not in data.index
    assert end in data.index if expect_end_included else end not in data.index


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (pd.Timestamp("2024-07-03T10:00:00"), None),
        (None, pd.Timestamp("2024-07-03T10:00:00")),
    ],
    ids=[
        "start only",
        "end only",
    ],
)
def test_load_nonmonotonic_with_nonexistent_time_index(nonmonotonic_dir, start, end):
    """Test that filtering non-monotonic data with start/end index values
    not in the data emits a warning and returns the full sorted dataframe.
    """
    with pytest.warns(UserWarning, match="out-of-order timestamps"):
        data = aeon.load(nonmonotonic_dir, social03.CameraTop.Pose, start=start, end=end)
    assert data.index.is_monotonic_increasing


@pytest.mark.parametrize(
    ("fixture_name", "device", "expected"),
    [
        ("pose_sleap_topdown_root_dir", social02.CameraTop.Pose, nullcontext()),
        ("pose_sleap_topdown_root_dir", social03.CameraTop.Pose, nullcontext()),
        ("pose_missing_config_file_dir", social03.CameraTop.Pose, pytest.raises(FileNotFoundError)),
        ("pose_sleap_centered_instance_root_dir", social03.CameraTop.Pose, nullcontext()),
        (
            "pose_sleap_topdown_missing_part_names_root_dir",
            social03.CameraTop.Pose,
            pytest.raises(KeyError),
        ),
    ],
    ids=[
        "local model dir",  # legacy, without register prefix
        "with register prefix",
        "missing config file",
        "missing class labels",
        "missing part names",
    ],
)
def test_pose_config_handling(fixture_name, device, expected, request):
    """Test `load` with Pose reader with various config file scenarios."""
    data_dir = request.getfixturevalue(fixture_name)
    with expected:
        data = aeon.load(data_dir, device)
        assert len(data) > 0


def test_pose_with_model_provenance(pose_sleap_centered_instance_root_dir):
    """Test that `include_model` parameter adds 'model' column to output
    for keeping track of model provenance.
    """
    data = aeon.load(pose_sleap_centered_instance_root_dir, social03.CameraTop.Pose, include_model=True)
    assert len(data) > 0
    assert "model" in data.columns
