"""Tests for the Pose stream."""

from pathlib import Path

import pytest

from swc import aeon
from tests.schema import social02, social03

pose_path = Path(__file__).parent.parent / "data" / "pose"
topdown_path = pose_path / "topdown"


@pytest.mark.api
def test_Pose_read_local_model_dir():
    """Test that the Pose reader can read a local model directory."""
    data = aeon.load(topdown_path, social02.CameraTop.Pose)
    assert len(data) > 0


@pytest.mark.api
def test_Pose_read_local_model_dir_with_register_prefix():
    """Test that the Pose reader can read a local model directory with a register prefix."""
    data = aeon.load(topdown_path, social03.CameraTop.Pose)
    assert len(data) > 0


@pytest.mark.api
def test_Pose_read_local_model_dir_with_missing_config_file():
    """Test that the Pose reader raises FileNotFoundError when config file is missing."""
    with pytest.raises(FileNotFoundError):
        aeon.load(pose_path / "missing-config-file", social03.CameraTop.Pose)


@pytest.mark.api
def test_Pose_read_local_model_dir_with_missing_class_vectors():
    """Test that the Pose reader raises KeyError when config file does not contain class vectors."""
    with pytest.raises(KeyError):
        aeon.load(pose_path / "missing-class-vectors", social03.CameraTop.Pose)


@pytest.mark.api
def test_Pose_read_local_model_dir_with_missing_part_names():
    """Test that the Pose reader raises KeyError when config file does not contain part names."""
    with pytest.raises(KeyError):
        aeon.load(pose_path / "missing-part-names", social03.CameraTop.Pose)


if __name__ == "__main__":
    pytest.main()
