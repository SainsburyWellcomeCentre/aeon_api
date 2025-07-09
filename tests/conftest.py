"""Fixtures and configurations shared by the entire test suite."""

from pathlib import Path

import pytest


@pytest.fixture
def test_data_dir():
    """Returns path to test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture
def monotonic_dir(test_data_dir):
    """Returns path to monotonic data directory."""
    return test_data_dir / "monotonic"


@pytest.fixture
def nonmonotonic_dir(test_data_dir):
    """Returns path to nonmonotonic data directory."""
    return test_data_dir / "nonmonotonic"


@pytest.fixture
def pose_dir(test_data_dir):
    """Returns path to pose data directory."""
    return test_data_dir / "pose"


@pytest.fixture
def pose_missing_config_file_dir(pose_dir):
    """Returns path to pose model directory missing the required config file."""
    return pose_dir / "missing-config-file"


@pytest.fixture
def pose_sleap_topdown_root_dir(pose_dir):
    """Returns path to pose SLEAP topdown model directory."""
    return pose_dir / "topdown" / "2024-03-01T16-46-12" / "CameraTop"


@pytest.fixture
def pose_sleap_topdown_config_dir(pose_sleap_topdown_root_dir):
    """Returns path to pose SLEAP topdown model directory."""
    return pose_sleap_topdown_root_dir / "test-node1" / "topdown-multianimal-id-133"


@pytest.fixture
def pose_shared_sleap_topdown_config_dir(pose_dir):
    """Returns path to pose SLEAP topdown model directory with shared config."""
    return pose_dir / "shared-config-file"


@pytest.fixture
def monotonic_epoch():
    """Returns the name of the epoch with monotonic data."""
    return "2022-06-13T13_14_25"


@pytest.fixture
def nonmonotonic_epoch():
    """Returns the name of the epoch with nonmonotonic data."""
    return "2022-06-06T09-24-28"


@pytest.fixture
def monotonic_file(monotonic_dir, monotonic_epoch):
    """Returns path to monotonic data file."""
    return monotonic_dir / monotonic_epoch / "Patch2" / "Patch2_90_2022-06-13T12-00-00.bin"


@pytest.fixture
def nonmonotonic_file(nonmonotonic_dir, nonmonotonic_epoch):
    """Returns path to nonmonotonic data file."""
    return nonmonotonic_dir / nonmonotonic_epoch / "Patch2" / "Patch2_90_2022-06-06T13-00-00.bin"


@pytest.fixture
def metadata_file(nonmonotonic_dir, nonmonotonic_epoch):
    """Returns path to metadata file."""
    return nonmonotonic_dir / nonmonotonic_epoch / "Metadata.yml"


@pytest.fixture
def video_csv_file(monotonic_dir, monotonic_epoch):
    """Returns path to a CSV file containing video metadata."""
    return monotonic_dir / monotonic_epoch / "CameraTop" / "CameraTop_2022-06-13T12-00-00.csv"


@pytest.fixture
def empty_csv_file(tmp_path):
    """Returns path to an empty CSV file."""
    empty_csv_path = tmp_path / "empty.csv"
    empty_csv_path.touch()
    return empty_csv_path


@pytest.fixture
def jsonl_file(monotonic_dir):
    """Returns path to a JSONL file."""
    return (
        monotonic_dir
        / "2024-06-19T10-55-14"
        / "Environment"
        / "Environment_ActiveConfiguration_2024-06-20T00-00-00.jsonl"
    )


@pytest.fixture
def bitmaskevent_file(monotonic_dir, monotonic_epoch):
    """Returns path to monotonic data file."""
    return monotonic_dir / monotonic_epoch / "Patch2" / "Patch2_32_2022-06-13T12-00-00.bin"


@pytest.fixture
def pose_topdown_config_file(pose_sleap_topdown_config_dir):
    """Returns path to a SLEAP topdown model config file."""
    return pose_sleap_topdown_config_dir / "confmap_config.json"


@pytest.fixture
def pose_topdown_config_file_missing_part_names(pose_dir):
    """Returns path to a SLEAP topdown model config file that is missing the required 'part_names'."""
    return (
        pose_dir
        / "missing-part-names"
        / "2024-03-01T16-46-12"
        / "CameraTop"
        / "test-node1"
        / "model-missing-part-names"
        / "confmap_config.json"
    )


@pytest.fixture
def pose_unsupported_config_file(tmp_path):
    """Returns path to a dummy unsupported (name does not match 'confmap_config') pose config file."""
    dummy_file = tmp_path / "dummy_config.json"
    dummy_file.write_text('{"unsupported": "config"}')
    return dummy_file


@pytest.fixture
def pose_supported_config_file_missing_required_key(tmp_path):
    """Returns path to a supported (name matches 'confmap_config') pose config file
    that is missing required key(s). In this case, it is missing 'heads'.
    """
    dummy_file = tmp_path / "confmap_config.json"
    dummy_file.write_text('{"model": {"head": {}}}')
    return dummy_file


@pytest.fixture
def pose_topdown_legacy_data_file(pose_sleap_topdown_root_dir):
    """Returns path to a legacy (Bonsai.SLEAP0.2) topdown pose data file."""
    return (
        pose_sleap_topdown_root_dir
        / "CameraTop_test-node1_topdown-multianimal-id-133_2024-03-02T12-00-00.bin"
    )


@pytest.fixture
def pose_topdown_data_file(pose_sleap_topdown_root_dir):
    """Returns path to a SLEAP topdown pose data file."""
    return (
        pose_sleap_topdown_root_dir
        / "CameraTop_202_test-node1_topdown-multianimal-id-133_2024-03-02T12-00-00.bin"
    )


@pytest.fixture
def pose_missing_config_topdown_data_file(pose_missing_config_file_dir):
    """Returns path to a SLEAP topdown pose data file with missing config."""
    return (
        pose_missing_config_file_dir
        / "CameraTop_202_test-node1_model-missing-config-file_2024-03-02T12-00-00.bin"
    )


@pytest.fixture
def pose_centered_instance_data_file(pose_dir):
    """Returns path to a SLEAP centered instance pose data file."""
    return (
        pose_dir
        / "centered-instance"
        / "2024-02-09T16-07-32"
        / "CameraTop"
        / "CameraTop_202_5899248_2024-02-09T17-00-00.bin"
    )
