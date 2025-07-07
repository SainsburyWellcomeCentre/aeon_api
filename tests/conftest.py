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
