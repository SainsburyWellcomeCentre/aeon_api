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
def monotonic_data(monotonic_dir):
    """Returns path to monotonic data file."""
    return monotonic_dir / "2022-06-13T13_14_25" / "Patch2" / "Patch2_90_2022-06-13T12-00-00.bin"


@pytest.fixture
def nonmonotonic_data(nonmonotonic_dir):
    """Returns path to nonmonotonic data file."""
    return nonmonotonic_dir / "2022-06-06T09-24-28" / "Patch2" / "Patch2_90_2022-06-06T13-00-00.bin"
