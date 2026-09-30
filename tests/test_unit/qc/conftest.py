"""Shared pytest fixtures for the swc.aeon.qc unit tests."""

import pytest

from swc.aeon.io.reader import Heartbeat, Video


@pytest.fixture
def heartbeat_reader():
    """Return a Heartbeat reader instance for tests."""
    return Heartbeat("TestDevice_8_*")


@pytest.fixture
def video_reader():
    """Return a Video reader instance for tests."""
    return Video("CameraTop_*")
