"""Tests for schema definitions for core Harp data streams."""

import pytest

import swc.aeon.io.reader as _reader
from swc.aeon.schema import core as core_mod


class MockReader:
    """Generic mock for reader classes that stores init args."""

    def __init__(self, *args, **kwargs):
        """Initialises the MockReader."""
        self._args = args
        self._kwargs = kwargs

    @property
    def pattern(self):
        """Returns the pattern argument, if present."""
        return self._args[0] if self._args else None

    @property
    def args(self):
        """Returns the positional arguments."""
        return self._args

    @property
    def kwargs(self):
        """Returns the keyword arguments."""
        return self._kwargs


@pytest.fixture(autouse=True)
def _patch_readers(monkeypatch):
    """Replace reader classes used by core.py with a simple MockReader."""
    for name in ("Heartbeat", "Video", "Position", "Encoder", "Csv", "Subject", "Log", "Metadata"):
        monkeypatch.setattr(_reader, name, MockReader)


@pytest.mark.parametrize(
    ("klass", "pattern", "expected"),
    [
        (core_mod.Heartbeat, "Patch1", "Patch1_8_*"),
        (core_mod.Video, "Camera", "Camera_*"),
        (core_mod.Position, "Camera", "Camera_200_*"),
        (core_mod.Encoder, "Patch2", "Patch2_90_*"),
        (core_mod.MessageLog, "Environment", "Environment_MessageLog_*"),
        (core_mod.Metadata, "Metadata", "Metadata"),
    ],
)
def test_stream_classes_wrap_readers(klass, pattern, expected):
    """Verify each Stream subclass constructs the correct reader pattern."""
    inst = klass(pattern)
    assert hasattr(inst, "reader")
    assert isinstance(inst.reader, MockReader)
    assert inst.reader.pattern == expected


def test_environment_group_yields_nested_streams():
    """Environment is a StreamGroup and should yield its nested streams."""
    env = core_mod.Environment("ExperimentalMetadata")
    items = list(iter(env))
    names, readers = zip(*items, strict=False)
    assert names == ("EnvironmentState", "SubjectState")
    assert readers[0].pattern == "ExperimentalMetadata_EnvironmentState_*"
    assert readers[0].args[1] == ["state"]
    assert readers[1].pattern == "ExperimentalMetadata_SubjectState_*"
