"""Tests for classes defining data streams and devices."""

from contextlib import nullcontext

import pytest

from swc.aeon.schema.streams import Device, Stream, StreamGroup


class DummyReader:
    """A dummy Reader for creating Streams."""

    def __init__(self, pattern):
        """Initialise the DummyReader with a pattern."""
        self.pattern = pattern


class DummyStream(Stream):
    """A dummy Stream."""

    def __init__(self, pattern):
        """Initialise the Stream with a DummyReader."""
        super().__init__(DummyReader(f"{pattern}"))


class DummyNestedStream(Stream):
    """A dummy Stream used in a nested StreamGroup."""

    def __init__(self, pattern):
        """Initialise the Stream with a DummyReader."""
        super().__init__(DummyReader(f"nested: {pattern}"))


class DummyNestedStreamGroup(StreamGroup):
    """A dummy StreamGroup nested in another StreamGroup."""

    def __init__(self, pattern):
        """Initialise the StreamGroup with a Stream."""
        super().__init__(pattern, DummyNestedStream)


class DummyStreamGroup(StreamGroup):
    """A dummy StreamGroup."""

    def __init__(self, pattern):
        """Initialise the StreamGroup with a Stream and a nested StreamGroup."""
        super().__init__(pattern, DummyStream, DummyNestedStreamGroup)


class DeprecatedStreamGroup:
    """A legacy StreamGroup without a custom __init__."""

    @staticmethod
    def dummy_composite_stream(pattern):
        """Mimic the output of `compositeStream` (deprecated and removed)."""
        return {"DummyStream": DummyReader(pattern)}


def test_stream_iter():
    """Test that Stream.__iter__ yields the correct name and reader with
    the expected pattern.
    """
    s = DummyStream("pattern")
    name, reader = next(iter(s))
    assert name == "DummyStream"
    assert reader.pattern == "pattern"


def test_streamgroup_iter():
    """Test that StreamGroup.__iter__ yields all stream names and readers,
    including nested streams.
    """
    sg = DummyStreamGroup("pattern")
    items = list(iter(sg))
    names, readers = zip(*items, strict=False)
    expected_names = ("DummyStream", "DummyNestedStream")
    expected_patterns = ["pattern", "nested: pattern"]
    assert names == expected_names
    assert [reader.pattern for reader in readers] == expected_patterns


@pytest.mark.parametrize(
    ("name", "args", "path", "expected_streams"),
    [
        ("DummyStream", DummyStream, None, None),
        (
            "multi_stream_device",
            DummyStreamGroup,
            "custom/path",
            {"DummyStream", "DummyNestedStream"},
        ),
        (
            "deprecated_device",
            DeprecatedStreamGroup,
            None,
            {"DummyStream"},
        ),
    ],
    ids=["Singleton device", "Multi-stream device", "Deprecated StreamGroup device"],
)
def test_device_iter(name, args, path, expected_streams):
    """Test that Device.__iter__ yields the correct device name and streams."""
    context = (
        pytest.warns(DeprecationWarning, match="Stream group classes with default constructors")
        if name == "deprecated_device"
        else nullcontext()
    )
    with context:
        d = Device(name, args, path=path)
    device_name, device_streams = list(iter(d))
    assert device_name == name
    expected_pattern = path if path is not None else name
    if name == "DummyStream":  # Singleton device
        # The reader is paired directly with the device
        assert device_streams.pattern == expected_pattern  # type: ignore
    else:
        assert set(device_streams.keys()) == expected_streams
        assert device_streams["DummyStream"].pattern == expected_pattern
        if "DummyNestedStream" in device_streams:
            assert device_streams["DummyNestedStream"].pattern == f"nested: {expected_pattern}"


def test_device_name_cannot_be_none():
    """Test that Device raises ValueError if name is None."""
    with pytest.raises(ValueError, match="name cannot be None"):
        Device(name=None)
