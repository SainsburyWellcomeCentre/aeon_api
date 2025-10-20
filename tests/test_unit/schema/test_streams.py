"""Tests for classes defining data streams and devices."""

from swc.aeon.schema.streams import Stream, StreamGroup


class DummyReader:
    """A dummy reader for creating Streams."""

    def __init__(self, pattern):
        """Initialises the DummyReader with a pattern."""
        self.pattern = pattern


class DummyStream(Stream):
    """A dummy stream."""

    def __init__(self, pattern):
        """Initialises the Stream with a DummyReader."""
        super().__init__(DummyReader(f"{pattern}"))


class DummyNestedStream(Stream):
    """A dummy stream used in a nested StreamGroup."""

    def __init__(self, pattern):
        """Initialises the Stream with a DummyReader."""
        super().__init__(DummyReader(f"nested: {pattern}"))


class DummyNestedStreamGroup(StreamGroup):
    """A dummy stream group nested in another StreamGroup."""

    def __init__(self, pattern):
        """Initialises the StreamGroup with two Streams."""
        super().__init__(pattern, DummyNestedStream, DummyNestedStream)


class DummyStreamGroup(StreamGroup):
    """A dummy stream group."""

    def __init__(self, pattern):
        """Initialises the StreamGroup with a Stream and a nested StreamGroup."""
        super().__init__(pattern, DummyStream, DummyNestedStreamGroup)


def test_stream_iter_yields_stream_name_and_reader():
    """Test that Stream.__iter__ yields the correct name and reader."""
    s = DummyStream("pattern")
    assert list(iter(s)) == [("DummyStream", s.reader)]


def test_streamgroup_iter_yields_all_stream_names_and_readers():
    """Test that StreamGroup.__iter__ yields all stream names and readers,
    including nested streams.
    """
    sg = DummyStreamGroup("pattern")
    items = list(iter(sg))
    names, readers = zip(*items, strict=False)
    assert names == ("DummyStream", "DummyNestedStream", "DummyNestedStream")
    assert all(isinstance(reader, DummyReader) for reader in readers)
