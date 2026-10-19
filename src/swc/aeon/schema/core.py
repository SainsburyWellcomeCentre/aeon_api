"""Schema definition for core Harp data streams."""

import warnings

from typing_extensions import deprecated

import swc.aeon.io.reader as _reader
from swc.aeon.schema.streams import STREAM_API_DEPRECATION, Stream, StreamGroup

warnings.warn(STREAM_API_DEPRECATION, DeprecationWarning, stacklevel=2)


@deprecated(STREAM_API_DEPRECATION, category=None)
class Heartbeat(Stream):
    """Heartbeat event for Harp devices."""

    def __init__(self, pattern):
        """Initializes the Heartbeat stream."""
        super().__init__(_reader.Heartbeat(f"{pattern}_8_*"))


@deprecated(STREAM_API_DEPRECATION, category=None)
class Video(Stream):
    """Video frame metadata."""

    def __init__(self, pattern):
        """Initializes the Video stream."""
        super().__init__(_reader.Video(f"{pattern}_*"))


@deprecated(STREAM_API_DEPRECATION, category=None)
class Position(Stream):
    """Position tracking data for the specified camera."""

    def __init__(self, pattern):
        """Initializes the Position stream."""
        super().__init__(_reader.Position(f"{pattern}_200_*"))


@deprecated(STREAM_API_DEPRECATION, category=None)
class Encoder(Stream):
    """Wheel magnetic encoder data."""

    def __init__(self, pattern):
        """Initializes the Encoder stream."""
        super().__init__(_reader.Encoder(f"{pattern}_90_*"))


@deprecated(STREAM_API_DEPRECATION, category=None)
class Environment(StreamGroup):
    """Metadata for environment mode and subjects."""

    def __init__(self, pattern):
        """Initializes the Environment stream group."""
        super().__init__(pattern, EnvironmentState, SubjectState)


@deprecated(STREAM_API_DEPRECATION, category=None)
class EnvironmentState(Stream):
    """Environment state log."""

    def __init__(self, pattern):
        """Initializes the EnvironmentState stream."""
        super().__init__(_reader.Csv(f"{pattern}_EnvironmentState_*", ["state"]))


@deprecated(STREAM_API_DEPRECATION, category=None)
class SubjectState(Stream):
    """Subject state log."""

    def __init__(self, pattern):
        """Initialises the SubjectState stream."""
        super().__init__(_reader.Subject(f"{pattern}_SubjectState_*"))


@deprecated(STREAM_API_DEPRECATION, category=None)
class MessageLog(Stream):
    """Message log data."""

    def __init__(self, pattern):
        """Initializes the MessageLog stream."""
        super().__init__(_reader.Log(f"{pattern}_MessageLog_*"))


@deprecated(STREAM_API_DEPRECATION, category=None)
class Metadata(Stream):
    """Metadata for acquisition epochs."""

    def __init__(self, pattern):
        """Initializes the Metadata stream."""
        super().__init__(_reader.Metadata(pattern))
