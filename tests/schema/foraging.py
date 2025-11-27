"""Stream classes used to load sample data in I/O test fixtures."""

import swc.aeon.io.reader as _reader
import swc.aeon.schema.core as _stream
from swc.aeon.schema.streams import Stream, StreamGroup


class _PatchState(_reader.Csv):
    """Extracts patch state data for linear depletion foraging patches.

    Columns:
        threshold (float): Distance to travel before the next pellet is delivered.
        d1 (float): y-intercept of the line specifying the depletion function.
        delta (float): Slope of the linear depletion function.
    """

    def __init__(self, pattern):
        super().__init__(pattern, columns=["threshold", "d1", "delta"])


class DepletionFunction(Stream):
    """State of the linear depletion function for foraging patches."""

    def __init__(self, pattern):
        """Initializes the DepletionFunction stream."""
        super().__init__(_PatchState(f"{pattern}_State_*"))


class Feeder(StreamGroup):
    """Feeder commands and events."""

    def __init__(self, pattern):
        """Initializes the Feeder stream."""
        super().__init__(pattern, BeamBreak, DeliverPellet)


class BeamBreak(Stream):
    """Beam break events for pellet detection."""

    def __init__(self, pattern):
        """Initializes the BeamBreak stream."""
        super().__init__(_reader.BitmaskEvent(f"{pattern}_32_*", 0x22, "PelletDetected"))


class DeliverPellet(Stream):
    """Pellet delivery commands."""

    def __init__(self, pattern):
        """Initializes the DeliverPellet stream."""
        super().__init__(_reader.BitmaskEvent(f"{pattern}_35_*", 0x01, "TriggerPellet"))


class Patch(StreamGroup):
    """Data streams for a patch."""

    def __init__(self, pattern):
        """Initializes the Patch stream."""
        super().__init__(pattern, DepletionFunction, _stream.Encoder, Feeder)
