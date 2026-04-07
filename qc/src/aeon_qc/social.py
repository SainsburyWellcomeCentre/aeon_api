"""Stream classes for social experiment data (social02, social03, social04).

Ported from ``aeon_mecha/aeon/schema/social_02.py``.
"""

import swc.aeon.io.reader as _reader
import swc.aeon.schema.core as _stream
from swc.aeon.schema.streams import Stream, StreamGroup

from aeon_qc.foraging import Feeder


class BlockState(Stream):
    """Block state log for social experiments."""

    def __init__(self, path):
        """Initializes the BlockState stream."""
        super().__init__(
            _reader.Csv(f"{path}_BlockState_*", columns=["pellet_ct", "pellet_ct_thresh", "due_time"])
        )


class LightEvents(Stream):
    """Light channel events."""

    def __init__(self, path):
        """Initializes the LightEvents stream."""
        super().__init__(_reader.Csv(f"{path}_LightEvents_*", columns=["channel", "value"]))


class Environment(StreamGroup):
    """Environment state streams for social experiments."""

    def __init__(self, path):
        """Initializes the Environment stream group."""
        super().__init__(path)

    EnvironmentState = _stream.EnvironmentState
    BlockState = BlockState
    LightEvents = LightEvents
    MessageLog = _stream.MessageLog


class SubjectState(Stream):
    """Subject state log."""

    def __init__(self, path):
        """Initializes the SubjectState stream."""
        super().__init__(_reader.Csv(f"{path}_SubjectState_*", columns=["id", "weight", "type"]))


class SubjectVisits(Stream):
    """Subject visit events."""

    def __init__(self, path):
        """Initializes the SubjectVisits stream."""
        super().__init__(_reader.Csv(f"{path}_SubjectVisits_*", columns=["id", "type", "region"]))


class SubjectWeight(Stream):
    """Subject weight measurements."""

    def __init__(self, path):
        """Initializes the SubjectWeight stream."""
        super().__init__(
            _reader.Csv(
                f"{path}_SubjectWeight_*", columns=["weight", "confidence", "subject_id", "int_id"]
            )
        )


class SubjectData(StreamGroup):
    """All subject-related data streams."""

    def __init__(self, path):
        """Initializes the SubjectData stream group."""
        super().__init__(path)

    SubjectState = SubjectState
    SubjectVisits = SubjectVisits
    SubjectWeight = SubjectWeight


class Pose(Stream):
    """Pose tracking data from SLEAP (legacy test-node1 pattern)."""

    def __init__(self, path):
        """Initializes the Pose stream."""
        super().__init__(_reader.Pose(f"{path}_test-node1*"))


class WeightRaw(Stream):
    """Raw weight measurement from nest scale (register 200)."""

    def __init__(self, path):
        """Initializes the WeightRaw stream."""
        super().__init__(_reader.Harp(f"{path}_200_*", columns=["weight(g)", "stability"]))


class WeightFiltered(Stream):
    """Filtered weight measurement from nest scale (register 202)."""

    def __init__(self, path):
        """Initializes the WeightFiltered stream."""
        super().__init__(_reader.Harp(f"{path}_202_*", columns=["weight(g)", "stability"]))


class DepletionState(Stream):
    """Depletion state log for social experiment patches."""

    def __init__(self, path):
        """Initializes the DepletionState stream."""
        super().__init__(_reader.Csv(f"{path}_State_*", columns=["threshold", "offset", "rate"]))


class ManualDelivery(Stream):
    """Manual pellet delivery events (register 201)."""

    def __init__(self, path):
        """Initializes the ManualDelivery stream."""
        super().__init__(_reader.Harp(f"{path}_201_*", columns=["manual_delivery"]))


class MissedPellet(Stream):
    """Missed pellet detection events (register 202)."""

    def __init__(self, path):
        """Initializes the MissedPellet stream."""
        super().__init__(_reader.Harp(f"{path}_202_*", columns=["missed_pellet"]))


class RetriedDelivery(Stream):
    """Retried pellet delivery events (register 203)."""

    def __init__(self, path):
        """Initializes the RetriedDelivery stream."""
        super().__init__(_reader.Harp(f"{path}_203_*", columns=["retried_delivery"]))


class Patch(StreamGroup):
    """Data streams for a social experiment patch."""

    def __init__(self, path):
        """Initializes the Patch stream group."""
        super().__init__(
            path, DepletionState, _stream.Encoder, Feeder, ManualDelivery, MissedPellet, RetriedDelivery
        )


class RfidEvents(Stream):
    """RFID tag detection events (register 32)."""

    def __init__(self, path):
        """Initializes the RfidEvents stream."""
        super().__init__(_reader.Harp(f"{path}_32_*", columns=["rfid"]))
