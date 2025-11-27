"""Stream classes for the pose API."""

import swc.aeon.io.reader as _reader
from swc.aeon.schema.streams import Stream


class Pose(Stream):
    """Pose tracking data stream."""

    def __init__(self, path):
        """Initializes the Pose stream."""
        super().__init__(_reader.Pose(f"{path}_202_*"))
