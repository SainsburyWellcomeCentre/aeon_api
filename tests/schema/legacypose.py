"""Stream classes for the legacy pose API."""

import swc.aeon.io.reader as _reader
from swc.aeon.schema import Stream


class Pose(Stream):
    """Pose tracking data stream."""

    def __init__(self, path):
        """Initializes the Pose stream."""
        super().__init__(_reader.Pose(f"{path}_test-node1*", "/ceph/aeon/aeon/data/processed"))
