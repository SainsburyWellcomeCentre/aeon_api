"""Contains schemas used to load sample data in test functions."""

from dotmap import DotMap

from swc.aeon.schema import Device
from swc.aeon.schema import core as stream
from tests.schema import foraging, legacypose, pose

exp02 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("ExperimentalMetadata", stream.Environment, stream.MessageLog),
        Device("CameraTop", stream.Video, stream.Position),
        Device("Patch1", foraging.Patch),
        Device("Patch2", foraging.Patch),
    ]
)

social02 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("CameraTop", stream.Video, legacypose.Pose),
    ]
)

social03 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("CameraTop", stream.Video, pose.Pose),
    ]
)

__all__ = ["exp02", "social02", "social03"]
