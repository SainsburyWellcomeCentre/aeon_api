from typing import ClassVar, Literal
from pydantic import Field
from swc.aeon.rigs.base import Device


class HarpDevice(Device):
    """A base class for creating Harp device models."""

    who_am_i: ClassVar[int] = Field(description="The unique identifier for the device type.")
    port_name: str = Field(examples=["COM"], description="The name of the device serial port.")


class HarpInputExpander(HarpDevice):
    """Represents a Harp Input Expander device."""

    device_type: Literal["HarpInputExpander"] = "HarpInputExpander"
    who_am_i: ClassVar[int] = 1106


class HarpOutputExpander(HarpDevice):
    """Represents a Harp Output Expander device."""

    device_type: Literal["HarpOutputExpander"] = "HarpOutputExpander"
    who_am_i: ClassVar[int] = 1108


class HarpClockSynchronizer(HarpDevice):
    """Represents a Harp Clock Synchronizer device."""

    device_type: Literal["HarpClockSynchronizer"] = "HarpClockSynchronizer"
    who_am_i: ClassVar[int] = 1152


class HarpTimestampGeneratorGen3(HarpDevice):
    """Represents a Harp Timestamp Generator Gen3 device."""

    device_type: Literal["HarpTimestampGeneratorGen3"] = "HarpTimestampGeneratorGen3"
    who_am_i: ClassVar[int] = 1158


class HarpCameraController(HarpDevice):
    """Represents a Harp Camera Controller device."""

    device_type: Literal["HarpCameraController"] = "HarpCameraController"
    who_am_i: ClassVar[int] = 1168


class HarpCameraControllerGen2(HarpDevice):
    """Represents a Harp Camera Controller Gen2 device."""

    device_type: Literal["HarpCameraControllerGen2"] = "HarpCameraControllerGen2"
    who_am_i: ClassVar[int] = 1170


class HarpBehavior(HarpDevice):
    """Represents a Harp Behavior device."""

    device_type: Literal["HarpBehavior"] = "HarpBehavior"
    who_am_i: ClassVar[int] = 1216


class HarpAudioSwitch(HarpDevice):
    """Represents a Harp Audio Switch device."""

    device_type: Literal["HarpAudioSwitch"] = "HarpAudioSwitch"
    who_am_i: ClassVar[int] = 1248


class HarpSoundCard(HarpDevice):
    """Represents a Harp Sound Card device."""

    device_type: Literal["HarpSoundCard"] = "HarpSoundCard"
    who_am_i: ClassVar[int] = 1280


class HarpRfidReader(HarpDevice):
    """Represents a Harp RFID Reader device."""

    device_type: Literal["HarpRfidReader"] = "HarpRfidReader"
    who_am_i: ClassVar[int] = 2094
