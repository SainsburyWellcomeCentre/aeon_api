"""Classes for defining Harp device configuration models."""

from typing import ClassVar

from pydantic import Field

from swc.aeon.schema.base import BaseSchema


class HarpDevice(BaseSchema):
    """A base class for creating Harp device models."""

    who_am_i: ClassVar[int] = Field(description="The unique identifier for the device type.")
    port_name: str = Field(examples=["COM"], description="The name of the device serial port.")


class HarpInputExpander(HarpDevice):
    """Represents a Harp Input Expander device."""

    who_am_i: ClassVar[int] = 1106


class HarpOutputExpander(HarpDevice):
    """Represents a Harp Output Expander device."""

    who_am_i: ClassVar[int] = 1108


class HarpClockSynchronizer(HarpDevice):
    """Represents a Harp Clock Synchronizer device."""

    who_am_i: ClassVar[int] = 1152


class HarpTimestampGeneratorGen3(HarpDevice):
    """Represents a Harp Timestamp Generator Gen3 device."""

    who_am_i: ClassVar[int] = 1158


class HarpCameraController(HarpDevice):
    """Represents a Harp Camera Controller device."""

    who_am_i: ClassVar[int] = 1168


class HarpCameraControllerGen2(HarpDevice):
    """Represents a Harp Camera Controller Gen2 device."""

    who_am_i: ClassVar[int] = 1170


class HarpBehavior(HarpDevice):
    """Represents a Harp Behavior device."""

    who_am_i: ClassVar[int] = 1216


class HarpAudioSwitch(HarpDevice):
    """Represents a Harp Audio Switch device."""

    who_am_i: ClassVar[int] = 1248


class HarpSoundCard(HarpDevice):
    """Represents a Harp Sound Card device."""

    who_am_i: ClassVar[int] = 1280


class HarpRfidReader(HarpDevice):
    """Represents a Harp RFID Reader device."""

    who_am_i: ClassVar[int] = 2094
