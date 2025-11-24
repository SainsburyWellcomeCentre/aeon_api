from typing import ClassVar, Literal
from pydantic import Field
from swc.aeon.rigs.base import Device


class HarpDevice(Device):
    who_am_i: ClassVar[int] = Field(description="The unique identifier for the device type.")
    port_name: str = Field(examples=["COM"], description="The name of the device serial port.")


class HarpInputExpander(HarpDevice):
    device_type: Literal["HarpInputExpander"] = "HarpInputExpander"
    who_am_i: ClassVar[int] = 1106


class HarpOutputExpander(HarpDevice):
    device_type: Literal["HarpOutputExpander"] = "HarpOutputExpander"
    who_am_i: ClassVar[int] = 1108


class HarpClockSynchronizer(HarpDevice):
    device_type: Literal["HarpClockSynchronizer"] = "HarpClockSynchronizer"
    who_am_i: ClassVar[int] = 1152


class HarpTimestampGeneratorGen3(HarpDevice):
    device_type: Literal["HarpTimestampGeneratorGen3"] = "HarpTimestampGeneratorGen3"
    who_am_i: ClassVar[int] = 1158


class HarpCameraController(HarpDevice):
    device_type: Literal["HarpCameraController"] = "HarpCameraController"
    who_am_i: ClassVar[int] = 1168


class HarpCameraControllerGen2(HarpDevice):
    device_type: Literal["HarpCameraControllerGen2"] = "HarpCameraControllerGen2"
    who_am_i: ClassVar[int] = 1170


class HarpBehavior(HarpDevice):
    device_type: Literal["HarpBehavior"] = "HarpBehavior"
    who_am_i: ClassVar[int] = 1216


class HarpAudioSwitch(HarpDevice):
    device_type: Literal["HarpAudioSwitch"] = "HarpAudioSwitch"
    who_am_i: ClassVar[int] = 1248


class HarpSoundCard(HarpDevice):
    device_type: Literal["HarpSoundCard"] = "HarpSoundCard"
    who_am_i: ClassVar[int] = 1280


class HarpRfidReader(HarpDevice):
    device_type: Literal["HarpRfidReader"] = "HarpRfidReader"
    who_am_i: ClassVar[int] = 2094
