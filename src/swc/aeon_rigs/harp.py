from typing import Literal
from pydantic import Field
from swc.aeon_rigs.base import BaseSchema


class HarpDevice(BaseSchema):
    device_type: str
    who_am_i: int | None = Field(description="The unique identifier for the device type.")
    port_name: str | None = Field(description="The name of the device serial port.")


class HarpOutputExpander(HarpDevice):
    device_type: Literal["OutputExpander"] = "OutputExpander"
    who_am_i: Literal[1108] = 1108


class HarpClockSynchronizer(HarpDevice):
    device_type: Literal["ClockSynchronizer"] = "ClockSynchronizer"
    who_am_i: Literal[1152] = 1152


class HarpTimestampGeneratorGen3(HarpDevice):
    device_type: Literal["TimestampGeneratorGen3"] = "TimestampGeneratorGen3"
    who_am_i: Literal[1158] = 1158
