from typing import ClassVar
from pydantic import Field
from swc.aeon_rigs.base import Device


class SpinnakerCamera(Device):
    device_type: ClassVar[str] = "SpinnakerCamera"
    serial_number: str = Field(examples=["00000"], description="The serial number of the camera.")
    exposure_time: float = Field(
        default=1000,
        ge=100,
        description="The exposure time of the sensor, in microseconds.",
    )
    gain: float = Field(default=0, ge=0, description="The camera gain.")
    binning: int = Field(default=1, ge=1, description="The camera binning configuration.")
