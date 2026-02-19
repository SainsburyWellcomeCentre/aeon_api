"""Classes for defining video capture device configuration models."""

from pydantic import Field

from swc.aeon.schema.base import BaseSchema


class SpinnakerCamera(BaseSchema):
    """Represents a FLIR Spinnaker camera acquisition module."""

    serial_number: str = Field(examples=["00000"], description="The serial number of the camera.")
    exposure_time: float = Field(
        default=1000,
        ge=100,
        description="The exposure time of the sensor, in microseconds.",
    )
    gain: float = Field(default=0, ge=0, description="The camera gain.")
    binning: int = Field(default=1, ge=1, description="The camera binning configuration.")
