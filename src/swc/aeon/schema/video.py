"""Classes for defining video capture device configuration models."""

import warnings

from pydantic import Field
from typing_extensions import deprecated

from swc.aeon.schema.base import DOMAIN_MODEL_DEPRECATION, BaseSchema

warnings.warn(DOMAIN_MODEL_DEPRECATION, DeprecationWarning, stacklevel=2)


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
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
