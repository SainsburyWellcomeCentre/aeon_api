"""Classes for defining environment configuration models."""

import warnings

from pydantic import Field
from typing_extensions import deprecated

from swc.aeon.schema.base import DOMAIN_MODEL_DEPRECATION, BaseSchema

warnings.warn(DOMAIN_MODEL_DEPRECATION, DeprecationWarning, stacklevel=2)


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class LightCycle(BaseSchema):
    """Represents a network client for automated room light control."""

    event_socket: str = Field(
        default=">tcp://localhost:4303",
        description="Specifies the endpoint from which the Light Server publishes events.",
    )
    command_socket: str = Field(
        default=">tcp://localhost:4304",
        description="Specifies the endpoint to send commands to the Light Server.",
    )
    room_name: str = Field(description="The name of the room to monitor and control.")
    config_file_name: str = Field(
        default="lightcycle.config",
        description="The name of the CSV file describing the light model, "
        "where each row represents one whole minute and the red, cold white "
        "and warm white, light levels set for that minute.",
    )


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class WeightScale(BaseSchema):
    """Represents acquisition functionality for automated habitat weighing scales."""

    port_name: str = Field(examples=["COM"], description="The name of the device serial port.")
    filter_window: int = Field(
        default=40, description="Sliding window size of the weight linear regression filter."
    )
