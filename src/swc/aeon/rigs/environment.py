"""Classes for defining environment configuration models."""

from typing import Literal

from pydantic import Field

from swc.aeon.rigs.base import BaseSchema, Device


class LightCycle(BaseSchema):
    """Represents a network client for automated room light control."""

    event_socket: str = Field(
        default=">tcp://localhost:4303",
        description="Specifies the endpoint to send commands to the Light Server.",
    )
    command_socket: str = Field(
        default=">tcp://localhost:4304",
        description="Specifies the endpoint to send commands to the Light Server.",
    )
    room_name: str = Field(description="The name of the room to monitor and control.")
    config_file_name: str = Field(
        default="lightcycle.config",
        description="The name of the CSV file describing the light model, "
        "where each row represents one whole minute and the red, cold white"
        "and warm white, light levels set for that minute.",
    )


class WeightScale(Device):
    """Represents acquisition functionality for automated habitat weighing scales."""

    device_type: Literal["WeightScale"] = "WeightScale"
    port_name: str = Field(examples=["COM"], description="The name of the device serial port.")
    filter_window: int = Field(
        default=40, description="Sliding window size of the weight linear regression filter."
    )
