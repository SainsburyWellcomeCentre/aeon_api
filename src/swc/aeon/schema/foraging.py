"""Classes for defining foraging rig configuration models."""

from enum import StrEnum

from pydantic import Field

from swc.aeon.schema.base import BaseSchema
from swc.aeon.schema.harp import HarpOutputExpander


class UndergroundFeeder(HarpOutputExpander):
    """Represents control and acquisition functionality for an underground feeder module."""

    pellet_delivery_retry_count: int = Field(
        default=2,
        ge=0,
        description="The number of times to retry a failed pellet delivery.",
    )
    pellet_delivery_timeout: float = Field(
        default=1,
        ge=0,
        description="The amount of time to wait for pellet detection before reporting a failure.",
    )
    wheel_radius: float = Field(default=-4.0, description="The radius of the wheel, in centimeters.")


class FeederCommand(StrEnum):
    """Specifies the type of command to send to an underground feeder controller."""

    DELIVER_PELLET = "DeliverPellet"
    RESET_FEEDER = "ResetFeeder"


class CreateFeederCommand(BaseSchema):
    """Creates a command to send to an underground feeder controller."""

    command: FeederCommand = Field(
        default=FeederCommand.DELIVER_PELLET, description="The command to send to the feeder device."
    )
