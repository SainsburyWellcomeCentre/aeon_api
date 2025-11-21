from pydantic import Field
from swc.aeon.rigs.harp import HarpOutputExpander


class UndergroundFeeder(HarpOutputExpander):
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
