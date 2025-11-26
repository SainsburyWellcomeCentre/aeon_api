"""Base classes for defining experiment and rig configuration models."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel, to_pascal


class BaseSchema(BaseModel):
    """The base class for all Aeon rig and experiment models."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        field_title_generator=lambda n, _: to_pascal(n),
        populate_by_name=True,
        from_attributes=True,
    )


class Device(BaseSchema):
    """The base class for creating Aeon hardware device models."""

    device_type: Any = Field(description="The type of the device.")
