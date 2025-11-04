from typing import ClassVar
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel, to_pascal


class BaseSchema(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        field_title_generator=lambda n, _: to_pascal(n),
        populate_by_name=True,
        from_attributes=True,
    )


class Device(BaseSchema):
    device_type: ClassVar[str] = Field(..., description="The type of the device.")
    name: str | None = Field(description="The name of the device instance.")
