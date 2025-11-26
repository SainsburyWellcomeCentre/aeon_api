"""Base classes for defining experiment configuration and data stream models."""

from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel, to_pascal


class BaseSchema(BaseModel):
    """The base class for all experiment configuration and data stream models."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        field_title_generator=lambda n, _: to_pascal(n),
        populate_by_name=True,
        from_attributes=True,
    )


class Device(BaseSchema):
    """The base class for creating hardware device models."""

    device_type: Any = Field(description="The type of the device.")


class DataSchema(BaseSchema):
    """The base class for creating experiment data stream models."""

    _device_name: str = ""

    @model_validator(mode="after")
    def _ensure_device_name(self) -> Self:
        for name in self.__class__.model_fields:
            f = getattr(self, name)
            if isinstance(f, dict):
                for nk, nv in f.items():
                    if isinstance(nv, DataSchema):
                        nv._device_name = nk
            if isinstance(f, DataSchema):
                f._device_name = to_pascal(name)
        return self


class Experiment(BaseSchema):
    """The base class for creating experiment models."""

    workflow: str = Field(description="Path to the workflow running the experiment.")
    commit: str = Field(description="Commit hash of the experiment repo.")
    repository_url: str = Field(
        description="The URL of the git repository used to version experiment source code."
    )
