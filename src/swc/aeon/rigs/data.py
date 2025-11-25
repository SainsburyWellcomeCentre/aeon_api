from typing import Dict, Self
from pydantic import model_validator
from pydantic.alias_generators import to_pascal
from swc.aeon.rigs.base import BaseSchema


class DataSchema(BaseSchema):
    """A mixin class for propagating device name across properties and collections."""

    _device_name: str = ""

    @model_validator(mode="after")
    def _ensure_device_name(self) -> Self:
        for name in self.__class__.model_fields:
            f = getattr(self, name)
            if isinstance(f, Dict):
                for nk, nv in f.items():
                    if isinstance(nv, DataSchema):
                        nv._device_name = nk
            if isinstance(f, DataSchema):
                f._device_name = to_pascal(name)
        return self
