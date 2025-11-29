"""Base classes for defining experiment configuration and data models."""

import datetime
from collections.abc import Callable
from functools import cached_property
from typing import Any, Self, TypeVar

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from pydantic.alias_generators import to_camel, to_pascal

from swc.aeon.io.reader import Reader


class BaseSchema(BaseModel):
    """The base class for all experiment configuration and data models."""

    _pattern: str = ""
    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        field_title_generator=lambda n, _: to_pascal(n),
        populate_by_name=True,
        from_attributes=True,
    )


class Experiment(BaseSchema):
    """The base class for creating experiment models."""

    workflow: str = Field(description="Path to the workflow running the experiment.")
    commit: str = Field(description="Commit hash of the experiment repo.")
    repository_url: str = Field(
        description="The URL of the git repository used to version experiment source code."
    )


class Device(BaseSchema):
    """The base class for creating hardware device models."""

    device_type: Any = Field(description="The type of the device.")


class DataSchema(BaseSchema):
    """The base class for creating experiment data models."""

    @model_validator(mode="after")
    def _ensure_device_name(self) -> Self:
        for name in self.__class__.model_fields:
            f = getattr(self, name)
            if isinstance(f, dict):
                for nk, nv in f.items():
                    if isinstance(nv, BaseSchema):
                        nv._pattern = nk
            if isinstance(f, BaseSchema):
                f._pattern = to_pascal(name)
        return self


ModelT = TypeVar("ModelT", bound=BaseSchema)


class Metadata(Reader):
    """Extracts metadata information from all epochs in the dataset."""

    def __init__(self, type: type[ModelT], pattern="Metadata"):  # noqa: A002
        """Initialize the reader object with the specified model type and optional pattern."""
        super().__init__(pattern, columns=["metadata", "epoch"], extension="json")
        self.type = TypeAdapter(type)

    def read(self, file):
        """Returns metadata for the epoch associated with the specified file."""
        epoch_str = file.parts[-2]
        date_str, time_str = epoch_str.split("T")
        time = datetime.datetime.fromisoformat(date_str + "T" + time_str.replace("-", ":"))
        metadata = file.read_text()
        data = {"metadata": [self.type.validate_json(metadata)], "epoch": [epoch_str]}
        return pd.DataFrame(data, index=pd.Series(time), columns=self.columns)


_SelfBaseSchema = TypeVar("_SelfBaseSchema", bound=BaseSchema)
_ReaderT = TypeVar("_ReaderT", bound=Reader)


def reader_factory(func: Callable[[_SelfBaseSchema, str], _ReaderT]) -> cached_property[_ReaderT]:
    """Decorator to include stream reader factory as `cached_property` in experiment data models."""

    def decorator(self: _SelfBaseSchema) -> _ReaderT:
        return func(self, self._pattern)  # pyright: ignore[reportPrivateUsage]

    return cached_property(decorator)
