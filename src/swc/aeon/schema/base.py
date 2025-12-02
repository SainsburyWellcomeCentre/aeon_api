"""Base classes for defining experiment configuration and data models."""

import datetime
import os
from collections.abc import Callable
from functools import cached_property
from typing import Any, Self, TypeVar

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator
from pydantic.alias_generators import to_camel, to_pascal

from swc.aeon.io.reader import Reader


class BaseSchema(BaseModel):
    """The base class for all experiment configuration and data models."""

    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        field_title_generator=lambda n, _: to_pascal(n),
        populate_by_name=True,
        from_attributes=True,
    )

    _container_prefix: str = ""
    _container: "BaseSchema | None" = None

    def _join_pattern_prefix(self, pattern_prefix: str) -> str:
        return self._container_prefix

    def _resolve_pattern_prefix(self) -> str:
        container = self._container
        pattern_prefix = self._container_prefix
        while container is not None:
            pattern_prefix = container._join_pattern_prefix(pattern_prefix)
            container = container._container

        return pattern_prefix

    @model_validator(mode="after")
    def _validate_container_prefix(self) -> Self:
        for name in self.__class__.model_fields:
            f = getattr(self, name)
            if isinstance(f, dict):
                for nk, nv in f.items():
                    if isinstance(nv, BaseSchema):
                        nv._container_prefix = nk
                        nv._container = self
            elif isinstance(f, BaseSchema):
                f._container_prefix = to_pascal(name)
                f._container = self
        return self


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


class Dataset(BaseSchema):
    """The base class for creating dataset models."""

    def _join_pattern_prefix(self, pattern_prefix: str) -> str:
        return os.path.join(self._container_prefix, pattern_prefix)


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


def data_reader(func: Callable[[_SelfBaseSchema, str], _ReaderT]) -> cached_property[_ReaderT]:
    """Decorator to include a data reader as `cached_property` in experiment dataset models."""

    def decorator(self: _SelfBaseSchema) -> _ReaderT:
        pattern_prefix = self._resolve_pattern_prefix()  # pyright: ignore[reportPrivateUsage]
        return func(self, pattern_prefix)

    return cached_property(decorator)
