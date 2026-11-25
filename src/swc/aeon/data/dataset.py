"""The base class for models declaring data readers, and the binding of datasets to their files."""

import re
from collections.abc import Callable
from enum import Enum
from typing import Any, Generic, Self, TypeVar, overload

from pydantic import BaseModel, PrivateAttr, RootModel, ValidationInfo, model_validator

from swc.aeon.io.reader import Reader

_ReaderT = TypeVar("_ReaderT", bound=Reader)


def _pascal_case(name: str) -> str:
    pascal = "".join(part[:1].upper() + part[1:] for part in re.split(r"[\W_]+", name))
    return pascal if pascal[:1].isalpha() else "_" + pascal


def _key(key: object) -> str:
    return str(key.value if isinstance(key, Enum) else key)


class Dataset(BaseModel):
    """A model with data readers for the files recorded during an acquisition epoch.

    Readers declared with `data_reader` find their files using the key of their dataset. The key
    is the prefix of the file names, and in an Aeon dataset it is also the name of the folder that
    holds the files.

    A dataset has a key only once the root model assigns it, when the root is validated, so the
    experiment model read from the metadata of an acquisition epoch must be a dataset itself. The
    root has no key of its own. It assigns a key to the first dataset nested below it on each
    path, through any models that are not datasets, and every further dataset shares that key.
    The key assigned is the name of the field that holds the dataset, converted to PascalCase, or
    the dictionary key, which validation rejects if it does not follow the naming convention.
    """

    _stem: str | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _bind_datasets(self, info: ValidationInfo) -> Self:
        if info.field_name is None:
            self._stem = None
            for name in type(self).model_fields:
                Dataset._bind(getattr(self, name), _pascal_case(name), None, False)
        return self

    @staticmethod
    def _bind(value: object, key: str | None, stem: str | None, keyed: bool) -> None:
        if isinstance(value, BaseModel):
            if isinstance(value, Dataset):
                if not keyed:
                    if key is not None and _pascal_case(key) != key:
                        raise ValueError(
                            f"Dataset key {key!r} must be in PascalCase, since it prefixes the file names."
                        )
                    stem, keyed = key, True
                value._stem = stem
            if isinstance(value, RootModel):
                Dataset._bind(value.root, key, stem, keyed)
            else:
                for name in type(value).model_fields:
                    Dataset._bind(getattr(value, name), _pascal_case(name), stem, keyed)
        elif isinstance(value, dict):
            for item_key, item in value.items():
                Dataset._bind(item, _key(item_key), stem, keyed)
        elif isinstance(value, list | tuple | set | frozenset):
            for item in value:
                Dataset._bind(item, None, stem, keyed)


class DataReader(property, Generic[_ReaderT]):
    """A property of a dataset that returns a reader for the files recorded under its key.

    Accessing the property calls the decorated function with the dataset and its key, and
    returns a new reader.

    Attributes:
        func: The function that creates the reader from the dataset and its key.
    """

    def __init__(self, func: Callable[[Any, str], _ReaderT]):
        """Declares a data reader whose reader is created by the specified function."""
        super().__init__(doc=func.__doc__)
        self.func = func
        self.name = func.__name__

    def __set_name__(self, owner: type, name: str) -> None:
        """Records the name of the data reader, and checks that it is declared by a dataset.

        Raises:
            TypeError: If the class declaring the reader is not a `Dataset`.
        """
        if not issubclass(owner, Dataset):
            raise TypeError(f"Data reader {name} must be declared by a Dataset, not {owner.__name__}.")
        self.name = name

    @overload
    def __get__(self, instance: None, owner: type | None = None) -> Self: ...
    @overload
    def __get__(self, instance: object, owner: type | None = None) -> _ReaderT: ...
    def __get__(self, instance: object | None, owner: type | None = None) -> "Self | _ReaderT":
        """Returns the reader for the files of the dataset, or the property itself from its class.

        Raises:
            ValueError: If the dataset has no key, since the root model holding it is not a
                dataset or was not validated, or it is the root model or an item of a list.
        """
        if instance is None:
            return self
        stem = instance._stem if isinstance(instance, Dataset) else None  # pyright: ignore[reportPrivateUsage]
        if stem is None:
            raise ValueError(
                f"Data reader {self.name} of {type(instance).__name__} has no dataset key. "
                "Validate the dataset holding it as the root model, in a field or a dictionary."
            )
        return self.func(instance, stem)


def data_reader(func: Callable[[Any, str], _ReaderT]) -> DataReader[_ReaderT]:
    """Declares a data reader for files in a dataset, created from the dataset and its key."""
    return DataReader(func)
