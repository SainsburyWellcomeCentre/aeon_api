"""Modules for building aeon schemas."""

# Set imports available directly under 'swc.aeon.schema'
from swc.aeon.schema.base import (
    BaseSchema,
    Dataset,
    DiscriminatorTypeMixin,
    Experiment,
    Metadata,
    SchemaEnum,
    bind_typename,
    data_reader,
)

__all__ = [
    "BaseSchema",
    "Dataset",
    "DiscriminatorTypeMixin",
    "Experiment",
    "Metadata",
    "SchemaEnum",
    "bind_typename",
    "data_reader",
]
