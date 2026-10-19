"""Deprecated modules for building aeon schemas, replaced by swc.aeon.data."""

import warnings

# Set imports available directly under 'swc.aeon.schema'
from swc.aeon.schema.base import (
    SCHEMA_PACKAGE_DEPRECATION,
    BaseSchema,
    Dataset,
    Experiment,
    Metadata,
    data_reader,
)

warnings.warn(SCHEMA_PACKAGE_DEPRECATION, DeprecationWarning, stacklevel=2)

__all__ = [
    "BaseSchema",
    "Dataset",
    "Experiment",
    "Metadata",
    "data_reader",
]
