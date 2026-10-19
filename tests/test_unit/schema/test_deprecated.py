"""Tests for the deprecation of the schema package, its domain models and the stream API."""

import importlib
import sys
import warnings

import pytest

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    from swc.aeon.schema.base import (
        DOMAIN_MODEL_DEPRECATION,
        SCHEMA_PACKAGE_DEPRECATION,
        BaseSchema,
        Dataset,
        Experiment,
        Metadata,
        data_reader,
    )
    from swc.aeon.schema.streams import STREAM_API_DEPRECATION


def test_schema_package_import_warns():
    """Test that importing the schema package warns with its deprecation message."""
    sys.modules.pop("swc.aeon.schema", None)
    with pytest.warns(DeprecationWarning) as records:
        importlib.import_module("swc.aeon.schema")
    assert any(str(record.message) == SCHEMA_PACKAGE_DEPRECATION for record in records)


@pytest.mark.parametrize("member", [BaseSchema, Dataset, Metadata, data_reader])
def test_schema_package_base_members_marked(member):
    """Test that the base classes and the data reader decorator of the schema package carry the
    package deprecation, so type checkers report each use.
    """
    assert getattr(member, "__deprecated__", None) == SCHEMA_PACKAGE_DEPRECATION


DEPRECATED_MODULES = [
    ("swc.aeon.schema.environment", DOMAIN_MODEL_DEPRECATION),
    ("swc.aeon.schema.foraging", DOMAIN_MODEL_DEPRECATION),
    ("swc.aeon.schema.harp", DOMAIN_MODEL_DEPRECATION),
    ("swc.aeon.schema.video", DOMAIN_MODEL_DEPRECATION),
    ("swc.aeon.schema.streams", STREAM_API_DEPRECATION),
    ("swc.aeon.schema.core", STREAM_API_DEPRECATION),
]


@pytest.mark.parametrize(("module_name", "message"), DEPRECATED_MODULES)
def test_deprecated_module_import_warns(module_name, message):
    """Test that importing a deprecated module warns with its deprecation message."""
    sys.modules.pop(module_name, None)
    with pytest.warns(DeprecationWarning) as records:
        importlib.import_module(module_name)
    assert any(str(record.message) == message for record in records)


@pytest.mark.parametrize(("module_name", "message"), DEPRECATED_MODULES)
def test_deprecated_module_classes_marked(module_name, message):
    """Test that every class of a deprecated module carries the deprecation, so type
    checkers report each use.
    """
    with pytest.warns(DeprecationWarning):
        module = importlib.reload(importlib.import_module(module_name))
    classes = [
        value
        for value in vars(module).values()
        if isinstance(value, type) and value.__module__ == module_name
    ]
    assert classes
    assert all(getattr(cls, "__deprecated__", None) == message for cls in classes)


def test_experiment_subclass_warns():
    """Test that deriving an experiment model from the deprecated base warns."""
    with pytest.warns(DeprecationWarning, match="aeon_acquisition"):

        class _Experiment(Experiment):  # pyright: ignore[reportUnusedClass]
            """An experiment model."""
