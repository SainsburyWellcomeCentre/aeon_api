"""Tests for binding schema types and enumeration members to generated type names."""

import sys
from enum import IntEnum, StrEnum
from typing import Annotated, Literal

import pytest
from pydantic import Field, TypeAdapter, ValidationError

from swc.aeon.schema import BaseSchema, DiscriminatorTypeMixin, SchemaEnum, bind_typename

NAMESPACE = "Aeon.Test"


@pytest.fixture
def namespaced(monkeypatch):
    """Declares a namespace on this module for the duration of a test."""
    monkeypatch.setattr(sys.modules[__name__], "SGEN_NAMESPACE", NAMESPACE, raising=False)
    return monkeypatch


@pytest.fixture(params=[NAMESPACE, None], ids=["namespace", "no namespace"])
def module_namespace(request, monkeypatch):
    """Declares a namespace on this module for the duration of a test, or leaves it absent."""
    if request.param is not None:
        monkeypatch.setattr(sys.modules[__name__], "SGEN_NAMESPACE", request.param, raising=False)
    return request.param


def typename(schema_type):
    """Returns the type name bound to a model or enumeration, if any."""
    return TypeAdapter(schema_type).json_schema().get("x-sgen-typename")


def member_names(schema_type):
    """Returns the member names supplied for an enumeration, if any."""
    return TypeAdapter(schema_type).json_schema().get("x-enumNames")


def test_bind_typename_annotates_in_place():
    """Test that `bind_typename` annotates in place and returns the schema it was given."""
    schema = {"type": "object"}
    result = bind_typename(schema, "Aeon.Test.Thing")
    assert result is schema
    assert schema["x-sgen-typename"] == "Aeon.Test.Thing"


def test_typename_derived_from_module_namespace(module_namespace):
    """Test that models are named in the namespace declared by their module, if any,
    and that a subclass takes its own name rather than the name of its base.
    """

    class Base(BaseSchema):
        """The base model."""

    class Sub(Base):
        """The subclass."""

    assert typename(Base) == (f"{NAMESPACE}.Base" if module_namespace else None)
    assert typename(Sub) == (f"{NAMESPACE}.Sub" if module_namespace else None)


@pytest.mark.usefixtures("module_namespace")
def test_class_keyword_overrides_module_namespace():
    """Test that `sgen_namespace` names a model owned by another package regardless of
    whether the module declares a namespace.
    """

    class Foreign(BaseSchema, sgen_namespace="OpenEphys.Onix1"):
        """A model describing a type owned elsewhere."""

    assert typename(Foreign) == "OpenEphys.Onix1.Foreign"


@pytest.mark.parametrize(
    ("extra", "expected"),
    [(None, None), (bind_typename({}, "Third.Party.Explicit"), "Third.Party.Explicit")],
    ids=["inherited", "explicit"],
)
def test_subclass_typename_without_namespace(namespaced, extra, expected):
    """Test that a subclass drops the name of its base unless it names itself explicitly.

    The name of the base is checked too, since pydantic shares `json_schema_extra` between
    a model and its base, so a subclass writing to it without a copy would rename its base.
    """

    class Base(BaseSchema):
        """Defined while the module declares a namespace."""

    namespaced.undo()

    class Sub(Base):
        """Defined after the namespace is gone, as a consumer module would be."""

        if extra is not None:
            model_config = {"json_schema_extra": extra}

    assert typename(Base) == f"{NAMESPACE}.Base"
    assert typename(Sub) == expected


def test_generated_schema_annotations_preserved(namespaced):
    """Test that a model keeps the schema annotations it generates and carries no type name."""

    def generate(schema):
        schema["x-generated"] = True

    class Generated(BaseSchema):
        """Supplies a callable rather than a mapping, which cannot be merged into."""

        model_config = {"json_schema_extra": generate}

    schema = TypeAdapter(Generated).json_schema()
    assert schema["x-generated"] is True
    assert typename(Generated) is None


def test_sibling_annotations_preserved(namespaced):
    """Test that binding a name leaves other schema annotations in place."""

    class Sibling(BaseSchema):
        """Carries an unrelated annotation."""

        model_config = {"json_schema_extra": {"x-unrelated": "kept"}}

    schema = TypeAdapter(Sibling).json_schema()
    assert schema["x-sgen-typename"] == f"{NAMESPACE}.Sibling"
    assert schema["x-unrelated"] == "kept"


def test_enum_typename_derived_from_module_namespace(module_namespace):
    """Test that an enumeration is named in the namespace declared by its module, if any."""

    class Colour(SchemaEnum):
        """An enumeration in a module."""

        RED = "Red"

    assert typename(Colour) == (f"{NAMESPACE}.Colour" if module_namespace else None)


def test_enum_typename_overridden_by_class_keyword():
    """Test that `sgen_namespace` names an enumeration owned by another package."""

    class Foreign(SchemaEnum, sgen_namespace="OpenEphys.Onix1"):
        """An enumeration describing a type owned elsewhere."""

        A = 1

    assert typename(Foreign) == "OpenEphys.Onix1.Foreign"


def test_member_names_absent_for_string_values():
    """Test that a string enumeration is left to name its own members.

    Supplying them would replace a name the generated code already takes from the value,
    which the YAML round trip depends on.
    """

    class Colour(SchemaEnum, StrEnum):
        """Members a string value already names."""

        RED = "Red"
        DARK_BLUE = "DarkBlue"

    assert member_names(Colour) is None


def test_member_names_supplied_for_integer_values():
    """Test that an integer enumeration carries its member names in Pascal case.

    A name outside the upper case convention is passed through, and an alias does not
    shift the names onto the wrong values. An alias appears in the values but not when
    iterating the class, so the names have to come from `__members__`.
    """

    class Colour(SchemaEnum, IntEnum):
        """Members an integer value cannot name, including an alias and a Pascal case name."""

        RED = 0
        CRIMSON = 0
        DARK_BLUE = 1
        DarkGreen = 2

    schema = TypeAdapter(Colour).json_schema()
    assert member_names(Colour) == ["Red", "Crimson", "DarkBlue", "DarkGreen"]
    assert len(schema["x-enumNames"]) == len(schema["enum"])


def test_discriminator_type_selects_union_member():
    """Test that `DiscriminatorTypeMixin` gives each type a literal of its own name, so that a
    union selects the matching member and rejects a type outside it.
    """

    class Headstage(BaseSchema):
        """The common base."""

    class Alpha(DiscriminatorTypeMixin, Headstage):
        """One member of the union."""

    class Beta(DiscriminatorTypeMixin, Headstage):
        """Another member of the union."""

    class NotInUnion(DiscriminatorTypeMixin, Headstage):
        """A subclass that is not a member of the union."""

    assert Alpha.model_fields["discriminator_type"].annotation == Literal["Alpha"]
    assert Beta().discriminator_type == "Beta"

    union = TypeAdapter(Annotated[Alpha | Beta, Field(discriminator="discriminator_type")])
    assert isinstance(union.validate_python({"discriminatorType": "Beta"}), Beta)
    with pytest.raises(ValidationError):
        union.validate_python(NotInUnion())
