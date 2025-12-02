"""Tests for dataset schema definitions and pattern prefix resolution."""

import os
from enum import StrEnum
from typing import ClassVar, Literal

from pydantic import Field

from swc.aeon.io.api import load
from swc.aeon.io.reader import BitmaskEvent, Csv, Harp
from swc.aeon.schema.base import BaseSchema, Dataset, Metadata, data_reader
from swc.aeon.schema.environment import LightCycle
from swc.aeon.schema.foraging import UndergroundFeeder
from swc.aeon.schema.harp import HarpDevice
from swc.aeon.schema.video import SpinnakerCamera


class DummyHarpDevice(HarpDevice):
    """A dummy Harp device."""

    device_type: Literal["DummyHarpDevice"] = "DummyHarpDevice"
    who_am_i: ClassVar[int] = 0000

    @data_reader
    def mock_register(self, prefix: str):
        """Returns a dummy data reader."""
        return Harp(f"{prefix}_32", columns=["mock"])


class DummyNestedDataset(Dataset):
    """A dummy dataset meant to test nesting subfolders."""

    nested_device: DummyHarpDevice


class DummyTracking(BaseSchema):
    """A dummy tracking module encoding position data."""

    @data_reader
    def position(self, prefix: str):
        """Returns a dummy reader for position data."""
        return Harp(f"{prefix}_202", columns=["mock"])


class DummyCamera(SpinnakerCamera):
    """A dummy camera with an optional tracking module."""

    tracking: DummyTracking | None = Field(default=None)


class DummyFeeder(UndergroundFeeder):
    """A dummy feeder module with a simple data reader."""

    @data_reader
    def beam_break(self, prefix: str):
        """Returns a dummy reader for beam break digital events."""
        return BitmaskEvent(f"{prefix}_32", 0x22, "PelletDetected")


class DummyLightCycle(LightCycle):
    """A dummy light cycle module with a simple data reader."""

    @data_reader
    def light_events(self, prefix: str) -> Csv:
        """Returns a dummy reader for light events."""
        return Csv(f"{prefix}_Events", columns=["channel", "value"])


class DummyFeederName(StrEnum):
    """Available feeder names for the dummy dataset."""

    FEEDER1 = "Feeder1"
    FEEDER2 = "Feeder2"


class DummyDataset(Dataset):
    """A dummy dataset."""

    dummy_device: DummyHarpDevice
    nested_data: DummyNestedDataset
    camera: DummyCamera
    feeder: dict[DummyFeederName, DummyFeeder]
    light_cycle: DummyLightCycle


def test_dataset_resolve_pattern_prefix():
    """Test that BaseSchema subclasses construct the correct reader pattern."""
    dataset = DummyDataset(
        dummy_device=DummyHarpDevice(port_name="COM3"),
        nested_data=DummyNestedDataset(nested_device=DummyHarpDevice(port_name="COM1")),
        camera=DummyCamera(serial_number="00000", tracking=DummyTracking()),
        feeder={DummyFeederName.FEEDER1: DummyFeeder(port_name="COM6")},
        light_cycle=DummyLightCycle(room_name="DummyRoom"),
    )
    assert dataset.dummy_device.mock_register.pattern == "DummyDevice_32"
    assert dataset.nested_data.nested_device.mock_register.pattern == os.path.join(
        "NestedData", "NestedDevice_32"
    )
    assert dataset.camera.tracking
    assert dataset.camera.tracking.position.pattern == "Camera_202"
    assert dataset.feeder[DummyFeederName.FEEDER1].beam_break.pattern == "Feeder1_32"
    assert dataset.light_cycle.light_events.pattern == "LightCycle_Events"


def test_dataset_read_metadata(request):
    """Test that dataset Metadata is loaded successfully."""
    root_dir = request.getfixturevalue("test_data_dir")
    metadata = load(root_dir, Metadata(DummyDataset))
    assert len(metadata) > 0
