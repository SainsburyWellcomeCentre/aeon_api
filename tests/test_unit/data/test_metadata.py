"""Tests for the reader of the experiment model of each acquisition epoch."""

from pydantic import BaseModel

from swc.aeon.data import Dataset, Metadata, data_reader
from swc.aeon.io.api import load
from swc.aeon.io.reader import Harp


class Device(Dataset):
    """A Harp device with a single register."""

    port_name: str

    @data_reader
    def register(self, stem: str) -> Harp:
        """Returns a reader for the register."""
        return Harp(f"{stem}_32_*", columns=["value"])


class NestedData(BaseModel):
    """A plain model grouping a device."""

    nested_device: Device


class Rig(Dataset):
    """The rig described by the sample metadata file."""

    dummy_device: Device
    nested_data: NestedData
    feeder: dict[str, Device]


def test_metadata_read_assigns_dataset_keys(test_data_dir):
    """Test that models read from metadata files have their keys assigned, so their readers are
    ready to use.
    """
    metadata = load(test_data_dir, Metadata(Rig))
    assert len(metadata) == 1
    rig = metadata.metadata.iloc[0]
    assert isinstance(rig, Rig)
    assert metadata.epoch.iloc[0] == "2025-12-02T13-45-23"
    assert rig.dummy_device.register.pattern == "DummyDevice_32_*"
    assert rig.nested_data.nested_device.register.pattern == "NestedDevice_32_*"
    assert rig.feeder["Feeder1"].register.pattern == "Feeder1_32_*"
