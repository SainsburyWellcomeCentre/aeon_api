"""Tests for the base class of experiment models and the provenance it records."""

import json

from swc.aeon.data import Dataset, Experiment, data_reader
from swc.aeon.io.reader import Harp

PROVENANCE = {
    "workflow": "Experiment.bonsai",
    "commit": "0123456789abcdef",
    "repositoryUrl": "https://github.com/example/experiment",
}


class Feeder(Dataset):
    """A feeder with a single register."""

    port_name: str

    @data_reader
    def beam_break(self, stem: str) -> Harp:
        """Returns a reader for beam break events."""
        return Harp(f"{stem}_32_*", columns=["value"])


class ForagingExperiment(Experiment):
    """An experiment model adding a feeder to the provenance."""

    feeder: Feeder


def test_experiment_reads_provenance_from_metadata_keys():
    """Test that an experiment validates the provenance under the keys used in Metadata.json."""
    experiment = Experiment.model_validate_json(json.dumps(PROVENANCE))
    assert experiment.repository_url == PROVENANCE["repositoryUrl"]


def test_experiment_dumps_metadata_keys():
    """Test that an experiment writes the provenance under the keys used in Metadata.json."""
    experiment = Experiment.model_validate(PROVENANCE)
    assert experiment.model_dump(by_alias=True) == PROVENANCE


def test_experiment_subclass_assigns_dataset_keys():
    """Test that an experiment model deriving from the base assigns the keys of its datasets."""
    experiment = ForagingExperiment.model_validate({**PROVENANCE, "feeder": {"port_name": "COM3"}})
    assert experiment.feeder.beam_break.pattern == "Feeder_32_*"
