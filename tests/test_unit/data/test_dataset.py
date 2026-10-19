"""Tests for datasets declaring data readers, and the assignment of their keys by validation."""

import typing
from enum import StrEnum

import pytest
from pydantic import BaseModel, RootModel, ValidationError

from swc.aeon.data import DataReader, Dataset, data_reader
from swc.aeon.io.reader import BitmaskEvent, Csv, Harp


class Tracking(Dataset):
    """A tracking module nested in a camera, sharing its files."""

    @data_reader
    def position(self, stem: str) -> Harp:
        """Returns a reader for position data."""
        return Harp(f"{stem}_202_*", columns=["x", "y"])


class Camera(Dataset):
    """A camera with an optional tracking module."""

    serial_number: str
    tracking: Tracking | None = None

    @data_reader
    def video(self, stem: str) -> Csv:
        """Returns a reader for video frame metadata."""
        return Csv(f"{stem}_*", columns=["hw_counter", "hw_timestamp"])


class Feeder(Dataset):
    """A feeder with a beam break sensor."""

    port_name: str

    @data_reader
    def beam_break(self, stem: str) -> BitmaskEvent:
        """Returns a reader for beam break events."""
        return BitmaskEvent(f"{stem}_32_*", 0x22, "PelletDetected")


class Environment(Dataset):
    """Streams with no configuration, held by the rig only for their readers."""

    @data_reader
    def light_events(self, stem: str) -> Csv:
        """Returns a reader for light events."""
        return Csv(f"{stem}_LightEvents_*", columns=["channel", "value"])


class FeederName(StrEnum):
    """The names of the feeders in the rig."""

    FEEDER1 = "Feeder1"
    FEEDER2 = "Feeder2"


class CameraUnion(RootModel[Camera]):
    """A root model that wraps a camera."""


class Rig(BaseModel):
    """A plain rig model."""

    camera_top: Camera
    camera_east: CameraUnion | None = None
    feeders: dict[FeederName, Feeder] = {}
    extra_cameras: dict[str, Camera] = {}
    spare_feeders: list[Feeder] = []
    environment: Environment = Environment()


class Experiment(Dataset):
    """The experiment model holding the rig, which is a dataset so that validating it assigns
    the keys of every dataset within it.
    """

    workflow: str
    rig: Rig


def make_rig() -> Rig:
    """Returns a rig with a tracked camera, a wrapped camera, two feeders and a spare one."""
    return Rig(
        camera_top=Camera(serial_number="1", tracking=Tracking()),
        camera_east=CameraUnion(Camera(serial_number="2")),
        feeders={
            FeederName.FEEDER1: Feeder(port_name="COM1"),
            FeederName.FEEDER2: Feeder(port_name="COM2"),
        },
        spare_feeders=[Feeder(port_name="COM9")],
    )


def make_experiment() -> Experiment:
    """Returns an experiment holding the rig of `make_rig`."""
    return Experiment(workflow="Experiment.bonsai", rig=make_rig())


def test_field_names_and_dictionary_keys_become_dataset_keys():
    """Test that a dataset reads the files named after its field in PascalCase, or its key in a
    dictionary, including enumeration keys, and that the plain rig between the experiment and
    its devices contributes no key.
    """
    experiment = make_experiment()
    assert experiment.rig.camera_top.video.pattern == "CameraTop_*"
    assert experiment.rig.feeders[FeederName.FEEDER1].beam_break.pattern == "Feeder1_32_*"
    assert experiment.rig.environment.light_events.pattern == "Environment_LightEvents_*"


def test_json_validation_assigns_keys():
    """Test that an experiment validated from JSON has its keys assigned."""
    experiment = Experiment.model_validate_json(make_experiment().model_dump_json())
    assert experiment.rig.camera_top.video.pattern == "CameraTop_*"
    assert experiment.rig.feeders[FeederName.FEEDER2].beam_break.pattern == "Feeder2_32_*"


def test_camel_case_field_keeps_capitals():
    """Test that the key of a dataset held in a camelCase field keeps the capitals of the name."""

    class Root(Dataset):
        """A root holding a camera in a camelCase field."""

        cameraEast: Camera

    assert Root(cameraEast=Camera(serial_number="1")).cameraEast.video.pattern == "CameraEast_*"


def test_pascal_case_dictionary_key_accepted():
    """Test that a dataset held under a PascalCase dictionary key reads the files of that key."""
    rig = Rig(camera_top=Camera(serial_number="1"), extra_cameras={"CameraWest": Camera(serial_number="2")})
    experiment = Experiment(workflow="Experiment.bonsai", rig=rig)
    assert experiment.rig.extra_cameras["CameraWest"].video.pattern == "CameraWest_*"


@pytest.mark.parametrize("key", ["nest", "cameraWest", "camera_west", "test-node1", ""])
def test_dictionary_key_not_pascal_case_raises_validation_error(key: str):
    """Test that validation rejects a dataset held under a dictionary key that is not in
    PascalCase, since the key is the prefix of the file names.
    """
    rig = Rig(camera_top=Camera(serial_number="1"), extra_cameras={key: Camera(serial_number="2")})
    with pytest.raises(ValidationError, match="must be in PascalCase"):
        Experiment(workflow="Experiment.bonsai", rig=rig)


def test_dictionary_key_of_plain_model_not_checked():
    """Test that the key of a dictionary holding plain models is not checked, since the datasets
    within them take the names of their fields.
    """

    class Calibration(BaseModel):
        """A plain model holding a camera."""

        camera: Camera

    class Root(Dataset):
        """A root holding calibrations under free-form keys."""

        calibrations: dict[str, Calibration]

    root = Root(calibrations={"left eye": Calibration(camera=Camera(serial_number="1"))})
    assert root.calibrations["left eye"].camera.video.pattern == "Camera_*"


def test_nested_dataset_inherits_outer_key():
    """Test that a dataset within another dataset reads the files of the outer one."""
    experiment = make_experiment()
    assert experiment.rig.camera_top.tracking
    assert experiment.rig.camera_top.tracking.position.pattern == "CameraTop_202_*"


def test_union_wrapper_passes_key_through():
    """Test that a root model passes its key to the model it wraps."""
    experiment = make_experiment()
    assert experiment.rig.camera_east
    assert experiment.rig.camera_east.root.video.pattern == "CameraEast_*"


def test_plain_root_assigns_no_keys():
    """Test that a plain model validated as the root assigns no keys, since only a dataset root
    assigns them, so the readers of the datasets within it raise.
    """
    rig = make_rig()
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = rig.camera_top.video


def test_wrapped_root_assigns_no_keys():
    """Test that a dataset validated within a root model at the root is not the root itself, so
    it assigns no keys.
    """
    experiment = RootModel[Experiment].model_validate(make_experiment().model_dump()).root
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = experiment.rig.camera_top.video


def test_root_dataset_contributes_no_key():
    """Test that a dataset validated as the root has no key of its own, so its readers raise while
    the datasets within it take their own keys.
    """
    camera = Camera(serial_number="1", tracking=Tracking())
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = camera.video
    assert camera.tracking
    assert camera.tracking.position.pattern == "Tracking_202_*"


def test_listed_dataset_reader_raises_value_error():
    """Test that a dataset in a list has no key, since its position does not name any files."""
    experiment = make_experiment()
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = experiment.rig.spare_feeders[0].beam_break


def test_unvalidated_dataset_reader_raises_value_error():
    """Test that a dataset created through `model_construct` has no key."""
    feeder = Feeder.model_construct(port_name="COM1")
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = feeder.beam_break


def test_assigned_dataset_reader_raises_value_error():
    """Test that a dataset assigned after validation has no key, since models are not mutated
    in place, and that validating the model again assigns it.
    """
    experiment = make_experiment()
    experiment.rig.camera_top = Camera(serial_number="3")
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = experiment.rig.camera_top.video
    experiment = Experiment.model_validate(experiment.model_dump())
    assert experiment.rig.camera_top.video.pattern == "CameraTop_*"


def test_copied_dataset_reader_raises_value_error():
    """Test that a dataset placed by `model_copy` has no key, since copies are not validated."""
    experiment = make_experiment().model_copy(update={"rig": make_rig()})
    with pytest.raises(ValueError, match="has no dataset key"):
        _ = experiment.rig.camera_top.video


def test_shared_dataset_takes_last_key():
    """Test that a dataset held in two places takes the key of the last place validated."""
    feeder = Feeder(port_name="COM1")
    experiment = Experiment(
        workflow="Experiment.bonsai",
        rig=Rig(
            camera_top=Camera(serial_number="1"),
            feeders={FeederName.FEEDER1: feeder, FeederName.FEEDER2: feeder},
        ),
    )
    assert experiment.rig.feeders[FeederName.FEEDER1].beam_break.pattern == "Feeder2_32_*"


def test_reader_declared_outside_dataset_raises_on_definition():
    """Test that declaring a data reader on a model that is not a dataset fails when the class is
    defined. Before Python 3.12 the error arrives as the cause of a `RuntimeError`.
    """
    with pytest.raises((TypeError, RuntimeError)) as info:

        class Plain(BaseModel):  # pyright: ignore[reportUnusedClass]
            """A model that is not a dataset."""

            @data_reader
            def beam_break(self, stem: str) -> Csv:
                """Returns a reader."""
                return Csv(stem, columns=[])

    error = info.value if isinstance(info.value, TypeError) else info.value.__cause__
    assert isinstance(error, TypeError)
    assert "must be declared by a Dataset" in str(error)


def test_readers_discoverable_from_class():
    """Test that the readers of a dataset class, and the types of reader they create, are found
    from the class alone, without an instance.
    """
    readers = {name: value for name, value in vars(Camera).items() if isinstance(value, DataReader)}
    assert list(readers) == ["video"]
    assert Camera.video is readers["video"]
    assert typing.get_type_hints(readers["video"].func)["return"] is Csv
    assert readers["video"].func(Camera.model_construct(), "CameraTop").pattern == "CameraTop_*"


def test_readers_not_model_fields():
    """Test that data readers stay out of the fields, schema and dumps of a dataset."""
    camera = Camera(serial_number="1")
    assert set(Camera.model_fields) == {"serial_number", "tracking"}
    assert "video" not in Camera.model_json_schema()["properties"]
    assert camera.model_dump() == {"serial_number": "1", "tracking": None}
