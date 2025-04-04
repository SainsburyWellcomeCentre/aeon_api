"""Module for reading data from raw files in an Aeon dataset."""

from __future__ import annotations

import datetime
import json
import os
from pathlib import Path
from typing import Any

import harp
import numpy as np
import pandas as pd
from dotmap import DotMap

from swc.aeon.io.api import chunk_key


class Reader:
    """Extracts data from raw files in an Aeon dataset.

    Attributes:
        pattern (str): Pattern used to find raw files,
            usually in the format `<Device>_<DataStream>`.
        columns (str or array-like): Column labels to use for the data.
        extension (str): Extension of data file pathnames.
    """

    def __init__(self, pattern, columns, extension):
        """Initialize the object with specified pattern, columns, and file extension."""
        self.pattern = pattern
        self.columns = columns
        self.extension = extension

    def read(self, file):
        """Reads data from the specified file."""
        return pd.DataFrame(columns=self.columns, index=pd.DatetimeIndex([]))


class Harp(Reader):
    """Extracts data from raw binary files encoded using the Harp protocol."""

    def __init__(self, pattern, columns, extension="bin"):
        """Initialize the object."""
        super().__init__(pattern, columns, extension)

    def read(self, file):
        """Reads data from the specified Harp binary file."""
        return harp.read(file, columns=self.columns)


class Chunk(Reader):
    """Extracts path and epoch information from chunk files in the dataset."""

    def __init__(self, reader=None, pattern=None, extension=None):
        """Initialize the object with optional reader, pattern, and file extension."""
        if isinstance(reader, Reader):
            pattern = reader.pattern
            extension = reader.extension
        super().__init__(pattern, columns=["path", "epoch"], extension=extension)

    def read(self, file):
        """Returns path and epoch information for the specified chunk."""
        epoch, chunk = chunk_key(file)
        data = {"path": file, "epoch": epoch}
        return pd.DataFrame(data, index=pd.Series(chunk), columns=self.columns)


class Metadata(Reader):
    """Extracts metadata information from all epochs in the dataset."""

    def __init__(self, pattern="Metadata"):
        """Initialize the object with the specified pattern."""
        super().__init__(pattern, columns=["workflow", "commit", "metadata"], extension="yml")

    def read(self, file):
        """Returns metadata for the specified epoch."""
        epoch_str = file.parts[-2]
        date_str, time_str = epoch_str.split("T")
        time = datetime.datetime.fromisoformat(date_str + "T" + time_str.replace("-", ":"))
        with open(file) as fp:
            metadata = json.load(fp)
        workflow = metadata.pop("Workflow")
        commit = metadata.pop("Commit", pd.NA)
        data = {"workflow": workflow, "commit": commit, "metadata": [DotMap(metadata)]}
        return pd.DataFrame(data, index=pd.Series(time), columns=self.columns)


class Csv(Reader):
    """Extracts data from comma-separated (CSV) text files.

    The first column stores the Aeon timestamp, in seconds.
    """

    def __init__(self, pattern, columns, dtype=None, extension="csv"):
        """Initialize the object with the specified pattern, columns, and data type."""
        super().__init__(pattern, columns, extension)
        self.dtype = dtype

    def read(self, file):
        """Reads data from the specified CSV text file."""
        return pd.read_csv(
            file,
            header=0,
            names=self.columns,
            dtype=self.dtype,
            index_col=0 if file.stat().st_size else None,
        )


class JsonList(Reader):
    """Extracts data from .jsonl files, where the key "seconds" stores the Aeon timestamp."""

    def __init__(self, pattern, columns=(), root_key="value", extension="jsonl"):
        """Initialize the object with the specified pattern, columns, and root key."""
        super().__init__(pattern, columns, extension)
        self.columns = columns
        self.root_key = root_key

    def read(self, file):
        """Reads data from the specified jsonl file."""
        with open(file) as f:
            df = pd.read_json(f, lines=True)
        df.set_index("seconds", inplace=True)
        for column in self.columns:
            df[column] = df[self.root_key].apply(lambda x: x[column])  # noqa B023
        return df


class Subject(Csv):
    """Extracts metadata for subjects entering and exiting the environment.

    Columns:

    - id (str): Unique identifier of a subject in the environment.
    - weight (float): Weight measurement of the subject on entering
      or exiting the environment.
    - event (str): Event type. Can be one of `Enter`, `Exit` or `Remain`.
    """

    def __init__(self, pattern):
        """Initialize the object with a specified pattern."""
        super().__init__(pattern, columns=["id", "weight", "event"])


class Log(Csv):
    """Extracts message log data.

    Columns:

    - priority (str): Priority level of the message.
    - type (str): Type of the log message.
    - message (str): Log message data. Can be structured using tab
      separated values.
    """

    def __init__(self, pattern):
        """Initialize the object with a specified pattern and columns."""
        super().__init__(pattern, columns=["priority", "type", "message"])


class Heartbeat(Harp):
    """Extract periodic heartbeat event data.

    Columns:

    - second (int): The whole second corresponding to the heartbeat, in seconds.
    """

    def __init__(self, pattern):
        """Initialize the object with a specified pattern."""
        super().__init__(pattern, columns=["second"])


class Encoder(Harp):
    """Extract magnetic encoder data.

    Columns:

    - angle (float): Absolute angular position, in radians, of the magnetic encoder.
    - intensity (float): Intensity of the magnetic field.
    """

    def __init__(self, pattern):
        """Initialize the object with a specified pattern and columns."""
        super().__init__(pattern, columns=["angle", "intensity"])


class Position(Harp):
    """Extract 2D position tracking data for a specific camera.

    Columns:

    - x (float): x-coordinate of the object center of mass.
    - y (float): y-coordinate of the object center of mass.
    - angle (float): angle, in radians, of the ellipse fit to the object.
    - major (float): length, in pixels, of the major axis of the ellipse
      fit to the object.
    - minor (float): length, in pixels, of the minor axis of the ellipse
      fit to the object.
    - area (float): number of pixels in the object mass.
    - id (float): unique tracking ID of the object in a frame.
    """

    def __init__(self, pattern):
        """Initialize the object with a specified pattern and columns."""
        super().__init__(pattern, columns=["x", "y", "angle", "major", "minor", "area", "id"])


class BitmaskEvent(Harp):
    """Extracts event data matching a specific digital I/O bitmask.

    Columns:

    - event (str): Unique identifier for the event code.
    """

    def __init__(self, pattern, value, tag):
        """Initialize the object with specified pattern, value, and tag."""
        super().__init__(pattern, columns=["event"])
        self.value = value
        self.tag = tag

    def read(self, file):
        """Reads a specific event code from digital data.

        Each data value is matched against the unique event identifier.
        """
        data = super().read(file)
        data = data[(data.event & self.value) == self.value]
        data["event"] = self.tag
        return data


class DigitalBitmask(Harp):
    """Extracts event data matching a specific digital I/O bitmask.

    Columns:

    - event (str): Unique identifier for the event code.
    """

    def __init__(self, pattern, mask, columns):
        """Initialize the object with specified pattern, mask, and columns."""
        super().__init__(pattern, columns)
        self.mask = mask

    def read(self, file):
        """Reads a specific event code from digital data.

        Each data value is checked against the specified bitmask.
        """
        data = super().read(file)
        state = data[self.columns] & self.mask
        return state[(state.diff() != 0).values] != 0


class Video(Csv):
    """Extracts video frame metadata.

    Columns:

    - hw_counter (int): Hardware frame counter value for the current frame.
    - hw_timestamp (int): Internal camera timestamp for the current frame.
    """

    def __init__(self, pattern):
        """Initialize the object with a specified pattern."""
        super().__init__(pattern, columns=["hw_counter", "hw_timestamp", "_frame", "_path", "_epoch"])
        self._rawcolumns = ["time"] + self.columns[0:2]

    def read(self, file):
        """Reads video metadata from the specified file."""
        data = pd.read_csv(file, header=0, names=self._rawcolumns)
        data["_frame"] = data.index
        data["_path"] = os.path.splitext(file)[0] + ".avi"
        data["_epoch"] = file.parts[-3]
        data.set_index("time", inplace=True)
        return data


class Pose(Harp):
    """Reader for Harp-binarized tracking data given a model that outputs id, parts, and likelihoods.

    Columns:

    - class (int): Int ID of a subject in the environment.
    - class_likelihood (float): Likelihood of the subject's identity.
    - part (str): Bodypart on the subject.
    - part_likelihood (float): Likelihood of the specified bodypart.
    - x (float): X-coordinate of the bodypart.
    - y (float): Y-coordinate of the bodypart.
    """

    def __init__(self, pattern: str, model_root: str = "/ceph/aeon/aeon/data/processed"):
        """Pose reader constructor.

        The pattern for this reader should typically be `<device>_<hpcnode>_<jobid>*`.
        If a register prefix is required, the pattern should end with a trailing
        underscore, e.g. `Camera_202_*`. Otherwise, the pattern should include a
        common prefix for the pose model folder excluding the trailing underscore,
        e.g. `Camera_model-dir*`.
        """
        super().__init__(pattern, columns=None)
        self._model_root = model_root
        self._pattern_offset = pattern.rfind("_") + 1

    def read(self, file: Path, include_model: bool = False) -> pd.DataFrame:
        """Reads data from the Harp-binarized tracking file."""
        # Get config file from `file`, then bodyparts from config file.
        model_dir = Path(file.stem[self._pattern_offset :].replace("_", "/")).parent

        # Check if model directory exists in local or shared directories.
        # Local directory is prioritized over shared directory.
        local_config_file_dir = file.parent / model_dir
        shared_config_file_dir = Path(self._model_root) / model_dir
        if local_config_file_dir.exists():
            config_file_dir = local_config_file_dir
        elif shared_config_file_dir.exists():
            config_file_dir = shared_config_file_dir
        else:
            raise FileNotFoundError(
                f"""Cannot find model dir in either local ({local_config_file_dir}) \
                    or shared ({shared_config_file_dir}) directories"""
            )

        config_file = self.get_config_file(config_file_dir)
        identities = self.get_class_names(config_file)
        parts = self.get_bodyparts(config_file)

        # Using bodyparts, assign column names to Harp register values, and read data in default format.
        BONSAI_SLEAP_V2 = 0.2
        BONSAI_SLEAP_V3 = 0.3
        try:  # Bonsai.Sleap0.2
            bonsai_sleap_v = BONSAI_SLEAP_V2
            columns = ["identity", "identity_likelihood"]
            for part in parts:
                columns.extend([f"{part}_x", f"{part}_y", f"{part}_likelihood"])
            self.columns = columns
            data = super().read(file)
        except ValueError:  # column mismatch; Bonsai.Sleap0.3
            bonsai_sleap_v = BONSAI_SLEAP_V3
            columns = ["identity"]
            columns.extend([f"{identity}_likelihood" for identity in identities])
            for part in parts:
                columns.extend([f"{part}_x", f"{part}_y", f"{part}_likelihood"])
            self.columns = columns
            data = super().read(file)

        # combine all identity_likelihood cols into a single column
        if bonsai_sleap_v == BONSAI_SLEAP_V3:
            identity_likelihood = data.apply(
                lambda row: {identity: row[f"{identity}_likelihood"] for identity in identities},
                axis=1,
            )
            data.drop(columns=columns[1 : (len(identities) + 1)], inplace=True)
            data.insert(1, "identity_likelihood", identity_likelihood)

        # Replace identity indices with identity labels
        data = self.class_int2str(data, identities)

        # Set new columns, and reformat `data`.
        n_parts = len(parts)
        new_columns = ["identity", "identity_likelihood", "part", "x", "y", "part_likelihood"]
        new_data = np.empty((len(data) * n_parts, len(new_columns)), dtype="O")
        new_index = np.empty(len(data) * n_parts, dtype=data.index.values.dtype)
        for i, part in enumerate(parts):
            min_col = 2 + i * 3
            max_col = 2 + (i + 1) * 3
            new_data[i::n_parts, 0:2] = data.values[:, 0:2]
            new_data[i::n_parts, 2] = part
            new_data[i::n_parts, 3:6] = data.values[:, min_col:max_col]
            new_index[i::n_parts] = data.index.values
        data = pd.DataFrame(new_data, new_index, columns=new_columns)

        # Set model column using model_dir
        if include_model:
            data["model"] = model_dir
        return data

    @staticmethod
    def get_class_names(config_file: Path) -> list[str]:
        """Returns a list of classes from a model's config file."""
        with open(config_file) as f:
            config = json.load(f)
        if config_file.stem != "confmap_config":  # SLEAP
            raise ValueError(f"The model config file '{config_file}' is not supported.")

        try:
            heads = config["model"]["heads"]
            class_vectors = Pose._recursive_lookup(heads, "class_vectors")
            if class_vectors is not None:
                return class_vectors["classes"]
            else:
                return list[str]()
        except KeyError as err:
            raise KeyError(f"Cannot find class_vectors in {config_file}.") from err

    @staticmethod
    def get_bodyparts(config_file: Path) -> list[str]:
        """Returns a list of bodyparts from a model's config file."""
        parts = []
        with open(config_file) as f:
            config = json.load(f)
        if config_file.stem == "confmap_config":  # SLEAP
            try:
                heads = config["model"]["heads"]
                parts = [f"anchor_{Pose._find_nested_key(heads, 'anchor_part')}"]
                parts += Pose._find_nested_key(heads, "part_names")
            except KeyError as err:
                raise KeyError(f"Cannot find anchor or bodyparts in {config_file}.") from err
        return parts

    @staticmethod
    def class_int2str(data: pd.DataFrame, classes: list[str]) -> pd.DataFrame:
        """Converts a class integer in a tracking data dataframe to its associated string (subject id)."""
        if classes:
            identity_mapping = dict(enumerate(classes))
            data["identity"] = data["identity"].replace(identity_mapping)
        return data

    @classmethod
    def get_config_file(cls, config_file_dir: Path, config_file_names: None | list[str] = None) -> Path:
        """Returns the config file from a model's config directory."""
        if config_file_names is None:
            config_file_names = ["confmap_config.json"]  # SLEAP (add for other trackers to this list)
        config_file = None
        for f in config_file_names:
            if (config_file_dir / f).exists():
                config_file = config_file_dir / f
                break
        if config_file is None:
            raise FileNotFoundError(f"Cannot find config file in {config_file_dir}")
        return config_file

    @staticmethod
    def _find_nested_key(obj: dict, key: str) -> Any:
        """Returns the value of the first found nested key."""
        value = Pose._recursive_lookup(obj, key)
        if value is None:
            raise KeyError(key)
        return value

    @staticmethod
    def _recursive_lookup(obj: Any, key: str) -> Any:
        """Returns the value of the first found nested key."""
        if isinstance(obj, dict):
            if found := obj.get(key):  # found it!
                return found
            for item in obj.values():
                if found := Pose._recursive_lookup(item, key):
                    return found
        elif isinstance(obj, list):
            for item in obj:
                if found := Pose._recursive_lookup(item, key):
                    return found  # pragma: no cover
