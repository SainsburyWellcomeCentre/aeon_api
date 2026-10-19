"""Readers for the files of an experiment dataset."""

import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import TypeAdapter

from swc.aeon.data.dataset import Dataset
from swc.aeon.io.reader import Reader


class Metadata(Reader):
    """Validates the experiment model of each acquisition epoch from its `Metadata.json` file.

    The model type must be a dataset, so validating it assigns the keys of its datasets and their
    data readers are ready to use.
    """

    def __init__(self, type: type[Dataset], pattern: str = "Metadata"):
        """Initializes the reader with the model type of the metadata, and an optional pattern."""
        super().__init__(pattern, columns=["metadata", "epoch"], extension="json")
        self.type: TypeAdapter[Any] = TypeAdapter(type)

    def read(self, path: Path) -> pd.DataFrame:
        """Returns the experiment model of the epoch associated with the specified file."""
        epoch_str = path.parts[-2]
        date_str, time_str = epoch_str.split("T")
        time = datetime.datetime.fromisoformat(date_str + "T" + time_str.replace("-", ":"))
        metadata = self.type.validate_json(path.read_text())
        data = {"metadata": [metadata], "epoch": [epoch_str]}
        return pd.DataFrame(data, index=pd.Series(time), columns=self.columns)
