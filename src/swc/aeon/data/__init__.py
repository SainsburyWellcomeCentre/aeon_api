"""Base classes for models describing the files of each experiment data stream and how to read them."""

from swc.aeon.data.dataset import DataReader, Dataset, data_reader
from swc.aeon.data.reader import Metadata

__all__ = [
    "DataReader",
    "Dataset",
    "Metadata",
    "data_reader",
]
