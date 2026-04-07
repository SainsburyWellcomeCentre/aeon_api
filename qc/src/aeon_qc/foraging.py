"""Stream classes for foraging experiment data (exp02).

Ported from ``aeon_mecha/aeon/schema/foraging.py``.
"""

from enum import Enum

import pandas as pd
import swc.aeon.io.reader as _reader
import swc.aeon.schema.core as _stream
from swc.aeon.schema.streams import Stream, StreamGroup


class Area(Enum):
    """Arena regions for foraging experiments."""

    Null = 0
    Nest = 1
    Corridor = 2
    Arena = 3
    Patch1 = 4
    Patch2 = 5


class RegionReader(_reader.Harp):
    """Harp reader that decodes region codes into categorical Area labels."""

    def __init__(self, pattern):
        """Initializes the reader with the given pattern."""
        super().__init__(pattern, columns=["region"])

    def read(self, file):
        """Reads region data and decodes integer codes to Area names."""
        data = super().read(file)
        categorical = pd.Categorical(data.region, categories=range(len(Area._member_names_)))
        data["region"] = categorical.rename_categories(Area._member_names_)
        return data


class Region(Stream):
    """Region tracking data for the top camera (register 201)."""

    def __init__(self, pattern):
        """Initializes the Region stream."""
        super().__init__(RegionReader(f"{pattern}_201_*"))


class DepletionFunction(Stream):
    """State of the linear depletion function for foraging patches."""

    def __init__(self, pattern):
        """Initializes the DepletionFunction stream."""
        super().__init__(_reader.Csv(f"{pattern}_State_*", columns=["threshold", "d1", "delta"]))


class BeamBreak(Stream):
    """Beam break events for pellet detection (register 32, bitmask 0x22)."""

    def __init__(self, pattern):
        """Initializes the BeamBreak stream."""
        super().__init__(_reader.BitmaskEvent(f"{pattern}_32_*", 0x22, "PelletDetected"))


class DeliverPellet(Stream):
    """Pellet delivery commands (register 35, bitmask 0x80)."""

    def __init__(self, pattern):
        """Initializes the DeliverPellet stream."""
        super().__init__(_reader.BitmaskEvent(f"{pattern}_35_*", 0x80, "TriggerPellet"))


class Feeder(StreamGroup):
    """Feeder commands and events."""

    def __init__(self, pattern):
        """Initializes the Feeder stream group."""
        super().__init__(pattern, BeamBreak, DeliverPellet)


class Patch(StreamGroup):
    """Data streams for a foraging patch (exp02)."""

    def __init__(self, pattern):
        """Initializes the Patch stream group."""
        super().__init__(pattern, DepletionFunction, _stream.Encoder, Feeder)


class WeightRaw(Stream):
    """Raw weight measurement from nest scale (register 200)."""

    def __init__(self, pattern):
        """Initializes the WeightRaw stream."""
        super().__init__(_reader.Harp(f"{pattern}_200_*", columns=["value", "stable"]))


class WeightFiltered(Stream):
    """Filtered weight measurement from nest scale (register 202)."""

    def __init__(self, pattern):
        """Initializes the WeightFiltered stream."""
        super().__init__(_reader.Harp(f"{pattern}_202_*", columns=["value", "stable"]))


class WeightSubject(Stream):
    """Subject weight measurement from nest scale (register 204)."""

    def __init__(self, pattern):
        """Initializes the WeightSubject stream."""
        super().__init__(_reader.Harp(f"{pattern}_204_*", columns=["value", "stable"]))


class Weight(StreamGroup):
    """All weight measurement streams for a nest (raw, filtered, subject)."""

    def __init__(self, pattern):
        """Initializes the Weight stream group."""
        super().__init__(pattern, WeightRaw, WeightFiltered, WeightSubject)
