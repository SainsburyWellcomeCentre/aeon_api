"""Stream classes for foraging experiment data (exp02).

Ported from ``aeon_mecha/aeon/schema/foraging.py``.
"""

from enum import Enum

import pandas as pd
import swc.aeon.io.reader as _reader
import swc.aeon.schema.core as _stream
from swc.aeon.schema.streams import Stream, StreamGroup


class Area(Enum):
    Null = 0
    Nest = 1
    Corridor = 2
    Arena = 3
    Patch1 = 4
    Patch2 = 5


class _RegionReader(_reader.Harp):
    def __init__(self, pattern):
        super().__init__(pattern, columns=["region"])

    def read(self, path):
        data = super().read(path)
        data["region"] = pd.Categorical(
            [Area(int(v)).name for v in data.region],
            categories=list(Area._member_names_),
        )
        return data


class _PatchState(_reader.Csv):
    def __init__(self, pattern):
        super().__init__(pattern, columns=["threshold", "d1", "delta"])


class _Weight(_reader.Harp):
    def __init__(self, pattern):
        super().__init__(pattern, columns=["value", "stable"])


class Region(Stream):
    """Region tracking data for the top camera (register 201)."""

    def __init__(self, pattern):
        super().__init__(_RegionReader(f"{pattern}_201_*"))


class DepletionFunction(Stream):
    """State of the linear depletion function for foraging patches."""

    def __init__(self, pattern):
        super().__init__(_PatchState(f"{pattern}_State_*"))


class BeamBreak(Stream):
    """Beam break events for pellet detection (register 32, bitmask 0x22)."""

    def __init__(self, pattern):
        super().__init__(_reader.BitmaskEvent(f"{pattern}_32_*", 0x22, "PelletDetected"))


class DeliverPellet(Stream):
    """Pellet delivery commands (register 35, bitmask 0x01)."""

    def __init__(self, pattern):
        super().__init__(_reader.BitmaskEvent(f"{pattern}_35_*", 0x01, "TriggerPellet"))


class Feeder(StreamGroup):
    """Feeder commands and events."""

    def __init__(self, pattern):
        super().__init__(pattern, BeamBreak, DeliverPellet)


class Patch(StreamGroup):
    """Data streams for a foraging patch (exp02)."""

    def __init__(self, pattern):
        super().__init__(pattern, DepletionFunction, _stream.Encoder, Feeder)


class WeightRaw(Stream):
    """Raw weight measurement from nest scale (register 200)."""

    def __init__(self, pattern):
        super().__init__(_Weight(f"{pattern}_200_*"))


class WeightFiltered(Stream):
    """Filtered weight measurement from nest scale (register 202)."""

    def __init__(self, pattern):
        super().__init__(_Weight(f"{pattern}_202_*"))


class WeightSubject(Stream):
    """Subject weight measurement from nest scale (register 204)."""

    def __init__(self, pattern):
        super().__init__(_Weight(f"{pattern}_204_*"))


class Weight(StreamGroup):
    """All weight measurement streams for a nest (raw, filtered, subject)."""

    def __init__(self, pattern):
        super().__init__(pattern, WeightRaw, WeightFiltered, WeightSubject)
