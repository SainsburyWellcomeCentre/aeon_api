"""Classes for defining Harp device configuration models."""

import warnings
from typing import ClassVar

from pydantic import Field
from typing_extensions import deprecated

from swc.aeon.schema.base import DOMAIN_MODEL_DEPRECATION, BaseSchema

warnings.warn(DOMAIN_MODEL_DEPRECATION, DeprecationWarning, stacklevel=2)


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpDevice(BaseSchema):
    """A base class for creating Harp device models."""

    who_am_i: ClassVar[int] = Field(description="The unique identifier for the device type.")
    port_name: str = Field(examples=["COM"], description="The name of the device serial port.")


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpInputExpander(HarpDevice):
    """Represents a Harp Input Expander device."""

    who_am_i: ClassVar[int] = 1106


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpOutputExpander(HarpDevice):
    """Represents a Harp Output Expander device."""

    who_am_i: ClassVar[int] = 1108


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpClockSynchronizer(HarpDevice):
    """Represents a Harp Clock Synchronizer device."""

    who_am_i: ClassVar[int] = 1152


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpTimestampGeneratorGen3(HarpDevice):
    """Represents a Harp Timestamp Generator Gen3 device."""

    who_am_i: ClassVar[int] = 1158


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpCameraController(HarpDevice):
    """Represents a Harp Camera Controller device."""

    who_am_i: ClassVar[int] = 1168


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpCameraControllerGen2(HarpDevice):
    """Represents a Harp Camera Controller Gen2 device."""

    who_am_i: ClassVar[int] = 1170


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpBehavior(HarpDevice):
    """Represents a Harp Behavior device."""

    who_am_i: ClassVar[int] = 1216


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpAudioSwitch(HarpDevice):
    """Represents a Harp Audio Switch device."""

    who_am_i: ClassVar[int] = 1248


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpSoundCard(HarpDevice):
    """Represents a Harp Sound Card device."""

    who_am_i: ClassVar[int] = 1280


@deprecated(DOMAIN_MODEL_DEPRECATION, category=None)
class HarpRfidReader(HarpDevice):
    """Represents a Harp RFID Reader device."""

    who_am_i: ClassVar[int] = 2094
