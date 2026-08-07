"""Classes for defining ONIX electrophysiology configuration models."""

from enum import StrEnum

from pydantic import Field, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from swc.aeon.schema.base import BaseSchema


def _bind_typename(schema: JsonSchemaValue, typename: str) -> JsonSchemaValue:
    """Tags a definition so `bonsai.sgen` binds an existing .NET type instead of generating one.

    Models express this through `model_config["json_schema_extra"]`; enums have no such hook, so
    they call this from `__get_pydantic_json_schema__`.
    """
    schema["x-sgen-typename"] = typename
    return schema


# Bound to ONIX's own PortName enum: the port value determines every child device address
# ((port << 8) + index), so it must be the real enum rather than a generated copy.
class PortName(StrEnum):
    """The headstage port on the ONIX breakout board."""

    PORT_A = "PortA"
    PORT_B = "PortB"

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        """Binds ONIX's `PortName` rather than generating an equivalent enum."""
        return _bind_typename(handler(core_schema), "OpenEphys.Onix1.PortName")

# Same again: the generated code has to reference ONIX's own enum for
# ConfigureHarpSyncInput.Source to accept it.
class HarpSyncSource(StrEnum):
    """The hardware source of the Harp synchronisation signal."""

    BREAKOUT = "Breakout"
    CLOCK_ADAPTER = "ClockAdapter"

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        """Binds ONIX's `HarpSyncSource` rather than generating an equivalent enum."""
        return _bind_typename(handler(core_schema), "OpenEphys.Onix1.HarpSyncSource")


# Bound to the NeuropixelsV2QuadShankProbeConfiguration. The property is declared on
# the decoder as the abstract NeuropixelsV2ProbeConfiguration, which YamlDotNet cannot
# instantiate.

class NeuropixelsV2QuadShankProbeConfiguration(BaseSchema):
    """Per-probe settings for a Neuropixels 2.0 quad-shank probe."""

    model_config = {
        "json_schema_extra": _bind_typename(
            {}, "OpenEphys.Onix1.NeuropixelsV2QuadShankProbeConfiguration"
        )
    }

    reference_serialized: str = Field(
        default="External",
        examples=["External", "Tip"],
        description="The probe reference to record against.",
    )
    invert_polarity: bool = Field(
        default=True, description="Whether to invert the polarity of the recorded signal."
    )
    gain_calibration_file_name: str = Field(
        default="",
        examples=["NP2_gain_calibration.csv"],
        description="Path to the gain calibration file supplied with the probe.",
    )
    probe_interface_file_name: str = Field(
        default="",
        examples=["NP2_probe_interface.json"],
        description="Path to the ProbeInterface file describing the probe geometry.",
    )


# Deliberately not bound to ConfigureNeuropixelsV2PsbDecoder: that type carries DeviceName and
# DeviceAddress which would be exposed by using this type directly. They should remain handled by ONIX.
class NeuropixelsV2Probe(BaseSchema):
    """One of the two probes addressed by a Neuropixels 2.0e headstage."""

    enable: bool = Field(default=False, description="Whether to acquire data from this probe.")
    probe_configuration: NeuropixelsV2QuadShankProbeConfiguration = Field(
        description="Calibration and reference settings for this probe."
    )


class AutoPortVoltage(BaseSchema):
    """The headstage port voltage, or auto-negotiation when left unset."""

    model_config = {"json_schema_extra": _bind_typename({}, "OpenEphys.Onix1.AutoPortVoltage")}

    requested: float | None = Field(
        default=None,
        description="Requested port voltage in volts. Leave unset to auto-negotiate.",
    )

# Port is declared first as the assignment order the workflow must
# follow Port, then Name.
class NeuropixelsV2eHeadstage(BaseSchema):
    """A Neuropixels 2.0e headstage carrying two quad-shank probes."""

    port: PortName = Field(
        default=PortName.PORT_A, description="The breakout-board port the headstage is connected to."
    )
    port_voltage: AutoPortVoltage = Field(
        description="Port voltage settings for the headstage link."
    )
    buffer_size: int = Field(
        default=30, gt=0, description="Number of frames buffered per probe read."
    )
    probe_a: NeuropixelsV2Probe = Field(description="Configuration for probe A.")
    probe_b: NeuropixelsV2Probe = Field(description="Configuration for probe B.")


class HarpSyncInput(BaseSchema):
    """The Harp clock synchronisation input on the ONIX breakout board."""

    enable: bool = Field(default=True, description="Whether to acquire Harp synchronisation data.")
    source: HarpSyncSource = Field(
        default=HarpSyncSource.CLOCK_ADAPTER,
        description="The hardware source of the synchronisation signal.",
    )


class EphysConfiguration(BaseSchema):
    """Top-level ONIX electrophysiology configuration loaded from YAML."""

    headstage: NeuropixelsV2eHeadstage = Field(
        description="Configuration for the Neuropixels 2.0e headstage."
    )
    harp_input: HarpSyncInput = Field(
        description="Configuration for the breakout board's Harp synchronisation input."
    )


