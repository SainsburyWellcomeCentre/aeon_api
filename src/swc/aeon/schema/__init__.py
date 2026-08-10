"""Modules for building aeon schemas."""

# Set imports available directly under 'swc.aeon.schema'
from swc.aeon.schema.base import (
    BaseSchema,
    Dataset,
    Experiment,
    Metadata,
    bind_typename,
    data_reader,
)
from swc.aeon.schema.ephys import (
    AutoPortVoltage,
    EphysConfiguration,
    HarpSyncInput,
    HarpSyncSource,
    NeuropixelsV2eHeadstage,
    NeuropixelsV2Probe,
    NeuropixelsV2QuadShankProbeConfiguration,
    PortName,
)

__all__ = [
    "BaseSchema",
    "Experiment",
    "Dataset",
    "Metadata",
    "data_reader",
    "bind_typename",
    "AutoPortVoltage",
    "EphysConfiguration",
    "HarpSyncInput",
    "HarpSyncSource",
    "NeuropixelsV2eHeadstage",
    "NeuropixelsV2Probe",
    "NeuropixelsV2QuadShankProbeConfiguration",
    "PortName",
]
