"""Registry of known Aeon experiment schemas for use with ``run_qc()``."""

from os import PathLike
from pathlib import Path
from typing import Any

import swc.aeon.schema.core as stream
from dotmap import DotMap
from swc.aeon.io.api import load
from swc.aeon.io.reader import Metadata as MetadataReader
from swc.aeon.schema.streams import Device

from aeon_qc import foraging, social

# Foraging experiment (exp0.2). Verified against aeon_mecha/aeon/schema/schemas.py.
exp02 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("ExperimentalMetadata", stream.Environment, stream.MessageLog),
        Device("CameraTop", stream.Video, stream.Position, foraging.Region),
        Device("CameraEast", stream.Video),
        Device("CameraNest", stream.Video),
        Device("CameraNorth", stream.Video),
        Device("CameraPatch1", stream.Video),
        Device("CameraPatch2", stream.Video),
        Device("CameraSouth", stream.Video),
        Device("CameraWest", stream.Video),
        Device("Nest", foraging.Weight),
        Device("Patch1", stream.Heartbeat, foraging.Patch),
        Device("Patch2", stream.Heartbeat, foraging.Patch),
    ]
)

# Social 0.2 experiment. Verified against aeon_mecha/aeon/schema/schemas.py (social02).
social02 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("Environment", social.Environment, social.SubjectData),
        Device("CameraTop", stream.Video, social.Pose),
        Device("CameraNorth", stream.Video),
        Device("CameraSouth", stream.Video),
        Device("CameraEast", stream.Video),
        Device("CameraWest", stream.Video),
        Device("CameraPatch1", stream.Video),
        Device("CameraPatch2", stream.Video),
        Device("CameraPatch3", stream.Video),
        Device("CameraNest", stream.Video),
        Device("Nest", social.WeightRaw, social.WeightFiltered),
        Device("Patch1", stream.Heartbeat, social.Patch),
        Device("Patch2", stream.Heartbeat, social.Patch),
        Device("Patch3", stream.Heartbeat, social.Patch),
        Device("GateRfid", stream.Heartbeat, social.RfidEvents),
        Device("NestRfid1", stream.Heartbeat, social.RfidEvents),
        Device("NestRfid2", stream.Heartbeat, social.RfidEvents),
        Device("Patch1Rfid", stream.Heartbeat, social.RfidEvents),
        Device("Patch2Rfid", stream.Heartbeat, social.RfidEvents),
        Device("Patch3Rfid", stream.Heartbeat, social.RfidEvents),
    ]
)

# Social 0.3 experiment. Verified against aeon_mecha/aeon/schema/schemas.py (social03).
# Same device types as social02 but RFID devices use older naming convention.
social03 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("Environment", social.Environment, social.SubjectData),
        Device("CameraTop", stream.Video, social.Pose),
        Device("CameraNorth", stream.Video),
        Device("CameraSouth", stream.Video),
        Device("CameraEast", stream.Video),
        Device("CameraWest", stream.Video),
        Device("CameraPatch1", stream.Video),
        Device("CameraPatch2", stream.Video),
        Device("CameraPatch3", stream.Video),
        Device("CameraNest", stream.Video),
        Device("Nest", social.WeightRaw, social.WeightFiltered),
        Device("Patch1", stream.Heartbeat, social.Patch),
        Device("Patch2", stream.Heartbeat, social.Patch),
        Device("Patch3", stream.Heartbeat, social.Patch),
        Device("RfidGate", stream.Heartbeat, social.RfidEvents),
        Device("RfidNest1", stream.Heartbeat, social.RfidEvents),
        Device("RfidNest2", stream.Heartbeat, social.RfidEvents),
        Device("RfidPatch1", stream.Heartbeat, social.RfidEvents),
        Device("RfidPatch2", stream.Heartbeat, social.RfidEvents),
        Device("RfidPatch3", stream.Heartbeat, social.RfidEvents),
    ]
)

# Social 0.4 experiment. Device types matching social03.
social04 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        # Harp timing devices
        Device("ClockSynchronizer", stream.Heartbeat),
        Device("VideoController", stream.Heartbeat),
        # Patches (UndergroundFeeder — same type as social02)
        Device("Patch1", stream.Heartbeat, social.Patch),
        Device("Patch2", stream.Heartbeat, social.Patch),
        Device("Patch3", stream.Heartbeat, social.Patch),
        Device("PatchDummy1", stream.Heartbeat, social.Patch),
        # Nest weight scale (registers 200, 202 confirmed on disk)
        Device("Nest", social.WeightRaw, social.WeightFiltered),
        # RFID readers (register 32, same as social02)
        Device("NestRfid1", social.RfidEvents),
        Device("NestRfid2", social.RfidEvents),
        Device("GateRfid", social.RfidEvents),
        Device("GateEastRfid", stream.Heartbeat, social.RfidEvents),
        Device("GateWestRfid", stream.Heartbeat, social.RfidEvents),
        Device("Patch1Rfid", social.RfidEvents),
        Device("Patch2Rfid", social.RfidEvents),
        Device("Patch3Rfid", social.RfidEvents),
        Device("PatchDummy1Rfid", stream.Heartbeat, social.RfidEvents),
        # Cameras (SpinnakerVideoSource)
        Device("CameraTop", stream.Video),
        Device("CameraWest", stream.Video),
        Device("CameraEast", stream.Video),
        Device("CameraNorth", stream.Video),
        Device("CameraSouth", stream.Video),
        Device("CameraNest", stream.Video),
        Device("CameraPatch1", stream.Video),
        Device("CameraPatch2", stream.Video),
        Device("CameraPatch3", stream.Video),
        Device("CameraLightMonitor", stream.Video),
    ]
)

REGISTRY: dict[str, Any] = {
    "exp02": exp02,
    "social02": social02,
    "social03": social03,
    "social04": social04,
}


def schema_from_root(root: str | PathLike) -> DotMap:
    """Build a QC schema by run_qcning actual files in the first epoch directory."""
    root_path = Path(root)
    epoch_dirs = sorted(d for d in root_path.iterdir() if d.is_dir() and "T" in d.name)
    if not epoch_dirs:
        raise FileNotFoundError(f"No epoch directories found under {root}")

    first_epoch = epoch_dirs[0]
    harp_devices: list[str] = []
    video_devices: list[str] = []

    for device_dir in sorted(first_epoch.iterdir()):
        if not device_dir.is_dir():
            continue
        name = device_dir.name
        if any(device_dir.glob(f"{name}_8_*.bin")):
            harp_devices.append(name)
        if any(device_dir.glob("*.avi")):
            video_devices.append(name)

    schema_devices: list[Device] = [Device("Metadata", stream.Metadata)]
    for name in harp_devices:
        schema_devices.append(Device(name, stream.Heartbeat))
    for name in video_devices:
        schema_devices.append(Device(name, stream.Video))

    return DotMap(schema_devices)


def schema_from_metadata(root: str | PathLike) -> DotMap:
    """Build a QC schema by matching the root path against the REGISTRY."""
    root_path = Path(root)

    # Try each path component as a potential registry key (e.g. "social0.4" → "social04")
    for part in root_path.parts:
        registry_key = part.replace(".", "")
        if registry_key in REGISTRY:
            return REGISTRY[registry_key]

    # Fallback: discover devices from Metadata.yml Devices block
    meta_df = load(root, MetadataReader())
    devices_dotmap = meta_df.iloc[0].metadata.Devices
    schema_devices: list[Device] = [Device("Metadata", stream.Metadata)]

    for device_name, device_cfg in devices_dotmap.items():
        is_harp = bool(getattr(device_cfg, "PortName", None))
        is_camera = getattr(device_cfg, "Type", "") == "SpinnakerVideoSource"

        if is_harp:
            schema_devices.append(Device(device_name, stream.Heartbeat))
        if is_camera:
            schema_devices.append(Device(device_name, stream.Video))

    return DotMap(schema_devices)
