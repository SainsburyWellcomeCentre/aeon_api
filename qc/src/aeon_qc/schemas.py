"""Registry of known Aeon experiment schemas for use with ``run_qc()``.

Each schema is a DotMap mapping device names to stream readers.

- ``schema_from_metadata(root)`` — matches a root path component against REGISTRY
  (e.g. ``social0.4`` = ``social04``); falls back to Heartbeat + Video discovery
  from ``Metadata.yml``.
- ``schema_from_root(root, start, end)`` — pure filesystem fallback (no ``Metadata.yml``
  needed); discovers Heartbeat and Video streams only.

"""

from os import PathLike
from pathlib import Path
from typing import Any

import pandas as pd
import swc.aeon.schema.core as stream
from dotmap import DotMap
from swc.aeon.io.api import load
from swc.aeon.io.reader import Metadata as MetadataReader
from swc.aeon.schema.streams import Device

from aeon_qc import foraging, octagon, social

# Foraging experiment (exp0.2). Verified against aeon_mecha/aeon/schema/schemas.py.
exp02 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        # Harp timing devices
        Device("ClockSynchronizer", stream.Heartbeat),
        Device("VideoController", stream.Heartbeat),
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
        # Harp timing devices
        Device("ClockSynchronizer", stream.Heartbeat),
        Device("VideoController", stream.Heartbeat),
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
# Same device types as social02 but RFID devices use different naming convention.
social03 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        # Harp timing devices
        Device("ClockSynchronizer", stream.Heartbeat),
        Device("VideoController", stream.Heartbeat),
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

# Octagon 0.1 experiment. Verified against aeon_mecha/aeon/schema/schemas.py (octagon01).
# Note: no Heartbeat or MessageLog devices — heartbeat_gaps, sync_delta, and
# harp_sync_alerts will return empty results for this schema.
octagon01 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("CameraTop", stream.Video),
        Device("CameraColorTop", stream.Video),
        Device("Photodiode", octagon.Photodiode),
        Device("OSC", octagon.OSC),
        Device("TaskLogic", octagon.TaskLogic),
        Device("Wall1", octagon.Wall),
        Device("Wall2", octagon.Wall),
        Device("Wall3", octagon.Wall),
        Device("Wall4", octagon.Wall),
        Device("Wall5", octagon.Wall),
        Device("Wall6", octagon.Wall),
        Device("Wall7", octagon.Wall),
        Device("Wall8", octagon.Wall),
    ]
)

REGISTRY: dict[str, Any] = {
    "exp02": exp02,
    "social02": social02,
    "social03": social03,
    "social04": social04,
    "octagon01": octagon01,
}


def parse_epoch_timestamp(epoch_dir: Path) -> pd.Timestamp:
    """Parse the UTC timestamp from an epoch directory name (``YYYY-MM-DDTHH-MM-SS``)."""
    date, time = epoch_dir.name.split("T", 1)
    return pd.Timestamp(f"{date}T{time.replace('-', ':')}", tz="UTC")


def match_registry(root: str | PathLike) -> str | None:
    """Return the REGISTRY key for *root*, or ``None`` if no path component matches.

    Strips dots and lowercases each component
    (e.g. ``social0.4`` = ``social04``) before checking against REGISTRY.
    """
    for part in Path(root).parts:
        key = part.replace(".", "").lower()
        if key in REGISTRY:
            return key
    return None


def first_epoch_dir(root: Path, start: pd.Timestamp, end: pd.Timestamp) -> Path:
    """Return the first epoch directory in [start, end)."""
    epoch_dirs = sorted(d for d in root.iterdir() if d.is_dir() and "T" in d.name)
    epoch_dirs = [d for d in epoch_dirs if start <= parse_epoch_timestamp(d) < end]
    if not epoch_dirs:
        raise FileNotFoundError(f"No epoch directories found under {root} in [{start}, {end})")
    return epoch_dirs[0]


def schema_from_registry(root: str | PathLike) -> DotMap | None:
    """Return the REGISTRY schema for root, or None if no path component matches."""
    key = match_registry(root)
    return REGISTRY[key] if key is not None else None


def schema_from_metadata(root: str | PathLike) -> DotMap | None:
    """Discover Heartbeat and Video devices from Metadata.yml, or None if unavailable."""
    try:
        meta_df = load(root, MetadataReader())
        devices_dotmap = meta_df.iloc[0].metadata.Devices
    except (AttributeError, IndexError, KeyError):
        return None

    schema_devices: list[Device] = [Device("Metadata", stream.Metadata)]
    for device_name, device_cfg in devices_dotmap.items():
        is_harp = bool(getattr(device_cfg, "PortName", None))
        is_camera = getattr(device_cfg, "Type", "") == "SpinnakerVideoSource"
        if is_harp:
            schema_devices.append(Device(device_name, stream.Heartbeat))
        elif is_camera:
            schema_devices.append(Device(device_name, stream.Video))

    return DotMap(schema_devices)


def schema_from_filesystem(
    root: str | PathLike,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> DotMap:
    """Discover Heartbeat and Video devices by scanning files in the first epoch directory."""
    first_epoch = first_epoch_dir(Path(root), start, end)
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


def build_schema(
    root: str | PathLike,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> DotMap:
    """Return the best available schema, trying registry → Metadata.yml → filesystem."""
    return (
        schema_from_registry(root)
        or schema_from_metadata(root)
        or schema_from_filesystem(root, start, end)
    )

def diagnose_devices(
    root: str | PathLike,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict:
    """Report device coverage from registry, Metadata.yml, and filesystem.

    Returns a dict with ``registry_key``, ``registry`` (device names from matched schema),
    ``metadata`` (device names from ``Metadata.yml``), and ``filesystem`` (devices with
    Heartbeat or Video files in the first epoch directory).
    """
    root_path = Path(root)

    # Source 1: registry (path-based match)
    registry_schema = schema_from_registry(root)
    registry_devices = (
        {name for name in registry_schema if name != "Metadata"}
        if registry_schema is not None
        else None
    )

    # Source 2: Metadata.yml
    metadata_devices = None
    try:
        meta_df = load(root, MetadataReader(), start, end)
        if not meta_df.empty:
            metadata_devices = set(meta_df.iloc[0].metadata.Devices.keys())
    except (AttributeError, IndexError, KeyError):
        pass

    # Source 3: filesystem
    filesystem_devices: set[str] = set()
    try:
        first_epoch = first_epoch_dir(root_path, start, end)
        for device_dir in sorted(first_epoch.iterdir()):
            if not device_dir.is_dir():
                continue
            name = device_dir.name
            if any(device_dir.glob(f"{name}_8_*.bin")) or any(device_dir.glob("*.avi")):
                filesystem_devices.add(name)
    except FileNotFoundError:
        pass

    return {
        "registry_key": match_registry(root),
        "registry": registry_devices,
        "metadata": metadata_devices,
        "filesystem": filesystem_devices,
    }
