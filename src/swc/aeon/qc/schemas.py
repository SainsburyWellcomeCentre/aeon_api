"""Registry of known Aeon experiment schemas for use with ``run_qc()``.

Each schema is a DotMap mapping device names to stream readers.

- ``schema_from_metadata(root)`` — matches a root path component against REGISTRY
  (e.g. ``social0.4`` = ``social04``); falls back to Heartbeat + Video discovery
  from ``Metadata.yml``.
- ``schema_from_root(root, start, end)`` — pure filesystem fallback (no ``Metadata.yml``
  needed); discovers Heartbeat and Video streams only.

"""

import datetime
import re
from os import PathLike
from pathlib import Path
from typing import Any

import pandas as pd
import swc.aeon.schema.core as stream
from dotmap import DotMap
from swc.aeon.io.api import load
from swc.aeon.io.reader import Metadata as MetadataReader
from swc.aeon.schema.streams import Device

from swc.aeon.qc import foraging, octagon, social

EPOCH_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}$")

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
        Device("PatchDummy1", stream.Heartbeat, social.Patch),
        Device("GateRfid", stream.Heartbeat, social.RfidEvents),
        Device("GateEastRfid", stream.Heartbeat, social.RfidEvents),
        Device("GateWestRfid", stream.Heartbeat, social.RfidEvents),
        Device("NestRfid1", stream.Heartbeat, social.RfidEvents),
        Device("NestRfid2", stream.Heartbeat, social.RfidEvents),
        Device("Patch1Rfid", stream.Heartbeat, social.RfidEvents),
        Device("Patch2Rfid", stream.Heartbeat, social.RfidEvents),
        Device("Patch3Rfid", stream.Heartbeat, social.RfidEvents),
        Device("PatchDummy1Rfid", stream.Heartbeat, social.RfidEvents),
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

# Octagon 0.1 experiment. The Photodiode is a continuous 1 kHz Harp stream
# (via PhotodiodeReader, a HarpRate subclass) so run_qc dispatches harp_gaps
# on it. The OSC, TaskLogic, and Wall stream classes live in swc.aeon.qc.octagon
# for future event-count metrics, but they are intentionally not listed
# here because run_qc has no dispatch for those reader types yet.
octagon01 = DotMap(
    [
        Device("Metadata", stream.Metadata),
        Device("CameraTop", stream.Video),
        Device("CameraColorTop", stream.Video),
        Device("Photodiode", octagon.Photodiode),
        Device("VideoController", octagon.VideoController),
    ]
)

REGISTRY: dict[str, Any] = {
    "exp02": exp02,
    "social02": social02,
    "social03": social03,
    "social04": social04,
    "octagon01": octagon01,
}

# Schemas whose epoch directories are standalone sessions, not chunks of a
# continuous experiment. For these, each epoch directory is the natural QC
# scope: per-reader loads use the epoch dir as the root (so chunks from
# neighbouring sessions can't bleed in), and epoch_gaps is skipped because
# inter-session pauses are expected.
SELF_CONTAINED_SCHEMAS: set[str] = {"octagon01"}


def parse_epoch_timestamp(epoch_dir: Path) -> pd.Timestamp:
    """Parse the UTC timestamp from an epoch directory name (``YYYY-MM-DDTHH-MM-SS``)."""
    date, time = epoch_dir.name.split("T", 1)
    return pd.Timestamp(f"{date}T{time.replace('-', ':')}", tz="UTC")


def is_epoch_dir(path: Path) -> bool:
    """Return True if the path is a directory whose name matches ``YYYY-MM-DDTHH-MM-SS``."""
    return path.is_dir() and EPOCH_DIR_PATTERN.match(path.name) is not None


def normalise_timestamp(ts: str | datetime.datetime) -> pd.Timestamp:
    """Accept either a filesystem epoch name or an ISO 8601 string and return a UTC Timestamp.

    Filesystem epoch directories use hyphens in the time part (``YYYY-MM-DDTHH-MM-SS``).
    Both that format and standard ISO 8601 strings are accepted; naive datetimes are treated as UTC.
    """
    if isinstance(ts, datetime.datetime):
        return pd.Timestamp(ts, tz="UTC") if ts.tzinfo is None else pd.Timestamp(ts)
    date_part, sep, time_part = ts.partition("T")
    if sep and "-" in time_part and ":" not in time_part:
        ts = f"{date_part}T{time_part.replace('-', ':')}+00:00"
    result = pd.Timestamp(ts)
    if result.tzinfo is None:
        result = result.tz_localize("UTC")
    return result


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
    epoch_dirs = sorted(d for d in root.iterdir() if is_epoch_dir(d))
    epoch_dirs = [d for d in epoch_dirs if start <= parse_epoch_timestamp(d) < end]
    if not epoch_dirs:
        raise FileNotFoundError(f"No epoch directories found under {root} in [{start}, {end})")
    return epoch_dirs[0]


def derive_epoch_window(epoch_dir: Path) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """Return (start, end) for load() based on actual filenames inside epoch_dir.

    For rigs whose Harp hub uses a non-UTC time origin (e.g. Harris-lab's
    1904-01-01 baseline), the chunk filenames inside an epoch directory
    won't match the epoch dir's wall-clock name. Walks all files two
    levels deep, parses the trailing _YYYY-MM-DDTHH-MM-SS portion of every
    filename, and returns (min, max + 1 hour). Returns None when no
    parseable filenames are found.
    """
    timestamps: list[pd.Timestamp] = []
    for fname in epoch_dir.glob("*/*"):
        if not fname.is_file() or "_" not in fname.stem:
            continue
        last_token = fname.stem.rsplit("_", 1)[-1]
        try:
            timestamps.append(parse_epoch_timestamp(Path(last_token)))
        except (ValueError, IndexError):
            continue
    if not timestamps:
        return None
    return min(timestamps), max(timestamps) + pd.Timedelta(hours=1)


def schema_from_registry(root: str | PathLike) -> DotMap | None:
    """Return the REGISTRY schema for root, or None if no path component matches."""
    key = match_registry(root)
    if key is not None:
        return REGISTRY[key]
    else:
        return None


def load_metadata_devices(root: str | PathLike):
    """Return the Devices DotMap from Metadata.yml, or None if unavailable."""
    try:
        meta_df = load(root, MetadataReader())
        return meta_df.iloc[0].metadata.Devices
    except (AttributeError, IndexError, KeyError):
        return None


def schema_from_metadata(root: str | PathLike) -> DotMap | None:
    """Discover Heartbeat and Video devices from Metadata.yml, or None if unavailable."""
    devices_dotmap = load_metadata_devices(root)
    if devices_dotmap is None:
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
    """Return the best available schema, trying registry, then Metadata.yml, then filesystem."""
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
    ``metadata`` (all device names from ``Metadata.yml``), and ``filesystem`` (devices with
    Heartbeat or Video files in the first epoch directory).
    """
    # Source 1: registry (path-based match)
    registry_schema = schema_from_registry(root)
    registry_devices = (
        {name for name in registry_schema if name != "Metadata"}
        if registry_schema is not None
        else None
    )

    # Source 2: Metadata.yml — full device list
    devices_dotmap = load_metadata_devices(root)
    metadata_devices = set(devices_dotmap.keys()) if devices_dotmap is not None else None

    # Source 3: filesystem
    filesystem_devices: set[str] = set()
    try:
        fs_schema = schema_from_filesystem(root, start, end)
        filesystem_devices = {name for name in fs_schema if name != "Metadata"}
    except FileNotFoundError:
        pass

    return {
        "registry_key": match_registry(root),
        "registry": registry_devices,
        "metadata": metadata_devices,
        "filesystem": filesystem_devices,
    }
