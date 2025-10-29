import json
from pathlib import Path
from typing import Annotated, List, Union

from pydantic import Field
from swc.aeon_rigs.base import BaseSchema
from swc.aeon_rigs.foraging import UndergroundFeeder
from swc.aeon_rigs.video import SpinnakerCamera
from swc.aeon_rigs.harp import HarpClockSynchronizer, HarpTimestampGeneratorGen3


class Rig(BaseSchema):
    cameras: List[SpinnakerCamera]
    feeders: List[UndergroundFeeder]
    clock_synchronizer: Annotated[
        Union[HarpClockSynchronizer, HarpTimestampGeneratorGen3],
        Field(discriminator="device_type"),
    ]


def main():
    schema = Rig.model_json_schema(union_format="primitive_type_array")
    Path("rig.json").write_text(json.dumps(schema, indent=2))


if __name__ == "__main__":
    main()
