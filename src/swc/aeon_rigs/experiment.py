from pydantic import Field
from swc.aeon_rigs.base import BaseSchema


class Experiment(BaseSchema):
    workflow: str = Field(description="Path to the workflow running the experiment.")
    commit: str = Field(description="Commit hash of the experiment repo.")
    repository_url: str = Field(
        description="The URL of the git repository used to version experiment source code."
    )
