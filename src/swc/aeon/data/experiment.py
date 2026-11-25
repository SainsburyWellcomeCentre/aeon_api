"""The base class for models of an experiment and the provenance of its acquisition epochs."""

from pydantic import Field

from swc.aeon.data.dataset import Dataset


class Experiment(Dataset):
    """The base class for models of an experiment, read from the metadata of each acquisition epoch.

    An experiment model derives from this class and adds the configuration of its rig. The fields
    declared here record the provenance of the epoch, under the names used in `Metadata.json`, so
    reading the metadata with this class alone returns the provenance of any experiment.
    """

    workflow: str = Field(description="Path to the workflow running the experiment.")
    commit: str = Field(description="Commit hash of the experiment repo.")
    repository_url: str = Field(
        alias="repositoryUrl",
        description="The URL of the git repository used to version experiment source code.",
    )
