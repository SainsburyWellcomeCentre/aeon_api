"""Helper functions for processing video data."""

import math
from collections.abc import Callable, Iterable, Iterator, Sequence

import cv2
import numpy as np
import pandas as pd

from swc.aeon.io import video


def gridframes(
    frames: list[np.ndarray],
    width: int,
    height: int,
    shape: None | int | tuple[int, int] = None,
) -> np.ndarray:
    """Arrange a set of frames into a grid layout with the specified pixel dimensions and shape.

    Args:
        frames: A list of frames to include in the grid layout.
        width: The width of the output grid image, in pixels.
        height: The height of the output grid image, in pixels.
        shape: Either the number of frames to include, or the number of rows and columns
            in the output grid image layout.

    Returns:
        A new image containing the arrangement of the frames in a grid.
    """
    if shape is None:
        shape = len(frames)
    if isinstance(shape, int):
        shape = math.ceil(math.sqrt(shape))
        shape = (shape, shape)

    dsize = (height, width, 3)
    cellsize = (height // shape[0], width // shape[1], 3)
    grid = np.zeros(dsize, dtype=np.uint8)
    for i in range(shape[0]):
        for j in range(shape[1]):
            k = i * shape[1] + j
            if k >= len(frames):
                continue
            frame = frames[k]
            i0 = i * cellsize[0]
            j0 = j * cellsize[1]
            i1 = i0 + cellsize[0]
            j1 = j0 + cellsize[1]
            grid[i0:i1, j0:j1] = cv2.resize(frame, (cellsize[1], cellsize[0]))
    return grid


def averageframes(frames: Sequence[np.ndarray]) -> np.ndarray:
    """Return the average of the specified collection of frames.

    Args:
        frames: A sequence of frames to average.

    Returns:
        A new image containing the average of the input frames.
    """
    return cv2.convertScaleAbs(np.sum(np.multiply(1 / len(frames), frames)))


def groupframes(
    frames: Iterable[np.ndarray],
    n: int,
    fun: Callable[[list[np.ndarray]], np.ndarray],
) -> Iterator[np.ndarray]:
    """Apply the specified function to each group of n frames.

    Args:
        frames: A sequence of frames to process.
        n: The number of frames in each group.
        fun: The function used to process each group of frames.

    Returns:
        An iterator returning the results of applying the function to each group.
    """
    i = 0
    group = []
    for frame in frames:
        group.append(frame)
        if len(group) >= n:
            yield fun(group)
            group.clear()
            i = i + 1


def triggerclip(
    data: pd.DataFrame,
    events: pd.Index | pd.Series,
    before: pd.Timedelta | None = None,
    after: pd.Timedelta | None = None,
) -> pd.DataFrame:
    """Split video data around the specified sequence of event timestamps.

    Args:
        data: A DataFrame where each row specifies video acquisition path and frame number.
        events: A sequence of timestamps to extract.
        before: The left offset from each timestamp used to clip the data.
        after: The right offset from each timestamp used to clip the data.

    Returns:
        A DataFrame containing the frames, clip and sequence numbers for each event timestamp.
    """
    if before is None:
        before = pd.Timedelta(0)

    if after is None:
        after = pd.Timedelta(0)

    if not isinstance(events, pd.Index):
        events = events.index

    clips = []
    for i, index in enumerate(events):
        clip = data.loc[(index - before) : (index + after)].copy()
        clip["frame_sequence"] = list(range(len(clip)))
        clip["clip_sequence"] = i
        clips.append(clip)
    return pd.concat(clips)


def collatemovie(
    clipdata: pd.DataFrame,
    fun: Callable[[list[np.ndarray]], np.ndarray],
) -> Iterator[np.ndarray]:
    """Collate a set of video clips into a single movie using the specified aggregation function.

    Args:
        clipdata: A DataFrame where each row specifies video path, frame number, clip and
            sequence number. This DataFrame can be obtained from the output of `triggerclip`.
        fun: The aggregation function used to process the frames in each clip.

    Returns:
        The sequence of processed frames representing the collated movie.
    """
    clipcount = len(clipdata.groupby("clip_sequence").frame_sequence.count())
    allframes = video.frames(clipdata.sort_values(by=["frame_sequence", "clip_sequence"]))
    return groupframes(allframes, clipcount, fun)


def gridmovie(
    clipdata: pd.DataFrame,
    width: int,
    height: int,
    shape: None | int | tuple[int, int] = None,
) -> Iterator[np.ndarray]:
    """Collate a set of video clips into a grid movie with the specified pixel dimensions and grid layout.

    Args:
        clipdata: A DataFrame where each row specifies video path, frame number, clip and
            sequence number. This DataFrame can be obtained from the output of `triggerclip`.
        width: The width of the output grid movie, in pixels.
        height: The height of the output grid movie, in pixels.
        shape: Either the number of frames to include, or the number of rows and columns
            in the output grid movie layout.

    Returns:
        The sequence of processed frames representing the collated grid movie.
    """
    return collatemovie(clipdata, lambda g: gridframes(g, width, height, shape))
