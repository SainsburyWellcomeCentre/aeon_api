"""Module for reading and writing video files using OpenCV."""

from collections.abc import Iterable

import cv2
import pandas as pd
from cv2.typing import MatLike


def frames(data: pd.DataFrame) -> Iterable[MatLike]:
    """Extracts the raw frames corresponding to the provided video metadata.

    Args:
        data: A DataFrame where each row specifies video acquisition path and frame number.

    Returns:
        iterable: An object to iterate over numpy arrays for each row in the DataFrame,
        containing the raw video frame data.
    """
    capture = None
    filename = None
    index = 0
    try:
        for frameidx, path in zip(data._frame, data._path, strict=False):
            if filename != path or capture is None:
                if capture is not None:
                    capture.release()
                capture = cv2.VideoCapture(path)
                filename = path
                index = 0

            if frameidx != index:
                capture.set(cv2.CAP_PROP_POS_FRAMES, frameidx)
                index = frameidx
            success, frame = capture.read()
            if not success:
                raise ValueError(f'Unable to read frame {frameidx} from video path "{path}".')
            yield frame
            index = index + 1
    finally:
        if capture is not None:
            capture.release()


def export(frames: Iterable[MatLike], filename: str, fps: float, fourcc: int | None = None) -> None:
    """Exports the specified frame sequence to a new video file.

    Args:
        frames: An object to iterate over the raw video frame data.
        filename: The path to the exported video file.
        fps: The frame rate of the exported video.
        fourcc: Specifies the four character code of the codec used to compress the frames.
    """
    writer = None
    try:
        for frame in frames:
            if writer is None:
                if fourcc is None:
                    fourcc = cv2.VideoWriter.fourcc(*"mp4v")
                writer = cv2.VideoWriter(filename, fourcc, fps, (frame.shape[1], frame.shape[0]))
            writer.write(frame)
    finally:
        if writer is not None:
            writer.release()
