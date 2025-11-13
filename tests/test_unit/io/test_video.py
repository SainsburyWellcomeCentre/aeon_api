"""Tests for the `swc.aeon.io.video` module."""

import cv2
import numpy as np
import pandas as pd
import pytest

from swc.aeon.io import video


@pytest.fixture
def video_metadata(video_csv_file):
    """Mimic sample video metadata DataFrame created from multiple video files."""
    df = pd.read_csv(
        video_csv_file,
        index_col=0,
        header=0,
        names=["seconds", "hw_counter", "hw_timestamp"],
    )
    df["_frame"] = range(len(df))
    df["_path"] = [(video_csv_file.parent / "video1.avi")] * 5 + [
        (video_csv_file.parent / "video2.avi")
    ] * 5
    df["_epoch"] = video_csv_file.parents[1].name
    return df


@pytest.fixture
def mock_frame():
    """Return a mock video frame (numpy array)."""
    return np.zeros((480, 640, 3), dtype=np.uint8)


class TestFrames:
    """Tests for the frames() function."""

    @pytest.fixture
    def mock_capture(self, mocker, mock_frame):
        """Return a mocked cv2.VideoCapture object and the constructor patch."""

        def factory(*, read_return=(True, mock_frame), read_side_effect=None):
            instance = mocker.Mock()
            if read_side_effect is not None:
                instance.read.side_effect = read_side_effect
            else:
                instance.read.return_value = read_return
            constructor = mocker.patch("swc.aeon.io.video.cv2.VideoCapture", return_value=instance)
            return instance, constructor

        return factory

    @pytest.mark.parametrize(
        (
            "start_idx",
            "end_idx",
            "frames_len",
            "video_capture_calls",
            "read_calls",
            "release_calls",
        ),
        [
            (0, 1, 1, 1, 1, 1),
            (0, 3, 3, 1, 3, 1),
            (0, 10, 10, 2, 10, 2),
            (2, 5, 3, 1, 3, 1),
        ],
        ids=[
            "single frame",
            "multiple sequential frames",
            "frames from multiple videos",
            "seek non-zeroth frame",
        ],
    )
    def test_valid_frames(
        self,
        video_metadata,
        mock_capture,
        start_idx,
        end_idx,
        frames_len,
        video_capture_calls,
        read_calls,
        release_calls,
        request,
    ):
        """Test reading frames with various parameters."""
        mock_capture_instance, mock_capture_constructor = mock_capture()
        data = video_metadata.iloc[start_idx:end_idx]
        result = list(video.frames(data))
        assert len(result) == frames_len
        assert all(isinstance(frame, np.ndarray) for frame in result)
        assert mock_capture_constructor.call_count == video_capture_calls
        assert mock_capture_instance.read.call_count == read_calls
        assert mock_capture_instance.release.call_count == release_calls
        if "seek" in request.node.callspec.id:
            mock_capture_instance.set.assert_called_with(cv2.CAP_PROP_POS_FRAMES, start_idx)

    @pytest.mark.parametrize(
        ("read_return", "read_side_effect", "expected_exception", "match_msg"),
        [
            ((False, None), None, ValueError, "Unable to read frame 0"),
            (None, RuntimeError("Read error"), RuntimeError, None),
        ],
        ids=["read_failure", "exception_during_read"],
    )
    def test_frames_error_handling(
        self, mock_capture, video_metadata, read_return, read_side_effect, expected_exception, match_msg
    ):
        """Test that `frames` handles read errors and exceptions and releases resources properly."""
        mock_capture_instance, _ = mock_capture(read_return=read_return, read_side_effect=read_side_effect)
        data = video_metadata.iloc[:1]
        with pytest.raises(expected_exception, match=match_msg):
            list(video.frames(data))
        mock_capture_instance.release.assert_called_once()
