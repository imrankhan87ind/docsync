import logging
import os
import tempfile
from typing import IO, Dict, Any, List, Optional
 
import cv2
import imagehash
import numpy as np
from PIL import Image
from pydantic import BaseModel, Field
from pymediainfo import MediaInfo


class VideoMetadata(BaseModel):
    """
    A data class to hold extracted video metadata.
    """
    title: Optional[str] = Field(default=None, description="The title of the video.")
    description: Optional[str] = Field(default=None, description="A short description or comment about the video.")
    tags: List[str] = Field(default_factory=list, description="A list of tags or keywords associated with the video.")
    creation_date: Optional[str] = Field(default=None, description="The creation or encoding date of the video.")
    make: Optional[str] = Field(default=None, description="The manufacturer of the recording device (e.g., 'Apple').")
    model: Optional[str] = Field(default=None, description="The model of the recording device (e.g., 'iPhone 14 Pro').")
    width: Optional[int] = Field(default=None, description="The width of the video in pixels.")
    height: Optional[int] = Field(default=None, description="The height of the video in pixels.")
    frame_rate: Optional[float] = Field(default=None, description="The frame rate of the video in frames per second.")
    location: Optional[Dict[str, Any]] = Field(default=None, description="Geospatial data (e.g., GPS coordinates) of where the video was recorded.")
    video_codecs: List[str] = Field(default_factory=list, description="A list of video codecs used in the file.")
    audio_codecs: List[str] = Field(default_factory=list, description="A list of audio codecs used in the file.")
    perceptual_hashes: List[str] = Field(default_factory=list, description="A list of perceptual hashes (e.g., aHash) for sampled frames.")
    laplacian_variances: List[float] = Field(default_factory=list, description="A list of Laplacian variances for sampled frames to detect blurriness.")
    raw_metadata: Optional[Dict[str, Any]] = Field(default=None, description="The complete, unprocessed metadata from MediaInfo.")

    @property
    def resolution(self) -> Optional[str]:
        """Returns the resolution as a 'WxH' string."""
        if self.width and self.height:
            return f"{self.width}x{self.height}"
        return None


class VideoMetadataExtractor:
    """
    Extracts metadata from video files.

    This class uses pymediainfo, which is a Python wrapper for MediaInfo.
    Therefore, the 'mediainfo' command-line tool must be installed and
    available in the system's PATH.

    It also uses OpenCV, Pillow, imagehash, and NumPy for frame analysis to
    calculate perceptual hash and blurriness. These libraries must be
    installed.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _analyze_video_frames(self, file_path: str, metadata: VideoMetadata):
        """
        Analyzes up to 10 evenly spaced frames from the video to extract
        perceptual hashes and laplacian variances (for blur detection).
        If the video has fewer than 10 frames, all frames are analyzed.
        """
        cap = None
        try:
            cap = cv2.VideoCapture(file_path)
            if not cap.isOpened():
                self.logger.warning(f"Could not open video file with OpenCV: {file_path}")
                return

            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if frame_count <= 0:
                return

            # Determine the indices of frames to analyze
            num_frames_to_sample = min(frame_count, 10)
            indices = np.linspace(
                0, frame_count - 1, num=num_frames_to_sample, dtype=int
            )

            for frame_index in indices:
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_index)
                ret, frame = cap.read()
                if ret:
                    # Convert to grayscale for blur calculation
                    gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

                    # 1. Calculate Laplacian variance (blur)
                    variance = cv2.Laplacian(gray_frame, cv2.CV_64F).var()
                    metadata.laplacian_variances.append(variance)

                    # 2. Calculate perceptual hash
                    # Convert from BGR (OpenCV) to RGB (Pillow)
                    pil_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
                    p_hash = str(imagehash.average_hash(pil_image))
                    metadata.perceptual_hashes.append(p_hash)
                else:
                    self.logger.warning(f"Could not read frame {frame_index} from {file_path}")
        except Exception as e:
            self.logger.error(f"Error analyzing video frames from {file_path}: {e}")
        finally:
            if cap:
                cap.release()

    def extract(self, file_content: IO[bytes]) -> VideoMetadata:
        """
        Extracts metadata from the given video byte stream.

        Args:
            file_content: A file-like object containing the video data in bytes.

        Returns:
            A VideoMetadata object containing the extracted information.

        Raises:
            ValueError: If the video stream cannot be processed or parsed.
        """
        temp_file_path = None
        try:
            # Create a temporary file to store the video content.
            # We use delete=False because on some OSes (like Windows), a file
            # cannot be opened by another process while it is still open.
            # We will manually close it and then clean it up in the finally block.
            with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_f:
                temp_file_path = temp_f.name
                temp_f.write(file_content.read())

            # Set `parse_speed=0` for a more thorough analysis
            media_info = MediaInfo.parse(temp_file_path, parse_speed=0)

            metadata = VideoMetadata()
            metadata.raw_metadata = media_info.to_data()

            general_track = next((t for t in media_info.tracks if t.track_type == 'General'), None)
            video_track = next((t for t in media_info.tracks if t.track_type == 'Video'), None)

            if general_track:
                metadata.title = general_track.title
                metadata.description = general_track.comment
                metadata.creation_date = general_track.encoded_date or general_track.tagged_date

                # Tags/keywords can be stored in different fields.
                if general_track.keywords:
                    metadata.tags = [tag.strip() for tag in general_track.keywords.split(',')]
                elif general_track.subject:
                    metadata.tags = [tag.strip() for tag in general_track.subject.split(',')]

            if video_track:
                metadata.width = video_track.width
                metadata.height = video_track.height
                try:
                    metadata.frame_rate = float(video_track.frame_rate)
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not parse frame rate: {video_track.frame_rate}")

                # Make and model might be in video track for some formats
                metadata.make = video_track.camera_manufacturer_name
                metadata.model = video_track.camera_model_name

            # Extract codecs
            for track in media_info.video_tracks:
                codec = track.codec_id or track.format
                if codec:
                    metadata.video_codecs.append(codec)

            for track in media_info.audio_tracks:
                codec = track.codec_id or track.format
                if codec:
                    metadata.audio_codecs.append(codec)

            self.logger.info("Location extraction is not implemented as it's highly format-dependent.")

            self._analyze_video_frames(temp_file_path, metadata)

            return metadata
        except Exception as e:
            self.logger.error(f"Failed to extract video metadata: {e}")
            raise ValueError(f"Failed to extract video metadata: {e}") from e
        finally:
            # Clean up the temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except OSError as e:
                    self.logger.error(f"Error removing temporary file {temp_file_path}: {e}")
