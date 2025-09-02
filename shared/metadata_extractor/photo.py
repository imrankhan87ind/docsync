"""
Photo Metadata Extraction Utilities.

This module provides classes for extracting and structuring metadata from photo files,
primarily using EXIF data, and computes a perceptual hash for similarity detection.
This helps in organizing and enriching photo assets with details like geolocation,
capture time, and camera settings, as well as identifying duplicate or similar images.

Dependencies:
- Pillow: For reading image files and accessing basic metadata.
- piexif: For parsing complex EXIF data blocks.
- imagehash: For calculating perceptual hashes.
- opencv-python-headless: For image processing tasks like blur detection.
- numpy: For numerical operations and interoperability between Pillow and OpenCV.

Install dependencies in the relevant service (e.g., photo-processor):
pip install Pillow piexif pydantic imagehash opencv-python-headless numpy
"""

import logging
from datetime import datetime
from typing import IO, Optional, Tuple

import cv2
import imagehash
import numpy as np
import piexif
from PIL import Image
from pydantic import BaseModel, Field


class GPSMetadata(BaseModel):
    """A model to hold structured GPS coordinate information."""
    latitude: Optional[float] = Field(None, description="GPS latitude in decimal degrees.")
    longitude: Optional[float] = Field(None, description="GPS longitude in decimal degrees.")
    altitude: Optional[float] = Field(None, description="GPS altitude in meters above sea level.")


class PhotoMetadata(BaseModel):
    """A model to hold structured EXIF metadata and perceptual hash from a photo."""
    width: Optional[int] = Field(None, description="The width of the image in pixels.")
    height: Optional[int] = Field(None, description="The height of the image in pixels.")
    orientation: Optional[int] = Field(None, description="The EXIF orientation tag of the image (1-8). 1 is normal/horizontal.")
    horizontal_resolution: Optional[float] = Field(None, description="Horizontal resolution, typically in dots per inch (DPI).")
    vertical_resolution: Optional[float] = Field(None, description="Vertical resolution, typically in dots per inch (DPI).")
    bit_depth: Optional[int] = Field(None, description="The number of bits per pixel, indicating color depth.")
    perceptual_hash: Optional[str] = Field(None, description="The perceptual hash of the image, for finding duplicates.")
    laplacian_variance: Optional[float] = Field(None, description="The variance of the Laplacian of the image, used to measure blur. Higher is sharper.")
    gps: Optional[GPSMetadata] = None
    date_time_original: Optional[datetime] = Field(None, description="The date and time when the original image data was generated.")
    camera_make: Optional[str] = Field(None, description="The manufacturer of the camera or device.")
    camera_model: Optional[str] = Field(None, description="The model name or number of the camera or device.")
    f_number: Optional[float] = Field(None, description="The F-number of the lens (aperture).")
    exposure_time: Optional[str] = Field(None, description="Exposure time, typically as a fraction of a second (e.g., '1/100').")
    iso_speed: Optional[int] = Field(None, description="The ISO speed rating of the camera sensor.")


class PhotoMetadataExtractor:
    """
    Extracts key EXIF metadata and computes a perceptual hash from a photo stream,
    returning it in a structured format.
    """

    def _decode_exif_string(self, value: bytes) -> str:
        """Decodes an EXIF byte string, stripping null terminators."""
        try:
            return value.decode('utf-8').strip('\x00').strip()
        except UnicodeDecodeError:
            # Fallback for other common encodings if UTF-8 fails
            return value.decode('latin-1').strip('\x00').strip()
        except Exception:
            return ""

    def _convert_dms_to_dd(
        self,
        dms: Tuple[Tuple[int, int], Tuple[int, int], Tuple[int, int]], ref: str
    ) -> float:
        """Converts Degrees, Minutes, Seconds (and a reference) to Decimal Degrees."""
        degrees = dms[0][0] / dms[0][1]
        minutes = dms[1][0] / dms[1][1]
        seconds = dms[2][0] / dms[2][1]
        dd = degrees + minutes / 60.0 + seconds / 3600.0
        if ref in ['S', 'W']:
            dd *= -1
        return dd

    def _parse_gps_info(self, gps_ifd: dict) -> Optional[GPSMetadata]:
        """Parses the GPS IFD dictionary into a structured GPSMetadata object."""
        try:
            lat_dms = gps_ifd.get(piexif.GPSIFD.GPSLatitude)
            lon_dms = gps_ifd.get(piexif.GPSIFD.GPSLongitude)

            # GPSLatitude and GPSLongitude are mandatory for coordinates.
            if not lat_dms or not lon_dms:
                return None

            lat_ref = gps_ifd.get(piexif.GPSIFD.GPSLatitudeRef, b'N').decode()
            lon_ref = gps_ifd.get(piexif.GPSIFD.GPSLongitudeRef, b'W').decode()

            latitude = self._convert_dms_to_dd(lat_dms, lat_ref)
            longitude = self._convert_dms_to_dd(lon_dms, lon_ref)

            alt_data = gps_ifd.get(piexif.GPSIFD.GPSAltitude)
            altitude = alt_data[0] / alt_data[1] if alt_data else None

            return GPSMetadata(latitude=latitude, longitude=longitude, altitude=altitude)
        except (KeyError, IndexError, TypeError, ZeroDivisionError, AttributeError) as e:
            logging.warning(f"Could not parse GPS info due to malformed data: {e}", exc_info=True)
            return None

    def _parse_exif_datetime(self, dt_str_bytes: bytes) -> Optional[datetime]:
        """Parses an EXIF datetime string (e.g., '2023:09:21 10:30:00')."""
        if not dt_str_bytes:
            return None
        try:
            dt_str = self._decode_exif_string(dt_str_bytes)
            return datetime.strptime(dt_str, '%Y:%m:%d %H:%M:%S')
        except (ValueError, TypeError):
            logging.warning(f"Could not parse datetime string: '{dt_str_bytes}'")
            return None

    def _rational_to_float(self, rational: Tuple[int, int]) -> Optional[float]:
        """Converts a rational number (numerator, denominator) to a float."""
        if not rational or len(rational) != 2 or rational[1] == 0:
            return None
        return rational[0] / rational[1]

    def extract(self, image_stream: IO[bytes]) -> PhotoMetadata:
        """
        Extracts key metadata, computes a perceptual hash, and calculates blurriness
        from an image file stream.

        Args:
            image_stream: A file-like object containing the image data.

        Returns:
            A PhotoMetadata object containing the extracted information, hash, and blur score.
        """
        try:
            img = Image.open(image_stream)
            width, height = img.size

            # Determine bit depth from image mode
            mode_to_bpp = {
                '1': 1, 'L': 8, 'P': 8, 'RGB': 24, 'RGBA': 32,
                'CMYK': 32, 'YCbCr': 24, 'I': 32, 'F': 32,
            }
            bit_depth = mode_to_bpp.get(img.mode)

            # Calculate perceptual hash
            try:
                # phash is generally robust for photos.
                # The hash is converted to a string for serialization (e.g., JSON).
                p_hash = str(imagehash.phash(img))
            except Exception as e:
                logging.warning(f"Could not calculate perceptual hash: {e}", exc_info=True)
                p_hash = None

            # Calculate Laplacian variance for blur detection
            laplacian_var = None
            try:
                # Convert Pillow Image to OpenCV format (numpy array)
                # Ensure image is RGB before converting, then switch to BGR for OpenCV
                cv_image = cv2.cvtColor(np.array(img.convert('RGB')), cv2.COLOR_RGB2BGR)

                # Convert to grayscale for Laplacian calculation
                gray = cv2.cvtColor(cv_image, cv2.COLOR_BGR2GRAY)

                # Calculate the variance of the Laplacian. A higher value means a sharper image.
                # CV_64F is used to avoid overflow for positive/negative gradients.
                laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
            except Exception as e:
                logging.warning(f"Could not calculate Laplacian variance: {e}", exc_info=True)

            exif_data = img.info.get('exif')

            if not exif_data:
                logging.info("No EXIF data found in the image.")
                return PhotoMetadata(
                    width=width,
                    height=height,
                    bit_depth=bit_depth,
                    perceptual_hash=p_hash,
                    laplacian_variance=laplacian_var,
                    # Explicitly set EXIF-related fields to None to satisfy linters
                    # when no EXIF data is present.
                    orientation=None,  # No EXIF means no orientation tag.
                    horizontal_resolution=None,
                    vertical_resolution=None,
                    gps=None,
                    date_time_original=None,
                    camera_make=None,
                    camera_model=None,
                    f_number=None,
                    exposure_time=None,
                    iso_speed=None,
                )

            exif_dict = piexif.load(exif_data)
            zeroth_ifd = exif_dict.get("0th", {})
            exif_ifd = exif_dict.get("Exif", {})
            gps_ifd = exif_dict.get("GPS", {})

            # Parse Orientation. It's in the 0th IFD.
            orientation = zeroth_ifd.get(piexif.ImageIFD.Orientation)

            # Parse Resolution. It's in the 0th IFD.
            horizontal_resolution = self._rational_to_float(zeroth_ifd.get(piexif.ImageIFD.XResolution))
            vertical_resolution = self._rational_to_float(zeroth_ifd.get(piexif.ImageIFD.YResolution))

            # Parse GPS
            gps_metadata = self._parse_gps_info(gps_ifd)

            # Parse other key EXIF fields
            date_time_original = self._parse_exif_datetime(exif_ifd.get(piexif.ExifIFD.DateTimeOriginal))
            camera_make = self._decode_exif_string(zeroth_ifd.get(piexif.ImageIFD.Make, b''))
            camera_model = self._decode_exif_string(zeroth_ifd.get(piexif.ImageIFD.Model, b''))
            f_number = self._rational_to_float(exif_ifd.get(piexif.ExifIFD.FNumber))

            exposure_time_rational = exif_ifd.get(piexif.ExifIFD.ExposureTime)
            exposure_time_str = None
            if exposure_time_rational and len(exposure_time_rational) == 2 and exposure_time_rational[1] != 0:
                # Represent as a fraction for precision, e.g., "1/100"
                exposure_time_str = f"{exposure_time_rational[0]}/{exposure_time_rational[1]}"

            iso_speed = exif_ifd.get(piexif.ExifIFD.ISOSpeedRatings)

            return PhotoMetadata(
                width=width,
                height=height,
                bit_depth=bit_depth,
                horizontal_resolution=horizontal_resolution,
                vertical_resolution=vertical_resolution,
                orientation=orientation,
                perceptual_hash=p_hash,
                laplacian_variance=laplacian_var,
                gps=gps_metadata,
                date_time_original=date_time_original,
                camera_make=camera_make or None,
                camera_model=camera_model or None,
                f_number=f_number,
                exposure_time=exposure_time_str,
                iso_speed=iso_speed,
            )
        except Exception as e:
            logging.error(f"An unexpected error occurred during metadata extraction: {e}", exc_info=True)
            # In case of a catastrophic failure, return an empty object with all fields as None.
            # This explicit initialization satisfies linters like Pylance.
            return PhotoMetadata(
                width=None,
                height=None,
                bit_depth=None,
                horizontal_resolution=None,
                vertical_resolution=None,
                orientation=None,
                perceptual_hash=None,
                laplacian_variance=None,
                gps=None,
                date_time_original=None,
                camera_make=None,
                camera_model=None,
                f_number=None,
                exposure_time=None,
                iso_speed=None,
            )
