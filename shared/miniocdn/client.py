import os
import logging
from enum import Enum
from typing import Union
from minio import Minio
from minio.error import S3Error
import urllib3

class BucketType(Enum):
    """
    Defines standard bucket types for Minio storage.
    The values of the enum members are the actual bucket names that will be used.
    This helps in standardizing bucket naming across services.
    """
    # For raw, unprocessed files
    RAW = "raw"

    # For processed videos with different resolutions
    PROCESSED_VIDEO_1080P = "processed-video-1080p"
    PROCESSED_VIDEO_720P = "processed-video-720p"
    PROCESSED_VIDEO_480P = "processed-video-480p"

    # For processed photos with different sizes
    PROCESSED_PHOTO_LARGE = "processed-photo-large"
    PROCESSED_PHOTO_THUMBNAIL = "processed-photo-thumbnail"


class MinioClient:
    """
    A robust wrapper class for Minio client operations, designed to be
    reused across different services.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        """
        Initializes the MinioClient. All parameters are required.

        Args:
            endpoint (str): The URL of the Minio server (e.g., 'localhost:9000').
            access_key (str): The access key for Minio.
            secret_key (str): The secret key for Minio.
            secure (bool): Whether to use TLS (HTTPS) for the connection. Defaults to False.
        
        Raises:
            ValueError: If any of the required connection parameters are missing.
        """
        if not all([endpoint, access_key, secret_key]):
            raise ValueError("Minio endpoint, access_key, and secret_key must be provided.")

        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            logging.info("MinioCDN client initialized successfully.")
        except Exception as e:
            logging.critical(f"Fatal: Failed to initialize Minio client: {e}", exc_info=True)
            raise

    def _ensure_bucket_exists(self, bucket_name: str):
        """
        A private helper method that checks if a bucket exists and creates it if it does not.
        This makes upload operations more resilient.

        Args:
            bucket_name (str): The name of the bucket to check/create.
        
        Raises:
            S3Error: If there is an API error creating the bucket.
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logging.info(f"Successfully created Minio bucket: {bucket_name}")
        except S3Error as e:
            logging.error(f"Error: Could not check or create bucket '{bucket_name}': {e}", exc_info=True)
            raise

    def file_exists(self, bucket_name: BucketType, object_name: str) -> bool:
        """
        Checks if an object already exists in a specific bucket.

        Args:
            bucket_name (BucketType): The `BucketType` enum member for the bucket.
            object_name (str): The unique name/key of the object (file).

        Returns:
            bool: True if the object exists, False otherwise.
        """
        bucket_name_str = bucket_name.value
        try:
            self.client.stat_object(bucket_name_str, object_name)
            return True
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return False
            # For any other S3 errors, it's better to let the caller handle it.
            logging.error(f"Error checking for object '{object_name}' in bucket '{bucket_name_str}': {exc}", exc_info=True)
            raise

    def upload_file(self, bucket_name: BucketType, object_name: str, file_path: str):
        """
        Uploads a local file to a specified Minio bucket. It will create the
        bucket if it doesn't already exist.

        Args:
            bucket_name (BucketType): The `BucketType` enum member for the destination bucket.
            object_name (str): The name for the object in the bucket.
            file_path (str): The local path to the file to upload.
        """
        bucket_name_str = bucket_name.value
        self._ensure_bucket_exists(bucket_name_str)

        try:
            self.client.fput_object(bucket_name_str, object_name, file_path)
        except S3Error as e:
            logging.error(f"Error uploading '{file_path}' to '{bucket_name_str}/{object_name}': {e}", exc_info=True)
            raise

    def upload_stream(self, bucket_name: BucketType, object_name: str, stream, part_size: int = 10 * 1024 * 1024):
        """
        Uploads data from a stream to a specified Minio bucket using multipart upload.
        This is suitable for streams of unknown length. It will create the bucket
        if it doesn't already exist.

        Args:
            bucket_name (BucketType): The `BucketType` enum member for the destination bucket.
            object_name (str): The name for the object in the bucket.
            stream: A file-like object (stream) to read data from.
            part_size (int): The part size for the multipart upload.
        """
        bucket_name_str = bucket_name.value
        self._ensure_bucket_exists(bucket_name_str)

        try:
            # Using length=-1 tells the client to perform a multipart upload
            # until the stream is exhausted.
            self.client.put_object(
                bucket_name_str,
                object_name,
                data=stream,
                length=-1,
                part_size=part_size
            )
        except S3Error as e:
            logging.error(f"Error uploading stream to '{bucket_name_str}/{object_name}': {e}", exc_info=True)
            raise

    def get_file_stream(self, bucket_name: BucketType, object_name: str) -> urllib3.response.BaseHTTPResponse:
        """
        Retrieves an object from a specified bucket as a stream.

        The caller is responsible for managing the stream, including closing it
        to release the connection (e.g., by using a `with` statement on the
        response or calling `response.close()`).

        Args:
            bucket_name (BucketType): The `BucketType` enum member for the bucket.
            object_name (str): The name of the object to retrieve.

        Returns:
            urllib3.response.BaseHTTPResponse: A stream-like object with the file's content.

        Raises:
            S3Error: If the object does not exist or another API error occurs.
        """
        bucket_name_str = bucket_name.value
        try:
            response = self.client.get_object(bucket_name_str, object_name)
            return response
        except S3Error as e:
            logging.error(f"Error getting object '{object_name}' from bucket '{bucket_name_str}': {e}", exc_info=True)
            raise

    def delete_file(self, bucket_name: BucketType, object_name: str):
        """
        Deletes an object from a specified bucket.

        Args:
            bucket_name (BucketType): The `BucketType` enum member for the bucket.
            object_name (str): The name of the object to delete.
        """
        bucket_name_str = bucket_name.value
        try:
            self.client.remove_object(bucket_name_str, object_name)
        except S3Error as e:
            logging.error(f"Error deleting object '{object_name}' from bucket '{bucket_name_str}': {e}", exc_info=True)
            raise
