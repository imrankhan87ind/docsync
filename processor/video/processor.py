import json
import logging
import time
from typing import Callable
import magic

from shared.environment.config import app_config
from shared.mongo.client import MongoDbClient, FileStatus, RawFileDetails
from shared.rabbitmq.client import RabbitMQClient, UploadMessage, VideoMessage, PhotoMessage
from shared.miniocdn.client import MinioClient, BucketType


class UploadProcessor:
    """
    A worker that consumes file upload messages from RabbitMQ and processes them.
    It handles the lifecycle of a file from 'UPLOADED' to 'COMPLETED' or 'FAILED'.
    """

    def __init__(self):
        """
        Initializes the UploadProcessor, setting up connections to MongoDB and RabbitMQ.
        """
        logging.info("Initializing UploadProcessor...")
        self.mongo_client = MongoDbClient(
                host=app_config.mongo.host,
                port=app_config.mongo.port,
                user=app_config.mongo.user,
                password=app_config.mongo.password,
                dbname=app_config.mongo.dbname
            )
        self.minio_client = MinioClient(
            endpoint=app_config.minio.endpoint,
            access_key=app_config.minio.access_key,
            secret_key=app_config.minio.secret_key,
        )
        self.rabbitmq_client = RabbitMQClient(
                host=app_config.rabbitmq.host,
                port=app_config.rabbitmq.port,
                user=app_config.rabbitmq.user,
                password=app_config.rabbitmq.password
            )
        logging.info("UploadProcessor initialized successfully.")

    def _extract_and_save_mime_type(self, file_id: str, file_data_stream) -> str | None:
        """
        Reads a file stream header, extracts the MIME type, and saves it to MongoDB.

        Args:
            file_id: The ID of the file being processed.
            file_data_stream: The stream object of the file from MinIO.

        Returns:
            The extracted MIME type as a string if successful, otherwise None.
        """
        try:
            # To avoid loading large files into memory, read only the first 2048 bytes,
            # which is sufficient for python-magic to determine the MIME type.
            file_header = file_data_stream.read(2048)

            mime_type = magic.from_buffer(file_header, mime=True)
            logging.info(f"Extracted MIME type '{mime_type}' for file_id: {file_id}")

            details = RawFileDetails(mime_type=mime_type)
            if not self.mongo_client.update_raw_file_details(file_id, details):
                logging.warning(f"Failed to update MIME type for file_id: {file_id}. The document may have been modified or deleted.")
                return None

            return mime_type
        except Exception as e:
            logging.error(f"An error occurred during MIME type extraction for file_id {file_id}: {e}", exc_info=True)
            return None

    def _prepare_for_processing(self, file_id: str) -> str | None:
        """
        Locks the file by setting its status to PROCESSING and retrieves its object_name.

        Args:
            file_id: The ID of the file to prepare.

        Returns:
            The object_name if successful, otherwise None.
        """
        logging.info(f"Updating status to PROCESSING for file_id: {file_id}")
        if not self.mongo_client.update_raw_file_status(file_id, FileStatus.PROCESSING):
            logging.warning(f"Could not find file_id {file_id} to update status. It may have been deleted.")
            return None

        file_metadata = self.mongo_client.get_raw_file_by_id(file_id)
        if not file_metadata or not file_metadata.object_name:
            logging.error(f"File document or object_name not found for file_id: {file_id}. Marking as FAILED.")
            self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
            return None

        return file_metadata.object_name

    def _route_by_mime_type(self, file_id: str, mime_type: str):
        """
        Routes a file to a specialized queue based on its MIME type or marks it as completed.
        """
        if mime_type.startswith('video/'):
            logging.info(f"Routing file {file_id} ({mime_type}) to video processing queue.")
            self.rabbitmq_client.publish_video_message(VideoMessage(raw_file_id=file_id))
        elif mime_type.startswith('image/'):
            logging.info(f"Routing file {file_id} ({mime_type}) to photo processing queue.")
            self.rabbitmq_client.publish_photo_message(PhotoMessage(raw_file_id=file_id))
        else:
            # For other file types with no specialized processor, we mark processing as complete.
            logging.info(f"No specific processor for MIME type {mime_type}. Marking file {file_id} as COMPLETED.")
            self.mongo_client.update_raw_file_status(file_id, FileStatus.COMPLETED)

    def _process_and_route_file(self, file_id: str, object_name: str):
        """
        Downloads the file, extracts MIME type, and routes it for further processing.
        This method manages the file stream resource.
        """
        file_data_stream = None
        try:
            raw_bucket_name = BucketType.RAW.value
            logging.info(f"Downloading '{object_name}' from bucket '{raw_bucket_name}' for file_id: {file_id}")
            file_data_stream = self.minio_client.get_file_stream(BucketType.RAW, object_name)

            mime_type = self._extract_and_save_mime_type(file_id, file_data_stream)
            if not mime_type:
                logging.error(f"Failed to extract or save MIME type for file_id: {file_id}. Marking as FAILED.")
                self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
                return

            # Note on original source info:
            # Original filesystem metadata like creation/modification dates are not part of the
            # file's content and are lost during a standard HTTP/gRPC upload.
            # The `uploaded_at` timestamp in our database serves as the authoritative creation time within our system.
            self._route_by_mime_type(file_id, mime_type)
            # If routed, the file status remains 'PROCESSING'. The next worker is responsible for the status update.
        finally:
            if file_data_stream:
                file_data_stream.close()
                file_data_stream.release_conn()

    def _message_callback(self, message: UploadMessage, ack_callback: Callable[[], None]):
        """
        Callback function to process a message from the upload queue.
        Orchestrates the file processing workflow.
        """
        file_id = message.file_id
        try:
            object_name = self._prepare_for_processing(file_id)
            if not object_name:
                # Failure is logged and status updated within the helper.
                # We just need to stop and acknowledge the message via the finally block.
                return

            self._process_and_route_file(file_id, object_name)

            logging.info(f"Successfully processed message for file_id: {file_id}")

        except Exception as e:
            logging.critical(f"An unhandled error occurred while processing message for file_id '{file_id}': {e}", exc_info=True)
            if file_id:
                # This is a safety net for unexpected errors in the workflow.
                self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
        finally:
            # Acknowledge the message to prevent requeue loops, regardless of outcome.
            ack_callback()

    def start_consuming(self):
        """
        Starts consuming messages from the UPLOAD_QUEUE. This is a blocking call.
        """
        logging.info("Starting to consume messages from UPLOAD_QUEUE...")
        self.rabbitmq_client.start_consuming_uploads(callback_function=self._message_callback)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    _ = app_config.mongo
    _ = app_config.rabbitmq
    processor = UploadProcessor()
    processor.start_consuming()
