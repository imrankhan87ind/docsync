import json
import logging
import time
from typing import Callable
import magic

from shared.environment.config import app_config
from shared.mongo.client import MongoDbClient, FileStatus, RawFileDetails
from shared.rabbitmq.client import RabbitMQClient, UploadMessage
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

    def _message_callback(self, message: UploadMessage, ack_callback: Callable[[], None]):
        """
        Callback function to process a message from the upload queue.
        This function is the core of the worker's logic.

        Args:
            message: An `UploadMessage` object containing file details.
            ack_callback: A function to call to acknowledge the message.
        """
        file_id = message.file_id
        file_data_stream = None
        try:
            # 1. Update status to PROCESSING. This acts as a lock.
            logging.info(f"Updating status to PROCESSING for file_id: {file_id}")
            updated = self.mongo_client.update_raw_file_status(file_id, FileStatus.PROCESSING)
            if not updated:
                logging.warning(f"Could not find file_id {file_id} to update status. It may have been deleted. Acknowledging message.")
                ack_callback()
                return

            # 2. Get file metadata from MongoDB to find the object_name
            file_metadata = self.mongo_client.get_raw_file_by_id(file_id)
            if not file_metadata or not file_metadata.object_name:
                logging.error(f"File document or object_name not found for file_id: {file_id}. Marking as FAILED.")
                self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
                ack_callback()
                return

            object_name = file_metadata.object_name
            raw_bucket_name = BucketType.RAW.value

            # 3. Download file from MinIO and extract details
            logging.info(f"Downloading '{object_name}' from bucket '{raw_bucket_name}' for file_id: {file_id}")
            file_data_stream = self.minio_client.get_file_stream(BucketType.RAW, object_name)
            mime_type = self._extract_and_save_mime_type(file_id, file_data_stream)

            if not mime_type:
                logging.error(f"Failed to extract or save MIME type for file_id: {file_id}. Marking as FAILED.")
                self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
                ack_callback()
                return

            # Note on original source info:
            # Original filesystem metadata like creation/modification dates are not part of the
            # file's content and are lost during a standard HTTP/gRPC upload.
            # The `uploaded_at` timestamp in our database serves as the authoritative creation time within our system.

            # 5. --- FURTHER PROCESSING LOGIC GOES HERE ---
            # Based on the mime_type, you can now route to different processors
            # (e.g., video, image, document processors).
            logging.info(f"Simulating further processing for file_id: {file_id}...")
            time.sleep(2)  # Simulate I/O or CPU-bound work
            logging.info(f"Processing simulation complete for file_id: {file_id}.")
            # --- END OF PROCESSING LOGIC ---

            # 6. On success, update status to COMPLETED.
            logging.info(f"Updating status to COMPLETED for file_id: {file_id}")
            self.mongo_client.update_raw_file_status(file_id, FileStatus.COMPLETED)

            logging.info(f"Successfully processed message for file_id: {file_id}")

        except Exception as e:
            logging.critical(f"An unhandled error occurred while processing message for file_id '{file_id}': {e}", exc_info=True)
            if file_id:
                self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
        finally:
            # 7. Clean up the MinIO stream and acknowledge the message.
            if file_data_stream:
                file_data_stream.close()
                file_data_stream.release_conn()
            # This prevents requeue loops on both success and failure.
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
