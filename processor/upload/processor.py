import json
import logging
import time
import sys
from typing import Callable

from shared.environment.config import app_config
from shared.mongo.client import MongoDbClient, FileStatus
from shared.rabbitmq.client import RabbitMQClient, UploadMessage


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
        self.rabbitmq_client = RabbitMQClient(
                host=app_config.rabbitmq.host,
                port=app_config.rabbitmq.port,
                user=app_config.rabbitmq.user,
                password=app_config.rabbitmq.password
            )
        logging.info("UploadProcessor initialized successfully.")

    def _message_callback(self, message: UploadMessage, ack_callback: Callable[[], None]):
        """
        Callback function to process a message from the upload queue.
        This function is the core of the worker's logic.

        Args:
            message: An `UploadMessage` object containing file details.
            ack_callback: A function to call to acknowledge the message.
        """
        file_id = message.file_id  # For access in the except block
        try:
            # 1. Update status to PROCESSING. This acts as a lock.
            logging.info(f"Updating status to PROCESSING for file_id: {message.file_id}")
            updated = self.mongo_client.update_raw_file_status(message.file_id, FileStatus.PROCESSING)
            if not updated:
                logging.warning(f"Could not find file_id {message.file_id} to update status. It may have been deleted. Acknowledging message.")
                return

            # 2. --- PROCESSING LOGIC GOES HERE ---
            # This is where you would perform the actual work, such as:
            # - Downloading the file from Minio.
            # - Converting it to another format (e.g., PDF to text).
            # - Performing analysis or extraction.
            # - Saving the results to another database or Minio bucket.
            logging.info(f"Simulating processing for file_id: {message.file_id}...")
            time.sleep(5)  # Simulate I/O or CPU-bound work
            logging.info(f"Processing simulation complete for file_id: {message.file_id}.")
            # --- END OF PROCESSING LOGIC ---

            # 3. On success, update status to COMPLETED.
            logging.info(f"Updating status to COMPLETED for file_id: {message.file_id}")
            self.mongo_client.update_raw_file_status(message.file_id, FileStatus.COMPLETED)

            logging.info(f"Successfully processed message for file_id: {message.file_id}")

        except Exception as e:
            logging.critical(f"An unhandled error occurred while processing message for file_id '{file_id}': {e}", exc_info=True)
            if file_id:
                self.mongo_client.update_raw_file_status(file_id, FileStatus.FAILED)
        finally:
            # 4. Always acknowledge the message to remove it from the queue.
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
