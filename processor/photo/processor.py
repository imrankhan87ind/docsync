import json
import logging
from typing import Callable

from shared.mongo.client import MongoDbClient, FileStatus
from shared.rabbitmq.client import RabbitMQClient, PhotoMessage, QueueType
from shared.miniocdn.client import MinioClient, BucketType
from shared.metadata_extractor.photo import PhotoMetadataExtractor
from shared.environment.config import app_config


class PhotoProcessor:
    """
    A processor that consumes messages from a photo queue, extracts metadata from the
    photo file, and updates the system.
    """

    def __init__(self):
        """Initializes all necessary clients for processing."""
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
        self.metadata_extractor = PhotoMetadataExtractor()
        logging.info("PhotoProcessor initialized and ready.")

    def run(self):
        """Starts consuming messages from the photo processing queue."""
        logging.info("Declaring photo queue to ensure it exists...")
        self.rabbitmq_client.declare_photo_queue()
        logging.info("Starting to consume photo messages...")
        self.rabbitmq_client.start_consuming_photos(self._process_photo_message)

    def _process_photo_message(self, message: PhotoMessage, ack_callback: Callable[[], None], nack_callback: Callable[[bool], None]):
        """
        Callback function to handle a single photo message from RabbitMQ.
        """
        try:
            logging.info(f"Received photo message for file_id: {message.raw_file_id}")
            file_metadata = self.mongo_client.get_raw_file_by_id(message.raw_file_id)

            if not file_metadata:
                logging.error(f"File with id {message.raw_file_id} not found in database. Discarding message (nack, no requeue).")
                nack_callback(False)
                return

            logging.info(f"Fetching '{file_metadata.object_name}' from RAW bucket.")
            with self.minio_client.get_file_stream(BucketType.RAW, file_metadata.object_name) as image_stream:
                metadata = self.metadata_extractor.extract(image_stream)

                logging.info(f"--- Extracted Metadata for {file_metadata.object_name} ---")
                print(metadata.model_dump_json(indent=2))
                logging.info("----------------------------------------------------")

                # Persist the extracted metadata and update status
                # self.mongo_client.update_raw_file_metadata(message.file_id, metadata.model_dump())
                self.mongo_client.update_raw_file_status(message.raw_file_id, FileStatus.COMPLETED)
                logging.info(f"Successfully processed and stored metadata for {message.raw_file_id}")

            ack_callback()

        except Exception as e:
            logging.error(f"An unexpected error occurred while processing message for file_id {message.raw_file_id}: {e}", exc_info=True)
            # Negatively acknowledge the message without requeueing to avoid poison pill loops.
            # This will discard the message or send it to a Dead Letter Queue if configured.
            nack_callback(False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    _ = app_config.mongo
    _ = app_config.rabbitmq
    processor = PhotoProcessor()
    processor.run()
