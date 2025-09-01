import pika
import json
from enum import Enum
from pika import exceptions as pika_exceptions
from typing import Optional, Dict, Any


class QueueType(Enum):
    """
    Defines standard queue names for RabbitMQ.
    This helps in standardizing queue naming across services.
    """
    UPLOAD_QUEUE = "file_upload_events"
    PHOTO_QUEUE = "photo_processing_queue"
    VIDEO_QUEUE = "video_processing_queue"
    DOCUMENT_QUEUE = "document_processing_queue"


class RabbitMQClient:
    """
    A client for interacting with RabbitMQ to publish and consume messages.
    This client is designed to be straightforward for publishing persistent messages.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
    ):
        """
        Initializes the RabbitMQ client and establishes a connection.

        Args:
            host: The RabbitMQ host.
            port: The RabbitMQ port.
            user: The username for authentication.
            password: The password for authentication.
        """
        self.connection = None
        self.channel = None
        try:
            credentials = pika.PlainCredentials(user, password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, port=port, credentials=credentials)
            )
            self.channel = self.connection.channel()
            print("Successfully connected to RabbitMQ.")
        except pika_exceptions.AMQPConnectionError as e:
            print(f"Fatal: Could not connect to RabbitMQ: {e}")
            raise

    def declare_upload_queue(self, durable: bool = True):
        """Declares the queue for file upload events."""
        self._declare_queue(QueueType.UPLOAD_QUEUE, durable)

    def declare_photo_queue(self, durable: bool = True):
        """Declares the queue for photo processing jobs."""
        self._declare_queue(QueueType.PHOTO_QUEUE, durable)

    def declare_video_queue(self, durable: bool = True):
        """Declares the queue for video processing jobs."""
        self._declare_queue(QueueType.VIDEO_QUEUE, durable)

    def declare_document_queue(self, durable: bool = True):
        """Declares the queue for document processing jobs."""
        self._declare_queue(QueueType.DOCUMENT_QUEUE, durable)

    def publish_upload_message(self, file_id: str, sha256: str):
        """
        Publishes a message to the file upload events queue with specific file details.

        Args:
            file_id: The unique ID of the file from the metadata store (MongoDB).
            sha256: The SHA256 hash of the file, used as the object name in Minio.
        """
        message_body = {
            "file_id": file_id,
            "sha256": sha256,
        }
        self._publish_message(QueueType.UPLOAD_QUEUE, message_body)

    def publish_photo_message(self, message_body: Dict[str, Any]):
        """Publishes a message to the photo processing queue."""
        self._publish_message(QueueType.PHOTO_QUEUE, message_body)

    def publish_video_message(self, message_body: Dict[str, Any]):
        """Publishes a message to the video processing queue."""
        self._publish_message(QueueType.VIDEO_QUEUE, message_body)

    def publish_document_message(self, message_body: Dict[str, Any]):
        """Publishes a message to the document processing queue."""
        self._publish_message(QueueType.DOCUMENT_QUEUE, message_body)

    def close(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection and self.connection.is_open:
            self.connection.close()
            print("RabbitMQ connection closed.")

    def _declare_queue(self, queue_name: QueueType, durable: bool = True):
        """
        Declares a queue, making sure it exists. This is an idempotent operation.

        Args:
            queue_name: The `QueueType` enum member for the queue.
            durable: If True, the queue will survive a broker restart.
        """
        if not self.channel:
            raise ConnectionError("RabbitMQ channel is not available.")
        self.channel.queue_declare(queue=queue_name.value, durable=durable)
        print(f"Queue '{queue_name.value}' is declared and ready.")

    def _publish_message(self, queue_name: QueueType, message_body: Dict[str, Any]):
        """
        Publishes a persistent message to a specified queue.

        Args:
            queue_name: The `QueueType` enum member for the target queue.
            message_body: A dictionary that will be converted to a JSON string.
        """
        if not self.channel:
            raise ConnectionError("RabbitMQ channel is not available.")

        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name.value,
            body=json.dumps(message_body),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
            )
        )
        print(f"Published message to queue '{queue_name.value}': {message_body}")

    def __del__(self):
        """Ensures the connection is closed when the object is destroyed."""
        self.close()
