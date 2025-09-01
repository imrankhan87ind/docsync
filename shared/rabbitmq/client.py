import pika
import json
import logging
from enum import Enum
from pika import exceptions as pika_exceptions
from typing import Optional, Dict, Any, Callable, Type
from dataclasses import dataclass, asdict, fields


class QueueType(Enum):
    """
    Defines standard queue names for RabbitMQ.
    This helps in standardizing queue naming across services.
    """
    UPLOAD_QUEUE = "file_upload_events"
    PHOTO_QUEUE = "photo_processing_queue"
    VIDEO_QUEUE = "video_processing_queue"
    DOCUMENT_QUEUE = "document_processing_queue"


@dataclass
class UploadMessage:
    file_id: str
    sha256: str


@dataclass
class PhotoMessage:
    raw_file_id: str


@dataclass
class VideoMessage:
    raw_file_id: str


@dataclass
class DocumentMessage:
    raw_file_id: str


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
            logging.info("Successfully connected to RabbitMQ.")
        except pika_exceptions.AMQPConnectionError as e:
            logging.critical(f"Fatal: Could not connect to RabbitMQ: {e}", exc_info=True)
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
    
    def publish_upload_message(self, message: UploadMessage):
        """
        Publishes a message to the file upload events queue with specific file details.

        Args:
            message: An `UploadMessage` object containing the file details.
        """
        self._publish_message(QueueType.UPLOAD_QUEUE, asdict(message))

    def publish_photo_message(self, message: PhotoMessage):
        """Publishes a message to the photo processing queue."""
        self._publish_message(QueueType.PHOTO_QUEUE, asdict(message))

    def publish_video_message(self, message: VideoMessage):
        """Publishes a message to the video processing queue."""
        self._publish_message(QueueType.VIDEO_QUEUE, asdict(message))

    def publish_document_message(self, message: DocumentMessage):
        """Publishes a message to the document processing queue."""
        self._publish_message(QueueType.DOCUMENT_QUEUE, asdict(message))
    
    def start_consuming_uploads(self, callback_function: Callable[[UploadMessage, Callable[[], None]], None], auto_ack: bool = False):
        """
        Starts consuming messages from the UPLOAD_QUEUE.
 
        This method wraps the raw pika callback to provide a cleaner interface.
        It handles JSON parsing and provides a simple `ack_callback` function.

        Args:
            callback_function: A function to be called for each message.
                It must have the signature:
                `callback(message: UploadMessage, ack_callback: Callable[[], None])`
            auto_ack: If True, messages are acknowledged automatically by the broker.
                WARNING: Setting this to True is not recommended for this method,
                as it bypasses the manual acknowledgment logic provided by `ack_callback`.
                The message will be considered handled upon delivery, risking data loss
                if the consumer crashes. Calling the `ack_callback` will likely result
                in a channel error.
        """
        pika_callback = self._create_callback_wrapper(callback_function, UploadMessage)
        self._start_consuming(QueueType.UPLOAD_QUEUE, pika_callback, auto_ack=auto_ack)

    def start_consuming_photos(self, callback_function: Callable[[PhotoMessage, Callable[[], None]], None], auto_ack: bool = False):
        """
        Starts consuming messages from the PHOTO_QUEUE.

        Args:
            callback_function: A function to be called for each message.
                It must have the signature:
                `callback(message: PhotoMessage, ack_callback: Callable[[], None])`
            auto_ack: If True, messages are acknowledged automatically.
        """
        pika_callback = self._create_callback_wrapper(callback_function, PhotoMessage)
        self._start_consuming(QueueType.PHOTO_QUEUE, pika_callback, auto_ack=auto_ack)

    def start_consuming_videos(self, callback_function: Callable[[VideoMessage, Callable[[], None]], None], auto_ack: bool = False):
        """
        Starts consuming messages from the VIDEO_QUEUE.

        Args:
            callback_function: A function to be called for each message.
                It must have the signature:
                `callback(message: VideoMessage, ack_callback: Callable[[], None])`
            auto_ack: If True, messages are acknowledged automatically.
        """
        pika_callback = self._create_callback_wrapper(callback_function, VideoMessage)
        self._start_consuming(QueueType.VIDEO_QUEUE, pika_callback, auto_ack=auto_ack)

    def start_consuming_documents(self, callback_function: Callable[[DocumentMessage, Callable[[], None]], None], auto_ack: bool = False):
        """
        Starts consuming messages from the DOCUMENT_QUEUE.

        Args:
            callback_function: A function to be called for each message.
                It must have the signature:
                `callback(message: DocumentMessage, ack_callback: Callable[[], None])`
            auto_ack: If True, messages are acknowledged automatically.
        """
        pika_callback = self._create_callback_wrapper(callback_function, DocumentMessage)
        self._start_consuming(QueueType.DOCUMENT_QUEUE, pika_callback, auto_ack=auto_ack)

    def close(self):
        """
        Closes the connection to RabbitMQ.
        """
        if self.connection and self.connection.is_open:
            self.connection.close()
            logging.info("RabbitMQ connection closed.")

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
        logging.info(f"Queue '{queue_name.value}' is declared and ready.")

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
        logging.info(f"Published message to queue '{queue_name.value}': {message_body}")

    def _create_callback_wrapper(self, user_callback: Callable, message_type: Type):
        """
        Generic factory to create a pika-compatible callback wrapper.

        This handles JSON deserialization, constructs the appropriate message
        dataclass, and provides a simplified `ack_callback` to the user function.

        Args:
            user_callback: The user's callback function.
            message_type: The dataclass type to construct (e.g., UploadMessage).

        Returns:
            A pika-compatible callback function.
        """
        def wrapper(ch, method, properties, body):
            try:
                message_data = json.loads(body)
                
                # Validate that all fields required by the dataclass are present
                required_fields = {f.name for f in fields(message_type)}
                if not required_fields.issubset(message_data.keys()):
                    missing = required_fields - message_data.keys()
                    logging.warning(f"Malformed message for type {message_type.__name__}, missing fields {missing}: {body.decode()}. Discarding.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                def ack_message():
                    """Acknowledges the message on the channel."""
                    if self.channel and self.channel.is_open:
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                # Construct the dataclass instance from the message data
                message_instance = message_type(**{k: v for k, v in message_data.items() if k in required_fields})
                
                # Call the user's simplified callback
                user_callback(message=message_instance, ack_callback=ack_message)

            except (json.JSONDecodeError, TypeError) as e:
                logging.error(f"Failed to decode or construct message for type {message_type.__name__}: {body.decode()}. Error: {e}. Discarding.", exc_info=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(f"An unhandled error occurred in the callback wrapper for body {body.decode()}: {e}. Discarding message.", exc_info=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)

        return wrapper

    def _start_consuming(self, queue_name: QueueType, callback_function, auto_ack: bool = False):
        """
        Starts consuming messages from a specified queue. This is a blocking call.

        Args:
            queue_name: The `QueueType` enum member for the queue to consume from.
            callback_function: The function to call when a message is received.
                               It should have the signature:
                               callback(channel, method, properties, body)
            auto_ack: If True, messages are acknowledged automatically. If False,
                      the callback is responsible for acknowledgment.
        """
        if not self.channel:
            raise ConnectionError("RabbitMQ channel is not available.")

        self.channel.basic_consume(
            queue=queue_name.value,
            on_message_callback=callback_function,
            auto_ack=auto_ack
        )

        logging.info(f"Waiting for messages on queue '{queue_name.value}'. To exit press CTRL+C")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("Interrupted. Stopping consumer...")
        except Exception as e:
            logging.error(f"An unexpected error occurred during consumption: {e}", exc_info=True)

    def __del__(self):
        """Ensures the connection is closed when the object is destroyed."""
        self.close()
