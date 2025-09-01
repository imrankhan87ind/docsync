import grpc
from concurrent import futures
import hashlib
import logging
from typing import Iterator, Optional
from shared.miniocdn.client import MinioClient, BucketType
from bson.objectid import ObjectId
from shared.mongo.client import MongoDbClient, FileStatus
from shared.rabbitmq.client import RabbitMQClient, QueueType
from shared.grpc_util.stream_wrapper import GrpcStreamWrapper
from shared.environment.config import app_config

# Import the generated classes
import upload_pb2, upload_pb2_grpc

class UploadServiceServicer(upload_pb2_grpc.UploadServiceServicer):
    """
    Implements the gRPC UploadService.
    """
    def __init__(self):
        """
        Initializes the service and the Minio client.
        """
        try:
            self.minio_client = MinioClient(
                endpoint=app_config.minio.endpoint,
                access_key=app_config.minio.access_key,
                secret_key=app_config.minio.secret_key,
                secure=app_config.minio.secure
            )
            logging.info("Minio client initialized.")
        except ValueError as e:
            logging.critical(f"Could not initialize Minio client: {e}")
            raise

        try:
            self.mongo_client = MongoDbClient(
                host=app_config.mongo.host,
                port=app_config.mongo.port,
                user=app_config.mongo.user,
                password=app_config.mongo.password,
                dbname=app_config.mongo.dbname
            )
            logging.info("MongoDB client initialized.")
        except Exception as e:
            logging.critical(f"Could not initialize MongoDB client: {e}")
            raise

        try:
            self.rabbitmq_client = RabbitMQClient(
                host=app_config.rabbitmq.host,
                port=app_config.rabbitmq.port,
                user=app_config.rabbitmq.user,
                password=app_config.rabbitmq.password
            )
            # Declare the queue we'll be publishing to. This is idempotent.
            self.rabbitmq_client.declare_upload_queue()
            logging.info("RabbitMQ client initialized.")
        except Exception as e:
            logging.critical(f"Could not initialize RabbitMQ client: {e}")
            raise

    def _rollback_upload(self, object_name: str, file_id: Optional[str] = None):
        """
        Rolls back a failed upload by deleting artifacts from Minio and MongoDB.

        This method is designed to be safe to call even if some artifacts don't
        exist. It logs errors but does not re-raise them to ensure all cleanup
        steps are attempted.

        Args:
            object_name: The name of the object to delete from Minio.
            file_id: The optional ID of the metadata record to delete from MongoDB.
        """
        logging.warning(f"Initiating rollback for object '{object_name}' and file ID '{file_id}'.")

        # 1. Delete file from Minio storage
        if object_name:
            try:
                logging.info(f"Rollback: Deleting object from Minio: {object_name}")
                self.minio_client.delete_file(BucketType.RAW, object_name)
            except Exception as e:
                logging.error(f"Rollback: Failed to delete object '{object_name}' from Minio: {e}", exc_info=True)

        # 2. Delete metadata from MongoDB
        if file_id:
            try:
                logging.info(f"Rollback: Deleting metadata record: {file_id}")
                self.mongo_client.delete_raw_file_by_id(file_id)
            except Exception as e:
                logging.error(f"Rollback: Failed to delete metadata record '{file_id}' from MongoDB: {e}", exc_info=True)

    def UploadFile(self, request_iterator: Iterator[upload_pb2.UploadFileRequest], context: grpc.ServicerContext) -> upload_pb2.UploadFileResponse:
        try:
            # 1. Receive file metadata from the first message in the stream.
            first_request: upload_pb2.UploadFileRequest = next(request_iterator)
            if not first_request.HasField("info"):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("First message must be FileInfo.")
                return upload_pb2.UploadFileResponse()

            file_info: upload_pb2.FileInfo = first_request.info
            expected_hash_hex = file_info.sha256
            logging.info(f"Received upload request for: {file_info.filename} ({expected_hash_hex})")

            # 2. Validate metadata.
            if not expected_hash_hex:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("SHA-256 hash cannot be empty.")
                return upload_pb2.UploadFileResponse()

            # 3. Check for duplicates and handle potential inconsistencies.
            object_name = expected_hash_hex
            existing_file = self.mongo_client.get_raw_file_by_sha256(object_name)
            if existing_file:
                # If the file exists in storage AND its status is not UNKNOWN, it's a true duplicate.
                # A file with status UNKNOWN is considered an incomplete upload that can be retried.
                if self.minio_client.file_exists(BucketType.RAW, object_name) and existing_file['status'] != FileStatus.UNKNOWN.value:
                    logging.info(f"Duplicate file detected with status '{existing_file['status']}': {object_name}. Rejecting upload.")
                    context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                    context.set_details(f"File with hash {object_name} already exists.")
                    return upload_pb2.UploadFileResponse(
                        message="File already exists.",
                        file_id=str(existing_file['_id']),
                        size=existing_file.get('size', 0)
                    )
                else:
                    # Inconsistent state found:
                    # 1. DB record exists but file is missing from storage (orphan).
                    # 2. File exists in storage but DB status is UNKNOWN (incomplete initial save).
                    # In either case, we treat the old record as invalid and start over.
                    logging.warning(
                        f"Inconsistency or incomplete state found for {object_name} with status "
                        f"'{existing_file.get('status', 'N/A')}'. Rolling back to retry."
                    )
                    # Use the rollback method to clean up both Minio and MongoDB before proceeding.
                    self._rollback_upload(object_name=object_name, file_id=str(existing_file['_id']))
                    logging.info(f"Rollback of inconsistent state for {object_name} complete. Proceeding with new upload.")

            # 4. Prepare for streaming upload by creating the hasher and stream wrapper.
            actual_hasher = hashlib.sha256()
            stream_wrapper = GrpcStreamWrapper(
                request_iterator=request_iterator,
                hasher=actual_hasher,
                context=context,
                chunk_extractor=lambda req: req.chunk_data if req.HasField("chunk_data") else None
            )

            # 5. Stream the file directly to Minio.
            # The wrapper will be read by the Minio client, which also populates the hash on the fly.
            logging.info(f"Streaming to Minio: bucket='{BucketType.RAW.value}', object='{object_name}'")
            self.minio_client.upload_stream(
                bucket_name=BucketType.RAW,
                object_name=object_name,
                stream=stream_wrapper
            )

            # 6. Verify the integrity of the fully uploaded file.
            actual_hash_hex = actual_hasher.hexdigest()
            if actual_hash_hex != expected_hash_hex:
                logging.warning(f"Hash mismatch for {file_info.filename}. Expected {expected_hash_hex}, got {actual_hash_hex}.")
                self._rollback_upload(object_name=object_name)
                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details("Uploaded data hash does not match the provided SHA-256 hash.")
                return upload_pb2.UploadFileResponse()

            # 7. Save metadata to MongoDB.
            total_bytes_received = stream_wrapper.total_bytes_received
            file_id: Optional[str] = None
            try:
                file_id = self.mongo_client.save_raw_file_metadata(
                    sha256=actual_hash_hex,
                    filename=file_info.filename,
                    size=total_bytes_received,
                    object_name=object_name
                )
                logging.info(f"Successfully saved metadata to MongoDB for file ID: {file_id}")

                # Publish event to RabbitMQ for downstream processing.
                try:
                    self.rabbitmq_client.publish_upload_message(
                        file_id=file_id, sha256=object_name
                    )
                    logging.info(f"Successfully published upload event for file ID: {file_id}")

                    # Update the status to signify the upload and publish steps are complete.
                    self.mongo_client.update_raw_file_status(file_id, FileStatus.UPLOADED)
                    logging.info(f"Updated file status to '{FileStatus.UPLOADED.value}' for file ID: {file_id}")
                except Exception as mq_e:
                    # CRITICAL: Failed to publish to RabbitMQ. This is a hard failure.
                    # We must roll back the MongoDB and Minio operations to ensure consistency.
                    logging.critical(f"Failed to publish RabbitMQ message for file ID {file_id}: {mq_e}. Rolling back.")
                    self._rollback_upload(object_name=object_name, file_id=file_id)

                    # Inform the client that the service is temporarily unable to accept the file.
                    # UNAVAILABLE is a good code for transient backend issues like the message broker being down.
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details("The service is temporarily unavailable to process the file. Please try again later.")
                    return upload_pb2.UploadFileResponse()
            except Exception as e:
                # CRITICAL: Failed to save metadata. Rollback the Minio upload.
                logging.critical(f"Failed to save metadata for {object_name} after upload: {e}", exc_info=True)
                self._rollback_upload(object_name=object_name, file_id=file_id)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Failed to save file metadata after upload. The operation was rolled back.")
                return upload_pb2.UploadFileResponse()

            # 8. Send a success response.
            logging.info(f"File '{file_info.filename}' streamed successfully to Minio as '{object_name}'. Size: {total_bytes_received} bytes.")
            return upload_pb2.UploadFileResponse(
                message=f"File '{file_info.filename}' uploaded successfully.",
                file_id=file_id, # The MongoDB document ID
                size=total_bytes_received
            )

        except Exception as e:
            logging.error(f"An unexpected error occurred during streaming upload: {e}", exc_info=True)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal server error occurred during streaming.")
            return upload_pb2.UploadFileResponse()

def serve():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app_config.assert_all()
    logging.info("All required configurations are loaded and validated.")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    upload_pb2_grpc.add_UploadServiceServicer_to_server(UploadServiceServicer(), server)
    # The port is configured via the shared app_config.
    server.add_insecure_port(f'[::]:{app_config.grpc_port}')
    logging.info(f"Server starting on port {app_config.grpc_port}, uploads will be sent to Minio...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()