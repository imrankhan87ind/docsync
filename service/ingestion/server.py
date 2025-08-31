import grpc
from concurrent import futures
import time
import os
import hashlib 
from typing import Iterator
from shared.miniocdn.client import MinioClient, BucketType
from shared.mongo.client import MongoDbClient

# Get the execution environment ('docker' or 'local'). Defaults to 'local'.
EXECUTION_ENVIRONMENT = os.environ.get('EXECUTION_ENVIRONMENT', 'local')

# Import the generated classes
import upload_pb2, upload_pb2_grpc



# Minio configuration from environment variables.
# The following are required and will cause the service to fail at startup if not set.
try:
    MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
    MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
    MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
except KeyError as e:
    raise RuntimeError(f"FATAL: Missing required environment variable: {e}. Please set it before running.") from e

# MongoDB configuration from environment variables.
try:
    MONGO_HOST = os.environ["MONGO_HOST"]
    MONGO_PORT = int(os.environ["MONGO_PORT"])
    MONGO_USER = os.environ["MONGO_USER"]
    MONGO_PASSWORD = os.environ["MONGO_PASSWORD"]
    MONGO_DBNAME = os.environ["MONGO_DBNAME"]
except (KeyError, ValueError) as e:
    raise RuntimeError(f"FATAL: Missing or invalid MongoDB environment variable: {e}.") from e

# This Minio setting is optional and has a default.
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

class GrpcStreamWrapper:
    """
    A file-like object wrapper for a gRPC stream iterator that also
    calculates a hash and total bytes on the fly.
    """
    def __init__(self, request_iterator: Iterator[upload_pb2.UploadFileRequest], hasher, context: grpc.ServicerContext):
        self._iterator = request_iterator
        self._hasher = hasher
        self._context = context
        self._buffer = b''
        self._total_bytes = 0

    def read(self, size: int = -1) -> bytes:
        """
        Reads data from the gRPC stream to satisfy the read request.
        This is called by the Minio client.
        """
        # If size is -1, we need to read everything. We can achieve this by
        # continuously filling the buffer until the stream is exhausted.
        # The subsequent logic will then return the entire buffer.
        is_read_all = size == -1

        # Keep reading from the gRPC stream until we have enough data in the buffer
        # to satisfy the read request, or until the stream is exhausted.
        while is_read_all or len(self._buffer) < size:
            try:
                request = next(self._iterator)
                if not request.HasField("chunk_data"):
                    self._context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    self._context.set_details("Expected chunk_data after the initial FileInfo message.")
                    raise IOError("Invalid gRPC message in stream")

                chunk = request.chunk_data
                self._hasher.update(chunk)
                self._buffer += chunk
                self._total_bytes += len(chunk)

            except StopIteration:
                # The client has finished sending data. No more chunks to read.
                break

        # If we need to read all, size becomes the length of the buffer.
        if is_read_all:
            size = len(self._buffer)

        # Slice the buffer to get the data to return
        data_to_return = self._buffer[:size]
        # Update the buffer to contain the remaining data
        self._buffer = self._buffer[size:]

        return data_to_return

    @property
    def total_bytes_received(self) -> int:
        return self._total_bytes

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
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE
            )
            print("Minio client initialized.")
        except ValueError as e:
            print(f"FATAL: Could not initialize Minio client: {e}")
            raise
        
        try:
            self.mongo_client = MongoDbClient(
                host=MONGO_HOST,
                port=MONGO_PORT,
                user=MONGO_USER,
                password=MONGO_PASSWORD,
                dbname=MONGO_DBNAME
            )
            print("MongoDB client initialized.")
        except Exception as e:
            print(f"FATAL: Could not initialize MongoDB client: {e}")
            raise

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
            print(f"Received upload request for: {file_info.filename} ({expected_hash_hex})")

            # 2. Validate metadata.
            if not expected_hash_hex:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("SHA-256 hash cannot be empty.")
                return upload_pb2.UploadFileResponse()

            # 3. Check for duplicates and handle potential inconsistencies.
            object_name = expected_hash_hex
            existing_file = self.mongo_client.get_raw_file_by_sha256(object_name)
            if existing_file:
                # Metadata exists. Let's verify if the file also exists in storage.
                if self.minio_client.file_exists(BucketType.RAW, object_name):
                    # Consistent state: file exists in both DB and storage.
                    print(f"Duplicate file detected in MongoDB and Minio: {object_name}. Rejecting upload.")
                    context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                    context.set_details(f"File with hash {object_name} already exists.")
                    return upload_pb2.UploadFileResponse(
                        message="File already exists.",
                        file_id=str(existing_file['_id']),
                        size=existing_file['size']
                    )
                else:
                    # Inconsistent state: metadata exists but file is missing from storage.
                    # This is an orphaned record. We should clean it up and proceed with the upload.
                    print(f"Inconsistency found: Metadata for {object_name} exists in DB, but file is missing from Minio.")
                    print(f"Deleting orphaned metadata record with ID: {existing_file['_id']}")
                    self.mongo_client.raw_files.delete_one({"_id": existing_file['_id']})
                    print(f"Orphaned record for {object_name} deleted. Proceeding with new upload.")

            # 4. Prepare for streaming upload by creating the hasher and stream wrapper.
            actual_hasher = hashlib.sha256()
            stream_wrapper = GrpcStreamWrapper(request_iterator, actual_hasher, context)

            # 5. Stream the file directly to Minio.
            # The wrapper will be read by the Minio client, which also populates the hash on the fly.
            print(f"Streaming to Minio: bucket='{BucketType.RAW.value}', object='{object_name}'")
            self.minio_client.upload_stream(
                bucket_name=BucketType.RAW,
                object_name=object_name,
                stream=stream_wrapper
            )

            # 6. Verify the integrity of the fully uploaded file.
            actual_hash_hex = actual_hasher.hexdigest()
            if actual_hash_hex != expected_hash_hex:
                print(f"Hash mismatch for {file_info.filename}. Expected {expected_hash_hex}, got {actual_hash_hex}.")
                print(f"Deleting invalid object from Minio: {object_name}")
                # IMPORTANT: Clean up the invalid file that was just uploaded.
                self.minio_client.delete_file(BucketType.RAW, object_name)

                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details("Uploaded data hash does not match the provided SHA-256 hash.")
                return upload_pb2.UploadFileResponse()

            # 7. Save metadata to MongoDB.
            total_bytes_received = stream_wrapper.total_bytes_received
            try:
                file_id = self.mongo_client.save_raw_file_metadata(
                    sha256=actual_hash_hex,
                    filename=file_info.filename,
                    size=total_bytes_received,
                    object_name=object_name
                )
                print(f"Successfully saved metadata to MongoDB for file ID: {file_id}")
            except Exception as e:
                # CRITICAL: Failed to save metadata. Rollback the Minio upload.
                print(f"CRITICAL: Failed to save metadata for {object_name} after upload: {e}")
                print(f"Attempting to roll back by deleting object from Minio: {object_name}")
                self.minio_client.delete_file(BucketType.RAW, object_name)
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details("Failed to save file metadata after upload. The operation was rolled back.")
                return upload_pb2.UploadFileResponse()

            # 8. Send a success response.
            print(f"File '{file_info.filename}' streamed successfully to Minio as '{object_name}'. Size: {total_bytes_received} bytes.")
            return upload_pb2.UploadFileResponse(
                message=f"File '{file_info.filename}' uploaded successfully.",
                file_id=file_id, # The MongoDB document ID
                size=total_bytes_received
            )

        except Exception as e:
            print(f"An unexpected error occurred during streaming upload: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal server error occurred during streaming.")
            return upload_pb2.UploadFileResponse()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    upload_pb2_grpc.add_UploadServiceServicer_to_server(UploadServiceServicer(), server)
    # The port is configurable via the GRPC_PORT environment variable.
    port = os.environ.get("GRPC_PORT", "5001")
    server.add_insecure_port(f'[::]:{port}')
    print(f"Server starting on port {port}, uploads will be sent to Minio...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()