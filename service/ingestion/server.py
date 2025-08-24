import grpc
from concurrent import futures
import time
import os
import hashlib
import tempfile
import shutil

# Import the generated classes
import upload_pb2
import upload_pb2_grpc

# UPLOAD_DIR is the directory where files will be stored.
# For production, it's better to use an environment variable.
UPLOAD_DIR = "uploads"

class UploadServiceServicer(upload_pb2_grpc.UploadServiceServicer):
    """
    Implements the gRPC UploadService.
    """
    def UploadFile(self, request_iterator, context):
        temp_file_path = None
        try:
            # 1. Receive file metadata from the first message in the stream.
            first_request = next(request_iterator)
            if not first_request.HasField("info"):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("First message must be FileInfo.")
                return upload_pb2.UploadFileResponse()

            file_info = first_request.info
            expected_hash_hex = file_info.sha256
            print(f"Received upload request for: {file_info.filename} ({expected_hash_hex})")

            # 2. Validate metadata.
            if not expected_hash_hex:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("SHA-256 hash cannot be empty.")
                return upload_pb2.UploadFileResponse()

            # 3. Check for duplicates. We use the hash as the canonical filename.
            final_filepath = os.path.join(UPLOAD_DIR, expected_hash_hex)
            if os.path.exists(final_filepath):
                print(f"Duplicate file detected: {expected_hash_hex}. Rejecting upload.")
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"File with hash {expected_hash_hex} already exists.")
                return upload_pb2.UploadFileResponse()

            # 4. Stream file content to a temporary file while calculating the hash.
            actual_hasher = hashlib.sha256()
            total_bytes_received = 0

            with tempfile.NamedTemporaryFile(delete=False, dir=UPLOAD_DIR, prefix="upload_temp_") as temp_file:
                temp_file_path = temp_file.name
                for request in request_iterator:
                    if not request.HasField("chunk_data"):
                        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                        context.set_details("Expected chunk_data after the initial FileInfo message.")
                        return upload_pb2.UploadFileResponse()
                    
                    chunk = request.chunk_data
                    actual_hasher.update(chunk)
                    temp_file.write(chunk)
                    total_bytes_received += len(chunk)

            # 5. Verify the integrity of the received file.
            actual_hash_hex = actual_hasher.hexdigest()
            if actual_hash_hex != expected_hash_hex:
                print(f"Hash mismatch for {file_info.filename}. Expected {expected_hash_hex}, got {actual_hash_hex}.")
                context.set_code(grpc.StatusCode.DATA_LOSS)
                context.set_details("Uploaded data hash does not match the provided SHA-256 hash.")
                return upload_pb2.UploadFileResponse()

            # 6. Commit the file by moving it to its final destination.
            shutil.move(temp_file_path, final_filepath)
            temp_file_path = None # Prevent deletion in the finally block

            # 7. Send a success response.
            print(f"File '{file_info.filename}' uploaded successfully to {final_filepath}. Size: {total_bytes_received} bytes.")
            return upload_pb2.UploadFileResponse(
                message=f"File '{file_info.filename}' uploaded successfully.",
                file_id=expected_hash_hex, # The hash is the unique ID
                size=total_bytes_received
            )

        except Exception as e:
            print(f"An unexpected error occurred during upload: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("An internal server error occurred.")
            return upload_pb2.UploadFileResponse()
        finally:
            # 8. Clean up the temporary file in case of any failure.
            if temp_file_path and os.path.exists(temp_file_path):
                print(f"Cleaning up temporary file: {temp_file_path}")
                os.remove(temp_file_path)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    upload_pb2_grpc.add_UploadServiceServicer_to_server(UploadServiceServicer(), server)
    port = "5000"
    server.add_insecure_port(f'[::]:{port}')
    if not os.path.exists(UPLOAD_DIR):
        os.makedirs(UPLOAD_DIR)
    print(f"Server starting on port {port}, uploads will be saved to '{UPLOAD_DIR}' directory...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()