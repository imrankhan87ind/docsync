import os
import hashlib
import grpc
from flask import Flask
from typing import Iterator

# Get the execution environment ('docker' or 'local'). Defaults to 'local'.
EXECUTION_ENVIRONMENT = os.environ.get('EXECUTION_ENVIRONMENT', 'local')

# Import the generated classes
import upload_pb2, upload_pb2_grpc

app = Flask(__name__)

FILE_TO_UPLOAD="test-data.mp4"
CHUNK_SIZE = 8192
UPLOAD_URL=os.environ.get("UPLOAD_SERVICE_URL", "localhost:5001")
APP_PORT = int(os.environ.get("PORT", 8000))

@app.route('/uploadtest')
def uploadtest() -> str:
    return upload_file(FILE_TO_UPLOAD)

def calculate_sha256(filepath: str) -> str:
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()

def generate_file_chunks(filepath: str) -> Iterator[upload_pb2.UploadFileRequest]:
    """Generator to stream file content."""
    filename = os.path.basename(filepath)
    file_hash = calculate_sha256(filepath)
    print(f"Calculated SHA-256 for '{filepath}': {file_hash}")
    file_info = upload_pb2.FileInfo(filename=filename, sha256=file_hash)
    yield upload_pb2.UploadFileRequest(info=file_info)
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield upload_pb2.UploadFileRequest(chunk_data=chunk)

def upload_file(filepath: str) -> str:
    if not os.path.exists(filepath):
        print(f"File '{filepath}' does not exist.")
        return "File doesn't exist"

    chunks_generator: Iterator[upload_pb2.UploadFileRequest] = generate_file_chunks(filepath)

    print(f"Uploading '{filepath}' to '{UPLOAD_URL}'")
    try:
        if not UPLOAD_URL:
            msg = "UPLOAD_SERVICE_URL environment variable not set."
            print(msg)
            return msg

        with grpc.insecure_channel(UPLOAD_URL) as channel:
            stub = upload_pb2_grpc.UploadServiceStub(channel)
            response: upload_pb2.UploadFileResponse = stub.UploadFile(chunks_generator)
            print(f"\nUpload successful!")
            print(f"  Server message: {response.message}")
            print(f"  File ID/Path: {response.file_id}")
            print(f"  Uploaded Size: {response.size} bytes")
            return "Success"
    except grpc.RpcError as e:
        print(f"An RPC error occurred during upload: {e.code()} - {e.details()}")
        if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
            print("This might be due to the file being too large or server limits. Try a smaller file or check server configuration.")
        return "Failed"
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        return "Failed"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=APP_PORT)