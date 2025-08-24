import os
import requests
import hashlib
import grpc
from flask import Flask

# Import the generated classes
import proto.upload_pb2 as upload_pb2
import proto.upload_pb2_grpc as upload_pb2_grpc

app = Flask(__name__)

HASH_HEADER_NAME = "X-File-Hash"
FILE_TO_UPLOAD="test-data.mp4"
CHUNK_SIZE = 8192
UPLOAD_URL=os.environ.get("UPLOAD_SERVICE_URL")

@app.route('/uploadtest')
def uploadtest():
    return upload_file(FILE_TO_UPLOAD)

def calculate_sha256(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()

def generate_file_chunks(filepath):
    """Generator to stream file content."""
    filename = os.path.basename(filepath)
    file_info = upload_pb2.FileInfo(filename = filename, hash = calculate_sha256(filepath))
    yield upload_pb2.UploadFileRequest(info = file_info)
    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            yield upload_pb2.UploadFileRequest(chunk_data = chunk)

def upload_file(filepath):
    if not os.path.exists(filepath):
        print(f"File '{filepath}' does not exist.")
        return "File doesn't exist"
    
    file_hash = calculate_sha256(filepath)
    print(f"Calculated SHA-256 for '{filepath}': {file_hash}")
    headers = {
        HASH_HEADER_NAME: file_hash,
        "Content-Type": "application/octet-stream"
    }

    chunks_generator = generate_file_chunks(filepath)

    print(f"Uploading '{filepath}' to '{UPLOAD_URL}'")
    try:
        with grpc.insecure_channel(UPLOAD_URL) as channel:
            stub = upload_pb2_grpc.UploadStub(channel)
            response = stub.UploadFile(chunks_generator)
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
    app.run(host='0.0.0.0', port=8000)