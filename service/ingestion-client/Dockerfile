# Stage 1: Builder - Generate gRPC code from .proto file
FROM python:3.10-slim as builder

# Install dependencies for code generation
RUN pip install grpcio-tools

# Set up the working directory
WORKDIR /app

# Copy the proto files from the top-level proto directory into a local proto folder
COPY ../../proto/ ./proto/

# Generate Python gRPC code into the current directory
RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    ./proto/upload.proto

# Stage 2: Final application image
FROM python:3.10-slim

WORKDIR /app

# Create a non-root user for better security
RUN useradd --create-home appuser

# Create the upload directory and set permissions for the non-root user
RUN mkdir /upload && chown appuser:appuser /upload

# Switch to the non-root user
USER appuser

# Install runtime dependencies for the user
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy generated gRPC code from the builder stage
COPY --from=builder /app/upload_pb2.py .
COPY --from=builder /app/upload_pb2_grpc.py .

# Copy the rest of the application's code into the container at /app
# This assumes the Dockerfile is in the ingestion-client directory
# and the proto files are in a subdirectory named 'proto'
COPY app.py .

# Copy the dummy file for testing the upload
COPY test-data.mp4 .

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable for the gRPC server URL
# This should be set during `docker run`, e.g., -e UPLOAD_SERVICE_URL=ingestion-server:50051

# Run app.py when the container launches
# CMD ["python", "app.py"]
CMD ["flask", "run", "--host", "0.0.0.0"]