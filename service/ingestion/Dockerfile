# Stage 1: Builder - Generate gRPC code from .proto file
FROM python:3.10-slim as builder

# Install dependencies for code generation
RUN pip install grpcio-tools

# Set up the working directory
WORKDIR /app

# Copy the proto files from the top-level proto directory into a local proto folder
# The path is relative to the build context (the project root)
COPY proto/ ./proto/

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
COPY service/ingestion/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy generated gRPC code from the builder stage
COPY --from=builder /app/upload_pb2.py .
COPY --from=builder /app/upload_pb2_grpc.py .

# Copy the shared library directly into the app directory.
# This allows it to be imported as a top-level module inside the container.
COPY shared/miniocdn ./shared/miniocdn
COPY shared/mongo ./shared/mongo

# Copy the server application code
COPY service/ingestion/server.py .

# Expose the gRPC port
EXPOSE 5000

# Set the entrypoint to run the server, ensuring the user's bin is in the PATH
ENV PATH="/home/appuser/.local/bin:${PATH}"
CMD ["python", "server.py"]