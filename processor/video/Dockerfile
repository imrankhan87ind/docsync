# Stage 1: Final application image
FROM python:3.10-slim

WORKDIR /app

# Create a non-root user for better security
RUN useradd --create-home appuser

# Switch to the non-root user
USER appuser

# Install runtime dependencies for the user
COPY processor/video/requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy the shared library directly into the app directory.
# This allows it to be imported as a top-level module inside the container.
COPY shared ./shared

# Copy the server application code
COPY processor/video/processor.py .

# Set the entrypoint to run the server, ensuring the user's bin is in the PATH
ENV PATH="/home/appuser/.local/bin:${PATH}"
CMD ["python", "processor.py"]