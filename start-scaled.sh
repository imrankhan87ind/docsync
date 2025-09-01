#!/bin/bash
#
# Starts the application stack with multiple upload-processor workers.
# The number of workers can be passed as an argument, defaulting to 4.
#

WORKERS=${1:-4}

echo "Starting services with $WORKERS upload-processor worker(s)..."
docker-compose up --build --scale upload-processor=$WORKERS
echo "Done."