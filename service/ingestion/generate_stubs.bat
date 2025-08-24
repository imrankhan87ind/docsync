@echo off
echo Generating gRPC Python stubs...

REM This script runs the gRPC tools to generate Python code from the .proto file.
REM The -I flag sets the search path for protos to the project root (two directories up).
REM The output files are placed in the current directory (.).
python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. upload.proto

echo Done.