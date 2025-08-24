FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. ./proto/upload.proto

CMD ["flask", "run", "--host", "0.0.0.0"]