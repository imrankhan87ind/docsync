import grpc
from typing import Any, Callable, Iterator, Optional

class GrpcStreamWrapper:
    """
    A file-like object wrapper for a gRPC stream iterator that also
    calculates a hash and total bytes on the fly.
    """

    def __init__(
        self,
        request_iterator: Iterator[Any],
        hasher: Any,
        context: grpc.ServicerContext,
        chunk_extractor: Callable[[Any], Optional[bytes]],
    ):
        """
        Args:
            chunk_extractor: A function that takes a gRPC request message and returns the byte chunk, or None if it's not a chunk message.
        """
        self._iterator = request_iterator
        self._hasher = hasher
        self._context = context
        self._buffer = b''
        self._chunk_extractor = chunk_extractor
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
                chunk = self._chunk_extractor(request)

                if chunk is None:
                    self._context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    self._context.set_details("Expected a message containing a data chunk.")
                    raise IOError("Invalid gRPC message in stream")
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
