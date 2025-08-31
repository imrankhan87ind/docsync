import os
from datetime import datetime
from typing import Optional, Dict, Any

from bson.objectid import ObjectId
from pymongo import MongoClient as PymongoClient, IndexModel, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure


class MongoDbClient:
    """
    A client for interacting with MongoDB to store and retrieve file metadata.

    This class provides a structured way to handle metadata for raw files,
    processed videos, and processed photos. It ensures that the necessary
    database, collections, and indexes are in place.
    """

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initializes the MongoDB client, connects to the database, and ensures
        that collections and indexes are set up correctly.

        Args:
            host: The MongoDB host.
            port: The MongoDB port.
            dbname: The name of the database to use.
            user: The optional username for authentication.
            password: The optional password for authentication.
        """
        try:
            self.client: PymongoClient = PymongoClient(
                host=host,
                port=port,
                username=user,
                password=password,
                authSource='admin'  # Often required when using root user
            )
            # The ismaster command is cheap and does not require auth.
            self.client.admin.command('ismaster')
            print("Successfully connected to MongoDB.")

            self.db: Database = self.client[dbname]
            self.raw_files: Collection = self.db.raw_files
            self.processed_files: Collection = self.db.processed_files

            self._ensure_indexes()

        except ConnectionFailure as e:
            print(f"Fatal: Could not connect to MongoDB: {e}")
            raise
        except Exception as e:
            print(f"Fatal: An error occurred during MongoDB initialization: {e}")
            raise

    def _ensure_indexes(self):
        """
        Ensures that the required indexes are created on the collections.
        This operation is idempotent.
        """
        # Index for raw_files collection to enforce content uniqueness
        raw_files_sha256_index = IndexModel(
            [("sha256", ASCENDING)], name="sha256_unique_index", unique=True
        )
        self.raw_files.create_indexes([raw_files_sha256_index])
        print("Ensured indexes for 'raw_files' collection.")

        # Index for processed_files to quickly find all processed versions of a raw file
        processed_raw_file_id_index = IndexModel(
            [("raw_file_id", ASCENDING)], name="raw_file_id_index"
        )
        self.processed_files.create_indexes([processed_raw_file_id_index])
        print("Ensured indexes for 'processed_files' collection.")

    def save_raw_file_metadata(
        self, sha256: str, filename: str, size: int, object_name: str
    ) -> str:
        """
        Saves metadata for a raw, unprocessed file.

        Schema for `raw_files`:
        - sha256 (str): Unique hash of the file content.
        - filename (str): Original name of the file.
        - size (int): Size of the file in bytes.
        - object_name (str): The key of the file in the 'raw' Minio bucket.
        - status (str): 'uploaded', 'processing', 'processed', 'failed'.
        - created_at (datetime): Timestamp of creation.

        Args:
            sha256: The SHA256 hash of the file.
            filename: The original filename.
            size: The size of the file in bytes.
            object_name: The object name in the Minio bucket.

        Returns:
            The string representation of the inserted document's ID.
        """
        document = {
            "sha256": sha256,
            "filename": filename,
            "size": size,
            "object_name": object_name,
            "status": "uploaded",
            "created_at": datetime.utcnow(),
        }
        result = self.raw_files.insert_one(document)
        return str(result.inserted_id)

    def get_raw_file_by_sha256(self, sha256: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a raw file's metadata document using its SHA256 hash.

        Args:
            sha256: The SHA256 hash of the file to find.

        Returns:
            The document if found, otherwise None.
        """
        return self.raw_files.find_one({"sha256": sha256})

    def save_processed_file_metadata(
        self,
        raw_file_id: str,
        file_type: str,
        scale: str,
        size: int,
        object_name: str,
        bucket_name: str,
    ) -> str:
        """
        Saves metadata for a processed file (video or photo).

        Schema for `processed_files`:
        - raw_file_id (ObjectId): Reference to the original file in `raw_files`.
        - type (str): 'video' or 'photo'.
        - scale (str): The scale/resolution (e.g., '1080p', 'thumbnail').
        - size (int): Size of the processed file in bytes.
        - object_name (str): The key of the file in its Minio bucket.
        - bucket_name (str): The name of the Minio bucket.
        - created_at (datetime): Timestamp of creation.

        Args:
            raw_file_id: The ID of the original raw file.
            file_type: The type of file, e.g., 'video' or 'photo'.
            scale: The scale or resolution, e.g., '1080p' or 'large'.
            size: The size of the processed file in bytes.
            object_name: The object name in the Minio bucket.
            bucket_name: The name of the bucket where the processed file is stored.

        Returns:
            The string representation of the inserted document's ID.
        """
        document = {
            "raw_file_id": ObjectId(raw_file_id),
            "type": file_type,
            "scale": scale,
            "size": size,
            "object_name": object_name,
            "bucket_name": bucket_name,
            "created_at": datetime.utcnow(),
        }
        result = self.processed_files.insert_one(document)
        return str(result.inserted_id)
