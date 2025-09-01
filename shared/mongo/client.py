from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
from bson import ObjectId

from pymongo import MongoClient as PymongoClient, IndexModel, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure

class FileStatus(Enum):
    """
    Defines the possible lifecycle states for a file in the system.
    """
    UNKNOWN = "UNKNOWN"          # Initial state before any processing
    UPLOADED = "UPLOADED"        # File successfully uploaded to Minio, metadata saved
    PUBLISHED = "PUBLISHED"      # Processing message published to RabbitMQ
    PROCESSING = "PROCESSING"    # A worker has picked up the message and is processing the file
    COMPLETED = "COMPLETED"      # Processing finished successfully
    FAILED = "FAILED"            # Processing failed


@dataclass
class RawFileDetails:
    """
    Represents the 'file_details' sub-document in the 'raw_files' collection.
    This holds metadata extracted from the file during processing.
    """
    mime_type: Optional[str] = None
    # Future fields like 'width', 'height', 'duration' can be added here.

    @classmethod
    def from_mongo(cls, data: Optional[Dict[str, Any]]) -> RawFileDetails:
        """
        Creates a RawFileDetails instance from a MongoDB sub-document (dictionary).
        Returns an empty RawFileDetails instance if data is None or empty.
        """
        if not data:
            return cls()
        return cls(**data)

    def to_mongo(self) -> Dict[str, Any]:
        """
        Converts the dataclass instance to a dictionary.
        """
        return asdict(self)


@dataclass
class RawFileMetadata:
    """
    Represents a document in the 'raw_files' collection in MongoDB, providing
    a structured way to handle raw file metadata.
    """
    sha256: str
    filename: str
    size: int
    object_name: str
    status: FileStatus
    uploaded_at: datetime
    file_details: RawFileDetails = field(default_factory=RawFileDetails)
    updated_at: Optional[datetime] = None
    _id: Optional[ObjectId] = field(default=None, repr=False)

    @classmethod
    def from_mongo(cls, data: Optional[Dict[str, Any]]) -> Optional[RawFileMetadata]:
        """
        Creates a RawFileMetadata instance from a MongoDB document (dictionary).
        Returns None if data is None or a required key is missing.
        """
        if not data:
            return None
        try:
            data_copy = data.copy()
            data_copy["file_details"] = RawFileDetails.from_mongo(data_copy.get("file_details"))
            data_copy["status"] = FileStatus(data_copy.get("status", "UNKNOWN"))
            return cls(**data_copy)
        except (KeyError, TypeError) as e:
            logging.error(f"Failed to create RawFileMetadata from mongo data: {e}. Data: {data}")
            return None

    def to_mongo(self) -> Dict[str, Any]:
        """
        Converts the dataclass instance to a dictionary suitable for MongoDB.
        """
        data = asdict(self)
        data['file_details'] = self.file_details.to_mongo()
        # Convert Enum to its value
        data['status'] = self.status.value
        # Remove fields that are None and shouldn't be stored if not set
        if self._id is None:
            del data['_id']
        if self.updated_at is None:
            del data['updated_at']
        return data



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
            logging.info("Successfully connected to MongoDB.")

            self.db: Database = self.client[dbname]
            self._raw_files: Collection = self.db.raw_files
            self._processed_files: Collection = self.db.processed_files

            self._ensure_indexes()

        except ConnectionFailure as e:
            logging.critical(f"Fatal: Could not connect to MongoDB: {e}", exc_info=True)
            raise
        except Exception as e:
            logging.critical(f"Fatal: An error occurred during MongoDB initialization: {e}", exc_info=True)
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
        self._raw_files.create_indexes([raw_files_sha256_index])
        logging.info("Ensured indexes for 'raw_files' collection.")

        # Index for processed_files to quickly find all processed versions of a raw file
        processed_raw_file_id_index = IndexModel(
            [("raw_file_id", ASCENDING)], name="raw_file_id_index"
        )
        self._processed_files.create_indexes([processed_raw_file_id_index])
        logging.info("Ensured indexes for 'processed_files' collection.")

    def save_raw_file_metadata(
        self, sha256: str, filename: str, size: int, object_name: str
    ) -> str:
        """
        Saves metadata for a raw, unprocessed file.

        The status field is designed to track the file's lifecycle using the
        FileStatus enum.

        Initial schema for `raw_files`:
        - sha256 (str): Unique hash of the file content.
        - filename (str): Original name of the file.
        - size (int): Size of the file in bytes.
        - object_name (str): The key of the file in the 'raw' Minio bucket.
        - status (str): Initial status, defaults to 'UNKNOWN'.
        - uploaded_at (datetime): Timestamp of when the file was uploaded.
        - file_details (dict): A sub-document for details discovered during processing.

        Args:
            sha256: The SHA256 hash of the file.
            filename: The original filename.
            size: The size of the file in bytes.
            object_name: The object name in the Minio bucket.

        Returns:
            The string representation of the inserted document's ID.
        """
        metadata = RawFileMetadata(
            sha256=sha256,
            filename=filename,
            size=size,
            object_name=object_name,
            status=FileStatus.UNKNOWN,
            uploaded_at=datetime.now(timezone.utc),
        )
        document = metadata.to_mongo()
        result = self._raw_files.insert_one(document)
        return str(result.inserted_id)

    def update_raw_file_status(self, file_id: str, new_status: FileStatus) -> bool:
        """
        Updates the status of a raw file document. Also adds/updates an
        `updated_at` timestamp.

        Args:
            file_id: The string representation of the document's ObjectId.
            new_status: The new status to set, as a FileStatus enum member.

        Returns:
            True if a document was found and modified, False otherwise.
        """
        result = self._raw_files.update_one(
            {"_id": ObjectId(file_id)},
            {
                "$set": {
                    "status": new_status.value,
                    "updated_at": datetime.now(timezone.utc)
                }
            },
        )
        return result.modified_count > 0

    def get_raw_file_by_id(self, file_id: str) -> Optional[RawFileMetadata]:
        """
        Retrieves a raw file's metadata document using its ObjectId.

        Args:
            file_id: The string representation of the document's ObjectId.

        Returns:
            A RawFileMetadata object if found, otherwise None.
        """
        try:
            document = self._raw_files.find_one({"_id": ObjectId(file_id)})
            return RawFileMetadata.from_mongo(document)
        except Exception as e:
            logging.error(f"Error finding file by id {file_id}: {e}", exc_info=True)
            return None

    def update_raw_file_details(self, file_id: str, details: RawFileDetails) -> bool:
        """
        Updates the details of a raw file document.

        This method updates fields in the nested 'file_details' document and
        also updates the top-level `updated_at` timestamp. It only updates
        fields that are not None in the provided `details` object.

        Args:
            file_id: The string representation of the MongoDB ObjectId.
            details: A RawFileDetails object with the new details to set.

        Returns:
            True if the document was modified, False otherwise.
        """
        try:
            details_dict = details.to_mongo()
            update_fields = {
                f"file_details.{k}": v for k, v in details_dict.items() if v is not None
            }

            if not update_fields:
                return False  # Nothing to update, so 0 modified.

            update_fields["updated_at"] = datetime.now(timezone.utc)
            result = self._raw_files.update_one(
                {"_id": ObjectId(file_id)},
                {"$set": update_fields}
            )
            return result.modified_count > 0
        except Exception as e:
            logging.error(f"Error updating file details for file_id {file_id}: {e}", exc_info=True)
            return False

    def delete_raw_file_by_id(self, file_id: str) -> bool:
        """
        Deletes a raw file metadata document using its ObjectId.

        Args:
            file_id: The string representation of the document's ObjectId.

        Returns:
            True if a document was deleted, False otherwise.
        """
        try:
            result = self._raw_files.delete_one({"_id": ObjectId(file_id)})
            return result.deleted_count > 0
        except Exception as e:
            logging.error(f"Error deleting file by id {file_id}: {e}", exc_info=True)
            return False

    def get_raw_file_by_sha256(self, sha256: str) -> Optional[RawFileMetadata]:
        """
        Retrieves a raw file's metadata document using its SHA256 hash.

        Args:
            sha256: The SHA256 hash of the file to find.

        Returns:
            A RawFileMetadata object if found, otherwise None.
        """
        try:
            document = self._raw_files.find_one({"sha256": sha256})
            return RawFileMetadata.from_mongo(document)
        except Exception as e:
            logging.error(f"Error finding file by sha256 {sha256}: {e}", exc_info=True)
            return None

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
            "uploaded_at": datetime.now(timezone.utc),
        }
        result = self._processed_files.insert_one(document)
        return str(result.inserted_id)
