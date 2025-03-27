from abc import ABC, abstractmethod
import hashlib
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic, ClassVar, Set
from urllib.parse import urlparse
import json
import uuid

import boto3
from botocore.exceptions import ClientError


class Storage(ABC):
    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def write(self, data: str | bytes) -> bool:
        pass

    @abstractmethod
    def read(self) -> str | bytes:
        pass

    @abstractmethod
    def delete(self) -> bool:
        pass

    @abstractmethod
    def calculate_checksum(self, algorithm: str = "SHA256") -> str:
        pass

    @abstractmethod
    def get_checksum(self, algorithm: str = "SHA256") -> str:
        pass


class POSIXStorage(Storage):
    def __init__(self, filepath: str | Path):
        self.filepath = filepath

    def exists(self) -> bool:
        return os.path.exists(self.filepath)

    def write(self, data: str | bytes) -> bool:
        if isinstance(data, str):
            data = data.encode("utf-8")
            
        with open(self.filepath, "wb") as f:
            f.write(data)
        return True

    def read(self) -> str | bytes:
        with open(self.filepath, "rb") as f:
            return f.read()

    def delete(self) -> bool:
        if self.exists():
            os.remove(self.filepath)
        return True

    def calculate_checksum(self, algorithm: str = "SHA256") -> str:
        """Calculate checksum by reading file in chunks."""
        hash_func = hashlib.new(algorithm)
        chunk_size = 65536  # 64KB per chunk
        with open(self.filepath, "rb") as f:
            while chunk := f.read(chunk_size):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    def get_checksum(self, algorithm: str = "SHA256") -> str:
        return self.calculate_checksum(algorithm=algorithm)


class S3Storage(Storage):
    def __init__(self, uri: str):
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError("URI must start with s3://")
        self.bucket = parsed.netloc
        self.key = parsed.path.lstrip("/")
        self.s3 = boto3.client("s3")

    def exists(self) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=self.key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise

    def write(self, data: str | bytes, checksum: str | None = None) -> bool:
        if isinstance(data, str):
            data = data.encode("utf-8")
        params = {
            "Bucket": self.bucket,
            "Key": self.key,
            "Body": data,
        }
        if checksum is not None:
            # Store the provided checksum in the object's metadata.
            params["Metadata"] = {"checksum": checksum}
        self.s3.put_object(**params)
        return True

    def read(self) -> bytes:
        response = self.s3.get_object(Bucket=self.bucket, Key=self.key)
        return response["Body"].read()

    def delete(self) -> bool:
        self.s3.delete_object(Bucket=self.bucket, Key=self.key)
        return True

    def calculate_checksum(self, algorithm: str = "SHA256") -> str:
        """
        Generate a checksum for the object by copying it over itself.

        This leverages S3's capability to calculate the checksum.
        Returns the checksum in hexdigest format.
        """
        import base64
        import binascii

        response = self.s3.copy_object(
            Bucket=self.bucket,
            Key=self.key,
            CopySource={"Bucket": self.bucket, "Key": self.key},
            ChecksumAlgorithm=algorithm.upper(),
        )
        checksum_key = f"Checksum{algorithm.upper()}"
        try:
            base64_checksum = response["CopyObjectResult"][checksum_key]
            # Decode base64 to bytes, then convert to hex string
            decoded_bytes = base64.b64decode(base64_checksum)
            return binascii.hexlify(decoded_bytes).decode("ascii")
        except KeyError:
            error_msg = f"Checksum {algorithm} not found in copy response."
            raise ValueError(error_msg)

    def get_checksum(self, algorithm: str = "SHA256") -> str:
        """Retrieve the checksum from the object's metadata via a HEAD request.

        Returns the checksum in hexdigest format.
        """
        import base64
        import binascii

        head_response = self.s3.head_object(
            Bucket=self.bucket,
            Key=self.key,
            ChecksumMode="ENABLED",
        )
        checksum_key = f"Checksum{algorithm.upper()}"
        if checksum_key in head_response:
            base64_checksum = head_response[checksum_key]
            # Decode base64 to bytes, then convert to hex string
            decoded_bytes = base64.b64decode(base64_checksum)
            return binascii.hexlify(decoded_bytes).decode("ascii")
        error_msg = (
            f"Object does not have a {algorithm} checksum: s3://{self.bucket}/{self.key}"
        )
        raise ValueError(error_msg)


class Database:
    """Database service that handles SQLite operations."""
    
    _instance = None
    
    def __new__(cls, db_path: str | Path = "mollusk.db"):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.db_path = db_path
            cls._instance.conn = cls._instance._get_connection()
            cls._instance._create_tables()
        return cls._instance
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a SQLite connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Item table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            item_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            metadata TEXT
        )
        ''')
        
        # File table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            filename TEXT NOT NULL,
            mimetype TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (item_id) REFERENCES items (item_id)
        )
        ''')
        
        # FileInstance table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_instances (
            file_instance_id TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            storage_type TEXT NOT NULL,
            storage_path TEXT NOT NULL,
            checksum TEXT,
            size INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY (file_id) REFERENCES files (file_id)
        )
        ''')
        
        self.conn.commit()
    
    def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a row into a table."""
        placeholders = ", ".join(["?"] * len(data))
        columns = ", ".join(data.keys())
        values = tuple(data.values())
        
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()
    
    def update(self, table: str, id_column: str, id_value: str, data: Dict[str, Any]) -> None:
        """Update a row in a table."""
        set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
        values = tuple(data.values()) + (id_value,)
        
        query = f"UPDATE {table} SET {set_clause} WHERE {id_column} = ?"
        cursor = self.conn.cursor()
        cursor.execute(query, values)
        self.conn.commit()
    
    def delete(self, table: str, id_column: str, id_value: str) -> None:
        """Delete a row from a table."""
        query = f"DELETE FROM {table} WHERE {id_column} = ?"
        cursor = self.conn.cursor()
        cursor.execute(query, (id_value,))
        self.conn.commit()
    
    def select_one(self, table: str, id_column: str, id_value: str) -> Dict[str, Any] | None:
        """Select a single row from a table."""
        query = f"SELECT * FROM {table} WHERE {id_column} = ?"
        cursor = self.conn.cursor()
        cursor.execute(query, (id_value,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def select_many(self, table: str, conditions: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        """Select multiple rows from a table."""
        query = f"SELECT * FROM {table}"
        params = []
        
        if conditions:
            where_clauses = []
            for key, value in conditions.items():
                where_clauses.append(f"{key} = ?")
                params.append(value)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            type(self)._instance = None


class Repository:
    """Base repository functionality."""
    
    @staticmethod
    def get_db(db_path: str | Path = "mollusk.db") -> Database:
        """Get the database instance."""
        return Database(db_path)


class Model:
    """Base class for all domain models."""
    
    _table: ClassVar[str] = ""
    _id_column: ClassVar[str] = ""
    
    def __init__(self):
        self._is_loaded = False
    
    @property
    def is_loaded(self) -> bool:
        """Check if the model has been loaded from the database."""
        return self._is_loaded
    
    def _to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary for database storage."""
        raise NotImplementedError("Subclasses must implement _to_dict")
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "Model":
        """Create model instance from dictionary."""
        raise NotImplementedError("Subclasses must implement _from_dict")
    
    def save(self, db_path: str | Path = "mollusk.db") -> "Model":
        """Save model to database."""
        db = Repository.get_db(db_path)
        id_value = getattr(self, self._id_column)
        data = self._to_dict()
        
        # Check if record exists
        existing = db.select_one(self._table, self._id_column, id_value)
        
        if existing:
            # Update
            if "updated_at" in data:
                data["updated_at"] = datetime.now().isoformat()
            db.update(self._table, self._id_column, id_value, data)
        else:
            # Insert
            if "created_at" in data and not data["created_at"]:
                data["created_at"] = datetime.now().isoformat()
            if "updated_at" in data:
                data["updated_at"] = datetime.now().isoformat()
            db.insert(self._table, data)
        
        self._is_loaded = True
        return self
    
    def delete(self, db_path: str | Path = "mollusk.db") -> bool:
        """Delete model from database."""
        db = Repository.get_db(db_path)
        id_value = getattr(self, self._id_column)
        db.delete(self._table, self._id_column, id_value)
        self._is_loaded = False
        return True
    
    @classmethod
    def find_by_id(cls, id_value: str, db_path: str | Path = "mollusk.db") -> "Model | None":
        """Find a model by ID."""
        db = Repository.get_db(db_path)
        data = db.select_one(cls._table, cls._id_column, id_value)
        if data:
            model = cls._from_dict(data)
            model._is_loaded = True
            return model
        return None
    
    @classmethod
    def find_all(cls, conditions: Dict[str, Any] | None = None, db_path: str | Path = "mollusk.db") -> List["Model"]:
        """Find all models matching conditions."""
        db = Repository.get_db(db_path)
        rows = db.select_many(cls._table, conditions)
        models = []
        for row in rows:
            model = cls._from_dict(row)
            model._is_loaded = True
            models.append(model)
        return models


class Item(Model):
    """Domain model for an item with active record capabilities."""
    
    _table = "items"
    _id_column = "item_id"
    
    def __init__(
        self,
        item_id: str | None = None,
        name: str = "",
        description: str = "",
        created_at: str | None = None,
        updated_at: str | None = None,
        metadata: Dict[str, Any] | None = None,
    ):
        super().__init__()
        self.item_id = item_id or str(uuid.uuid4())
        self.name = name
        self.description = description
        self.created_at = created_at
        self.updated_at = updated_at
        self.metadata = metadata or {}
        self._loaded_files: List[File] | None = None
    
    def _to_dict(self) -> Dict[str, Any]:
        """Convert Item to dictionary for database storage."""
        return {
            "item_id": self.item_id,
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": json.dumps(self.metadata) if self.metadata else None,
        }
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "Item":
        """Create Item instance from dictionary."""
        metadata = json.loads(data["metadata"]) if data.get("metadata") else {}
        
        return cls(
            item_id=data["item_id"],
            name=data["name"],
            description=data["description"],
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            metadata=metadata,
        )
    
    @classmethod
    def create(cls, name: str, description: str = "", metadata: Dict[str, Any] | None = None, db_path: str | Path = "mollusk.db") -> "Item":
        """Create and save a new item."""
        item = cls(name=name, description=description, metadata=metadata)
        return item.save(db_path)
    
    def rename(self, name: str) -> "Item":
        """Rename the item."""
        self.name = name
        return self
    
    def update_description(self, description: str) -> "Item":
        """Update the item description."""
        self.description = description
        return self
    
    def update_metadata(self, metadata: Dict[str, Any]) -> "Item":
        """Update the item metadata."""
        self.metadata.update(metadata)
        return self
    
    def files(self, db_path: str | Path = "mollusk.db") -> List["File"]:
        """Get all files associated with this item."""
        if self._loaded_files is None:
            self._loaded_files = File.find_all({"item_id": self.item_id}, db_path)
        return self._loaded_files
    
    def add_file(self, filename: str, mimetype: str | None = None, db_path: str | Path = "mollusk.db") -> "File":
        """Create and add a new file to this item."""
        file = File(item_id=self.item_id, filename=filename, mimetype=mimetype)
        file.save(db_path)
        
        # Update in-memory cache if already loaded
        if self._loaded_files is not None:
            self._loaded_files.append(file)
            
        return file


class File(Model):
    """Domain model for a file with active record capabilities."""
    
    _table = "files"
    _id_column = "file_id"
    
    def __init__(
        self,
        file_id: str | None = None,
        item_id: str | None = None,
        filename: str = "",
        mimetype: str | None = None,
        created_at: str | None = None,
        updated_at: str | None = None,
    ):
        super().__init__()
        self.file_id = file_id or str(uuid.uuid4())
        self.item_id = item_id
        self.filename = filename
        self.mimetype = mimetype
        self.created_at = created_at
        self.updated_at = updated_at
        self._loaded_instances: List[FileInstance] | None = None
    
    def _to_dict(self) -> Dict[str, Any]:
        """Convert File to dictionary for database storage."""
        return {
            "file_id": self.file_id,
            "item_id": self.item_id,
            "filename": self.filename,
            "mimetype": self.mimetype,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "File":
        """Create File instance from dictionary."""
        return cls(
            file_id=data["file_id"],
            item_id=data["item_id"],
            filename=data["filename"],
            mimetype=data.get("mimetype"),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
        )
    
    @classmethod
    def find_by_item_id(cls, item_id: str, db_path: str | Path = "mollusk.db") -> List["File"]:
        """Find all files for a given item."""
        return cls.find_all({"item_id": item_id}, db_path)
    
    def rename(self, filename: str) -> "File":
        """Rename the file."""
        self.filename = filename
        return self
    
    def set_mimetype(self, mimetype: str) -> "File":
        """Set the file mimetype."""
        self.mimetype = mimetype
        return self
    
    def instances(self, db_path: str | Path = "mollusk.db") -> List["FileInstance"]:
        """Get all file instances associated with this file."""
        if self._loaded_instances is None:
            self._loaded_instances = FileInstance.find_all({"file_id": self.file_id}, db_path)
        return self._loaded_instances
    
    def add_instance(self, storage: Storage, db_path: str | Path = "mollusk.db") -> "FileInstance":
        """Create and add a new file instance."""
        instance = FileInstance(file_id=self.file_id, storage=storage)
        instance.save(db_path)
        
        # Update in-memory cache if already loaded
        if self._loaded_instances is not None:
            self._loaded_instances.append(instance)
            
        return instance
    
    def item(self, db_path: str | Path = "mollusk.db") -> Item | None:
        """Get the item this file belongs to."""
        if not self.item_id:
            return None
        return Item.find_by_id(self.item_id, db_path)


class FileInstance(Model):
    """Domain model for a file instance with active record capabilities."""
    
    _table = "file_instances"
    _id_column = "file_instance_id"
    
    def __init__(
        self,
        file_instance_id: str | None = None,
        file_id: str | None = None,
        storage: Storage | None = None,
        checksum: str | None = None,
        size: int | None = None,
        created_at: str | None = None,
    ):
        super().__init__()
        self.file_instance_id = file_instance_id or str(uuid.uuid4())
        self.file_id = file_id
        self.storage = storage
        self.checksum = checksum
        self.size = size
        self.created_at = created_at
        self._storage_type = self._get_storage_type()
        self._storage_path = self._get_storage_path()
    
    def _get_storage_type(self) -> str:
        """Get the storage type."""
        if not self.storage:
            return "unknown"
        if isinstance(self.storage, POSIXStorage):
            return "posix"
        if isinstance(self.storage, S3Storage):
            return "s3"
        return "unknown"
    
    def _get_storage_path(self) -> str:
        """Get the storage path."""
        if not self.storage:
            return ""
        if isinstance(self.storage, POSIXStorage):
            return str(self.storage.filepath)
        if isinstance(self.storage, S3Storage):
            return f"s3://{self.storage.bucket}/{self.storage.key}"
        return ""
    
    def _to_dict(self) -> Dict[str, Any]:
        """Convert FileInstance to dictionary for database storage."""
        return {
            "file_instance_id": self.file_instance_id,
            "file_id": self.file_id,
            "storage_type": self._storage_type,
            "storage_path": self._storage_path,
            "checksum": self.checksum,
            "size": self.size,
            "created_at": self.created_at,
        }
    
    @classmethod
    def _from_dict(cls, data: Dict[str, Any]) -> "FileInstance":
        """Create FileInstance from dictionary."""
        storage = None
        if data.get("storage_type") == "posix":
            storage = POSIXStorage(data["storage_path"])
        elif data.get("storage_type") == "s3":
            storage = S3Storage(data["storage_path"])
        
        return cls(
            file_instance_id=data["file_instance_id"],
            file_id=data["file_id"],
            storage=storage,
            checksum=data.get("checksum"),
            size=data.get("size"),
            created_at=data["created_at"],
        )
    
    @classmethod
    def find_by_file_id(cls, file_id: str, db_path: str | Path = "mollusk.db") -> List["FileInstance"]:
        """Find all file instances for a given file."""
        return cls.find_all({"file_id": file_id}, db_path)
    
    def write(self, data: str | bytes, db_path: str | Path = "mollusk.db") -> "FileInstance":
        """Write data to the storage and update metadata."""
        if not self.storage:
            raise ValueError("No storage assigned to this file instance")
        
        result = self.storage.write(data)
        
        if result:
            # Update instance properties
            if isinstance(data, str):
                self.size = len(data.encode("utf-8"))
            else:
                self.size = len(data)
            
            self.checksum = self.storage.calculate_checksum()
            self.save(db_path)
        
        return self
    
    def read(self) -> str | bytes:
        """Read data from the storage."""
        if not self.storage:
            raise ValueError("No storage assigned to this file instance")
        return self.storage.read()
    
    def delete_content(self) -> bool:
        """Delete the file content from storage (but keep the record)."""
        if not self.storage:
            raise ValueError("No storage assigned to this file instance")
        return self.storage.delete()
    
    def file(self, db_path: str | Path = "mollusk.db") -> File | None:
        """Get the file this instance belongs to."""
        if not self.file_id:
            return None
        return File.find_by_id(self.file_id, db_path)


# Example usage:
"""
# Create item with a file and file instance
item = Item.create(
    name="Report Collection",
    description="Collection of monthly reports",
    metadata={"department": "Finance", "year": 2024}
)

# Add a file to the item
file = item.add_file(
    filename="march_report.pdf",
    mimetype="application/pdf"
)

# Add storage for the file
storage = POSIXStorage("/tmp/march_report.pdf")
instance = file.add_instance(storage)

# Write content to the file instance
instance.write("This is the March report content")

# Read content
content = instance.read()

# Load relationships
for file in item.files():
    print(f"File: {file.filename}")
    for instance in file.instances():
        print(f"  Instance: {instance.file_instance_id}")
        print(f"  Checksum: {instance.checksum}")
        print(f"  Size: {instance.size}")
"""