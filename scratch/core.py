from abc import ABC, abstractmethod
import datetime
import hashlib
import os
from typing import Literal
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, TIMESTAMP, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship



Base = declarative_base()


class Storage(ABC):
    name = ...

    def __init__(self, uri: str):
        self.uri = uri

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
    name = "posix"

    def __init__(self, uri: str):
        super().__init__(uri)
        self.uri = uri
        self.filepath = uri.removeprefix("file://")

    def exists(self) -> bool:
        return os.path.exists(self.filepath)

    def write(self, data: str | bytes) -> bool:
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
    name = "s3"

    def __init__(self, uri: str):
        super().__init__(uri)
        self.uri = uri
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

    def write(
        self,
        data: str | bytes,
        algorithm: Literal["MD5", "SHA256"] = "SHA256",
    ) -> bool:
        if isinstance(data, str):
            data = data.encode("utf-8")
        params = {
            "Bucket": self.bucket,
            "Key": self.key,
            "Body": data,
            "ChecksumAlgorithm": algorithm,
        }
        self.s3.put_object(**params)
        return True

    def read(self) -> bytes:
        response = self.s3.get_object(Bucket=self.bucket, Key=self.key)
        return response["Body"].read()

    def delete(self) -> bool:
        self.s3.delete_object(Bucket=self.bucket, Key=self.key)
        return True

    def calculate_checksum(self, algorithm: Literal["MD5", "SHA256"] = "SHA256") -> str:
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
            decoded_bytes = base64.b64decode(base64_checksum)
            return binascii.hexlify(decoded_bytes).decode("ascii")
        except KeyError:
            error_msg = f"Checksum {algorithm} not found in copy response."
            raise ValueError(error_msg)

    def get_checksum(self, algorithm: Literal["MD5", "SHA256"] = "SHA256") -> str:
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
    """Provides database connectivity and operations."""

    def __init__(self, db_path=None):
        """
        Initialize database connection.

        Args:
            db_path (str, optional): Path to the SQLite database file.
                                    If None, uses default location.
        """
        if db_path is None:
            db_path = os.path.join(os.path.expanduser("~"), ".mollusk", "database.sqlite")
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables defined in the models."""
        Base.metadata.create_all(self.engine)

    def get_session(self):
        """Get a new database session."""
        return self.Session()

    def close(self):
        """Close database connection."""
        self.engine.dispose()


class Item(Base):
    """Database ORM model for Item."""

    __tablename__ = 'item'

    item_id = Column(String(255), primary_key=True)
    title = Column(String(255))
    created_date = Column(TIMESTAMP, default=datetime.datetime.utcnow)
    updated_date = Column(TIMESTAMP, default=datetime.datetime.utcnow,
                          onupdate=datetime.datetime.utcnow)

    # Relationships
    files = relationship("File", back_populates="item")


class File(Base):
    """Database ORM model for File."""

    __tablename__ = 'file'

    file_id = Column(String(36), primary_key=True)
    item_id = Column(String(255), ForeignKey('item.item_id'))
    filename = Column(String(255))
    mimetype = Column(String(100))
    created_date = Column(TIMESTAMP, default=datetime.datetime.utcnow)

    # Relationships
    item = relationship("Item", back_populates="files")
    instances = relationship("FileInstance", back_populates="file")


class FileInstance(Base):
    """Database ORM model for FileInstance."""

    __tablename__ = 'file_instance'

    file_instance_id = Column(String(36), primary_key=True)
    file_id = Column(String(36), ForeignKey('file.file_id'))
    storage_class = Column(String(50))  # Storage type (posix, s3, etc.)
    uri = Column(String(1024))
    checksum = Column(String(1024))
    size = Column(Integer)
    created_date = Column(TIMESTAMP, default=datetime.datetime.utcnow)

    # Relationships
    file = relationship("File", back_populates="instances")

    def get_storage(self):
        if self.storage_class == "POSIXStorage":
            storage = POSIXStorage(self.uri)
        elif self.storage_class == "S3Storage":
            storage = S3Storage(self.uri)
        else:
            raise ValueError(f"Storage class not recognized: {self.storage_class}")
        return storage