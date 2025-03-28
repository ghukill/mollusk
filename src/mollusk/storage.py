from abc import ABC, abstractmethod
import base64
import binascii
import hashlib
import os
from typing import Literal
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


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
